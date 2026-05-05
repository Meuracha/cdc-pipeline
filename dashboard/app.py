import os
import time
import duckdb
import pandas as pd
import streamlit as st
import plotly.express as px
from notifications import send_slack_alert

# ─── 1. Configuration ─────────────────────────────────────────────
st.set_page_config(page_title="CDC Real-time Analytics", page_icon="⚡", layout="wide")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DWH_PATH = os.getenv("DWH_PATH", os.path.join(BASE_DIR, "..", "warehouse", "analytics.db"))

if "sent_alerts" not in st.session_state:
    st.session_state.sent_alerts = set()

# ─── 2. DB Connection ─────────────────────────────────────────────
def get_connection():
    try:
        if not os.path.exists(DWH_PATH):
            return None
        con = duckdb.connect(":memory:")
        con.execute(f"ATTACH '{DWH_PATH}' AS src (READ_ONLY)")
        con.execute("USE src")
        return con
    except Exception:
        return None

def q(sql):
    for _ in range(3):
        con = get_connection()
        if con is None:
            time.sleep(0.5)
            continue
        try:
            df = con.execute(sql).df()
            return df
        except Exception:
            time.sleep(0.5)
            continue
        finally:
            try:
                con.close()
            except Exception:
                pass
    return pd.DataFrame()

# ─── 3. Sidebar ───────────────────────────────────────────────────
st.sidebar.header("⚙️ Dashboard Settings")
refresh = st.sidebar.slider("Refresh Interval (sec)", 2, 30, 5)

# ─── 4. Header ────────────────────────────────────────────────────
st.title("⚡ CDC Real-time eCommerce Dashboard")
st.markdown("**Live Stream:** `PostgreSQL` → `Kafka` → `DuckDB` → `Streamlit`")

# ─── 5. Main Content ──────────────────────────────────────────────
if not os.path.exists(DWH_PATH):
    st.warning(f"⏳ Waiting for Database File at: {DWH_PATH}")
    st.info("กรุณารัน `sink_duckdb.py` และ `generator.py` เพื่อเริ่มสร้างข้อมูล")
else:
    current_ts = int(time.time() * 1000)

    # --- 5.1 Inventory (ใช้ร่วมกันทั้ง Slack และ Table) ---
    df_inv_source = q("""
        SELECT 
            'Product ID: ' || CAST(product_id AS VARCHAR) as product_id_display, 
            CAST(product_id AS VARCHAR) as raw_id,
            TRY_CAST(available AS INTEGER) as stock_available,
            'System' as category
        FROM inventory 
        ORDER BY stock_available ASC 
        LIMIT 10
    """)

    # --- 5.2 Slack Alert ---
    if not df_inv_source.empty:
        low_stock_now = df_inv_source[df_inv_source['stock_available'] < 10]
        for _, row in low_stock_now.iterrows():
            alert_key = f"{row['raw_id']}_{row['stock_available']}"
            if alert_key not in st.session_state.sent_alerts:
                msg = f"🚨 *Low Stock Alert:* {row['product_id_display']} เหลือเพียง *{row['stock_available']}* ชิ้น!"
                if send_slack_alert(msg):
                    st.session_state.sent_alerts.add(alert_key)

    # --- 5.3 KPI Metrics ---
    df_kpis = q("""
        SELECT 
            (SELECT COUNT(*) FROM orders) as total_orders,
            (SELECT COALESCE(SUM(TRY_CAST(total AS DOUBLE)), 0) FROM orders WHERE status != 'cancelled') as revenue,
            (SELECT COUNT(*) FROM customers) as total_cust,
            (SELECT COUNT(*) FROM inventory WHERE TRY_CAST(available AS INTEGER) < 20) as low_stock_count
    """)

    c1, c2, c3, c4 = st.columns(4)
    if not df_kpis.empty:
        c1.metric("📦 Total Orders", f"{int(df_kpis['total_orders'][0]):,}")
        c2.metric("💰 Total Revenue", f"฿{float(df_kpis['revenue'][0]):,.0f}")
        c3.metric("👤 Active Customers", f"{int(df_kpis['total_cust'][0]):,}")
        c4.metric("⚠️ Low Stock Alert", f"{int(df_kpis['low_stock_count'][0]):,}", delta_color="inverse")

    st.write("")

    # --- 5.4 Charts ---
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("📊 Order Status Distribution")
        df_status = q("SELECT status, COUNT(*) as count FROM orders GROUP BY status")
        if not df_status.empty:
            fig = px.pie(df_status, values='count', names='status', hole=0.4,
                         color_discrete_sequence=px.colors.qualitative.Pastel)
            st.plotly_chart(fig, use_container_width=True, key=f"pie_{current_ts}")

    with col2:
        st.subheader("📈 Top Customer Tiers")
        df_tier = q("SELECT tier, COUNT(*) as count FROM customers GROUP BY tier ORDER BY count DESC")
        if not df_tier.empty:
            fig_bar = px.bar(df_tier, x='tier', y='count', color='tier',
                             text_auto=True, color_discrete_sequence=px.colors.qualitative.Bold)
            st.plotly_chart(fig_bar, use_container_width=True, key=f"bar_{current_ts}")

    # --- 5.5 Tables ---
    st.divider()
    col3, col4 = st.columns([3, 2])

    with col3:
        st.subheader("📦 Inventory Tracking (Live Stock)")
        if not df_inv_source.empty:
            display_df = df_inv_source[['product_id_display', 'stock_available', 'category']]
            st.dataframe(
                display_df.style.highlight_between(left=0, right=9, subset=['stock_available'], color='#ffcccc'),
                use_container_width=True,
                hide_index=True
            )
        else:
            st.info("🔍 ตาราง inventory ยังไม่มีข้อมูล")

    with col4:
        st.subheader("🛒 Recent Activity")
        df_recent = q("""
            SELECT order_number, status, 
                   '฿' || CAST(CAST(total AS DOUBLE) AS VARCHAR) as price
            FROM orders 
            ORDER BY id DESC LIMIT 10
        """)
        if not df_recent.empty:
            st.table(df_recent)

# ─── 6. Auto Refresh ──────────────────────────────────────────────
st.caption(f"🕒 Last updated: {time.strftime('%H:%M:%S')} | Auto-refreshing every {refresh}s")
time.sleep(refresh)
st.rerun()