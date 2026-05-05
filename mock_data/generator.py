import os
import time
import uuid
import random
import psycopg2
from datetime import datetime

# ─── Configuration ──────────────────────────────────────────
# ดึงจาก Environment Variable หรือใช้ค่า Default
DB_CONFIG = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5432)),
    "dbname":   os.getenv("DB_NAME", "mydb"),
    "user":     os.getenv("DB_USER", "admin"),
    "password": os.getenv("DB_PASSWORD", "secret"),
    "connect_timeout": 5
}

# Master Data สำหรับสุ่ม
STATUSES    = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
TIERS       = ["bronze", "silver", "gold", "platinum"]
FIRST_NAMES = ["สมชาย","วรรณา","ธนา","สุดา","ชัยวัฒน์","พิมพ์","ณัฐ","รัตนา","กมล","อนุ"]
LAST_NAMES  = ["สุขใจ","ดีมาก","เก่งมาก","ขยัน","มีสุข","ใจดี","รักงาน"]

def get_conn():
    """สร้างการเชื่อมต่อกับ PostgreSQL"""
    return psycopg2.connect(**DB_CONFIG)

# ─── 1. Bootstrap System (ปรับปรุงให้ชัวร์ว่ามีของขาย) ───
def bootstrap_data(cur):
    print("📋 [Bootstrap] Initializing system data...")

    # 1. สร้างสินค้า (ถ้ายังไม่มี)
    cur.execute("SELECT count(*) FROM ecommerce.products")
    if cur.fetchone()[0] == 0:
        print("   📦 Seeding products...")
        initial_products = [
            ('SKU-001', 'Smartphone X1', 'Electronics', 25000, 18000),
            ('SKU-002', 'Wireless Earbuds', 'Electronics', 3500, 1500),
            ('SKU-003', 'Mechanical Keyboard', 'Accessories', 4500, 2800),
            ('SKU-004', 'Ergonomic Chair', 'Furniture', 12000, 7500),
            ('SKU-005', 'Coffee Beans 1kg', 'Food', 850, 400)
        ]
        cur.executemany("""
            INSERT INTO ecommerce.products (sku, name, category, price, cost, is_available)
            VALUES (%s, %s, %s, %s, %s, TRUE)
        """, initial_products)
    
    # 2. เติมสต็อกสินค้า (ตรวจสอบว่าสินค้าทุกชิ้นต้องมีแถวใน inventory)
    cur.execute("""
        INSERT INTO ecommerce.inventory (product_id, quantity, available)
        SELECT id, 100, 100 FROM ecommerce.products p
        WHERE NOT EXISTS (SELECT 1 FROM ecommerce.inventory i WHERE i.product_id = p.id)
    """)
    
    # เพิ่มเติม: อัปเดตสต็อกที่เหลือน้อยให้กลับมาเต็ม (เผื่อของหมดจากรอบก่อน)
    cur.execute("UPDATE ecommerce.inventory SET available = 100 WHERE available < 10")

    # 3. สร้างลูกค้าตั้งต้น (ถ้าไม่มีเลย)
    cur.execute("SELECT count(*) FROM ecommerce.customers")
    if cur.fetchone()[0] == 0:
        print("   👤 Seeding initial customers...")
        for _ in range(10): # เพิ่มเป็น 10 คนให้มีตัวเลือกเยอะขึ้น
            add_customer(cur)
    
    heartbeat(cur)
    print("✅ [Bootstrap] Database is ready for orders.")

# ─── 2. Transaction Logic ───

def add_customer(cur):
    """จำลองลูกค้าใหม่เดินเข้าเว็บ"""
    name, lname = random.choice(FIRST_NAMES), random.choice(LAST_NAMES)
    email = f"{uuid.uuid4().hex[:8]}@example.com"
    tier = random.choices(TIERS, weights=[65, 20, 10, 5])[0]
    cur.execute("""
        INSERT INTO ecommerce.customers (first_name, last_name, email, tier) 
        VALUES (%s, %s, %s, %s)
    """, (name, lname, email, tier))

# ─── 2. Transaction Logic (เพิ่ม Log เพื่อ Debug) ───

def create_order(cur):
    """จำลองการสั่งซื้อจริง"""
    # 1. เช็คว่ามีลูกค้าไหม
    cur.execute("SELECT id FROM ecommerce.customers ORDER BY random() LIMIT 1")
    res_cust = cur.fetchone()
    if not res_cust:
        print("   ❌ Order Failed: No customers in DB")
        return None
    cust_id = res_cust[0]

    # 2. เช็คว่ามีสินค้าที่พร้อมขายและมีสต็อกไหม
    cur.execute("""
        SELECT p.id, p.price FROM ecommerce.products p
        JOIN ecommerce.inventory i ON p.id = i.product_id
        WHERE i.available > 2 AND p.is_available = TRUE 
        ORDER BY random() LIMIT 1
    """)
    res_prod = cur.fetchone()
    if not res_prod:
        print("   ❌ Order Failed: Out of stock or No products available")
        # ถ้าของหมด ให้เติมสต็อกอัตโนมัติเพื่อให้ระบบรันต่อได้
        cur.execute("UPDATE ecommerce.inventory SET available = 100")
        return None
    
    prod_id, price = res_prod
    qty = random.randint(1, 2)
    total = float(price) * qty
    order_no = f"ORD-{datetime.now().strftime('%m%d')}-{uuid.uuid4().hex[:4].upper()}"

    # 3. Insert Order
    cur.execute("""
        INSERT INTO ecommerce.orders (order_number, customer_id, status, total)
        VALUES (%s, %s, 'pending', %s) RETURNING id
    """, (order_no, cust_id, total))
    
    # 4. ตัดสต็อก
    cur.execute("""
        UPDATE ecommerce.inventory 
        SET available = available - %s, updated_at = NOW() 
        WHERE product_id = %s
    """, (qty, prod_id))
    
    return order_no

def update_order_status(cur):
    """จำลองการขยับสถานะในคลังสินค้า/ขนส่ง"""
    transitions = {"pending": "confirmed", "confirmed": "shipped", "shipped": "delivered"}
    
    cur.execute("""
        SELECT id, status, order_number FROM ecommerce.orders 
        WHERE status NOT IN ('delivered', 'cancelled') 
        ORDER BY random() LIMIT 1
    """)
    order = cur.fetchone()
    if order:
        oid, current_status, ono = order
        # สุ่มยกเลิกเล็กน้อย (5%)
        new_status = "cancelled" if random.random() < 0.05 else transitions.get(current_status, current_status)
        
        cur.execute("UPDATE ecommerce.orders SET status = %s, updated_at = NOW() WHERE id = %s", (new_status, oid))
        print(f"  🔄 {ono}: {current_status} -> {new_status}")

def heartbeat(cur):
    """พ่นข้อมูลลง Heartbeat table เพื่อให้ Debezium เคลื่อนไหวตลอดเวลา"""
    cur.execute("""
        INSERT INTO ecommerce.cdc_heartbeat (id, ts) 
        VALUES (1, NOW()) 
        ON CONFLICT (id) DO UPDATE SET ts = NOW()
    """)

# ─── 3. Main Runner ───

def main():
    print("="*50)
    print("🚀 E-Commerce Data Generator (CDC Ready)")
    print(f"📍 Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}")
    print("="*50)
    
    # รอบแรก: Bootstrap
    while True:
        try:
            conn = get_conn()
            with conn.cursor() as cur:
                bootstrap_data(cur)
                conn.commit()
            conn.close()
            break 
        except Exception as e:
            print(f"❌ Waiting for Database... ({e})")
            time.sleep(5)

    # รอบสอง: Loop Simulation
    cycle = 0
    while True:
        try:
            with get_conn() as conn:
                with conn.cursor() as cur:
                    cycle += 1
                    
                    # 1. สร้าง Order ใหม่
                    ono = create_order(cur)
                    if ono: print(f"[{datetime.now().strftime('%H:%M:%S')}] 🛒 New Order: {ono}")
                    
                    # 2. ขยับสถานะ Order เก่าๆ
                    update_order_status(cur)
                    
                    # 3. เพิ่มลูกค้าใหม่ทุกๆ 5 รอบ
                    if cycle % 5 == 0:
                        add_customer(cur)
                        print("  👤 New customer joined the platform")
                    
                    # 4. ส่ง Heartbeat
                    heartbeat(cur)
                    
                    conn.commit()
        except Exception as e:
            print(f"⚠️ Runtime Error: {e}")
        
        # หน่วงเวลาสุ่มเพื่อให้ดูเป็นธรรมชาติ (3-6 วินาที)
        time.sleep(random.uniform(3, 6))

if __name__ == "__main__":
    main()