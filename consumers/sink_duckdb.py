import duckdb
import json
import os
import socket
import time
from confluent_kafka import Consumer, KafkaError

# ─── 1. Configuration ──────────────────────────────────────────────

def get_kafka_broker():
    env_broker = os.getenv('KAFKA_BROKER')
    if env_broker: return env_broker
    try:
        socket.gethostbyname('kafka')
        return 'kafka:9092'
    except socket.gaierror:
        return 'localhost:29092'

KAFKA_BROKER = get_kafka_broker()
DB_PATH = os.getenv('DWH_PATH', 'warehouse/analytics.db')

KAFKA_CONF = {
    'bootstrap.servers': KAFKA_BROKER,
    'group.id': 'duckdb-sink-group-vfinal',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

TOPICS = [
    'cdc.ecommerce.customers',
    'cdc.ecommerce.products',
    'cdc.ecommerce.orders',
    'cdc.ecommerce.inventory',
    'cdc.ecommerce.cdc_heartbeat'
]

os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)

# ─── 2. Data Logic ──────────────────────────────────────────────────

def clean_payload(payload):
    """แกะข้อมูลจาก Debezium Envelope ให้เหลือแค่ตัวข้อมูลจริง"""
    actual_data = payload.get('payload', payload) if isinstance(payload, dict) else payload
    data = actual_data.get('after', actual_data) if isinstance(actual_data, dict) and 'after' in actual_data else actual_data
    
    meta_fields = {'op', 'ts_ms', 'table', 'lsn', 'txId', '__op', '__table', '__lsn', '__source_ts_ms', '__deleted'}
    if isinstance(data, dict):
        return {k: v for k, v in data.items() if k not in meta_fields and not k.startswith('__')}
    return None

def handle_upsert(table_name, raw_payload):
    """จัดการ Insert/Update พร้อมระบบ Retry เมื่อติด Lock"""
    data = clean_payload(raw_payload)
    if not data or 'id' not in data: return

    columns = list(data.keys())
    values = list(data.values())
    placeholders = ", ".join(["?" for _ in values])
    # SQL สำหรับ Update เมื่อข้อมูลซ้ำ (Conflict)
    update_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'id'])
    
    sql = f"""
    INSERT INTO {table_name} ({", ".join(columns)})
    VALUES ({placeholders})
    ON CONFLICT (id) DO UPDATE SET {update_clause}
    """
    
    # 🔄 Retry Loop เพื่อหลบจังหวะ Dashboard Copy ไฟล์
    for attempt in range(5):
        try:
            with duckdb.connect(DB_PATH) as con:
                try:
                    con.execute(sql, values)
                except duckdb.CatalogException:
                    # ถ้ายังไม่มีตาราง ให้สร้างและทำ Unique Index ที่ ID ทันที
                    print(f"📦 [Schema] Creating table: {table_name}")
                    cols_def = ", ".join([f"{col} VARCHAR" for col in columns])
                    con.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({cols_def})")
                    con.execute(f"CREATE UNIQUE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name} (id)")
                    con.execute(sql, values)
            return
        except duckdb.IOException as e:
            if "Could not set lock" in str(e):
                time.sleep(0.2)
                continue
            print(f"⚠️ IO Error: {e}")
            break
        except Exception as e:
            print(f"⚠️ SQL Error [{table_name}]: {e}")
            break

def handle_delete(table_name, raw_payload):
    """จัดการเมื่อมีการลบข้อมูลจากต้นทาง"""
    actual_data = raw_payload.get('payload', raw_payload)
    before_data = actual_data.get('before')
    if before_data and 'id' in before_data:
        try:
            with duckdb.connect(DB_PATH) as con:
                con.execute(f"DELETE FROM {table_name} WHERE id = ?", [before_data['id']])
                print(f"🗑️ Deleted from {table_name} (id: {before_data['id']})")
        except Exception as e:
            print(f"⚠️ Delete Error: {e}")

# ─── 3. Main Runner ───────────────────────────────────────────────

def main():
    print("=" * 60)
    print(f"🚀 DuckDB Sink Manager (Reliable Mode)")
    print(f"📡 Broker: {KAFKA_BROKER} | 📁 DB: {DB_PATH}")
    print("=" * 60)

    consumer = Consumer(KAFKA_CONF)
    consumer.subscribe(TOPICS)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error(): continue

            try:
                raw_payload = json.loads(msg.value().decode('utf-8'))
                table_name = msg.topic().split('.')[-1]
                
                payload_content = raw_payload.get('payload', raw_payload)
                op = payload_content.get('op', 'c') if isinstance(payload_content, dict) else 'c'

                if op in ('c', 'u', 'r'):
                    handle_upsert(table_name, raw_payload)
                    clean_data = clean_payload(raw_payload)
                    print(f"✨ Sync [{op}] -> {table_name} (id: {str(clean_data.get('id'))[:8]}...)")
                elif op == 'd':
                    handle_delete(table_name, raw_payload)

            except Exception as e:
                print(f"⚠️ Process Error: {e}")
    except KeyboardInterrupt:
        print("\n👋 Shutdown.")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()