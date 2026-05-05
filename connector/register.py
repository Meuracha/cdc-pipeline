import os
import time
import requests

CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")  # Localhost

if not os.getenv("POSTGRES_USER") or not os.getenv("POSTGRES_PASSWORD"):
    raise ValueError("Missing DB credentials")

CONNECTOR_CONFIG = {
    "name": "ecommerce-cdc-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
        "database.hostname": "postgres",
        "database.port": "5432",
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": "mydb",
        "topic.prefix": "cdc",
        "plugin.name": "pgoutput",
        "publication.name": "cdc_publication",
        "slot.name": "debezium_slot",
        "publication.autocreate.mode": "filtered",
        "schema.include.list": "ecommerce",
        "table.include.list": (
            "ecommerce.customers,"
            "ecommerce.products,"
            "ecommerce.orders,"
            "ecommerce.inventory,"
            "ecommerce.cdc_heartbeat"
        ),
        "transforms": "unwrap",
        "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
        "transforms.unwrap.add.fields": "op,ts_ms,table",
        "transforms.unwrap.delete.handling.mode": "rewrite",
        "decimal.handling.mode": "double",
        "snapshot.mode": "initial"
    }
}

def register():
    # ลบอันเก่าออกก่อน (ไม่ต้อง error ถ้ายังไม่มี)
    try:
        requests.delete(f"{CONNECT_URL}/connectors/{CONNECTOR_CONFIG['name']}")
        time.sleep(1)
    except Exception:
        pass

    r = requests.post(
        f"{CONNECT_URL}/connectors",
        json=CONNECTOR_CONFIG,
        headers={"Content-Type": "application/json"}
    )

    if r.status_code in (200, 201):
        print(f"✅ Registered: {CONNECTOR_CONFIG['name']}")
    else:
        print(f"❌ Failed: {r.text}")

if __name__ == "__main__":
    register()