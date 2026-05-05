import requests
import time
import os

CONNECT_URL = os.getenv("CONNECT_URL", "http://kafka-connect:8083")

def wait_for_connect():
    print("Waiting for Kafka Connect...")
    while True:
        try:
            r = requests.get(f"{CONNECT_URL}/connectors", timeout=5)
            if r.status_code == 200:
                print("Kafka Connect is ready!")
                return
        except Exception:
            print("Not ready yet, retrying in 10s...")
            time.sleep(10)

if __name__ == "__main__":
    wait_for_connect()
    # import และรัน register
    from register import register
    register()