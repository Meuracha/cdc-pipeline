import os
from pathlib import Path
from dotenv import load_dotenv
import requests
import json

# ระบุ Path ไปยังไฟล์ .env ที่อยู่สูงขึ้นไป 1 ชั้น
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(dotenv_path=env_path)

def send_slack_alert(message):
    # ดึงค่าจาก Environment Variable
    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    print(f"DEBUG: Webhook URL is {webhook_url}")
    
    if not webhook_url:
        print("❌ Error: SLACK_WEBHOOK_URL not found in .env file")
        return False

    payload = {
        "text": message,
        "username": "CDC Monitor Bot",
        "icon_emoji": ":warning:"
    }
    
    try:
        response = requests.post(webhook_url, data=json.dumps(payload), timeout=5)
        return response.status_code == 200
    except Exception as e:
        print(f"Slack Alert Error: {e}")
        return False
    
if __name__ == "__main__":
    test_result = send_slack_alert("🧪 Test: ถ้าเห็นข้อความนี้ แสดงว่า Webhook ทำงานปกติ!")
    print(f"ส่งสำเร็จไหม?: {test_result}")