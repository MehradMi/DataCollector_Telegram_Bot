import os
import requests
from dotenv import load_dotenv
from database import get_payload_data

# Load environment variables and API_URL
load_dotenv()
API_URL = os.getenv("API_URL")
# ===========================

HEADERS = {"Content-Type": "application/json"}

rows = get_payload_data()

for row in rows:
    url, category, date, description = row
    payload = [
        {
        "post_url": url,
        "date": date,
        "category": category,
        "description": description
    }
    ]

    try:
        response = requests.post(API_URL, json=payload, headers=HEADERS)
        if response.status_code == 200:
            print("✅ Sent:", payload)
        else:
            print(f"❌ Failed for {url}: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error sending {url}:", e)