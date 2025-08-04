import os
import requests
from dotenv import load_dotenv
from database import get_payload_data, change_upload_status
from module_openai_categorizer import categorize

# Load environment variables and API_URL
load_dotenv()
API_URL = os.getenv("API_URL")
# ===========================

HEADERS = {"Content-Type": "application/json"}
CORRECT_CATEGORIES = ["general", "clothing", "medical",
                      "restaurant", "AI", "fun",
                      "beauty", "medical", "education", "inspirational", "other"
                      ]

rows = get_payload_data()

for row in rows:
    rowid, telegram_id, username, url, category, date, upload_status, description= row
    categorized_value = categorize(category)
    while categorized_value not in CORRECT_CATEGORIES:
        categorized_value = categorize(category) 
    
    payload = [
        {
        "post_url": url,
        "date": date,
        "category": categorized_value,
        "description": description
    }
    ]

    try:
        response = requests.post(API_URL, json=payload, headers=HEADERS)
        if response.status_code == 200:
            print("✅ Sent:", payload)
            print(description)
            change_upload_status(rowid, telegram_id, username,
                                 url, categorized_value, date, 
                                 description, upload_status
                                 )
        else:
            print(f"❌ Failed for {url}: {response.status_code} - {response.text}")
    except Exception as e:
        print(f"❌ Error sending {url}:", e)