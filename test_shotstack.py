import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SHOTSTACK_API_KEY")
BASE_URL = os.getenv("SHOTSTACK_BASE_URL", "https://api.shotstack.io/sandbox/v1")

RENDER_URL = f"{BASE_URL}/render"

headers = {
    "x-api-key": API_KEY,
    "Content-Type": "application/json",
}

print("🔍 Testing Shotstack Sandbox /render endpoint...\n")
print("Base URL:", BASE_URL)
print("Render URL:", RENDER_URL, "\n")

# Simple 5-second title render payload (same idea as insert_job.py)
payload = {
    "timeline": {
        "soundtrack": {
            "src": "https://shotstack-assets.s3-ap-southeast-2.amazonaws.com/music/freeflow.mp3",
            "effect": "fadeInFadeOut",
        },
        "tracks": [
            {
                "clips": [
                    {
                        "asset": {
                            "type": "title",
                            "text": "Cre8 Studio Test Render",
                            "style": "minimal",
                        },
                        "start": 0,
                        "length": 5,
                        "effect": "fadeIn",
                    }
                ]
            }
        ],
    },
    "output": {
        "format": "mp4",
        "resolution": "sd",
        "fps": 25,
    },
}

try:
    resp = requests.post(RENDER_URL, headers=headers, json=payload, timeout=30)
    print("📡 STATUS CODE:", resp.status_code)
    print("\n🔎 RAW RESPONSE (first 500 chars):\n")
    print(resp.text[:500], "...\n")

    if resp.status_code in (200, 201, 202):
        print("✅ Render request accepted — connection + key are GOOD.")
    elif resp.status_code == 401:
        print("❌ Unauthorized — check SHOTSTACK_API_KEY in .env")
    elif resp.status_code == 403:
        print("❌ Forbidden — key / environment mismatch (prod vs sandbox).")
    else:
        print("⚠️ Got HTTP", resp.status_code, "- but connection is working.")
except Exception as e:
    print("❌ ERROR connecting to Shotstack:")
    print(str(e))
