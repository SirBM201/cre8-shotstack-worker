import os
import sys
import json
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("SHOTSTACK_API_KEY")
BASE_URL = "https://api.shotstack.io/stage/render"  # same environment as main.py

if not API_KEY:
    raise RuntimeError("SHOTSTACK_API_KEY is not set in .env")

if len(sys.argv) < 2:
    print("Usage: python check_render.py <render_id>")
    sys.exit(1)

render_id = sys.argv[1]
url = f"{BASE_URL}/{render_id}"

headers = {
    "Accept": "application/json",
    "x-api-key": API_KEY,
}

print(f"🔍 Checking render status for: {render_id}")
print(f"URL: {url}\n")

resp = requests.get(url, headers=headers, timeout=30)

print(f"📡 STATUS CODE: {resp.status_code}\n")

try:
    data = resp.json()
except Exception:
    data = {"raw": resp.text}

print("🔎 RESPONSE:")
print(json.dumps(data, indent=2)[:2000])
