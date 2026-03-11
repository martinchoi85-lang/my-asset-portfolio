import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

headers = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Accept": "application/json"
}

r = requests.get(f"{SUPABASE_URL}/rest/v1/", headers=headers)
if r.status_code == 200:
    spec = r.json()
    paths = spec.get("paths", {})
    if "/transactions" in paths:
        print(json.dumps(paths["/transactions"], indent=2))
        
    print("\n--- definitions ---")
    defs = spec.get("definitions", {})
    if "transactions" in defs:
        print(json.dumps(defs["transactions"], indent=2))
else:
    print("Error:", r.status_code, r.text)
