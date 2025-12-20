# backend/supabase_client.py
import os
from supabase import create_client
from dotenv import load_dotenv

# -------------------------------------------------------------------
# 1. Supabase 연결 초기화
# -------------------------------------------------------------------
load_dotenv()
def get_supabase_client():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        raise RuntimeError("Supabase env not set")
    return create_client(url, key)
