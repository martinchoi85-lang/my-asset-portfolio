import os
import sys

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.insert(0, project_root)

from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.holding_period_service import calculate_holding_periods

def run_test():
    supabase = get_supabase_client()
    # 첫번째 유저 아이디 가져오기
    user_resp = supabase.table("users").select("id").limit(1).execute()
    if not user_resp.data:
        print("No users found")
        return
        
    user_id = user_resp.data[0]["id"]
    
    print(f"Testing for user_id: {user_id}")
    
    # 전체 계좌 대상으로 실행
    df = calculate_holding_periods(user_id, "__ALL__")
    
    if df.empty:
        print("Empty DataFrame returned. No data to analyze.")
    else:
        print("Holding Period Analysis Result:")
        print(df.to_string())

if __name__ == "__main__":
    run_test()
