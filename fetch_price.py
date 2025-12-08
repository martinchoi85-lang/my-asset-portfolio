import os
from dotenv import load_dotenv
import pandas as pd
from supabase import create_client, Client
import yfinance as yf
from FinanceDataReader import data as fdr
from datetime import datetime, timezone

# ----------------------------------------------------
# 1. Supabase 접속 정보 설정 (환경 변수 또는 설정 파일 사용 권장)
# ----------------------------------------------------
load_dotenv()
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    print("FATAL ERROR: Supabase URL 또는 Key가 설정되지 않았습니다. .env 파일을 확인하세요.")
    exit()

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
print("Supabase 연결 성공!")

def get_current_price(ticker, market="KR"):
    """
    종목 티커와 시장 정보를 바탕으로 현재가를 조회합니다.
    """
    try:
        if market == "us":
            # yfinance를 사용하여 미국 주식(USD) 현재가 조회
            stock = yf.Ticker(ticker)
            price = stock.info.get('regularMarketPrice')
            return price
        
        elif market == "korea":
            # limit=1 인수를 제거합니다.
            df = fdr.DataReader(ticker)
            if df.empty:
                return None
            # 가장 최근의 종가 ('Close' 컬럼의 마지막 값)를 가져옵니다.
            price = df['Close'].iloc[-1]
            return price
        
        else:
            print(f"[{ticker}] 지원하지 않는 시장/통화 유형입니다.")
            return None

    except Exception as e:
        print(f"[{ticker}] 가격 조회 중 오류 발생: {e}")
        return None

def fetch_and_update_prices():
    """
    DB에서 종목 목록을 가져와 현재가를 조회하고 DB에 업데이트합니다.
    """
    print("----- 현재가 크롤링 및 DB 업데이트 시작 -----")
    
    # 2. assets 테이블에서 모든 종목 티커 및 ID 조회
    try:
        # DB에서 id, ticker, market(시장/국적) 필드를 가져옵니다.
        response = supabase.table('assets').select("id, ticker, market").execute()
        assets_data = response.data
        
        if not assets_data:
            print("assets 테이블에 종목이 없습니다.")
            return

    except Exception as e:
        print(f"DB 데이터 조회 오류: {e}")
        return

    update_list = []
    
    for asset in assets_data:
        asset_id = asset['id']
        ticker = asset['ticker']
        # 예: KR, US 등의 코드
        market = asset['market'] 
        
        # 🚨 추가된 조건: 티커가 비어 있으면 건너뛰기
        if not ticker:
            # print(f"건너뛰기: [{asset['name_kr']}] (티커 없음)")
            continue

        # 3. 현재가 조회 함수 호출(티커가 있는 경우에만 실행됨)
        current_price = get_current_price(ticker, market)
        
        if current_price is not None:
            update_data = {
                'current_price': float(current_price),
                'last_updated': datetime.now(timezone.utc).isoformat()
            }
            try:
                # 🚨 UPDATE 메서드를 사용하여 ID에 해당하는 행의 특정 컬럼만 업데이트
                response = supabase.table('assets').update(update_data).eq('id', asset_id).execute()
                print(f"성공: [{ticker}] 현재가: {current_price}, ID {asset_id} 업데이트 완료.")
            except Exception as e:
                print(f"🚨 DB 개별 업데이트 오류 (ID: {asset_id}, Ticker: {ticker}): {e}")
        else:
            print(f"실패: [{ticker}] 가격을 업데이트하지 못했습니다.")
            
    # 5. DB에 일괄 업데이트
    if update_list:
        try:
            # 여러 건을 동시에 업데이트(upsert)
            response = supabase.table('assets').upsert(update_list, on_conflict='id').execute()
            print(f"\n총 {len(update_list)}개 종목 현재가 DB 업데이트 완료.")
        except Exception as e:
            print(f"DB 업데이트 중 오류 발생: {e}")

if __name__ == "__main__":
    fetch_and_update_prices()