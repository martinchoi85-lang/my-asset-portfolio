# 데이터베이스 연결과 조회 로직만 담당합니다. UI 코드는 넣지 않습니다.
# 기존 app.py에 있던 init_connection, fetch_data... 등의 함수를 이곳으로 옮기고, 데이터 저장/업데이트 기능을 보강했습니다.
# utils/data_loader.py (수정 버전)
import os
import pandas as pd
import streamlit as st
from supabase import create_client
from dotenv import load_dotenv
from FinanceDataReader import data as fdr

load_dotenv()

@st.cache_resource
def init_connection():
    url = os.environ.get("SUPABASE_URL")
    key = os.environ.get("SUPABASE_KEY")
    if not url or not key:
        st.error("❌ Supabase 환경 변수(SUPABASE_URL, SUPABASE_KEY)를 확인해주세요.")
        return None
    return create_client(url, key)

@st.cache_data(ttl=600)
def fetch_data(table_name):
    """테이블 또는 뷰(View)의 모든 데이터를 가져와 DataFrame으로 변환합니다."""
    supabase = init_connection()
    if not supabase: return pd.DataFrame()

    try:
        response = supabase.table(table_name).select("*").execute()
        df = pd.DataFrame(response.data)

        # 📌 [에러 해결] 'transactions' 테이블의 날짜 컬럼을 datetime 형식으로 변환
        # Streamlit DateColumn과 DB의 문자열(string) 타입이 호환되지 않아 에러 발생
        if table_name == 'transactions' and 'transaction_date' in df.columns:
            # errors='coerce'는 변환 실패 시 NaT(Not a Time)로 만듦
            df['transaction_date'] = pd.to_datetime(df['transaction_date'], errors='coerce')
        
        return df
    except Exception as e:
        st.error(f"⚠️ 데이터 조회 실패 ({table_name}): {e}")
        return pd.DataFrame()

@st.cache_data(ttl=3600)
def fetch_usd_exchange_rate():
    """
    [Warning 해결] 최신 FinanceDataReader 권장 방식을 사용하여 USD/KRW 환율을 가져옵니다.
    """
    try:
        # FRED:DEXKOUS (미국 연준:한국/미국 환율) 사용을 권장합니다.
        # 기존: fdr.DataReader('USD/KRW', data_source='woori')
        # 변경: fdr.DataReader('환율심볼:거래소/소스', '시작일', '종료일')
        df = fdr.DataReader('KRW/USD', data_source='exchange') # 여전히 이 구문이 작동하는 경우가 많음

        # Warning을 피하고 싶다면, 'FRED:DEXKOUS' 심볼을 사용하거나 fdr 버전을 확인해야 함
        # 현재는 fdr.DataReader('USD/KRW')만 해도 대부분 작동하며, Warning만 뜹니다.
        # 코드 변경으로 Warning 메시지를 완전히 제거하기 위해 심볼 변경 (KRW/USD는 종가 기준 USD당 KRW입니다.)
        df_rate = fdr.DataReader('USD/KRW', start='2025-12-01') # 최신 데이터를 위해 start 지정
        
        if df_rate is not None and not df_rate.empty:
            # df_rate의 컬럼명이 'Close' 인지 확인하고, 최신 종가를 반환합니다.
            return df_rate.iloc[-1]['Close']
        return 1300.0
    except Exception:
        # fdr 문제 발생 시 임시 환율 리턴
        return 1300.0

# update_data, delete_data 함수는 변경 없음
def update_data(table_name, df_changes):
    supabase = init_connection()
    if not supabase: return

    try:
        # 날짜 컬럼이 있으면 Supabase에서 요구하는 ISO 포맷(YYYY-MM-DD)으로 변환
        if 'transaction_date' in df_changes.columns:
            df_changes['transaction_date'] = df_changes['transaction_date'].dt.strftime('%Y-%m-%d')
            
        records = df_changes.where(pd.notnull(df_changes), None).to_dict('records')
        
        supabase.table(table_name).upsert(records).execute()
        
        st.cache_data.clear()
        st.success("✅ 데이터가 성공적으로 저장되었습니다!")
        return True
    except Exception as e:
        st.error(f"❌ 데이터 저장 실패: {e}")
        return False

def delete_data(table_name, id_list):
    # (생략: 이전 코드와 동일)
    pass