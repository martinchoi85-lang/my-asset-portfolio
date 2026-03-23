import pandas as pd
from asset_portfolio.backend.services.importer.base import ImportProfile
from asset_portfolio.backend.services.importer.engine import TransactionParser

def kiwoom_2row_preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    키움증권 [0365] 화면 등 2줄로 표시되는 데이터를 1줄로 병합합니다.
    """
    return TransactionParser.merge_2_row_hts(df)


def mirae_domestic_preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    미래에셋 국내주식 매매내역 전처리 (필수 필드 보강 버전)
    """
    def parse_mirae_date(val):
        if pd.isna(val): return None
        try:
            val_str = str(val).strip()
            if val_str.replace('.', '').isdigit():
                return pd.to_datetime(int(float(val_str)), unit='D', origin='1899-12-30').strftime('%Y-%m-%d')
            return pd.to_datetime(val_str).strftime('%Y-%m-%d')
        except:
            return None

    def to_num(v):
        if pd.isna(v): return 0
        try:
            return float(str(v).replace(',', ''))
        except:
            return 0

    new_rows = []
    # 첫 번째 헤더 행 제외하고 반복
    for _, row in df.iterrows():
        date_str = parse_mirae_date(row.iloc[0])
        if not date_str: continue

        name = str(row.iloc[1]).strip() if pd.notna(row.iloc[1]) else ""
        if not name or name == "종목명": continue
        
        # 원본 데이터 위치 기반 추출
        buy_qty = to_num(row.iloc[2])
        buy_prc = to_num(row.iloc[3])
        buy_amt = to_num(row.iloc[4])  # 매수금액
        
        sell_qty = to_num(row.iloc[5])
        sell_prc = to_num(row.iloc[6])
        sell_amt = to_num(row.iloc[7]) # 매도금액
        
        fee = to_num(row.iloc[8])      # 매매비용
        
        # 공통 데이터 생성 함수
        def create_item(t_type, qty, prc, amt, f):
            return {
                'transaction_date': date_str,
                'asset_name': name,
                'trade_type': t_type,
                'quantity': qty,
                'price': prc,
                'total_amount': amt, # 필수: 실제 현금 흐름
                'fee': f,
                'tax': 0,            # 국내 주식 기본값
                'currency': 'krw',   # 필수: 통화 설정
                'market_type': 'korea' # 선택: 시장 구분
            }

        if buy_qty > 0:
            new_rows.append(create_item('매수', buy_qty, buy_prc, buy_amt, fee if sell_qty == 0 else 0))
            
        if sell_qty > 0:
            # 매수/매도가 한 줄에 다 있는 경우 수수료는 매도 쪽에 포함하거나 안분
            new_rows.append(create_item('매도', sell_qty, sell_prc, sell_amt, fee))
            
    return pd.DataFrame(new_rows)


MIRAE_0615_PROFILE = ImportProfile(
    name="Mirae_Domestic_Summary",
    display_name="미래에셋증권 - 국내주식 기간별 매매상세",
    column_map={
        "transaction_date": "transaction_date", # 데이터프레임의 컬럼명 : 표준 키
        "asset_name": "asset_name",             # "asset_name" 컬럼을 "asset_name" 키로 저장
        "trade_type": "trade_type",
        "quantity": "quantity",
        "price": "price",
        "total_amount": "total_amount",
        "fee": "fee",
        "tax": "tax",
        "currency": "currency",
        "market_type": "market_type"
    },
    trade_type_map={
        "매수": "BUY",
        "매도": "SELL"
    },
    preprocess_func=mirae_domestic_preprocess,
    default_currency="KRW",
    default_market="kor"
)

KIWOOM_0365_PROFILE = ImportProfile(
    name="Kiwoom_0365",
    display_name="키움증권 - [0365] 해외주식 거래내역 (2줄 양식)",
    column_map={
        "종목명": "asset_name",
        "단축코드": "ticker",
        "거래일자": "transaction_date",
        "매매구분": "trade_type",
        "체결수량": "quantity",
        "체결단가": "price",
        "수수료": "fee",
        "인지세": "tax",  # 예시 컬럼매핑 (실제 데이터에 맞게 보정 필요)
        "제세금": "tax"
    },
    trade_type_map={
        "매수": "BUY",
        "현금매수": "BUY",
        "매도": "SELL",
        "현금매도": "SELL"
    },
    preprocess_func=kiwoom_2row_preprocess,
    default_currency="usd",
    default_market="usa"
)

TOSS_STOCK_PROFILE = ImportProfile(
    name="Toss_Stock",
    display_name="토스증권 - 주식 거래내역",
    column_map={
        "종목명": "asset_name",
        "티커": "ticker",
        "체결일": "transaction_date",
        "거래구분": "trade_type",
        "수량": "quantity",
        "단가": "price",
        "수수료": "fee",
        "제세금": "tax"
    },
    trade_type_map={
        "매수": "BUY",
        "구매": "BUY",
        "매도": "SELL",
        "판매": "SELL"
    }
)

AVAILABLE_PROFILES = [
    MIRAE_0615_PROFILE,
    KIWOOM_0365_PROFILE, 
    TOSS_STOCK_PROFILE
]

def get_profile_by_name(name: str) -> ImportProfile:
    for p in AVAILABLE_PROFILES:
        if p.name == name:
            return p
    raise ValueError(f"Unknown ImportProfile: {name}")