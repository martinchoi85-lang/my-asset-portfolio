import pandas as pd
from asset_portfolio.backend.services.importer.base import ImportProfile
from asset_portfolio.backend.services.importer.engine import TransactionParser

def kiwoom_2row_preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    키움증권 [0365] 화면 등 2줄로 표시되는 데이터를 1줄로 병합합니다.
    """
    return TransactionParser.merge_2_row_hts(df)

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
    default_currency="USD",
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

AVAILABLE_PROFILES = [KIWOOM_0365_PROFILE, TOSS_STOCK_PROFILE]

def get_profile_by_name(name: str) -> ImportProfile:
    for p in AVAILABLE_PROFILES:
        if p.name == name:
            return p
    raise ValueError(f"Unknown ImportProfile: {name}")
