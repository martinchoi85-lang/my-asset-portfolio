from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Callable
import pandas as pd
from datetime import date

@dataclass
class ImportProfile:
    """
    특정 HTS 화면의 데이터 포맷 및 파싱 규칙을 정의하는 클래스입니다.
    """
    name: str  # 프로파일 식별자 (예: "Kiwoom_History")
    display_name: str  # UI에 표시될 이름 (예: "키움증권 - 거래내역")
    
    # 원본 컬럼명 -> 표준 컬럼명 매핑
    # 표준 컬럼: account_name, transaction_date, ticker, asset_name, trade_type, quantity, price, fee, tax, memo, currency, market
    column_map: Dict[str, str]
    
    # 매매구분 값 매핑 (원본 -> BUY/SELL/DIVIDEND/DEPOSIT/WITHDRAW)
    trade_type_map: Dict[str, str]
    
    # 숫자형 컬럼 리스트 (쉼표 제거 및 float 변환 대상)
    numeric_columns: List[str] = field(default_factory=lambda: ["quantity", "price", "fee", "tax", "dividend_net", "dividend_gross"])
    
    # 전처리 함수 (예: 2-row 병합). pd.DataFrame을 인자로 받아 처리된 DataFrame 반환
    preprocess_func: Optional[Callable[[pd.DataFrame], pd.DataFrame]] = None

    # 통화/시장 기본값 (데이터에 없을 경우 사용)
    default_currency: Optional[str] = None
    default_market: Optional[str] = None

@dataclass
class ParsedTransaction:
    """
    임포트 엔진에 의해 표준화된 단일 거래 데이터입니다.
    """
    raw_row: Dict[str, Any]
    standard_data: Dict[str, Any]  # 정규화된 데이터
    status: str = "PENDING"  # PENDING, READY, ERROR, DUPLICATE
    message: Optional[str] = None
    asset_id: Optional[int] = None
    account_id: Optional[str] = None
    is_new_asset: bool = False
