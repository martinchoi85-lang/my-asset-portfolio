import logging
import pandas as pd
import re
from typing import List, Dict, Any, Optional, Tuple
from asset_portfolio.backend.services.importer.base import ImportProfile, ParsedTransaction
from asset_portfolio.backend.services.asset_alias_service import AssetAliasService
from asset_portfolio.backend.services.asset_service import AssetService

logger = logging.getLogger(__name__)

class TransactionParser:
    def __init__(self, user_id: str, profile: ImportProfile):
        self.user_id = user_id
        self.profile = profile
        self.alias_map = AssetAliasService.get_alias_map(user_id)
        self.assets_df = AssetService.get_assets_lookup_df()

    def parse(self, df: pd.DataFrame) -> List[ParsedTransaction]:
        """
        주어진 DataFrame을 프로파일에 따라 파싱하여 표준 거래 리스트로 반환합니다.
        """
        # 1. Preprocessing (2-row merge 등)
        if self.profile.preprocess_func:
            df = self.profile.preprocess_func(df)

        results = []
        for idx, row in df.iterrows():
            parsed_tx = self._parse_row(row, idx)
            results.append(parsed_tx)
        
        return results

    def _parse_row(self, row: pd.Series, row_idx: int) -> ParsedTransaction:
        raw_data = row.to_dict()
        standard_data = {}
        
        # 2. Column Mapping & Basic Normalization
        for raw_col, std_col in self.profile.column_map.items():
            val = row.get(raw_col)
            if std_col in self.profile.numeric_columns:
                val = self._clean_numeric(val)
            standard_data[std_col] = val

        # 3. Trade Type Normalization
        raw_trade_type = standard_data.get("trade_type")
        if raw_trade_type:
            standard_data["trade_type"] = self.profile.trade_type_map.get(str(raw_trade_type).strip(), "UNKNOWN")

        # 4. Asset Resolution (Strict + Alias)
        asset_name = str(standard_data.get("asset_name", "")).strip()
        ticker = str(standard_data.get("ticker", "")).strip().upper()
        
        asset_id = None
        # (1) Alias DB 우선 확인
        if asset_name in self.alias_map:
            asset_id = self.alias_map[asset_name]
        # (2) Ticker 기반 Strict 매칭
        elif ticker:
            matched = self.assets_df[self.assets_df["ticker"] == ticker]
            if not matched.empty:
                asset_id = int(matched.iloc[0]["asset_id"])
        # (3) name_kr 기반 Strict 매칭
        elif asset_name:
            matched = self.assets_df[self.assets_df["name_kr"] == asset_name]
            if not matched.empty:
                asset_id = int(matched.iloc[0]["asset_id"])

        parsed = ParsedTransaction(raw_row=raw_data, standard_data=standard_data, asset_id=asset_id)
        
        if not asset_id:
            parsed.status = "PENDING"
            parsed.message = "자산을 찾을 수 없습니다. (Alias 등록 필요)"
        else:
            parsed.status = "READY"
            
        return parsed

    def _clean_numeric(self, val: Any) -> float:
        if pd.isna(val) or val == "":
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        # 쉼표 등 제거
        cleaned = re.sub(r'[^\d.-]', '', str(val))
        try:
            return float(cleaned)
        except ValueError:
            return 0.0

    @staticmethod
    def merge_2_row_hts(df: pd.DataFrame, key_id_col: Optional[str] = None) -> pd.DataFrame:
        """
        2-row 구조의 HTS 데이터를 1-row로 병합하는 유틸리티 함수입니다.
        데이터가 '짝수행이 홀수행의 보조 정보'인 경우를 가정합니다.
        """
        if len(df) < 2:
            return df
        
        merged_rows = []
        for i in range(0, len(df) - 1, 2):
            row1 = df.iloc[i].to_dict()
            row2 = df.iloc[i+1].to_dict()
            # 겹치지 않는 필드 혹은 비어있는 필드를 보강
            combined = {**row2, **row1} # row1(상단행) 정보 우선
            merged_rows.append(combined)
            
        return pd.DataFrame(merged_rows)
