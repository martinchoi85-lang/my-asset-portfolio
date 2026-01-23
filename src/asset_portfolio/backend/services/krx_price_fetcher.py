from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from typing import Optional, Dict, Any

import pandas as pd
import requests


KST = timezone(timedelta(hours=9))


@dataclass
class KRXPriceResult:
    """
    ✅ KRX 가격 조회 결과
    - price: 종가/기준가 등 숫자형 가격
    - used_trade_date: 실제로 조회에 사용한 거래일(YYYYMMDD)
    - reason: 실패 사유
    """
    price: Optional[float]
    used_trade_date: Optional[str]
    reason: Optional[str]


class KRXPriceFetcher:
    """
    ✅ KRX(한국거래소) 자동 가격 수집기
    - OTP 기반 CSV 다운로드 방식
    - ETF 전종목 시세/기준가 등 "조회 가능한" 가격을 가져오는 용도
    - 실시간이 아닌 "참고 가격"을 일관적으로 수집하는 목적
    """

    OTP_URL = "https://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    DOWNLOAD_URL = "https://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    REFERER = "https://data.krx.co.kr/contents/MDC/MDI/mdiLoader"

    @staticmethod
    def _normalize_code(code: str) -> str:
        # ✅ KRX 종목코드는 보통 6자리 숫자이므로, 숫자일 때는 0 padding을 보장합니다.
        raw = (code or "").strip()
        if raw.isdigit() and len(raw) < 6:
            return raw.zfill(6)
        return raw

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            # ✅ 쉼표가 포함된 숫자 문자열을 안전하게 float으로 변환
            if isinstance(value, str):
                value = value.replace(",", "").strip()
            price = float(value)
            return price if price > 0 else None
        except Exception:
            return None

    @staticmethod
    def _download_csv(bld: str, params: Dict[str, Any]) -> pd.DataFrame:
        """
        ✅ KRX OTP → CSV 다운로드 파이프라인
        1) GenerateOTP로 OTP 코드 발급
        2) download_csv로 실제 CSV 파일 수신
        3) pandas로 CSV 파싱
        """
        payload = {
            "bld": bld,
            "locale": "ko_KR",
            **(params or {}),
        }

        # ✅ KRX는 Referer 헤더가 없으면 OTP 발급이 거절되는 경우가 많습니다.
        otp_res = requests.post(
            KRXPriceFetcher.OTP_URL,
            data=payload,
            headers={"Referer": KRXPriceFetcher.REFERER},
            timeout=30,
        )
        otp_res.raise_for_status()
        otp_code = (otp_res.text or "").strip()
        if not otp_code:
            raise RuntimeError("KRX OTP 발급 실패")

        csv_res = requests.post(
            KRXPriceFetcher.DOWNLOAD_URL,
            data={"code": otp_code},
            headers={"Referer": KRXPriceFetcher.REFERER},
            timeout=30,
        )
        csv_res.raise_for_status()

        # ✅ KRX CSV는 보통 CP949 인코딩입니다.
        return pd.read_csv(BytesIO(csv_res.content), encoding="cp949")

    @staticmethod
    def fetch_reference_price(
        *,
        code: str,
        source_params: Dict[str, Any],
        max_lookback_days: int = 7,
    ) -> KRXPriceResult:
        """
        ✅ KRX 기반 "참고 가격" 조회
        - code: KRX 종목코드(6자리)
        - source_params: KRX 조회용 파라미터 집합
          예) {
                "bld": "dbms/MDC/STAT/standard/MDCSTAT04301",
                "code_field": "종목코드",
                "price_field": "종가",
                "date_field": "trdDd",
                "query_params": {"mktId": "ALL"}
              }
        - max_lookback_days: 오늘~과거 N일 범위에서 데이터가 있는 날짜를 찾음
        """
        bld = (source_params.get("bld") or "").strip()
        if not bld:
            return KRXPriceResult(price=None, used_trade_date=None, reason="bld 파라미터 누락")

        code_field = source_params.get("code_field") or "종목코드"
        price_field = source_params.get("price_field") or "종가"
        date_field = source_params.get("date_field") or "trdDd"
        query_params = source_params.get("query_params") or {}

        target_code = KRXPriceFetcher._normalize_code(code)
        if not target_code:
            return KRXPriceResult(price=None, used_trade_date=None, reason="KRX 종목코드 누락")

        # ✅ KRX는 거래일이 아니면 데이터가 없을 수 있으므로,
        #    최근 며칠을 순차적으로 조회하여 "가장 최근 데이터"를 찾습니다.
        today_kst = datetime.now(KST).date()
        last_error = None

        for offset in range(max_lookback_days + 1):
            trade_date = (today_kst - timedelta(days=offset)).strftime("%Y%m%d")

            # ✅ date_field가 이미 query_params에 있으면 덮어쓰지 않도록 방어
            params = dict(query_params)
            if date_field not in params:
                params[date_field] = trade_date

            try:
                df = KRXPriceFetcher._download_csv(bld, params)
                if df is None or df.empty:
                    last_error = f"KRX 데이터 없음: {trade_date}"
                    continue

                # ✅ 종목코드 컬럼을 문자열로 정규화해서 비교합니다.
                df[code_field] = df[code_field].astype(str).str.zfill(6)
                row = df.loc[df[code_field] == target_code]
                if row.empty:
                    last_error = f"KRX 코드 미존재: {target_code}"
                    continue

                price_raw = row.iloc[0][price_field]
                price = KRXPriceFetcher._safe_float(price_raw)
                if price is None:
                    last_error = "KRX 가격 값이 비정상"
                    continue

                return KRXPriceResult(price=price, used_trade_date=trade_date, reason=None)
            except Exception as e:
                last_error = f"KRX 조회 실패: {e}"

        return KRXPriceResult(price=None, used_trade_date=None, reason=last_error)
