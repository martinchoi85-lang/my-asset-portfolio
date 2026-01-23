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

    DEFAULT_CODE_FIELDS = ["종목코드", "단축코드", "표준코드", "ISIN코드", "ISIN"]
    DEFAULT_PRICE_FIELDS = ["종가", "종가(원)", "종가(원화)"]

    @staticmethod
    def _normalize_code(code: str) -> str:
        # ✅ KRX 종목코드는 보통 6자리 숫자이므로, 숫자일 때는 0 padding을 보장합니다.
        raw = (code or "").strip()
        if raw.isdigit() and len(raw) < 6:
            return raw.zfill(6)
        return raw

    @staticmethod
    def _convert_alnum_code_to_numeric(code: str) -> Optional[str]:
        """
        ✅ 문자 혼합 티커(예: 0064K0)를 KRX 숫자 코드로 변환합니다.

        초보자 설명:
        - KRX CSV의 종목코드는 "숫자 6자리"가 일반적입니다.
        - 알파벳이 섞인 티커는 KRX CSV에서 매칭이 실패할 수 있어요.
        - 그래서 "A=10, B=11, ..., K=20" 처럼 알파벳을 숫자로 바꿔봅니다.
          (ASCII 코드에서 55를 빼는 방식: 'K'(75) - 55 = 20)

        예시:
        - 0064K0 → "0064200" (K=20으로 변환)
        - 길이가 7이 되면, KRX의 6자리 규칙에 맞추기 위해 앞 6자리만 사용: "006420"
        """
        raw = (code or "").strip().upper()
        if not raw:
            return None

        # ✅ 알파벳이 하나도 없으면 변환할 필요가 없음
        if raw.isdigit():
            return raw.zfill(6) if len(raw) < 6 else raw

        converted_parts: list[str] = []
        for ch in raw:
            if ch.isdigit():
                converted_parts.append(ch)
                continue
            if "A" <= ch <= "Z":
                # ✅ 알파벳 → 숫자 (A=10, B=11 ... Z=35)
                numeric = ord(ch) - 55  # 'A'(65) - 55 = 10
                converted_parts.append(str(numeric))
                continue
            # ✅ 기타 문자는 변환 불가 → None 처리
            return None

        converted = "".join(converted_parts)
        if len(converted) > 6:
            # ✅ KRX 표준 6자리 규칙에 맞추기 위해 앞 6자리만 사용
            converted = converted[:6]
        elif len(converted) < 6:
            converted = converted.zfill(6)
        return converted

    @staticmethod
    def _build_candidate_codes(code: str) -> list[str]:
        """
        ✅ KRX 매칭을 위해 여러 후보 코드를 만든다.

        초보자 설명:
        - 1순위: 원본 코드(숫자면 6자리 보정)
        - 2순위: 알파벳이 섞인 경우 숫자 코드로 변환
        - 이렇게 하면 "0064K0" 같은 티커도 KRX CSV와 매칭될 가능성이 생깁니다.
        """
        candidates: list[str] = []
        normalized = KRXPriceFetcher._normalize_code(code)
        if normalized:
            candidates.append(normalized)

        converted = KRXPriceFetcher._convert_alnum_code_to_numeric(code)
        if converted and converted not in candidates:
            candidates.append(converted)

        return candidates

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
    def _normalize_code_value(value: Any) -> str:
        raw = str(value or "").strip().upper()
        if raw.isdigit():
            return raw.zfill(6)
        return raw

    @staticmethod
    def _pick_column(df: pd.DataFrame, preferred: str, fallbacks: list[str]) -> Optional[str]:
        if preferred and preferred in df.columns:
            return preferred
        for name in fallbacks:
            if name in df.columns:
                return name
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

        candidate_codes = KRXPriceFetcher._build_candidate_codes(code)
        if not candidate_codes:
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

                code_col = KRXPriceFetcher._pick_column(df, code_field, KRXPriceFetcher.DEFAULT_CODE_FIELDS)
                price_col = KRXPriceFetcher._pick_column(df, price_field, KRXPriceFetcher.DEFAULT_PRICE_FIELDS)
                if not code_col or not price_col:
                    last_error = (
                        "KRX 컬럼 누락: "
                        f"code_field={code_field}, price_field={price_field}, "
                        f"available={list(df.columns)[:12]}"
                    )
                    continue

                # ✅ 종목코드 컬럼을 문자열로 정규화해서 비교합니다.
                # - 숫자 코드는 zfill(6), 영문 혼합은 대문자로 통일
                df["_code_norm"] = df[code_col].map(KRXPriceFetcher._normalize_code_value)
                candidate_norms = [KRXPriceFetcher._normalize_code_value(c) for c in candidate_codes]

                # ✅ 원본 코드/변환 코드 모두 후보로 비교합니다.
                # - 예: 0064K0 → ["0064K0", "006420"] 형태로 비교
                row = df.loc[df["_code_norm"].isin(candidate_norms)]
                if row.empty:
                    last_error = f"KRX 코드 미존재: {candidate_codes}"
                    continue

                price_raw = row.iloc[0][price_col]
                price = KRXPriceFetcher._safe_float(price_raw)
                if price is None:
                    last_error = "KRX 가격 값이 비정상"
                    continue

                return KRXPriceResult(price=price, used_trade_date=trade_date, reason=None)
            except Exception as e:
                last_error = f"KRX 조회 실패: {e}"

        return KRXPriceResult(price=None, used_trade_date=None, reason=last_error)
