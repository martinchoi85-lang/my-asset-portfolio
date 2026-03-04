from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import re
from typing import Dict, List, Optional, Tuple

import pandas as pd
import streamlit as st

from asset_portfolio.backend.infra.supabase_client import get_supabase_client
from asset_portfolio.backend.services.asset_service import AssetService
from asset_portfolio.backend.services.transaction_service import (
    TransactionService,
    CreateTransactionRequest,
)
from asset_portfolio.dashboard.transaction_editor import _load_accounts_df, _load_assets_df


@dataclass
class PreparedTransaction:
    request: CreateTransactionRequest
    created_asset_payload: Optional[Dict[str, str]] = None


def _normalize_column_key(value: str) -> str:
    """컬럼명을 비교하기 위해 공백/특수문자를 제거하고 소문자로 통일한다."""
    cleaned = re.sub(r"[^0-9a-zA-Z가-힣]", "", str(value)).lower()
    return cleaned


def _normalize_trade_type(value: str) -> Optional[str]:
    """매수/매도 표기를 BUY/SELL로 표준화한다."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    mapping = {
        "매수": "BUY",
        "buy": "BUY",
        "매도": "SELL",
        "sell": "SELL",
    }
    if normalized in mapping:
        return mapping[normalized]
    if normalized.upper() in {"BUY", "SELL"}:
        return normalized.upper()
    return None


def _normalize_currency(value: str) -> Optional[str]:
    """통화 표기를 KRW/USD 중심으로 표준화한다."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    mapping = {
        "krw": "KRW",
        "won": "KRW",
        "원": "KRW",
        "usd": "USD",
        "달러": "USD",
        "us$": "USD",
        "$": "USD",
    }
    if normalized in mapping:
        return mapping[normalized]
    return normalized.upper()


def _normalize_market(value: str) -> Optional[str]:
    """시장 구분을 내부 표준값으로 맞춘다."""
    if value is None:
        return None
    normalized = str(value).strip().lower()
    mapping = {
        "kospi": "korea",
        "코스피": "korea",
        "kosdaq": "korea",
        "코스닥": "korea",
        "krx": "korea",
        "korea": "korea",
        "nyse": "usa",
        "nasdaq": "usa",
        "usa": "usa",
        "us": "usa",
        "america": "usa",
    }
    return mapping.get(normalized, normalized)


def _parse_numeric(val) -> float:
    """문자열에 포함된 쉼표 등 서식을 제거하고 숫자로 변환한다."""
    if pd.isna(val):
        return float('nan')
    if isinstance(val, (int, float)):
        return float(val)
    # 쉼표 등 불필요한 서식 제거
    cleaned = re.sub(r'[^\d.-]', '', str(val))
    if not cleaned or cleaned == '.' or cleaned == '-':
        return float('nan')
    try:
        return float(cleaned)
    except ValueError:
        return float('nan')


def _map_columns(df: pd.DataFrame, aliases: Dict[str, List[str]]) -> Tuple[pd.DataFrame, List[str]]:
    """업로드된 컬럼을 표준 컬럼명으로 매핑한다."""
    normalized_columns = { _normalize_column_key(col): col for col in df.columns }
    rename_map: Dict[str, str] = {}
    missing_fields: List[str] = []

    for canonical, candidates in aliases.items():
        found = None
        for candidate in candidates:
            key = _normalize_column_key(candidate)
            if key in normalized_columns:
                found = normalized_columns[key]
                break
        if found:
            rename_map[found] = canonical
        else:
            missing_fields.append(canonical)

    return df.rename(columns=rename_map), missing_fields


def _get_account_id_by_name(accounts_df: pd.DataFrame, account_name: str) -> Tuple[Optional[str], Optional[str]]:
    """계좌명을 account_id로 매칭하고, 문제 발생 시 오류 메시지를 돌려준다."""
    matched = accounts_df[accounts_df["name"] == account_name]
    if matched.empty:
        return None, f"계좌명 '{account_name}' 이(가) 등록된 계좌와 일치하지 않습니다."
    if len(matched) > 1:
        return None, f"계좌명 '{account_name}' 이(가) 중복되어 계좌를 확정할 수 없습니다."
    return str(matched.iloc[0]["id"]), None


def _get_asset_row_by_ticker(assets_df: pd.DataFrame, ticker: str) -> Optional[pd.Series]:
    matched = assets_df[assets_df["ticker"].fillna("").str.upper() == ticker]
    if matched.empty:
        return None
    return matched.iloc[0]


def _find_existing_duplicate(
    *,
    account_id: str,
    asset_id: int,
    transaction_date: date,
    trade_type: str,
    quantity: float,
    price: float,
    tax: float,
) -> bool:
    """기존 transactions 테이블에 같은 거래가 있는지 확인한다."""
    supabase = get_supabase_client()
    resp = (
        supabase.table("transactions")
        .select("id, quantity, price")
        .eq("account_id", account_id)
        .eq("asset_id", asset_id)
        .eq("transaction_date", transaction_date.isoformat())
        .eq("trade_type", trade_type)
        .execute()
    )
    if not resp.data:
        return False
        
    for row in resp.data:
        # 부동소수점 미세 오차 허용 (0.01 범위 내)
        q_diff = abs(float(row["quantity"]) - quantity)
        p_diff = abs(float(row["price"]) - price)
        if q_diff < 0.01 and p_diff < 0.01:
            return True
            
    return False


def _render_required_fields_table(field_rows: List[Dict[str, str]]) -> None:
    st.markdown("#### ✅ 업로드 필수 필드 & 예시")
    st.dataframe(pd.DataFrame(field_rows))


def _render_account_reference_table(user_id: str) -> None:
    accounts_df = _load_accounts_df(user_id)
    if accounts_df.empty:
        st.warning("등록된 계좌가 없습니다. 계좌를 먼저 등록하세요.")
        return
    st.markdown("#### ✅ 현재 등록된 계좌 목록")
    display_df = accounts_df[["brokerage", "name", "type", "old_owner"]].copy()
    display_df.rename(
        columns={
            "brokerage": "증권사",
            "name": "계좌명",
            "type": "계좌유형",
            "owner": "소유자",
        },
        inplace=True,
    )
    st.dataframe(display_df, width='stretch')


def _get_latest_transaction_dates(user_id: str) -> pd.DataFrame:
    """계좌별 최근 거래일을 조회해 중복 입력을 예방하도록 돕는다."""
    accounts_df = _load_accounts_df(user_id)
    if accounts_df.empty:
        return pd.DataFrame()

    supabase = get_supabase_client()
    latest_rows = []
    for _, row in accounts_df.iterrows():
        account_id = str(row["id"])
        resp = (
            supabase.table("transactions")
            .select("transaction_date")
            .eq("account_id", account_id)
            .order("transaction_date", desc=True)
            .limit(1)
            .execute()
        )
        tx_date = None
        if resp.data:
            tx_date = resp.data[0]["transaction_date"]
        latest_rows.append({
            "증권사": row["brokerage"],
            "계좌명": row["name"],
            "최근 거래일": tx_date or "-",
        })
    return pd.DataFrame(latest_rows)


def _clean_dataframe_strings(df: pd.DataFrame) -> pd.DataFrame:
    """모든 값을 문자열로 읽은 DataFrame에서 NaN 관련 문자열이나 공백만 있는 셀을 빈 문자열로 정제하고 빈 행을 삭제한다."""
    # 모든 값 앞뒤 공백 제거
    df = df.map(lambda x: str(x).strip() if pd.notna(x) else "")
    # 'NaN' 등 문자로 읽힌 결측치를 빈 문자열로 치환
    df = df.replace(r'(?i)^(nan|<na>|none|na)$', '', regex=True)
    # 내용이 아예 없는 완전히 빈 행 삭제
    df = df.replace('', pd.NA).dropna(how='all').fillna('')
    
    # '.0'으로 끝나는 숫자형 문자열(예: '161510.0')을 정수로 복원
    # (엑셀/복붙 환경에서 161510이 Float로 파싱되어 '.0'이 붙는 현상 방지)
    for col in df.columns:
        if 'ticker' in col.lower() or '티커' in col or '종목코드' in col:
            df[col] = df[col].str.replace(r'\.0$', '', regex=True)
            
    return df


def _read_uploaded_file(uploaded_file) -> Optional[pd.DataFrame]:
    if uploaded_file is None:
        return None
    file_name = uploaded_file.name.lower()
    df = None
    if file_name.endswith(".csv"):
        df = pd.read_csv(uploaded_file, dtype=str, keep_default_na=False)
    elif file_name.endswith(".xlsx") or file_name.endswith(".xls"):
        df = pd.read_excel(uploaded_file, dtype=str, keep_default_na=False)
    else:
        st.error("지원하지 않는 파일 형식입니다. CSV 또는 XLSX를 업로드하세요.")
        return None

    if df is not None:
        df = _clean_dataframe_strings(df)
    return df


def _read_pasted_text(pasted_text: str) -> Optional[pd.DataFrame]:
    if not pasted_text or not pasted_text.strip():
        return None
    import io
    try:
        # 엑셀/스프레드시트에서 복사한 텍스트는 탭(\t)으로 구분됨
        df = pd.read_csv(io.StringIO(pasted_text), sep='\t', dtype=str, keep_default_na=False)
        return _clean_dataframe_strings(df)
    except Exception as exc:
        st.error(f"붙여넣은 데이터를 분석하는 중 오류가 발생했습니다: {exc}")
        return None


def _prepare_trade_rows(
    df: pd.DataFrame, user_id: str
) -> Tuple[List[PreparedTransaction], List[str], List[str]]:
    """거래 행을 검증해 (준비된 거래 목록, 오류 목록, 중복 거래 목록)을 반환한다."""
    errors: List[str] = []       # 포맷/유효성 오류 → 전체 업로드 차단
    duplicates: List[str] = []   # DB 중복 거래 → 스킵하되 화면에 표시
    prepared: List[PreparedTransaction] = []
    accounts_df = _load_accounts_df(user_id)
    assets_df = _load_assets_df()

    seen_keys = set()

    for idx, row in df.iterrows():
        row_number = idx + 2  # CSV 헤더 포함을 고려한 행 번호 표시
        account_name = str(row.get("account_name") or "").strip()
        if not account_name:
            errors.append(f"{row_number}행: 계좌명이 비어 있습니다.")
            continue

        account_id, account_error = _get_account_id_by_name(accounts_df, account_name)
        if account_error:
            errors.append(f"{row_number}행: {account_error}")
            continue

        ticker = str(row.get("ticker") or "").strip().upper()
        if ticker in ("NAN", "<NA>", "NONE", "NA"):
            ticker = ""
        if not ticker:
            errors.append(f"{row_number}행: 티커가 비어 있습니다.")
            continue

        trade_type = _normalize_trade_type(row.get("trade_type"))
        if not trade_type:
            errors.append(f"{row_number}행: 거래 타입이 매수/매도/BUY/SELL 중 하나여야 합니다.")
            continue

        quantity = _parse_numeric(row.get("quantity"))
        price = _parse_numeric(row.get("price"))
        fee = _parse_numeric(row.get("fee"))
        tax = _parse_numeric(row.get("tax"))

        if pd.isna(quantity) or quantity <= 0:
            errors.append(f"{row_number}행: 수량(quantity)은 0보다 커야 합니다.")
            continue
        if pd.isna(price) or price <= 0:
            errors.append(f"{row_number}행: 단가(price)는 0보다 커야 합니다.")
            continue

        tx_date = pd.to_datetime(row.get("transaction_date"), errors="coerce")
        if pd.isna(tx_date):
            errors.append(f"{row_number}행: 거래일(transaction_date)을 읽을 수 없습니다.")
            continue

        normalized_currency = _normalize_currency(row.get("currency"))
        normalized_market = _normalize_market(row.get("market"))
        asset_row = _get_asset_row_by_ticker(assets_df, ticker)
        created_asset_payload: Optional[Dict[str, str]] = None

        if asset_row is None:
            asset_name = str(row.get("asset_name") or "").strip()
            asset_type = str(row.get("asset_type") or "").strip() or "stock"

            if not asset_name:
                errors.append(f"{row_number}행: 신규 자산 생성에 필요한 종목명(asset_name)이 없습니다.")
                continue
            if not normalized_currency:
                errors.append(f"{row_number}행: 신규 자산 생성에 필요한 통화(currency)가 없습니다.")
                continue

            created_asset_payload = {
                "ticker": ticker,
                "name_kr": asset_name,
                "asset_type": asset_type,
                "currency": normalized_currency,
                "market": normalized_market,
            }
        else:
            if normalized_currency:
                existing_currency = _normalize_currency(asset_row.get("currency"))
                if existing_currency and existing_currency != normalized_currency:
                    errors.append(
                        f"{row_number}행: 업로드 통화({normalized_currency})가 기존 자산 통화({existing_currency})와 다릅니다."
                    )
                    continue

        fee_value = float(fee) if not pd.isna(fee) else 0.0
        tax_value = float(tax) if not pd.isna(tax) else 0.0
        memo = str(row.get("memo") or "").strip() or None

        # ✅ 파일 내부 중복 체크 (포맷 오류로 간주 → errors에 추가)
        dedupe_key = (account_id, ticker, tx_date.date().isoformat(), trade_type, float(quantity), float(price))
        if dedupe_key in seen_keys:
            errors.append(f"{row_number}행: 업로드 파일 내 중복 거래가 있습니다.")
            continue
        seen_keys.add(dedupe_key)

        # ✅ 기존 DB 중복 체크 (자산이 이미 있는 경우에만)
        #    중복이어도 나머지 거래는 계속 업로드 → duplicates 리스트에 별도 수집
        if asset_row is not None:
            if _find_existing_duplicate(
                account_id=account_id,
                asset_id=int(asset_row["id"]),
                transaction_date=tx_date.date(),
                trade_type=trade_type,
                quantity=float(quantity),
                price=float(price),
                tax=tax_value,
            ):
                duplicates.append(
                    f"{row_number}행: [{ticker}] {tx_date.date()} {trade_type} "
                    f"수량={float(quantity)} 단가={float(price)} — 이미 등록된 거래"
                )
                continue  # 중복 거래는 업로드 목록에서 제외

        prepared.append(
            PreparedTransaction(
                request=CreateTransactionRequest(
                    account_id=account_id,
                    asset_id=int(asset_row["id"]) if asset_row is not None else -1,
                    transaction_date=tx_date.date(),
                    trade_type=trade_type,
                    quantity=float(quantity),
                    price=float(price),
                    fee=fee_value,
                    tax=tax_value,
                    memo=memo,
                ),
                created_asset_payload=created_asset_payload,
            )
        )

    return prepared, errors, duplicates


def _prepare_dividend_rows(
    df: pd.DataFrame, user_id: str
) -> Tuple[List[PreparedTransaction], List[str], List[str]]:
    """배당금 행을 검증해 (준비된 거래 목록, 오류 목록, 중복 거래 목록)을 반환한다."""
    errors: List[str] = []       # 포맷/유효성 오류 → 전체 업로드 차단
    duplicates: List[str] = []   # DB 중복 거래 → 스킵하되 화면에 표시
    prepared: List[PreparedTransaction] = []
    accounts_df = _load_accounts_df(user_id)

    seen_keys = set()

    for idx, row in df.iterrows():
        row_number = idx + 2
        account_name = str(row.get("account_name") or "").strip()
        if not account_name:
            errors.append(f"{row_number}행: 계좌명이 비어 있습니다.")
            continue

        account_id, account_error = _get_account_id_by_name(accounts_df, account_name)
        if account_error:
            errors.append(f"{row_number}행: {account_error}")
            continue

        ticker = str(row.get("ticker") or "").strip().upper()
        asset_name = str(row.get("asset_name") or "").strip()
        market = _normalize_market(row.get("market"))
        currency = _normalize_currency(row.get("currency"))

        if not ticker or not asset_name:
            errors.append(f"{row_number}행: 티커 또는 종목명이 비어 있습니다.")
            continue
        if not currency:
            errors.append(f"{row_number}행: 통화(currency)가 비어 있습니다.")
            continue

        gross = _parse_numeric(row.get("dividend_gross"))
        net = _parse_numeric(row.get("dividend_net"))
        if pd.isna(gross) or pd.isna(net):
            errors.append(f"{row_number}행: 배당금(세전/세후) 값을 숫자로 읽을 수 없습니다.")
            continue
        if gross < net:
            errors.append(f"{row_number}행: 배당금(세전)이 세후보다 작습니다.")
            continue

        payout_date = pd.to_datetime(row.get("transaction_date"), errors="coerce")
        if pd.isna(payout_date):
            errors.append(f"{row_number}행: 지급일자(transaction_date)을 읽을 수 없습니다.")
            continue

        try:
            cash_asset_id = TransactionService._get_cash_asset_id_by_currency(currency)
        except Exception as exc:
            errors.append(f"{row_number}행: {exc}")
            continue

        tax_value = float(gross - net)
        memo = f"배당금 | {ticker} | {asset_name}"
        if market:
            memo += f" | {market}"

        # ✅ 파일 내부 중복 체크 (포맷 오류로 간주 → errors에 추가)
        dedupe_key = (account_id, cash_asset_id, payout_date.date().isoformat(), float(net), tax_value)
        if dedupe_key in seen_keys:
            errors.append(f"{row_number}행: 업로드 파일 내 중복 배당금이 있습니다.")
            continue
        seen_keys.add(dedupe_key)

        # ✅ 기존 DB 중복 체크
        #    중복이어도 나머지 거래는 계속 업로드 → duplicates 리스트에 별도 수집
        if _find_existing_duplicate(
            account_id=account_id,
            asset_id=cash_asset_id,
            transaction_date=payout_date.date(),
            trade_type="DEPOSIT",
            quantity=float(net),
            price=1.0,
            tax=tax_value,
        ):
            duplicates.append(
                f"{row_number}행: [{ticker}] {payout_date.date()} 배당금(세후)={float(net)} — 이미 등록된 거래"
            )
            continue  # 중복 거래는 업로드 목록에서 제외

        prepared.append(
            PreparedTransaction(
                request=CreateTransactionRequest(
                    account_id=account_id,
                    asset_id=cash_asset_id,
                    transaction_date=payout_date.date(),
                    trade_type="DEPOSIT",
                    quantity=float(net),
                    price=1.0,
                    fee=0.0,
                    tax=tax_value,
                    memo=memo,
                )
            )
        )

    return prepared, errors, duplicates


@st.cache_data(show_spinner=False, ttl=300)
def _load_google_sheet(url: str) -> pd.DataFrame:
    """구글 스프레드시트 링크를 바탕으로 데이터를 CSV 형태로 읽어온다."""
    import urllib.parse
    
    match = re.search(r'/d/([a-zA-Z0-9-_]+)', url)
    if not match:
        raise ValueError("올바른 구글 스프레드시트 링크가 아닙니다. 링크에 고유 ID(/d/...)가 포함되어 있는지 확인하세요.")
    
    file_id = match.group(1)
    
    # URL에서 gid를 추출. 없으면 0(첫 번째 시트)
    gid_match = re.search(r'[#&?]gid=([0-9]+)', url)
    gid = gid_match.group(1) if gid_match else "0"
    
    export_url = f"https://docs.google.com/spreadsheets/d/{file_id}/export?format=csv&gid={gid}"
        
    df = pd.read_csv(export_url, dtype=str, keep_default_na=False)
    return _clean_dataframe_strings(df)


def _execute_upload(prepared_rows: List[PreparedTransaction], auto_cash: bool) -> Tuple[int, List[str]]:
    """검증이 끝난 거래를 실제로 insert한다."""
    created_assets: List[str] = []
    success_count = 0

    for prepared in prepared_rows:
        req = prepared.request

        if prepared.created_asset_payload:
            # ✅ 신규 자산을 먼저 생성하고 asset_id를 갱신한다.
            created = AssetService.create_asset_minimal(**prepared.created_asset_payload)
            req = CreateTransactionRequest(
                account_id=req.account_id,
                asset_id=int(created["id"]),
                transaction_date=req.transaction_date,
                trade_type=req.trade_type,
                quantity=req.quantity,
                price=req.price,
                fee=req.fee,
                tax=req.tax,
                memo=req.memo,
            )
            created_assets.append(created["ticker"])

        TransactionService.create_transaction_and_rebuild(req, auto_cash=auto_cash)
        success_count += 1

    return success_count, created_assets


def render_transaction_importer(user_id: str) -> None:
    st.title("📥 거래내역 업로더(from HTS)")
    st.caption("CSV/XLSX 업로드로 매매 내역 또는 배당금 내역을 일괄 등록합니다.")

    import_type = st.radio(
        "업로드 유형 선택",
        ["매매 내역", "배당금 내역"],
        horizontal=True,
    )

    with st.expander("📌 필수 필드 & 예시 보기", expanded=True):
        if import_type == "매매 내역":
            _render_required_fields_table([
                {"필드": "계좌명", "예시": "키움증권_홍길동_위탁"},
                {"필드": "거래일", "예시": "2024-12-31"},
                {"필드": "티커", "예시": "AAPL / 005930"},
                {"필드": "거래타입", "예시": "매수 / 매도 / BUY / SELL"},
                {"필드": "수량", "예시": "10"},
                {"필드": "단가", "예시": "150.5"},
                {"필드": "(선택) 수수료", "예시": "1.25"},
                {"필드": "(선택) 세금", "예시": "0.75"},
                {"필드": "(선택) 메모", "예시": "해외주식 매수"},
                {"필드": "(선택) 종목명", "예시": "Apple Inc"},
                {"필드": "(선택) 통화", "예시": "USD / KRW"},
                {"필드": "(선택) 시장", "예시": "korea / usa"},
                {"필드": "(선택) 자산유형", "예시": "stock"},
            ])
        else:  # 배당 예시
            _render_required_fields_table([
                {"필드": "계좌명", "예시": "키움증권_홍길동_위탁"},
                {"필드": "지급일자", "예시": "2024-12-31"},
                {"필드": "티커", "예시": "AAPL / 005930"},
                {"필드": "시장구분", "예시": "korea / usa"},
                {"필드": "통화", "예시": "USD / KRW"},
                {"필드": "배당금(세후)", "예시": "85.5"},
                {"필드": "배당금(세전)", "예시": "100.0"},
                {"필드": "(선택) 종목명", "예시": "Apple Inc"},
            ])

    with st.expander("📌 등록된 계좌 확인", expanded=False):
        _render_account_reference_table(user_id)

    with st.expander("📌 계좌별 최근 거래일", expanded=False):
        latest_df = _get_latest_transaction_dates(user_id)
        if latest_df.empty:
            st.info("최근 거래일 정보를 불러올 수 없습니다.")
        else:
            st.dataframe(latest_df, width='stretch')

    upload_method = st.radio("데이터 입력 방식", ["클립보드 붙여넣기", "구글 스프레드시트 링크 연동", "파일 업로드"], horizontal=True)
    
    raw_df = None
    if upload_method == "파일 업로드":
        uploaded_file = st.file_uploader("CSV 또는 XLSX 파일 업로드", type=["csv", "xlsx", "xls"])
        if uploaded_file:
            raw_df = _read_uploaded_file(uploaded_file)
    elif upload_method == "클립보드 붙여넣기":
        st.info("💡 엑셀이나 구글 스프레드시트에서 데이터 전체(헤더 포함)를 드래그해서 복사(`Ctrl+C`)한 뒤 아래 칸에 붙여넣기(`Ctrl+V`) 하세요.")
        pasted_data = st.text_area("데이터 붙여넣기", height=200, placeholder="여기에 데이터를 붙여넣으세요...")
        if pasted_data:
            with st.spinner("데이터를 분석하는 중입니다..."):
                raw_df = _read_pasted_text(pasted_data)
    else:
        st.info("💡 스프레드시트의 공유 권한을 반드시 **'링크가 있는 모든 사용자'**가 볼 수 있도록 설정해야 합니다. (비공개 시 빈 화면이 로드됩니다.)")
        sheet_url = st.text_input(
            "구글 스프레드시트 링크",
            help="브라우저 주소창의 링크를 그대로 복사해 붙여넣으세요. (가져올 시트를 띄워둔 상태에서 복사하면 해당 시트만 정확히 가져옵니다.)",
            placeholder="예: https://docs.google.com/spreadsheets/d/.../edit?gid=12345#gid=12345"
        )
        
        if sheet_url:
            try:
                with st.spinner("데이터를 불러오는 중입니다..."):
                    raw_df = _load_google_sheet(sheet_url)
            except Exception as exc:
                st.error(f"데이터를 불러오는 중 오류가 발생했습니다: {exc}")

    if raw_df is None:
        return

    if raw_df.empty:
        st.error("불러온 데이터에 내용이 없습니다.")
        return

    if import_type == "매매 내역":
        aliases = {
            "account_name": ["계좌", "계좌명", "account", "account_name"],
            "transaction_date": ["주문일자", "체결일", "거래일", "매매일자", "transaction_date"],
            "ticker": ["티커", "종목코드", "ticker", "단축코드", "Ticker"],
            "trade_type": ["거래타입", "매수/매도", "구분", "trade_type"],
            "quantity": ["수량", "거래수량", "quantity", "결제수량"],
            "price": ["단가", "체결가", "price"],
            "fee": ["수수료", "fee", "매매수수료"],
            "tax": ["세금", "tax"],
            "memo": ["메모", "memo"],
            "asset_name": ["종목명", "자산명", "상품명", "asset_name"],
            "currency": ["통화", "currency"],
            "market": ["시장", "시장구분", "market"],
            "asset_type": ["자산유형", "asset_type"],
        }
    else:   # 배당 업로드
        aliases = {
            "account_name": ["계좌명", "account", "account_name"],
            "transaction_date": ["지급일자", "거래일", "transaction_date"],
            "ticker": ["티커", "종목코드", "ticker"],
            "asset_name": ["종목명", "자산명", "asset_name"],
            "market": ["시장구분", "시장", "market"],
            "currency": ["통화", "currency"],
            "dividend_net": ["배당금세후", "배당금(세후)", "dividend_net"],
            "dividend_gross": ["배당금세전", "배당금(세전)", "dividend_gross"],
        }

    mapped_df, missing = _map_columns(raw_df, aliases)
    required_fields = [
        "account_name",
        "transaction_date",
        "ticker",
    ]
    if import_type == "매매 내역":
        required_fields += ["trade_type", "quantity", "price"]
    else:
        required_fields += ["asset_name", "market", "currency", "dividend_net", "dividend_gross"]

    missing_required = [field for field in required_fields if field in missing]
    if missing_required:
        st.error(f"필수 필드가 누락되었습니다: {', '.join(missing_required)}")
        return

    st.markdown("### ✅ 업로드 데이터 미리보기")
    st.dataframe(mapped_df, width='stretch')

    auto_cash = False
    if import_type == "매매 내역":
        auto_cash = st.checkbox("BUY/SELL 시 CASH 자동 반영", value=True)

    if import_type == "매매 내역":
        prepared, errors, duplicates = _prepare_trade_rows(mapped_df, user_id)
    else:
        prepared, errors, duplicates = _prepare_dividend_rows(mapped_df, user_id)

    # ❌ 포맷/유효성 오류가 있으면 전체 업로드 차단
    if errors:
        st.error("업로드 오류가 발견되어 전체 업로드가 취소되었습니다.")
        st.dataframe(pd.DataFrame({"오류 내용": errors}))
        return

    # ⚠️ DB 중복 거래는 경고로 표시하되 업로드는 계속 진행
    if duplicates:
        st.warning(
            f"⚠️ {len(duplicates)}건의 중복 거래가 발견되었습니다. "
            "해당 거래는 건너뛰고 나머지 거래는 정상 업로드됩니다."
        )
        st.dataframe(pd.DataFrame({"중복 거래 (스킵됨)": duplicates}))

    if not prepared:
        st.info("업로드할 신규 거래가 없습니다. (모든 거래가 중복이거나 오류입니다.)")
        return

    st.success(f"✅ {len(prepared)}건의 신규 거래가 검증되었습니다. 업로드를 진행할 수 있습니다.")

    if st.button("업로드 실행"):
        try:
            inserted_count, created_assets = _execute_upload(prepared, auto_cash)
        except Exception as exc:
            st.error(f"업로드 중 오류가 발생했습니다: {exc}")
            return

        st.success(f"총 {inserted_count}건이 등록되었습니다.")
        if created_assets:
            unique_assets = sorted(set(created_assets))
            st.warning(
                "다음 자산을 신규 등록하고 거래내역을 입력했습니다. "
                "가격 업데이트를 위해 price_source를 업데이트해 주세요: "
                + ", ".join(unique_assets)
            )
        st.info("업로드 완료 후 거래 내역 및 스냅샷을 확인해 주세요.")
