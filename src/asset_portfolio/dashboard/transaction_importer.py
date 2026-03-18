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
from asset_portfolio.backend.services.importer.engine import TransactionParser
from asset_portfolio.backend.services.importer.profiles import AVAILABLE_PROFILES
from asset_portfolio.backend.services.asset_alias_service import AssetAliasService


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
    st.caption("CSV/XLSX/클립보드로 매매 내역을 일괄 등록합니다.")

    # [1단계] 증권사/HTS 양식 선택
    st.markdown("### 1️⃣ 데이터 양식 선택 (필수)")
    selected_idx = st.selectbox(
        "사용할 증권사 HTS 화면(템플릿)을 정확히 선택해주세요",
        range(len(AVAILABLE_PROFILES)),
        format_func=lambda i: AVAILABLE_PROFILES[i].display_name
    )
    profile = AVAILABLE_PROFILES[selected_idx]

    with st.expander("📌 선택된 템플릿의 필수 컬럼 확인", expanded=False):
        st.write(f"아래 컬럼명들이 업로드할 데이터에 포함되어 있어야 합니다.")
        st.write(list(profile.column_map.keys()))

    with st.expander("📌 등록된 계좌 및 최근 거래일 힌트", expanded=False):
        _render_account_reference_table(user_id)
        st.divider()
        latest_df = _get_latest_transaction_dates(user_id)
        if not latest_df.empty:
            st.dataframe(latest_df, width='stretch')

    # [2단계] 데이터 입력
    st.markdown("### 2️⃣ 데이터 붙여넣기 / 업로드")
    upload_method = st.radio("데이터 입력 방식", ["클립보드 붙여넣기", "파일 업로드"], horizontal=True)
    
    raw_df = None
    if upload_method == "파일 업로드":
        uploaded_file = st.file_uploader("CSV 또는 XLSX 파일 업로드", type=["csv", "xlsx", "xls"])
        if uploaded_file:
            raw_df = _read_uploaded_file(uploaded_file)
    elif upload_method == "클립보드 붙여넣기":
        pasted_data = st.text_area("엑셀/HTS 데이터 붙여넣기 (Ctrl+V)", height=150, placeholder="헤더 행부터 전체를 복사해서 붙여넣어주세요...")
        if pasted_data:
            with st.spinner("데이터 정리 중..."):
                raw_df = _read_pasted_text(pasted_data)

    if raw_df is None or raw_df.empty:
        return

    st.markdown("### ✅ 파싱 결과 미리보기")
    
    # [3단계] 파서 엔진 실행
    with st.spinner("프로파일 규칙 단위로 파싱 중..."):
        parser = TransactionParser(user_id, profile)
        parsed_results = parser.parse(raw_df)
    
    # 결과 분류
    ready_items = []
    pending_items = []
    
    for item in parsed_results:
        if item.status == "READY":
            ready_items.append(item)
        else:
            pending_items.append(item)
            
    # 에러 및 추가 작업(Alias 등록) 표시
    if pending_items:
        st.error(f"⚠️ {len(pending_items)}건의 거래에 매칭되지 않은 [종목명/Ticker]가 존재합니다.")
        st.warning("아래에서 수동으로 자산(Ticker) 대상을 매핑해주시면 Alias 사전에 등록되어 현재 작업 및 향후 자동 인식됩니다.")
        
        assets_df = _load_assets_df()
        asset_options = assets_df['ticker'] + " - " + assets_df['name'] if not assets_df.empty else pd.Series()
        
        need_alias_set = set(item.standard_data.get('asset_name', '') for item in pending_items)
        alias_saved = False
        
        with st.form("alias_registry_form"):
            new_aliases = {}
            for missing_name in need_alias_set:
                col1, col2 = st.columns([1, 2])
                with col1:
                    st.write(f"👉 **{missing_name}**")
                with col2:
                    matched_idx = st.selectbox(f"[{missing_name}] 과 매핑될 실제 자산", options=range(len(asset_options)), format_func=lambda i: asset_options.iloc[i], key=f"alias_{missing_name}")
                    new_aliases[missing_name] = int(assets_df.iloc[matched_idx]['id'])
                    
            if st.form_submit_button("선택한 항목 Alias로 저장 (새로고침)"):
                for m_name, a_id in new_aliases.items():
                    AssetAliasService.add_alias(user_id, m_name, a_id)
                st.success("Alias 사전 업데이트 완료! 데이터 파싱이 재시작됩니다.")
                st.rerun()
                
        # 미매칭 항목이 있으면 업로드 중단
        return

    # 모두 READY 인 경우
    # 렌더링용 DF 생성
    display_rows = []
    for p in ready_items:
        d = p.standard_data
        display_rows.append({
            "거래일자": d.get("transaction_date", ""),
            "종목": d.get("asset_name") or d.get("ticker", ""),
            "매매": d.get("trade_type", ""),
            "수량": d.get("quantity", 0),
            "단가": d.get("price", 0),
            "수수료/세금": f"{d.get('fee',0)} / {d.get('tax',0)}",
        })
    st.dataframe(pd.DataFrame(display_rows), width='stretch')
    
    # (선택) 계좌 ID 매핑
    st.markdown("### 4️⃣ 계좌 연결 및 전송")
    accounts_df = _load_accounts_df(user_id)
    if accounts_df.empty:
        st.error("계좌가 없습니다.")
        return
        
    target_account_idx = st.selectbox(
        "이 거래내역들을 어느 계좌로 밀어넣을까요?", 
        options=range(len(accounts_df)), 
        format_func=lambda i: accounts_df.iloc[i]['name']
    )
    selected_account_id = str(accounts_df.iloc[target_account_idx]['id'])
    auto_cash = st.checkbox("BUY/SELL 시 CASH 자동 증감 반영", value=True)

    if st.button("🚀 최종 업로드 실행", type="primary"):
        success_count = 0
        with st.spinner("DB 저장 중..."):
            for p in ready_items:
                req = CreateTransactionRequest(
                    account_id=selected_account_id,
                    asset_id=p.asset_id,
                    transaction_date=pd.to_datetime(p.standard_data.get("transaction_date")).date(),
                    trade_type=p.standard_data.get("trade_type", "UNKNOWN"),
                    quantity=float(p.standard_data.get("quantity", 0)),
                    price=float(p.standard_data.get("price", 0)),
                    fee=float(p.standard_data.get("fee", 0)),
                    tax=float(p.standard_data.get("tax", 0)),
                    memo=p.standard_data.get("memo", f"Import via {profile.name}"),
                )
                try:
                    TransactionService.create_transaction_and_rebuild(req, auto_cash=auto_cash)
                    success_count += 1
                except Exception as e:
                    st.toast(f"저장 실패: {e}")
            
        st.success(f"🎉 총 {success_count} 건 업로드 성공!")
