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
        .select("id")
        .eq("account_id", account_id)
        .eq("asset_id", asset_id)
        .eq("transaction_date", transaction_date.isoformat())
        .eq("trade_type", trade_type)
        .eq("quantity", quantity)
        .eq("price", price)
        # .eq("tax", tax)   // 세금은 필터링 조건에서 제외
        .limit(1)
        .execute()
    )
    return bool(resp.data)


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


def _read_uploaded_file(uploaded_file) -> Optional[pd.DataFrame]:
    if uploaded_file is None:
        return None
    file_name = uploaded_file.name.lower()
    if file_name.endswith(".csv"):
        return pd.read_csv(uploaded_file)
    if file_name.endswith(".xlsx") or file_name.endswith(".xls"):
        return pd.read_excel(uploaded_file)
    st.error("지원하지 않는 파일 형식입니다. CSV 또는 XLSX를 업로드하세요.")
    return None


def _prepare_trade_rows(df: pd.DataFrame, user_id: str) -> Tuple[List[PreparedTransaction], List[str]]:
    errors: List[str] = []
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
        if not ticker:
            errors.append(f"{row_number}행: 티커가 비어 있습니다.")
            continue

        trade_type = _normalize_trade_type(row.get("trade_type"))
        if not trade_type:
            errors.append(f"{row_number}행: 거래 타입이 매수/매도/BUY/SELL 중 하나여야 합니다.")
            continue

        quantity = pd.to_numeric(row.get("quantity"), errors="coerce")
        price = pd.to_numeric(row.get("price"), errors="coerce")
        fee = pd.to_numeric(row.get("fee"), errors="coerce")
        tax = pd.to_numeric(row.get("tax"), errors="coerce")

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

        # ✅ 파일 내부 중복 체크
        dedupe_key = (account_id, ticker, tx_date.date().isoformat(), trade_type, float(quantity), float(price))
        if dedupe_key in seen_keys:
            errors.append(f"{row_number}행: 업로드 파일 내 중복 거래가 있습니다.")
            continue
        seen_keys.add(dedupe_key)

        # ✅ 기존 DB 중복 체크 (자산이 이미 있는 경우에만)
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
                errors.append(f"{row_number}행: 동일한 거래가 이미 등록되어 있습니다.")
                continue

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

    return prepared, errors


def _prepare_dividend_rows(df: pd.DataFrame, user_id: str) -> Tuple[List[PreparedTransaction], List[str]]:
    errors: List[str] = []
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

        gross = pd.to_numeric(row.get("dividend_gross"), errors="coerce")
        net = pd.to_numeric(row.get("dividend_net"), errors="coerce")
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

        dedupe_key = (account_id, cash_asset_id, payout_date.date().isoformat(), float(net), tax_value)
        if dedupe_key in seen_keys:
            errors.append(f"{row_number}행: 업로드 파일 내 중복 배당금이 있습니다.")
            continue
        seen_keys.add(dedupe_key)

        if _find_existing_duplicate(
            account_id=account_id,
            asset_id=cash_asset_id,
            transaction_date=payout_date.date(),
            trade_type="DEPOSIT",
            quantity=float(net),
            price=1.0,
            tax=tax_value,
        ):
            errors.append(f"{row_number}행: 동일한 배당금 입금 거래가 이미 등록되어 있습니다.")
            continue

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

    return prepared, errors


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
    st.title("📥 Transaction Importer")
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
        else:
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

    uploaded_file = st.file_uploader("CSV 또는 XLSX 파일 업로드", type=["csv", "xlsx", "xls"])
    if not uploaded_file:
        return

    raw_df = _read_uploaded_file(uploaded_file)
    if raw_df is None or raw_df.empty:
        st.error("업로드한 파일에 데이터가 없습니다.")
        return

    if import_type == "매매 내역":
        aliases = {
            "account_name": ["계좌명", "account", "account_name"],
            "transaction_date": ["거래일", "체결일", "매매일자", "transaction_date"],
            "ticker": ["티커", "종목코드", "ticker"],
            "trade_type": ["거래타입", "매수/매도", "구분", "trade_type"],
            "quantity": ["수량", "거래수량", "quantity"],
            "price": ["단가", "체결가", "price"],
            "fee": ["수수료", "fee"],
            "tax": ["세금", "tax"],
            "memo": ["메모", "memo"],
            "asset_name": ["종목명", "자산명", "asset_name"],
            "currency": ["통화", "currency"],
            "market": ["시장", "시장구분", "market"],
            "asset_type": ["자산유형", "asset_type"],
        }
    else:
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
    st.dataframe(mapped_df.head(10), width='stretch')

    auto_cash = False
    if import_type == "매매 내역":
        auto_cash = st.checkbox("BUY/SELL 시 CASH 자동 반영", value=True)

    if import_type == "매매 내역":
        prepared, errors = _prepare_trade_rows(mapped_df, user_id)
    else:
        prepared, errors = _prepare_dividend_rows(mapped_df, user_id)

    if errors:
        st.error("업로드 오류가 발견되어 전체 업로드가 취소되었습니다.")
        st.dataframe(pd.DataFrame({"오류": errors}))
        return

    st.success("✅ 모든 행이 검증되었습니다. 업로드를 진행할 수 있습니다.")

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
