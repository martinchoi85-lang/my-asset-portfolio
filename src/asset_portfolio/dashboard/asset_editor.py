import json
import streamlit as st
import pandas as pd

from asset_portfolio.backend.services.asset_service import AssetService
from asset_portfolio.dashboard.transaction_editor import _load_assets_df  # 이미 있다면 재사용
from asset_portfolio.backend.infra.supabase_client import get_supabase_client


def _load_asset_price_source(asset_id: int) -> dict:
    """
    ✅ asset_price_sources에서 특정 자산의 설정을 가져옵니다.
    - 없으면 빈 dict 반환
    """
    supabase = get_supabase_client()
    rows = (
        supabase.table("asset_price_sources")
        .select("id, asset_id, source_type, priority, source_params, active")
        .eq("asset_id", asset_id)
        .order("priority")
        .execute()
        .data or []
    )
    if not rows:
        return {}
    return rows[0]


def _upsert_asset_price_source(payload: dict) -> None:
    """
    ✅ asset_price_sources 업서트
    - asset_id + source_type 조합을 기준으로 덮어쓰기
    """
    supabase = get_supabase_client()
    supabase.table("asset_price_sources").upsert(
        payload,
        on_conflict="asset_id,source_type",
    ).execute()

@st.cache_data(ttl=60)
def _load_latest_holding_asset_ids_global() -> set[int]:
    """
    Load asset_ids held on the latest snapshot date across all accounts.
    """
    supabase = get_supabase_client()
    latest_row = (
        supabase.table("daily_snapshots")
        .select("date")
        .order("date", desc=True)
        .limit(1)
        .execute()
        .data or []
    )
    if not latest_row:
        return set()

    latest_date = latest_row[0]["date"]
    rows = (
        supabase.table("daily_snapshots")
        .select("asset_id, quantity")
        .eq("date", latest_date)
        .gt("quantity", 0)
        .execute()
        .data or []
    )
    return {int(r["asset_id"]) for r in rows if r.get("asset_id") is not None}

def render_asset_editor():
    st.title("🧩 Asset Editor (V1)")

    assets_df = _load_assets_df()
    if assets_df.empty:
        st.info("등록된 자산이 없습니다.")
        return

    # ✅ 보기 좋은 정렬(원하시면 더 정교하게)
    assets_df = assets_df.sort_values(["market", "asset_type", "underlying_asset_class", "ticker"])

    # ✅ 자산 선택
    selected_label = st.selectbox("자산 선택", assets_df["label"].tolist())
    row = assets_df.loc[assets_df["label"] == selected_label].iloc[0]
    asset_id = int(row["id"])

    st.subheader("✏️ 자산 정보 수정")

    # ✅ 핵심 필드들만 V1에서 노출
    ticker = st.text_input("티커", value=str(row["ticker"]), disabled=True)  # 안전하게 비활성
    name_kr = st.text_input("자산명(한글)", value=str(row["name_kr"]))

    market_options = ["korea", "us", "etc"]
    market_value = str(row.get("market") or "etc").lower().strip()
    market = st.selectbox(
        "시장",
        market_options,
        index=market_options.index(market_value if market_value in market_options else "etc"),
    )
    asset_type_options = ["cash", "deposit", "etf", "fund", "tdf"]
    asset_type_value = str(row.get("asset_type") or "etc").lower().strip()
    asset_type = st.selectbox(
        "자산유형",
        asset_type_options,
        index=asset_type_options.index(asset_type_value if asset_type_value in asset_type_options else "cash"),
    )
    currency = st.selectbox("통화", ["krw", "usd"], index=["krw","usd"].index(str(row.get("currency") or "krw").lower()))

    # 분류는 V1에서는 선택 옵션을 최소화
    underlying_asset_class = st.text_input("자산군(underlying_asset_class)", value=str(row.get("underlying_asset_class") or "Unknown"))
    economic_exposure_region = st.text_input("노출 지역(economic_exposure_region)", value=str(row.get("economic_exposure_region") or "Unknown"))
    vehicle_type = st.text_input("상품 형태(vehicle_type)", value=str(row.get("vehicle_type") or "Unknown"))

    current_price = st.number_input("현재가(current_price)", min_value=0.0, value=float(row.get("current_price") or 0.0))

    lookthrough_available = st.checkbox("룩스루 가능(ETF/TDF/Fund의 내부 구성 자산을 분해해서 보는 기능)", value=bool(row.get("lookthrough_available") or False))

    st.divider()
    st.subheader("💡 가격 소스 설정")

    # ✅ price_source는 정책 컬럼이므로 사용자가 직접 선택하도록 노출
    # - manual: 스냅샷 에디터에서 수동 입력
    # - yfinance: 기존 자동 가격 업데이트
    # - krx: KRX 자동 가격 업데이트(이번 추가 기능)
    current_price_source = str(row.get("price_source") or "manual").lower().strip()
    price_source = st.selectbox(
        "price_source",
        ["manual", "yfinance", "krx"],
        index=["manual", "yfinance", "krx"].index(current_price_source if current_price_source in {"manual", "yfinance", "krx"} else "manual"),
        help="manual은 수동평가, yfinance는 자동, krx는 KRX 자동 가격입니다.",
    )

    # ✅ KRX 소스 설정 입력 UI
    krx_source = _load_asset_price_source(asset_id)
    krx_params = krx_source.get("source_params") or {}
    holding_asset_ids = _load_latest_holding_asset_ids_global()

    if price_source == "krx":
        st.caption("KRX 종목은 한국 ETF만 선택 가능합니다. (KRX 미지원 종목은 직접 입력)")
        krx_df = assets_df.copy()
        krx_df["market_norm"] = krx_df["market"].fillna("").str.lower().str.strip()
        krx_df["asset_type_norm"] = krx_df["asset_type"].fillna("").str.lower().str.strip()
        krx_df = krx_df[(krx_df["market_norm"] == "korea") & (krx_df["asset_type_norm"] == "etf")]
        if holding_asset_ids:
            krx_df = krx_df[krx_df["id"].isin(list(holding_asset_ids))].copy()
        if krx_df.empty:
            krx_df = assets_df[(assets_df["market"].fillna("").str.lower().str.strip() == "korea") & (assets_df["asset_type"].fillna("").str.lower().str.strip() == "etf")].copy()

        krx_df["krx_label"] = krx_df.apply(lambda r: f"{r['ticker']} | {r['name_kr']}", axis=1)
        krx_options = krx_df["krx_label"].tolist()
        label_to_code = {lb: lb.split("|")[0].strip() for lb in krx_options}

        default_krx_code = str(krx_params.get("code") or row.get("ticker") or "").strip()
        select_options = ["직접 입력"] + krx_options if krx_options else ["직접 입력"]
        default_index = 0
        if default_krx_code:
            for idx, lb in enumerate(select_options):
                if lb != "직접 입력" and label_to_code.get(lb) == default_krx_code:
                    default_index = idx
                    break

        selected_krx_label = st.selectbox(
            "KRX 종목 선택(6자리)",
            select_options,
            index=default_index,
            help="예: 069500",
        )
        if selected_krx_label == "직접 입력":
            krx_code = st.text_input(
                "KRX 종목코드(6자리) 직접 입력",
                value=default_krx_code,
                help="예: 069500",
            )
        else:
            krx_code = label_to_code.get(selected_krx_label, default_krx_code)
        krx_bld = st.text_input(
            "KRX bld 파라미터",
            value=str(krx_params.get("bld") or "dbms/MDC/STAT/standard/MDCSTAT04301"),
            help="KRX OTP 생성용 bld 문자열",
        )
        krx_code_field = st.text_input(
            "KRX 코드 컬럼명(code_field)",
            value=str(krx_params.get("code_field") or "종목코드"),
        )
        krx_price_field = st.text_input(
            "KRX 가격 컬럼명(price_field)",
            value=str(krx_params.get("price_field") or "종가"),
        )
        krx_date_field = st.text_input(
            "KRX 거래일 컬럼명(date_field)",
            value=str(krx_params.get("date_field") or "trdDd"),
        )
        # ✅ JSON 문자열을 그대로 입력받아 저장합니다.
        # - 초보자도 보기 쉽도록 기본값을 JSON 형태로 보여줍니다.
        krx_query_params_text = st.text_area(
            "KRX 추가 파라미터(query_params, JSON)",
            value=json.dumps(krx_params.get("query_params") or {"mktId": "ALL"}, ensure_ascii=False, indent=2),
            help="예: {\"mktId\": \"ALL\"}",
            height=120,
        )

    st.divider()
    col1, col2 = st.columns([1, 1])
    with col1:
        save = st.button("저장", type="primary")
    with col2:
        st.button("새로고침", on_click=lambda: st.rerun())

    if save:
        try:
            with st.spinner("자산 정보를 저장 중..."):
                updates = {
                    "name_kr": name_kr,
                    "market": market,
                    "asset_type": asset_type,
                    "currency": currency.lower() if currency in ("krw","usd") else currency,
                    "underlying_asset_class": underlying_asset_class,
                    "economic_exposure_region": economic_exposure_region,
                    "vehicle_type": vehicle_type,
                    "current_price": current_price,
                    "lookthrough_available": lookthrough_available,
                    "price_source": price_source,
                }
                # ✅ 빈 값이 들어가지 않도록 최소 방어(원하면 더 강화 가능)
                updates = {k: v for k, v in updates.items() if v is not None}

                AssetService.update_asset(asset_id, updates)

                # ✅ price_source가 KRX라면 price source 설정을 저장합니다.
                if price_source == "krx":
                    # ✅ JSON 문자열 → dict로 변환
                    # - JSON 문법 오류가 나면 기본값으로 대체
                    try:
                        query_params = json.loads(krx_query_params_text or "{}")
                    except json.JSONDecodeError:
                        # ✅ JSON 파싱 실패 시 기본값으로 fallback
                        query_params = {"mktId": "ALL"}

                    # ✅ KRX용 설정을 asset_price_sources에 저장
                    # - asset_id + source_type 조합으로 업서트(있으면 갱신)
                    source_payload = {
                        "asset_id": asset_id,
                        "source_type": "krx",
                        "priority": 1,
                        "active": True,
                        "source_params": {
                            "code": krx_code,
                            "bld": krx_bld,
                            "code_field": krx_code_field,
                            "price_field": krx_price_field,
                            "date_field": krx_date_field,
                            "query_params": query_params,
                        },
                    }
                    _upsert_asset_price_source(source_payload)
                else:
                    # ✅ KRX 미사용 시 비활성화 처리(선택 사항)
                    if krx_source.get("id"):
                        _upsert_asset_price_source({
                            "asset_id": asset_id,
                            "source_type": "krx",
                            "priority": int(krx_source.get("priority") or 1),
                            "active": False,
                            "source_params": krx_source.get("source_params") or {},
                        })

            st.success("저장 완료")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"저장 실패: {e}")
