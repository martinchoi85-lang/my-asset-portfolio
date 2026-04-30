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

def _render_suggestion_selectbox(label: str, column_name: str, assets_df: pd.DataFrame, current_value: str) -> str:
    """
    ✅ DB의 기존 값들을 드롭다운으로 보여주고, 필요시 직접 입력할 수 있게 합니다.
    """
    # 1. 기존 DB 값 추출 (Unique Set)
    options = sorted([str(x) for x in assets_df[column_name].dropna().unique().tolist() if str(x).strip()])
    
    # 2. 현재 값이 옵션에 없으면 추가
    if current_value and current_value not in options:
        options.append(current_value)
        options = sorted(list(set(options)))

    final_options = options + ["[직접 입력]"]
    
    # 3. 기본 인덱스 설정
    default_index = 0
    if current_value in options:
        default_index = options.index(current_value)
    
    # 4. UI 렌더링
    choice = st.selectbox(f"{label}", final_options, index=default_index, key=f"sel_{column_name}")
    
    if choice == "[직접 입력]":
        val = st.text_input(f"↳ {label} 직접 입력", value=current_value if current_value not in options else "", key=f"txt_{column_name}")
    else:
        val = choice
        
    return val

def render_asset_editor():
    st.title("🧩 Asset Editor (V1)")

    # ✅ 작업 모드 선택
    mode = st.radio("작업 선택", ["기존 자산 수정", "신규 자산 등록"], horizontal=True)
    is_edit_mode = mode == "기존 자산 수정"

    assets_df = _load_assets_df()
    
    asset_id = None
    row = {}

    if is_edit_mode:
        if assets_df.empty:
            st.info("등록된 자산이 없습니다.")
            return
        
        # ✅ 보기 좋은 정렬
        assets_df = assets_df.sort_values(["market", "asset_type", "underlying_asset_class", "ticker"])

        # ✅ 자산 선택
        selected_label = st.selectbox("자산 선택", assets_df["label"].tolist())
        row = assets_df.loc[assets_df["label"] == selected_label].iloc[0]
        asset_id = int(row["id"])
        st.subheader("✏️ 자산 정보 수정")
    else:
        st.subheader("✨ 신규 자산 등록")
        # 신규 등록을 위한 초기값
        row = {
            "ticker": "",
            "name_kr": "",
            "market": "korea",
            "asset_type": "etf",
            "currency": "krw",
            "underlying_asset_class": "equity",
            "economic_exposure_region": "korea",
            "vehicle_type": "stock",
            "current_price": 0.0,
            "lookthrough_available": False,
            "price_source": "manual"
        }

    # ✅ 핵심 필드들 노출
    ticker = st.text_input("티커", value=str(row.get("ticker", "")), disabled=is_edit_mode, help="신규 등록 시에만 수정 가능합니다.")
    name_kr = st.text_input("자산명(한글)", value=str(row.get("name_kr", "")))

    # ✅ 메타데이터 필드들 (드롭다운 적용)
    col1, col2 = st.columns(2)
    with col1:
        asset_type = _render_suggestion_selectbox("자산 유형 (asset_type)", "asset_type", assets_df, str(row.get("asset_type") or ""))
        currency = _render_suggestion_selectbox("통화 (currency)", "currency", assets_df, str(row.get("currency") or "krw"))
    with col2:
        market_options = ["korea", "us", "etc"]
        market_value = str(row.get("market") or "etc").lower().strip()
        market = st.selectbox(
            "시장 (market)",
            market_options,
            index=market_options.index(market_value if market_value in market_options else "etc"),
        )
        vehicle_type = _render_suggestion_selectbox("상품 형태 (vehicle_type)", "vehicle_type", assets_df, str(row.get("vehicle_type") or ""))

    st.divider()
    st.caption("🔍 세부 분류 및 전략 설정")
    
    c1, c2, c3 = st.columns(3)
    with c1:
        underlying_asset_class = _render_suggestion_selectbox("자산군 (underlying_asset_class)", "underlying_asset_class", assets_df, str(row.get("underlying_asset_class") or ""))
        economic_exposure_region = _render_suggestion_selectbox("노출 지역 (economic_exposure_region)", "economic_exposure_region", assets_df, str(row.get("economic_exposure_region") or ""))
        asset_nature = _render_suggestion_selectbox("자산 성격 (asset_nature)", "asset_nature", assets_df, str(row.get("asset_nature") or ""))
    with c2:
        fx_exposure_type = _render_suggestion_selectbox("환노출 타입 (fx_exposure_type)", "fx_exposure_type", assets_df, str(row.get("fx_exposure_type") or ""))
        return_driver = _render_suggestion_selectbox("수익 동인 (return_driver)", "return_driver", assets_df, str(row.get("return_driver") or ""))
    with c3:
        strategy_type = _render_suggestion_selectbox("전략 유형 (strategy_type)", "strategy_type", assets_df, str(row.get("strategy_type") or ""))
        lookthrough_available = st.checkbox("룩스루 가능", value=bool(row.get("lookthrough_available") or False))

    st.divider()
    current_price = st.number_input("현재가 (current_price)", min_value=0.0, value=float(row.get("current_price") or 0.0))

    st.divider()
    st.subheader("💡 가격 소스 설정")

    # ✅ price_source는 정책 컬럼이므로 사용자가 직접 선택하도록 노출
    # - manual: 스냅샷 에디터에서 수동 입력
    # - yfinance: 기존 자동 가격 업데이트
    # - krx: KRX 자동 가격 업데이트(이번 추가 기능)
    current_price_source = str(row.get("price_source") or "manual").lower().strip()
    price_source_options = ["manual", "yfinance", "krx"]
    price_source = st.selectbox(
        "price_source",
        price_source_options,
        index=price_source_options.index(current_price_source if current_price_source in price_source_options else "manual"),
        help="'manual'은 예적금/펀드 등 '총액' 기반, 'yfinance/krx'는 자동 가격입니다.",
    )

    # ✅ KRX 소스 설정 입력 UI
    krx_source = _load_asset_price_source(asset_id) if asset_id else {}
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
        button_label = "자산 정보 수정" if is_edit_mode else "신규 자산 등록"
        save = st.button(button_label, type="primary")
    with col2:
        st.button("새로고침", on_click=lambda: st.rerun())

    if save:
        try:
            with st.spinner("자산 정보를 저장 중..."):
                payload = {
                    "name_kr": name_kr,
                    "market": market,
                    "asset_type": asset_type,
                    "currency": currency.lower() if currency in ("krw","usd") else currency,
                    "underlying_asset_class": underlying_asset_class,
                    "economic_exposure_region": economic_exposure_region,
                    "vehicle_type": vehicle_type,
                    "asset_nature": asset_nature,
                    "fx_exposure_type": fx_exposure_type,
                    "return_driver": return_driver,
                    "strategy_type": strategy_type,
                    "current_price": current_price,
                    "lookthrough_available": lookthrough_available,
                    "price_source": price_source,
                }
                # ✅ 빈 값이 들어가지 않도록 최소 방어
                payload = {k: v for k, v in payload.items() if v is not None}

                if is_edit_mode:
                    AssetService.update_asset(asset_id, payload)
                else:
                    # 신규 등록
                    if not ticker:
                        st.error("티커를 입력해주세요.")
                        st.stop()
                    
                    payload["ticker"] = ticker
                    new_asset = AssetService.create_asset_minimal(**payload)
                    asset_id = new_asset["id"]

                # ✅ price_source가 KRX라면 price source 설정을 저장합니다.
                if price_source == "krx":
                    try:
                        query_params = json.loads(krx_query_params_text or "{}")
                    except json.JSONDecodeError:
                        query_params = {"mktId": "ALL"}

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
                    # ✅ KRX 미사용 시 기존 설정이 있다면 비활성화
                    if is_edit_mode and krx_source.get("id"):
                        _upsert_asset_price_source({
                            "asset_id": asset_id,
                            "source_type": "krx",
                            "priority": int(krx_source.get("priority") or 1),
                            "active": False,
                            "source_params": krx_source.get("source_params") or {},
                        })

            st.success(f"성공적으로 {button_label}되었습니다.")
            st.cache_data.clear()
            st.rerun()
        except Exception as e:
            st.error(f"저장 실패: {e}")
