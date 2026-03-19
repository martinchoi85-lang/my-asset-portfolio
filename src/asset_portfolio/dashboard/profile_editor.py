import streamlit as st
import pandas as pd
import json
from asset_portfolio.backend.infra import query

def _safe_json_dumps(obj):
    try:
        if isinstance(obj, str):
            return json.dumps(json.loads(obj), ensure_ascii=False, indent=2)
        return json.dumps(obj, ensure_ascii=False, indent=2)
    except Exception:
        return "{}"

def render_profile_editor(user_id: str):
    st.subheader("📑 HTS 템플릿 관리 (Import Profile)")
    st.caption("거래 내역 업로드 시 증권사별 엑셀 파일 컬럼 구조를 파싱하기 위한 매핑 규칙을 관리합니다.")

    # 1. 기존 프로필 목록 조회
    profiles = query.get_import_profiles(user_id)
    
    if profiles:
        st.markdown("##### 📌 등록된 템플릿 목록")
        df = pd.DataFrame(profiles)
        display_df = df[["name", "display_name", "default_currency", "default_market", "active"]].copy()
        st.dataframe(display_df, use_container_width=True, hide_index=True)
    else:
        st.info("등록된 템플릿이 없습니다. 신규 템플릿을 생성해주세요.")

    st.divider()

    # 2. 편집 선택기
    profile_names = ["(신규 템플릿 생성)"] + [p["name"] for p in profiles]
    selected_name = st.selectbox("편집할 템플릿 선택", profile_names)

    if selected_name == "(신규 템플릿 생성)":
        current_profile = {
            "name": "", 
            "display_name": "", 
            "column_map": '{\n  "종목명": "asset_name",\n  "체결단가": "price",\n  "체결수량": "quantity",\n  "매매구분": "trade_type",\n  "거래일자": "transaction_date"\n}',
            "trade_type_map": '{\n  "매수": "BUY",\n  "현금매수": "BUY",\n  "매도": "SELL",\n  "현금매도": "SELL"\n}', 
            "numeric_columns": '[\n  "quantity",\n  "price",\n  "fee",\n  "tax"\n]',
            "preprocess_func_name": "", 
            "default_currency": "KRW", 
            "default_market": "korea", 
            "active": True
        }
    else:
        profile_data = next((p for p in profiles if p["name"] == selected_name), None)
        current_profile = dict(profile_data) if profile_data else {}
        current_profile["column_map"] = _safe_json_dumps(current_profile.get("column_map", {}))
        current_profile["trade_type_map"] = _safe_json_dumps(current_profile.get("trade_type_map", {}))
        current_profile["numeric_columns"] = _safe_json_dumps(current_profile.get("numeric_columns", []))

    st.markdown(f"##### {'✨ 신규 템플릿 작성' if selected_name == '(신규 템플릿 생성)' else '✏️ 템플릿 편집'}")
    
    with st.form("profile_editor_form"):
        col1, col2 = st.columns(2)
        with col1:
            p_name = st.text_input(
                "Name (임의의 고유 식별자 영어 권장) *", 
                value=current_profile.get("name", ""),
                placeholder="예: kiwoom_domestic",
                help="시스템 내부식별자로 사용됩니다. 공백 없이 소문자/언더바(_) 조합을 권장합니다."
            )
            p_display = st.text_input(
                "Display Name (화면에 보일 이름) *", 
                value=current_profile.get("display_name", ""),
                placeholder="예: 키움증권 (국내 주식)",
                help="업로드 화면의 드롭다운 목록에 표시될 이름입니다."
            )
            p_curr = st.text_input(
                "Default Currency (KRW, USD 등)", 
                value=current_profile.get("default_currency", ""),
                placeholder="KRW",
                help="파일에 통화 정보가 없을 때 기본값으로 사용됩니다."
            )
            p_market = st.text_input(
                "Default Market (korea, usa 등)", 
                value=current_profile.get("default_market", ""),
                placeholder="korea",
                help="파일에 시장 정보가 없을 때 기본값으로 사용됩니다."
            )
            p_active = st.checkbox("목록에 활성화 (Active)", value=current_profile.get("active", True))
            p_func = st.text_input(
                "특수 전처리 함수명 (선택)", 
                value=current_profile.get("preprocess_func_name", ""),
                placeholder="예: kiwoom_2row_preprocess",
                help="복잡한 2줄 병합 등 특수한 전처리가 필요한 경우 미리 정의된 함수명을 입력합니다."
            )
            
        with col2:
            st.markdown("**설정 매핑 가이드** (엄격한 JSON 형식 준수 요망)")
            p_col_map = st.text_area(
                "컬럼 매핑 (원본컬럼명 -> 표준컬럼명)", 
                value=current_profile.get("column_map", ""), 
                height=150,
                help='엑셀의 헤더명(Key)을 시스템 표준 필드명(Value: asset_name, price, quantity, trade_type, transaction_date 등)으로 연결합니다.'
            )
            p_trade_map = st.text_area(
                "거래유형 매핑 (원본텍스트 -> BUY/SELL/등)", 
                value=current_profile.get("trade_type_map", ""), 
                height=110,
                help='엑셀에 적힌 유형(예: "현금매수")을 시스템 표준(BUY, SELL, DEPOSIT, WITHDRAW)으로 연결합니다.'
            )
            p_num_cols = st.text_area(
                "숫자형 클렌징 대상 컬럼 목록 (배열)", 
                value=current_profile.get("numeric_columns", ""), 
                height=80,
                help='쉼표나 특수문자가 포함된 숫자 데이터에서 문자를 제거하고 숫자로 변환할 필드 목록입니다.'
            )

        submitted = st.form_submit_button("템플릿 저장", use_container_width=True)
        
        if submitted:
            if not p_name or not p_display:
                st.error("Name과 Display Name은 필수 입력 항목입니다.")
            else:
                try:
                    col_map_json = json.loads(p_col_map)
                    trade_map_json = json.loads(p_trade_map)
                    num_cols_json = json.loads(p_num_cols)
                    
                    data_to_save = {
                        "user_id": user_id,
                        "name": p_name.strip(),
                        "display_name": p_display.strip(),
                        "column_map": col_map_json,
                        "trade_type_map": trade_map_json,
                        "numeric_columns": num_cols_json,
                        "preprocess_func_name": p_func.strip() if p_func.strip() else None,
                        "default_currency": p_curr.strip() if p_curr.strip() else None,
                        "default_market": p_market.strip() if p_market.strip() else None,
                        "active": p_active
                    }
                    
                    if selected_name != "(신규 템플릿 생성)" and "id" in current_profile:
                        # Existing ID handling
                        data_to_save["id"] = current_profile.get("id")
                        
                    result = query.upsert_import_profile(data_to_save)
                    if result:
                        st.success(f"'{p_name}' 템플릿이 성공적으로 저장되었습니다!")
                        st.rerun()
                    else:
                        st.error("저장에 실패했습니다. DB 연결을 확인해주세요.")
                except json.JSONDecodeError as e:
                    st.error(f"JSON 파싱 실패 (따옴표나 콤마를 확인하세요): {e}")
