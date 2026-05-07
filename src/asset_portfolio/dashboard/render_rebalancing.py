import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from asset_portfolio.backend.services.rebalancing_service import RebalancingService
from asset_portfolio.dashboard.render import load_asset_grouping_summary

def get_grouping_options_and_maps():
    group_options = {
        "자산 유형 (asset_type)": "asset_type",
        "기초자산 클래스 (underlying_asset_class)": "underlying_asset_class",
        "통화 (currency)": "currency",
        "경제적 노출 지역 (economic_exposure_region)": "economic_exposure_region",
        "자산 성격 (asset_nature)": "asset_nature",
        "투자 상품 유형 (vehicle_type)": "vehicle_type",
        "환율 노출 유형 (fx_exposure_type)": "fx_exposure_type",
        "수익원 (return_driver)": "return_driver",
        "투자 전략 (strategy_type)": "strategy_type"
    }
    
    type_map = {"fund": "펀드", "etf": "ETF", "tdf": "TDF", "cash": "현금(예수금)", "stock": "주식", "deposit": "예적금", "reits": "리츠"}
    class_map = {
        "multi_asset": "멀티에셋", 
        "real_asset": "실물자산", 
        "fixed_income": "채권", 
        "equity": "주식", 
        "cash": "예수금",
        "derivative": "파생상품",
        "Multi-Asset": "멀티에셋", 
        "Real Asset": "실물자산", 
        "Fixed Income": "채권", 
        "Equity": "주식", 
        "Other": "기타"
    }
    region_map = {"korea": "국내", "us": "미국"}
    currency_map = {"krw": "원", "usd": "달러"}
    asset_nature_map = {"physical": "실물자산", "debt": "채권", "equity": "주식", "hybrid": "혼합형", "derivative": "파생상품"}
    vehicle_type_map = {"etf": "ETF", "tdf": "TDF", "fund": "펀드", "stock": "주식", "cash_account": "현금(예수금)", "deposit": "예적금", "reits": "리츠", "mmf": "MMF"}
    fx_exposure_type_map = {"unhedged": "환노출 없음", "natural_hedge": "헤지", "krw_denominated": "원화표시"}
    return_driver_map = {"inflation_hedge": "인플레이션 헤지", "yield": "인컴", "diversification": "분산", "price_appreciation": "시세차익", "alpha": "알파(시장 대비 초과 수익 추구)"}
    strategy_type_map = {"passive_beta": "패시브(시장 추종)", "absolute_return": "절대수익 추구", "thematic": "테마형", "active": "액티브(시장 초과수익 추구)", "factor": "팩터(요인)"}
    
    def map_label(val, group_key):
        s = str(val).strip()
        if group_key == "asset_type": return type_map.get(s.lower(), s)
        elif group_key == "underlying_asset_class": return class_map.get(s, class_map.get(s.lower(), s))
        elif group_key == "economic_exposure_region": return region_map.get(s, s)
        elif group_key == "currency": return currency_map.get(s, s)
        elif group_key == "asset_nature": return asset_nature_map.get(s, s)
        elif group_key == "vehicle_type": return vehicle_type_map.get(s, s)
        elif group_key == "fx_exposure_type": return fx_exposure_type_map.get(s, s)
        elif group_key == "return_driver": return return_driver_map.get(s, s)
        elif group_key == "strategy_type": return strategy_type_map.get(s, s)
        return s

    return group_options, map_label

def render_rebalancing_page(user_id: str, account_id: str):
    st.subheader("⚖️ 리밸런싱 (Rebalancing)")
    st.caption("현재 자산 비중과 목표 비중을 비교하고 리밸런싱 규모를 파악합니다.")
    
    # Look-through 적용 여부 토글 (상태 관리를 위해 session_state 활용)
    if "rebalance_lt_mode" not in st.session_state:
        st.session_state.rebalance_lt_mode = False
        
    st.toggle(
        "룩스루(Look-through) 분석 적용", 
        key="rebalance_lt_mode", 
        help="TDF/펀드 등의 내부 비중을 분해하여 실제 자산 노출도를 계산합니다."
    )
    
    group_options, map_label = get_grouping_options_and_maps()
    
    # 목표 비중 기준 선택
    selected_label = st.selectbox(
        "목표 비중 기준 (Grouping Criteria)",
        list(group_options.keys()),
        help="어떤 기준으로 목표 비중을 설정할지 선택하세요."
    )
    grouping_criteria = group_options[selected_label]
    
    # [Validation] 룩스루 모드 시 카테고리 권장 경고
    if st.session_state.rebalance_lt_mode and grouping_criteria in ["asset_type", "vehicle_type"]:
        st.warning("⚠️ 룩스루 모드에서는 '자산 유형'이나 '투자 상품 유형'보다는 **'기초자산 클래스'** 기준으로 목표를 설정하는 것을 권장합니다.")
    
    # 현재 자산 데이터 로드 (룩스루 토글 상태 반영)
    raw_df = load_asset_grouping_summary(
        user_id=user_id, 
        account_id=account_id,
        apply_lookthrough=st.session_state.rebalance_lt_mode
    )
    if raw_df.empty:
        st.info("표시할 자산 데이터가 없습니다.")
        return
        
    # 현재 데이터에 한글 매핑 적용 (UI 일관성 및 편집 용이성)
    current_df = raw_df.copy()
    current_df[grouping_criteria] = current_df[grouping_criteria].apply(lambda x: map_label(x, grouping_criteria))

    # [Filtering] 룩스루 모드 시 멀티에셋 항목 제외 (이미 세부 항목으로 분해되었으므로)
    if st.session_state.rebalance_lt_mode and grouping_criteria == "underlying_asset_class":
        # '멀티에셋' 매핑된 값과 원본 영어 값을 모두 체크하여 제외
        current_df = current_df[~current_df[grouping_criteria].isin(["멀티에셋", "multi_asset", "Multi-Asset"])]
    
    # DB에서 설정된 목표 비중 로드
    db_target_weights = RebalancingService.get_target_weights(user_id, account_id, grouping_criteria)
    
    # 현재 존재하는 카테고리 목록 확보
    existing_categories = current_df[grouping_criteria].unique().tolist()
    
    # Editor에 넣을 데이터 구성
    editor_data = []
    # DB에 저장된 것 우선
    saved_cats = set()
    for w in db_target_weights:
        # DB에 저장된 영문 키 등을 한글 레이블로 변환하여 표시
        cat = map_label(w["target_category"], grouping_criteria)
        editor_data.append({"분류 항목": cat, "목표 비중 (%)": w["target_weight"]})
        saved_cats.add(cat)
        
    # DB에 없지만 현재 보유중인 카테고리는 0%로 자동 추가
    for cat in existing_categories:
        if cat not in saved_cats:
            editor_data.append({"분류 항목": cat, "목표 비중 (%)": 0.0})
            
    editor_df = pd.DataFrame(editor_data)
    if editor_df.empty:
        editor_df = pd.DataFrame(columns=["분류 항목", "목표 비중 (%)"])

    # UI Layout
    col_input, col_chart = st.columns([1, 2])
    
    with col_input:
        st.markdown("#### 🎯 목표 비중 설정")
        edited_df = st.data_editor(
            editor_df,
            num_rows="dynamic",
            column_config={
                "분류 항목": st.column_config.TextColumn("분류 항목 (Category)"),
                "목표 비중 (%)": st.column_config.NumberColumn("목표 비중 (%)", min_value=0.0, max_value=100.0, format="%.1f%%")
            },
            hide_index=True,
            width='stretch'
        )
        
        total_weight = edited_df["목표 비중 (%)"].sum() if not edited_df.empty else 0.0
        
        if total_weight != 100.0:
            st.warning(f"목표 비중의 합이 100%가 아닙니다. (현재: {total_weight:.1f}%)")
        else:
            st.success("목표 비중의 합이 100%입니다.")
            
        if st.button("목표 비중 저장", type="primary"):
            save_list = [{"target_category": row["분류 항목"], "target_weight": row["목표 비중 (%)"]} for _, row in edited_df.iterrows()]
            success = RebalancingService.save_target_weights_bulk(user_id, account_id, grouping_criteria, save_list)
            if success:
                st.success("저장되었습니다.")
                st.rerun()
            else:
                st.error("저장에 실패했습니다.")
                
    # 갭 계산 및 차트 렌더링
    target_list_for_calc = [{"target_category": row["분류 항목"], "target_weight": row["목표 비중 (%)"]} for _, row in edited_df.iterrows()]
    gap_df = RebalancingService.calculate_rebalancing_gap(current_df, target_list_for_calc, grouping_criteria)
    
    if not gap_df.empty:
        with col_chart:
            st.markdown("#### 📊 비중 비교 (Target vs Actual)")
            
            # Bar 차트 그리기
            fig = go.Figure()
            
            # 현재 비중 막대
            fig.add_trace(go.Bar(
                x=gap_df["target_category"],
                y=gap_df["current_weight"],
                name="현재 비중",
                marker_color="#3b82f6",
                text=gap_df["current_weight"].apply(lambda x: f"{x:.1f}%"),
                textposition="auto"
            ))
            
            # 목표 비중 막대
            fig.add_trace(go.Bar(
                x=gap_df["target_category"],
                y=gap_df["target_weight"],
                name="목표 비중",
                marker_color="#10b981",
                text=gap_df["target_weight"].apply(lambda x: f"{x:.1f}%"),
                textposition="auto"
            ))
            
            fig.update_layout(
                barmode="group",
                height=350,
                margin=dict(t=30, l=10, r=10, b=10),
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                yaxis_title="비중 (%)"
            )
            st.plotly_chart(fig, width='stretch')
            
        st.markdown("---")
        st.markdown("#### 💰 리밸런싱 필요 금액 (Action Table)")
        
        # Action Table 포맷팅
        action_df = gap_df[["target_category", "current_weight", "target_weight", "weight_gap", "current_amount", "target_amount", "amount_gap"]].copy()
        action_df.sort_values("weight_gap", inplace=True)
        
        def action_text(val):
            if val > 0: return f"🟢 {val:,.0f} 매수"
            elif val < 0: return f"🔴 {abs(val):,.0f} 매도"
            return "유지"
            
        action_df["필요 조치(KRW)"] = action_df["amount_gap"].apply(action_text)
        
        view_df = action_df[["target_category", "current_weight", "target_weight", "weight_gap", "current_amount", "필요 조치(KRW)"]].rename(columns={
            "target_category": "분류 항목",
            "current_weight": "현재 비중 (%)",
            "target_weight": "목표 비중 (%)",
            "weight_gap": "비중 차이 (%p)",
            "current_amount": "현재 평가금액 (KRW)"
        })
        
        st.dataframe(
            view_df.style.format({
                "현재 비중 (%)": "{:.1f}",
                "목표 비중 (%)": "{:.1f}",
                "비중 차이 (%p)": "{:+.1f}",
                "현재 평가금액 (KRW)": "{:,.0f}"
            }),
            width='stretch',
            hide_index=True
        )

        # 💡 룰 기반 리밸런싱 가이드 (No-AI)
        st.markdown("---")
        st.markdown("#### 💡 리밸런싱 가이드")
        
        # 허용 오차(tolerance)를 ±1.0%로 설정하여 가이드 생성
        tolerance = 1.0
        overweight = action_df[action_df["weight_gap"] < -tolerance].sort_values("weight_gap").head(3) # 과다 (음수 갭이 큰 순)
        underweight = action_df[action_df["weight_gap"] > tolerance].sort_values("weight_gap", ascending=False).head(3) # 과소 (양수 갭이 큰 순)
        
        if overweight.empty and underweight.empty:
            st.success("🎉 현재 포트폴리오가 목표 비중과 거의 일치합니다! 특별한 리밸런싱 조치가 필요하지 않습니다.")
        else:
            if not overweight.empty:
                for _, row in overweight.iterrows():
                    cat = row["target_category"]
                    gap = abs(row["weight_gap"])
                    amt = abs(row["amount_gap"])
                    st.error(f"🔴 **{cat}** 비중이 목표보다 **{gap:.1f}%p** 높습니다. 약 **{amt:,.0f}원**을 매도하여 비중을 줄이세요.")
            
            if not underweight.empty:
                for _, row in underweight.iterrows():
                    cat = row["target_category"]
                    gap = row["weight_gap"]
                    amt = row["amount_gap"]
                    st.success(f"🟢 **{cat}** 비중이 목표보다 **{gap:.1f}%p** 낮습니다. 약 **{amt:,.0f}원**을 추가 매수하여 비중을 늘리세요.")
                    
            st.caption(f"※ 위 가이드는 허용 오차(±{tolerance}%)를 벗어난 주요 자산군에 대한 우선 조치 사항입니다.")
