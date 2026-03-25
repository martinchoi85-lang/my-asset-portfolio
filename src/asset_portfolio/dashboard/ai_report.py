# import streamlit as st
# from asset_portfolio.backend.infra import query
# from asset_portfolio.backend.services.portfolio_service import load_portfolio_daily_snapshots_krw
# from asset_portfolio.backend.services.portfolio_weight_service import build_asset_weight_df

# def render_ai_report(user_id: str, start_date: str, end_date: str):
#     """AI 연동용 포트폴리오 요약 보고서를 생성하고 화면에 표시한다.
    
#     현재 구현은 기본적인 메타데이터와 자산 비중, 최근 KPI를 마크다운 형태로 출력한다.
#     추후 필요에 따라 JSON 형태 혹은 보다 정교한 분석을 추가할 수 있다.
#     """
#     st.subheader("🤖 AI 포트폴리오 보고서")
#     st.caption("AI 모델에 전달할 수 있는 요약 정보를 생성합니다.")

#     # 1️⃣ 사용자 계정 조회
#     accounts = query.get_accounts(user_id)
#     if not accounts:
#         st.warning("계정이 없습니다. 먼저 계정을 추가해주세요.")
#         return
#     account = accounts[0]  # 현재는 첫 번째 계정만 사용 (다중 계정 지원은 추후 구현)
#     account_id = account["id"]

#     # 2️⃣ 최신 스냅샷(valuation) 조회
#     # 일일 스냅샷 테이블에서 가장 최신 날짜의 데이터를 가져온다.
#     snapshots = load_portfolio_daily_snapshots_krw(user_id=user_id, account_id=account_id, start_date=start_date, end_date=end_date)
#     if not snapshots:
#         st.warning("스냅샷 데이터가 없습니다.")
#         return
#     latest_snapshot = snapshots[-1]
#     total_valuation = latest_snapshot["valuation_amount"]

#     # 3️⃣ 자산 비중 계산 (Look‑through 포함 여부는 기존 로직을 재활용)
#     weight_df = build_asset_weight_df(user_id=user_id, account_id=account_id)
#     # 비중을 백분율 문자열로 변환
#     weight_df["weight_pct"] = (weight_df["weight"] * 100).round(2).astype(str) + "%"

#     # 4️⃣ KPI (예: 연간 수익률) – 간단히 최근 30일 수익률을 계산
#     # 여기서는 가상의 KPI를 사용한다. 실제 구현에서는 portfolio_service 에서 제공하는 함수를 호출한다.
#     recent_kpi = "N/A"

#     rows_text = "\n".join([f"| {row['asset_class']} | {row['weight_pct']} |" for _, row in weight_df.iterrows()])

#     # 5️⃣ 보고서 마크다운 생성
#     report_md = f"""
#     # 포트폴리오 요약 (User ID: {user_id})
    
#     **계정**: {account.get('name', 'Unnamed')}
    
#     **총 평가액 (KRW)**: {total_valuation:,.0f}
    
#     **자산 비중**:
    
#     | 자산군 | 비중 |
#     |---|---|
#     {rows_text}
    
#     **최근 KPI**: {recent_kpi}
    
#     **생성 일시**: {st.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
#     """
#     st.markdown(report_md)
#     # 복사 버튼 제공 (Streamlit 기본 기능은 없으므로 사용자가 직접 복사 가능)
#     st.info("위 내용을 복사하여 AI 모델에 전달하세요.")
