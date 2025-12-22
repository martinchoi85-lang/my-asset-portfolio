■ 2025-12-21 버전 프로젝트 구조
transactions
   ↓
portfolio_calculator (누적 수량, 평가금액 계산)
   ↓
daily_snapshots (date, asset_id, account_id 단위 저장)
   ↓
Streamlit 차트


■ 2025-12-20 버전 프로젝트 구조

[Transaction]
   ↓
daily_snapshot_generator
   ↓
[Asset daily_snapshot]
   ↓
asset_return_series (calculator)
   ↓
[Portfolio daily_snapshot]
   ↓
portfolio_aggregator (TWR)

✔️ 모든 계산 로직은 DB 독립
✔️ 모든 핵심은 테스트로 고정
✔️ Streamlit / API / Supabase 변경에도 흔들리지 않음

■ Supabase → Snapshot → 계산 → 시각화 흐름
Supabase
  ↓
daily_snapshots 조회
  ↓
portfolio_service (집계)
  ↓
portfolio_aggregator (TWR)
  ↓
Streamlit 시각화


■ 자산별 수익률 차트 데이터 흐름
daily_snapshots (asset_id 단위)
   ↓
asset_id + date 기준 필터
   ↓
calculate_asset_return_series_from_snapshots
   ↓
자산별 누적 수익률 시각화

