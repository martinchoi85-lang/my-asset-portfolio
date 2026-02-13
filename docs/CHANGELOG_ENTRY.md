## [Unreleased] - 2026-02-13

### Fixed
- **대시보드 차트 데이터 조회 제한 해제**: Supabase 클라이언트의 기본 row limit(1000행)으로 인해 기간을 "YTD" 등으로 길게 설정했을 때 차트 데이터가 중간에 끊기는 문제 수정. 모든 시계열 데이터 조회 쿼리에 `.limit(100000)`을 적용하여 전체 데이터를 정상적으로 불러오도록 개선.
  - `load_asset_contribution_data` (자산별 기여도)
  - `load_asset_weight_timeseries` (자산 비중 변화)
  - `load_portfolio_daily_snapshots` (포트폴리오 수익률)
  - `render_asset_return_section` (자산별 수익률 추이)
