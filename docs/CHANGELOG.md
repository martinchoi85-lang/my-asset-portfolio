# 버전별로 “Added/Changed/Fixed/Deprecated/Removed/Security” 형태로 변경사항을 누적 기록

# Changelog

## [Unreleased]
### Added
- Transaction Editor 페이지 추가 (BUY/SELL/DEPOSIT/WITHDRAW 입력)
- Main Dashboard에 Transactions 탭 추가 (기간 내 거래 조회)
- Price Updater 설계 초안 (yfinance 기반 current_price 갱신 예정)
- Treemap 색상/레이아웃 개선 옵션 (asset_type 색상 분리, height 조정)

### Changed
- daily_snapshots 리빌드 전략을 멱등적으로 설계 (delete-range + upsert on_conflict)
- cash 자산 모델: asset_type='cash'로 판별, valuation_price=1 고정 정책 도입
- Treemap hover 라벨 한글화 지원을 위한 labels/hover_data 구조화

### Fixed
- (예정/적용) transaction_date 비교 로직을 문자열 비교에서 date 비교로 교정
- (예정/적용) DEPOSIT/WITHDRAW가 스냅샷에 반영되지 않는 문제 해결 (cash 거래 처리 추가)
- Treemap 모드 전환 시 hover_data 설정 오류로 발생하는 ValueError 수정


​


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

