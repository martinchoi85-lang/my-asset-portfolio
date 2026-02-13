# Changelog

## [Unreleased] - 2026-02-13

### Added
- **자산별 수익률 차트 고도화**: "자산별 수익률 추이" 차트에 해당 자산의 시장 가격 정보를 보조축(우측 Y축)으로 추가하여 수익률과 가격 변화를 동시에 비교 분석할 수 있도록 개선 (Plotly Dual Axis 적용).

### Fixed
- **대시보드 차트 데이터 조회 제한 해제**: Supabase 클라이언트의 1,000행 강제 제한(Hard Limit)을 우회하기 위해 **페이지네이션(Pagination)** 로직을 구현했습니다. 데이터를 1,000행씩 분할 조회하여 병합하는 방식으로 변경함으로써, "YTD"나 "ALL" 기간 조회 시 데이터가 잘리는 문제를 원천적으로 해결했습니다.
  - `fetch_all_pagination` 헬퍼 함수 추가 및 적용 (`query.py`)
  - `load_asset_contribution_data`, `load_asset_weight_timeseries`, `load_portfolio_daily_snapshots`, `render_asset_return_section` 등 주요 시계열 데이터 조회 함수에 적용.

## [Unreleased] - 2026-02-12

### Added
- **자산별 거래 내역 조회 기능 추가**: Dashboard 앱에 "자산별 거래" 탭을 추가하여 보유 중인 자산별로 상세 거래 내역과 메모를 확인할 수 있는 기능 구현.
  - 현재 보유 중인 자산(수량 > 0)만 선택할 수 있는 드롭다운 메뉴 제공.
  - 선택한 자산의 모든 거래 내역을 최신순으로 표시 (계좌, 거래일, 수량, 단가, 수수료, 세금, 메모 포함).
  - 대시보드 상단에 총 매수 수량, 총 매도 수량, 순 보유 수량 통계 정보 표시.

### Fixed
- `daily_snapshots` 조회 시 `date` 컬럼 누락으로 인해 최신 수량이 아닌 전체 수량이 합산되던 버그 수정.
- 거래 내역 데이터의 한글화 처리가 통계 계산보다 먼저 수행되어 매수/매도 수량이 0.00으로 표시되던 로직 오류 수정.
- pandas `SettingWithCopyWarning` 경고 해결을 위해 pandas DataFrame slicing 시 `.copy()` 및 `.loc` 사용하도록 코드 개선.
- 자산별 거래 통계 계산 시 `INIT` 거래 타입이 누락되던 부분 개선하여 총 매수 수량에 합산되도록 수정.
