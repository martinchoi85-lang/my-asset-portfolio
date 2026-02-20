# Changelog

## [Unreleased] - 2026-02-13

### Added
- **기간별 성과 분석 (Period Analysis)**: "성과" 탭에 특정 기간(오늘, 1주일, 1달, YTD 등) 동안의 자산 증감을 분석하는 기능 추가.
  - Modified Dietz 방식으로 기간 수익률 계산 (순입출금 반영).
  - 기간 내 기초 자산, 기말 자산, 순입출금(Net Flow), 투자 손익(Gain), 수익률(Return) 지표 제공.
- **대시보드 UI/UX 전면 개편**:
  - 사이드바 메뉴를 기능별(대시보드, 거래 관리, 자산 관리, 시스템 관리)로 그룹화하여 직관성 개선.
  - 메인 대시보드 탭을 "요약(Overview) - 성과(Performance) - 이력(History)" 구조로 재편하여 분석 흐름 최적화.
  - "자산 비중 변화" 차트 및 "누적 기여도" 차트 시각화 개선 (범례 위치 조정, Top N 필터링 등).

### Changed
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
