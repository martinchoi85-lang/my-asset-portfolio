# Changelog

## [Unreleased] - 2026-02-25

### Added
- **실현손익(Realized P&L) 계산 및 대시보드 표시 기능 구현**:
  - `transactions` 테이블에 `realized_pnl` 컬럼을 활용하여 각 매도 거래의 확정 수익을 추적합니다.
  - 대시보드의 주요 포트폴리오 수익률 KPI 영역에 '실현손익 누적' 금액을 표시하며, 거래 내역 테이블에서도 개별 거래의 실현손익 확인이 가능하도록 추가했습니다.
  - 과거 거래 추가, 수정, 삭제 시 발생할 수 있는 데이터 정합성 문제를 방지하기 위해, 해당 자산의 모든 거래를 순회하며 실현손익을 자동 재계산 및 보정하는 로직(`TransactionService.rebuild_realized_pnl_for_asset`)을 도입했습니다.

### Changed
- **실현손익 비용 반영 로직 개선**: `portfolio_calculator.py`에서 자산 합산 및 실현손익 도출 시, 사용자가 기입한 수수료(fee)와 세금(tax)이 매수 시에는 투자원금에 가산되고 매도 시에는 실현손익에서 차감되도록 반영했습니다.

## [Unreleased] - 2026-02-24

### Added
- **FX 변환 유틸리티 모듈 신규 추가** (`dashboard/fx_utils.py`): USD → KRW 환산 로직을 중앙화하여 각 컴포넌트가 개별적으로 `FxService`를 호출하지 않아도 되도록 개선.
  - `get_usdkrw_rate()`: `@st.cache_data(ttl=600)` 적용으로 세션 내 환율 조회를 1회로 제한.
  - `apply_fx_to_df(df, usd_krw, amount_cols, currency_col)`: DataFrame의 USD 행 금액 컬럼을 KRW로 일괄 환산.
  - `fx_caption(usd_krw, source)`: 환율 안내 캡션 문자열 생성 유틸.
- **`load_portfolio_daily_snapshots_krw()` 추가** (`portfolio_service.py`): `daily_snapshots`를 date 기준으로 집계할 때 USD 자산을 KRW로 환산한 후 합산하는 서비스 함수 추가. 트렌드 차트 등 KRW 기준 시계열이 필요한 컴포넌트에서 사용.

### Changed
- **`render_kpi_section`**: 내부에 직접 작성된 `FxService` 호출 코드를 `get_usdkrw_rate()` / `fx_caption()` 유틸로 교체.
- **`render_latest_snapshot_table`**: `valuation_amount`, `purchase_amount`에 `apply_fx_to_df()` 적용. USD 자산의 평가금액/원금이 KRW로 환산되어 표시됨. 테이블 캡션에 적용 환율 정보 추가.
- **`load_asset_grouping_summary`** (동적 그룹화 차트): `asset_summary_live` (currency 컬럼 없음) 대신 `daily_snapshots`에서 `currency` 포함 직접 조회 후 `apply_fx_to_df()` 적용. 파이 차트·표의 평가금액 합계가 KRW 기준으로 정상화.
- **`render_portfolio_trend_chart`**: `get_portfolio_return_series()` 대신 `load_portfolio_daily_snapshots_krw()`를 사용해 트렌드 차트 Y축이 KRW 환산 기준 금액으로 표시됨.
- **`render_asset_weight_section`**: `build_asset_weight_df()` 호출 후 `apply_fx_to_df()` 적용하여 자산 비중 계산 시 USD 자산이 KRW로 환산됨.

## [Unreleased] - 2026-02-21

### Added
- **거래 내역 일괄 업로드 기능 개선**: 거래 내역 업로드 시 "매매 내역"과 "입출금 내역"을 구분하여 처리하는 기능을 추가했습니다.
  - **매매 내역**: 수량, 단가, 수수료, 세금 정보를 포함하는 거래 기록.
  - **입출금 내역**: 계좌 잔액 조정을 위한 입금/출금 기록 (수량/단가 필드 제거).
  - **중복 거래 방지 강화**: 거래일, 티커, 거래 유형, 수량, 단가, 수수료, 세금 정보를 모두 비교하여 동일한 거래가 이미 존재하는지 검증하는 로직 추가.
  - **계좌명 매칭 개선**: 계좌명뿐만 아니라 계좌 번호로도 계좌를 식별할 수 있도록 지원.

### Changed
- **거래 내역 업로드 UI 개선**: 업로드할 거래 내역의 유형(매매/입출금)을 선택할 수 있는 라디오 버튼 추가.
- **데이터 검증 로직 강화**:
  - 매매 내역: 수량, 단가, 거래 유형, 거래일, 계좌명 등 필수 필드 검증 강화.
  - 입출금 내역: 거래일, 거래 유형(입금/출금), 금액, 계좌명 등 필수 필드 검증 강화.
  - 중복 거래 검증 로직 정교화.

### Fixed
- **거래 내역 업로드 시 데이터 타입 오류 수정**: 수량, 단가, 수수료, 세금 필드를 숫자로 변환하는 과정에서 발생하는 오류 수정.
- **계좌명 매칭 오류 수정**: 계좌명에 공백이나 특수문자가 포함된 경우에도 올바르게 매칭되도록 수정.

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
