# Changelog

## [Current State] - 2026-04-30

### Added
- **Asset Grouping Pie Chart: 분류기준 추가 지원**: 기본 분류 기준에 '전략 유형'을 추가하고, 이를 지원하기 위해 `asset_grouping_pie_chart` 함수에 `group_options` 파라미터를 도입했습니다.
  - 연계하여 자산 분류 기준(asset_groupings) DB 스키마를 확장하고, 쿼리 및 집계 로직을 개선했습니다.
- **Add a new Asset UI 추가**: Asset Editor에 신규 자산 추가 UI를 추가했습니다. 

## [Current State] - 2026-04-27

### Added
- **Transaction Mirroring 리팩토링 (is_external_flow)**: 투자 성과 분석(TWR) 왜곡을 방지하기 위해 '외부 자본 흐름'과 매매 시 발생하는 '시스템 자동 현금 미러링'을 DB 수준에서 구분하도록 리팩토링했습니다.
- **Portfolio Trend Chart Cold Start 보정**: 데이터 시작점(0원)에서 발생하는 비정상적인 등락폭 왜곡을 제거하기 위해 `get_valid_fluctuation` 가드 로직을 도입했습니다.

### Fixed
- **Realized PNL Chart 데이터 정합성 개선**: 
  - DB Join 과정에서 발생할 수 있는 Fan-out(데이터 증폭) 문제를 방지하기 위해 트랜잭션 ID 기준 중복 제거 로직을 추가했습니다.
  - 현금/예금(Cash, Deposit) 자산의 비정상적인 매도 트랜잭션이 실현손익 지표에 합산되어 차트를 오염시키던 버그를 수정(필터링)했습니다.

## [Phase 3.1 Milestone] - 2026-03-19

### Added
- **`import_profiles` DB 테이블 및 관리 UI (`profile_editor.py`)**: **Phase 3.1 진행:** HTS 증권사별 파싱 규칙을 DB에서 동적으로 관리할 수 있는 테이블을 생성하고 전용 에디터 UI를 추가했습니다.
- **계좌 관리 UI (`account_editor.py`)**: **Phase 3.1 진행:** 신규 계좌 등록 및 조회가 가능한 관리 페이지를 신설했습니다. `[증권사]_[별명]_[소유자]` 명명 규칙 자동 도우미를 포함합니다.
- `src/asset_portfolio/backend/infra/query.py`: 계좌 생성(`create_account`) 및 HTS 프로필 조회/저축(`get_import_profiles`, `upsert_import_profile`) 함수 추가.

### Changed
- `src/asset_portfolio/dashboard/transaction_importer.py`: 하드코딩된 `AVAILABLE_PROFILES` 대신 DB(`import_profiles`)에서 실시간으로 템플릿을 로드하도록 연동했습니다.
- `src/asset_portfolio/backend/services/asset_handler.py`: **백엔드 고도화:** 
  - `ManualAssetHandler`에 **Carry-Forward** 로직을 도입하여, 사용자가 매일 업데이트하지 않는 수동 자산도 가장 최근의 가치를 유지하며 시계열에 포함되도록 개선했습니다.
  - `AssetManager`가 `asset_type='cash'`인 자산은 설정과 무관하게 `AutoAssetHandler`를 사용하도록 강제하여, 현금 잔고가 트랜잭션(입출금)에 따라 자동 관리되도록 최적화했습니다.
- `src/asset_portfolio/dashboard/render.py`: **대시보드 지표 정교화 (As-Of 구현):**
  - **KPI 집계 버그 수정**: 여러 계좌에 걸쳐 있는 동일 자산이 중복 제거 로직 오류로 누락되던 문제를 해결했습니다. 이제 `[account_id, asset_id]` 쌍을 기준으로 정확히 합산합니다.
  - **지표 간 일관성 확보**: KPI, 최신 스냅샷 테이블, 기간별 성과 차트 모두 "최근 14일 내 가장 최신 상태"를 각 자산별로 찾아 합산하는 **As-Of lookup** 방식으로 로직을 통일했습니다. 
  - **테이블 중복 노출 수정**: 조회 기간 연장(14일)에 따라 발생하던 테이블 내 중복 행 노출 버그를 수정했습니다.

### Fixed
- **데이터 복구 및 백필(Backfill)**: 과거 특정 시점에 고립되어 대시보드 집계에서 누락되던 295개의 수동 자산 스냅샷을 최신 날짜까지 복제(Carry-Forward)하여 전체 자산 가치가 정확히 반영되도록 조치했습니다.

## [Phase 3.1 Baseline] - 2026-03-18

### Added
- `src/asset_portfolio/backend/services/importer`: **Phase 3 진행:** 클립보드(HTS) 업로더 구조 전면 개편. 증권사 화면별로 데이터 파싱 규칙을 정의하는 `ImportProfile` 프로파일 엔진 및 2-row 구조 병합 필터 도입.
- `src/asset_portfolio/backend/services/asset_alias_service.py`: **Phase 3 진행:** 종목명 <-> 시스템 자산 DB 간의 안전한 사용자 정의 매핑을 지원하기 위해 `asset_aliases` DB 테이블 생성 및 연동.
- `src/asset_portfolio/dashboard/transaction_importer.py`: **Phase 3 반영:** 명시적 프로파일(템플릿) 선택 기능 및 미인식 거래 맵핑 실패 시, 수동 대응 및 Alias 등록 UI 적용.
- `docs/FUNCTION_LIST.md`: 코드 분석 효율성을 높이기 위한 전체 함수 및 클래스 맵 생성.
- `src/asset_portfolio/dashboard/snapshot_editor.py`: **Phase 2 보완:** "정적 자산 평가액 갱신" 메뉴 고도화.
  - 전용 드롭다운 제거 및 전체 자산 자동 노출로 UI 단순화.
  - **'납입원금' 직접 수정 기능** 추가 및 현재 저장된 원금값이 테이블에 즉시 표시되도록 개선.
  - 수정 시 `cost_basis_events`를 통해 **원금 정보를 영구 저장**하여 리빌드 시에도 유지되도록 로직 강화.
- `src/asset_portfolio/dashboard/static_asset_action.py`: **Phase 2 진행:** 정적 자산(수동 자산) 전용 액션 폼(원금 출금, 이자 입력, 현금 미러링 지원) 및 보유내역 테이블 뷰 추가.
- `src/asset_portfolio/backend/services/asset_handler.py`: **Phase 1 진행:** 자산 유형(Auto/Manual)별 추상화 계층(`AssetManager`, `AssetHandler`, `AutoAssetHandler`, `ManualAssetHandler`)을 도입. 수동 기입 자산(Manual)의 수익률 계산을 위한 비례 차감 공식(`calculate_withdrawal_cost_delta`)을 백엔드에 내재화.

### Changed
- `src/asset_portfolio/dashboard/render.py`: **Phase 2 보완:** 메인 대시보드 스냅샷 테이블에 **[전체, 📈 시장 연동, 🏦 정적 자산] 필터링 탭**을 추가하여 통합 보유내역 조회가 가능하도록 개편.
- `src/asset_portfolio/dashboard/render.py`: **Bug Fix:** 수동 자산 표시를 위해 `object` 타입으로 명시적 형변환을 수행하여 pandas/pyarrow의 특정 버전에서 발생하는 `ArrowTypeError` 해결.
- `src/asset_portfolio/dashboard/app.py`: **Phase 2 반영:** 통합 대시보드 개편에 따라 중복된 "정적 자산 보유내역" 메뉴 및 라우팅 제거.
- `src/asset_portfolio/dashboard/static_asset_action.py`: **Bug Fix:** 잘못된 모듈 경로(`asset_portfolio.dashboard.query`)로 인한 `ImportError`를 올바른 경로(`asset_portfolio.backend.infra.query`)로 수정하여 "만기/해지/출금 관리" 메뉴 정상화.
- `src/asset_portfolio/dashboard/static_asset_action.py`: **Cleanup:** 사용되지 않는 `render_static_asset_holdings` 함수 제거 및 코드 정리.


---

## [Unreleased] - 2026-03-11

### Added
- **다중 통화 구조 및 과거 환율 연동 (Historical FX)**: 과거 10년 전 기록된 외화 자산의 평가금액 및 투자원금이 무조건 '현재 환율'로 변환되어 시계열의 기준이 뒤틀리던 문제(왜곡)를 해결하고자, `yfinance`를 연동하여 특정 날짜의 환율 데이터를 가져와 반영하는 기능(`fetch_historical_usdkrw`)을 구현했습니다.
- **스냅샷 증분 빌드(Incremental Build) 최적화**: 트랜잭션 추가 시 과거 전체 기간을 순회하며 재계산하던 비효율적인 시계열 리빌드 메커니즘을 획기적으로 개선했습니다. 직전 일자 스냅샷 상태를 기반으로 변경된 트랜잭션분만 가감하도록 하여 성능과 반응속도를 비약적으로 높였습니다.
- **수동 가격 업데이트 로직 세분화 및 UI 기능 분리**: API 지원이 불가한 자산에 대해, 수동으로 가격을 갱신하는 방식을 두 가지 구조로 나누어 처리하도록 개선했습니다.
  - **총액(잔액) 입력형 (`manual`)**: 예적금, 연금, 펀드 등 "현재까지 들어간/평가된 총 금액"을 갱신하는 타겟. 기존처럼 "스냅샷 수정(`snapshot_editor.py`)" 메뉴에서 단가를 `1.0`으로 고정하고 전체 잔액을 입력하는 방식으로 정상 복구.
  - **단가 입력형 (`manual_price`)**: KRX금현물, 비상장 주식 등 "고정된 수량에 변동되는 1주당 단가"를 새로 입력하는 타겟. "자산가격 업데이트(`price_updater.py`)" 메뉴 내 **[수동 단가 입력]** 탭을 별도로 신설하여 자산 가격(1.0이 아닌 실제 시장 가격)만을 입력하도록 구조 변경.
- **`manual_price` 자산의 단가 갱신 시 자동 스냅샷 리빌드 연동**: 수동 단가 입력 탭에서 가격을 갱신(`asset_prices` 및 `assets.current_price` 업데이트) 시, 해당 자산이 포함된 모든 계좌의 관련 일자 `daily_snapshots`을 재계산하여 올바르게 갱신해 주도록 로직을 연동했습니다.

### Fixed
- **스냅샷 생성 시 잘못된 `valuation_price = 1.0` 오계산 버그 시정**: 단가 입력을 요구하는 자산(펀드, 금 등)이 "스냅샷 수정" 메뉴에서 일괄적으로 잔액 처리되어 1.0원으로 무적용 되던 버그를 확인하고, `manual_price` 속성 분리를 통해 단가가 정상적으로 곱해지도록 버그를 해결했습니다.
- 과도하게 잘못 들어가 있던 이전 `close_price = 1.0` 데이터들과 스냅샷들을 정리하는 데이터 마이그레이션을 수행했습니다.
- **불필요한 레거시 코드 정리 및 중복 import 제거**: 파일 구조 리팩토링 및 클린업을 진행하여 시스템 안정성을 강화했습니다.
- **과거 단일 기준 환율 변환에 따른 지표 버그 수정**: 기간별 성과 대시보드 및 실현손익 합계에서 모두 매매/입출금 당일의 당시 환율을 역산 매핑하도록 수정하여, 순자산의 실질 증가와 확정 수익을 원화 기준으로 완벽하게 분리 반영시켰습니다.

## [Unreleased] - 2026-03-04

### Added
- **클립보드 데이터 직접 붙여넣기 기능 추가**: `transaction_importer.py`에서 엑셀 및 구글 스프레드시트의 데이터를 원본 형태 그대로 손쉽게 붙여넣기 할 수 있는 기능을 도입하여, 외부 링크 공유 권한 문제 및 빈 HTML 반환 이슈를 우회하도록 개선했습니다.
- **다양한 사용자 컬럼명 지원 확장 (Alias Mapping)**: 구글 시트 및 엑셀 업로드 시 '상품명'을 '종목명'으로, '주문일자'를 최우선 '거래일'로 유연하게 인식하도록 헤더 단어 매핑 사전을 확장했습니다.

### Fixed
- **영문+숫자 혼합 티커(Ticker) 결측치(NaN/None) 누락 버그 수정**: 데이터 파싱 과정의 내부 정규표현식 로직(`(?i)`) 오류를 수정하고, `.dropna(how='all')` 시 문자열 기반 빈 행만 안전하게 삭제하도록 개선하여 `0080G0`과 같이 문자가 포함된 티커 행이 통째로 삭제되던 치명적인 파싱 버그를 해결했습니다.
- **숫자형 티커의 Float 변환(소수점 추가) 현상 수정**: 엑셀/클립보드에서 `161510`과 같은 번호를 붙여넣을 때 파이썬이 `161510.0`으로 자동 렌더링하면서, DB 내의 티커와 달라져버려 중복 검사를 무사통과 하던 문제를 정규표현식 트리밍(`\.0$`) 로직으로 방어했습니다.
- **중복 거래 검증 로직 내 미세 오차(Float) 허용**: 정확히 일치(`==`) 연산으로만 비교하던 단가 및 수량을 0.01 범위 내 오차 검사로 변경시켜, 외부 소스 파싱 시 발생하는 십진수 부동소수점 오차로 인한 2중 등록을 원천 차단했습니다.
- **미리보기 시트 테이블 행 노출 제한 해제**: 업로드 검토 시 `.head(10)`옵션으로 10개의 거래 내역만 볼 수 있던 화면 제한을 해제하여 사용자가 전체 내역 스크롤 검증을 할 수 있도록 개선했습니다.

## [Unreleased] - 2026-03-03

### Added
- **자산별 보유기간 분석(Holding Period) 기능 추가**: '이력(History)' 탭 내에 FIFO(선입선출) 기반 잔여 자산의 보유기간을 추적하여 보여주는 기능을 신설했습니다.
  - 가중 평균 보유일수, 최초 매수일을 계산하여 자산별 상세 테이블로 제공합니다.
  - 1년(365일)을 기준으로 "장기(Long-term)"와 "단기(Short-term)" 보유 비중을 Stacked Bar 차트로 직관적으로 확인할 수 있도록 시각화했습니다.
- **포트폴리오 추세 차트 하위 등락폭 서브 차트 추가**: 최근 30일(일간), 12개월(월간), 5년(연간) 구간의 총 평가금액 등락폭을 Bar 차트(상승 빨강, 하락 파랑)로 나란히 제공하여 단기/중장기 변동 흐름을 직관적으로 확인하도록 구현했습니다.
- **거래 내역 데이터 필터링용 카테고리 탭 도입**: 대시보드 하단의 '전체 거래 내역' 테이블 영역 상단에 `[전체, 매수, 매도, 입금, 출금]` 탭을 신설했습니다. 각 거래 유형별로 필터링 된 내역을 빠르게 전환하여 조회할 수 있으며, 탭 전환 시에도 "✏️거래 수정/삭제" 토글이 문제없이 독립 동작하도록 UI를 개편했습니다.

### Changed
- **포트폴리오 추세 차트 보조축(Secondary Y-axis) 적용**: '총 평가금액'과 '투자원금'의 단위 격차가 커질 때 발생하는 왜곡 문제를 해결했습니다. Plotly의 `make_subplots` 레이아웃을 통해, 투자원금 지표를 우측 보조 y축에 맵핑시켜 상호 추세 변동을 명확하게 파악할 수 있도록 수정했습니다.
- **차트 툴팁(Tooltip, Hover) 가독성 극대화 (소수점 제거포맷팅)**: 거의 모든 차트 툴팁에서 마우스 오버 시 필요 이상의 소수점이 길게 표시되던 이슈를 해결했습니다. 각 금액 파이프라인 차트들의 `hovertemplate` 및 UI `tickformat` 속성을 개별적으로 `,.0f` (천단위 정수) 포맷으로 오버라이딩 적용하였으며, 수익률(%) 데이터 차트들은 `,.2f`로 유지되도록 방어 코드를 추가했습니다.

## [Unreleased] - 2026-02-25
- **실현손익(Realized P&L) 계산 및 대시보드 표시 기능 구현**:
  - `transactions` 테이블에 `realized_pnl` 컬럼을 활용하여 각 매도 거래의 확정 수익을 추적합니다.
  - 대시보드의 주요 포트폴리오 수익률 KPI 영역에 '실현손익 누적' 금액을 표시하며, 거래 내역 테이블에서도 개별 거래의 실현손익 확인이 가능하도록 추가했습니다.
  - 과거 거래 추가, 수정, 삭제 시 발생할 수 있는 데이터 정합성 문제를 방지하기 위해, 해당 자산의 모든 거래를 순회하며 실현손익을 자동 재계산 및 보정하는 로직(`TransactionService.rebuild_realized_pnl_for_asset`)을 도입했습니다.
- **기간 내 실현손익 분석 시각화 기능 추가**:
  - `성과(Performance)` 탭 하단과 `이력(History)` 탭 상단에 "기간 내 실현손익 분석" 차트 섹션을 신설했습니다.
  - 선택한 기간 동안 실현손익에 크게 기여한 자산(Top 10)을 수평 Bar 차트로 시각화합니다.
  - 선택한 기간 동안 발생한 전체 실현손익 누적 추이를 월별 누적 Stacked Bar 차트로 제공하여, 확정 배당/매도 타이밍에 따른 수익 흐름을 한눈에 파악할 수 있도록 개선했습니다.

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
