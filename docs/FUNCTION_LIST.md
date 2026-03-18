# Function List for Gemini Reference

이 파일은 Gemini가 코드 구조를 빠르게 파악하고 토큰 사용량을 줄이기 위해 참조하는 파일입니다.

## backend\infra\query.py

- `def get_user_by_password()`: 비밀번호로 사용자를 조회합니다.
- `def get_accounts()`: 특정 사용자의 모든 계좌 정보를 불러옵니다.
- `def _as_date_str()`: 
- `def build_daily_snapshots_query()`: daily_snapshots 공통 쿼리 빌더
- `def load_asset_contribution_data()`: 
- `def get_transactions()`: 사용자의 모든 거래내역을 불러옵니다.
- `def get_recurring_orders()`: 사용자의 모든 정기주문을 불러옵니다.
- `def get_assets()`: 모든 자산 정보를 불러옵니다.
- `def load_asset_prices()`: 특정 자산의 가격 데이터를 조회합니다.
- `def fetch_all_pagination()`: Supabase 1000행 제한을 우회하기 위한 페이지네이션 헬퍼.
- `def get_period_cash_flow()`: 특정 기간의 입출금(DEPOSIT, WITHDRAW) 내역을 조회합니다.

## backend\infra\supabase_client.py

- `def get_supabase_client()`: 

## backend\services\asset_service.py

- `class AssetService`
  - `def create_asset_minimal()`: ✅ 최소 입력으로 assets에 신규 자산을 생성합니다.
  - `def update_asset()`: ✅ assets 단일 row 업데이트
  - `def get_assets_lookup_df()`: asset_id → 자산명(name_kr) 매핑 및 자산 타입 정보를 위한 전체 목록 조회

## backend\services\benchmark_service.py

- `def load_cash_benchmark_series()`: 현금 기준 benchmark
- `def merge_portfolio_and_benchmark()`: Portfolio 수익률과 Benchmark 수익률 병합
- `def _normalize_yf_download_df()`: yfinance.download 결과가
- `def load_sp500_benchmark_series()`: S&P 500 (^GSPC) 누적 수익률 시계열 생성.
- `def align_portfolio_to_benchmark_dates()`: Portfolio의 date를 Benchmark의 거래일(date)에 맞춰 정렬한다.
- `def merge_portfolio_and_benchmark_ffill()`: Benchmark 거래일(date)을 기준으로:
- `def align_portfolio_to_benchmark_calendar()`: 벤치마크 날짜 캘린더를 기준으로 포트폴리오 시계열을 reindex 후 forward-fill

## backend\services\daily_snapshot_generator.py

- `def generate_daily_snapshots()`: 특정 account에 대해

## backend\services\data_contracts.py

- `def _ensure_columns()`: 
- `def _ensure_unique_columns()`: 
- `def _to_date_series()`: 
- `def normalize_snapshot_df()`: daily_snapshots 기반 스냅샷 DataFrame 정규화.
- `def normalize_weight_df()`: 자산 비중 DataFrame 정규화.
- `def normalize_latest_weight_df()`: 
- `def normalize_contribution_df()`: 
- `def normalize_benchmark_df()`: 

## backend\services\fx_service.py

- `class FxRate`
- `class FxService`
  - `def fetch_usdkrw()`: ✅ USD/KRW 환율(근사)을 yfinance로 가져옵니다.
  - `def apply_fx_to_df()`: DataFrame에서 USD 행의 금액 컬럼을 KRW로 일괄 환산한다.
  - `def fx_caption()`: 표준화된 환율 안내 캡션 문자열을 반환.
  - `def fetch_historical_usdkrw()`: 특정 기간의 USD/KRW 과거 환율을 가져옵니다.
  - `def apply_historical_fx_to_df()`: 스냅샷 DataFrame에 날짜(date)별 과거 환율을 매핑하여 USD 금액을 KRW로 일괄 환산한다.

## backend\services\holding_period_service.py

- `def _to_date()`: 
- `def calculate_holding_periods()`: TWR/FIFO 기반으로 자산별 보유 기간(Holding Period)을 계산합니다.

## backend\services\krx_price_fetcher.py

- `class KRXPriceResult`
- `class KRXPriceFetcher`
  - `def _normalize_code()`: 
  - `def _convert_alnum_code_to_numeric()`: ✅ 문자 혼합 티커(예: 0064K0)를 KRX 숫자 코드로 변환합니다.
  - `def _build_candidate_codes()`: ✅ KRX 매칭을 위해 여러 후보 코드를 만든다.
  - `def _safe_float()`: 
  - `def _normalize_code_value()`: 
  - `def _pick_column()`: 
  - `def _download_csv()`: ✅ KRX OTP → CSV 다운로드 파이프라인
  - `def fetch_reference_price()`: ✅ KRX 기반 "참고 가격" 조회

## backend\services\manual_cost_basis_service.py

- `def _build_cost_basis_map()`: manual_asset_cost_basis_current 테이블 결과를
- `def fetch_cost_basis_current()`: manual_asset_cost_basis_current를 조회해 (account_id, asset_id) → 원금 정보를 반환한다.
- `def attach_manual_cost_basis()`: 스냅샷 DataFrame에 수동 자산 원금(cost basis)을 붙인다.
- `def record_cost_basis_events()`: manual_asset_cost_basis_events에 이벤트를 기록하고

## backend\services\portfolio_calculator.py

- `def _to_date()`: ✅ Supabase에서 오는 timestamp(with tz) / 문자열 / datetime을 date로 정규화
- `def _load_asset_price_history()`: 자산 가격 히스토리를 날짜 범위로 로드한다.
- `def calculate_portfolio_state_at_date()`: 특정 계좌(account_id)에 대해
- `def calculate_asset_return_series_from_snapshots()`: daily_snapshots 데이터를 기반으로
- `def apply_transactions()`: 거래 내역을 순서대로 처리하여
- `def calculate_daily_snapshots_for_asset()`: 특정 자산에 대해 일별 snapshot 데이터를 계산한다.
- `def calculate_portfolio_return_series_from_snapshots()`: daily_snapshots 데이터를 기반으로 포트폴리오 전체 누적 수익률 시계열을 계산한다.

## backend\services\portfolio_service.py

- `def get_asset_return_series()`: 특정 자산 + 계좌의 기간별 수익률 시계열 조회
- `def load_portfolio_daily_snapshots()`: daily_snapshots에서
- `def load_portfolio_daily_snapshots_krw()`: daily_snapshots를 date 기준으로 집계하되,
- `def get_portfolio_return_series()`: Streamlit / API에서 사용하는 최종 함수
- `def calculate_asset_contributions()`: daily_snapshots 기반 자산별 수익률 기여도 계산
- `def calculate_period_performance()`: 기간별 성과 분석 (Cash Flow 및 외환 변동 고려)
- `def get_realized_pnl_by_period()`: 선택한 기간 동안 발생한 실현손익 내역을 조회하고 DataFrame으로 반환한다.

## backend\services\portfolio_weight_service.py

- `def load_asset_weight_timeseries()`: 자산 비중 시계열 원천 데이터 로드
- `def build_asset_weight_df()`: ✅ ALL/단일 계좌 모두 안전한 비중 DF 생성 + USD 환산 반영
- `def _safe_float_series()`: ✅ Supabase 응답에서 numeric이 str/Decimal/None 등으로 섞여 들어와도 안전하게 float로 변환
- `def load_latest_asset_weights()`: Treemap용 최신 비중 데이터

## backend\services\price_updater_service.py

- `class PriceUpdateResult`
- `class PriceUpdaterService`
  - `def _normalize_ticker_for_yf()`: ✅ yfinance가 요구하는 티커 형식으로 정규화
  - `def _fetch_last_close_price()`: ✅ yfinance로 최근 종가(또는 마지막 close)를 가져옵니다.
  - `def _safe_float()`: 
  - `def _candidate_tickers()`: ✅ yfinance ticker 후보 생성
  - `def fetch_price_from_yfinance()`: return: (price, used_ticker, reason)
  - `def update_asset_price()`: ✅ 단일 자산 current_price 업데이트 + 메타데이터 기록
  - `def update_many()`: 
  - `def _load_price_sources()`: ✅ asset_price_sources 테이블에서 price source 설정을 읽어옵니다.
  - `def _fetch_price_from_sources()`: ✅ price source 우선순위에 따라 가격을 조회합니다.
  - `def _carry_forward_last_price()`: ✅ 가격 조회 실패 시 가장 최근 가격을 가져와 당일 가격으로 사용합니다.
  - `def update_asset_prices_for_date()`: ✅ price source 기반으로 asset_prices 테이블을 갱신합니다.
  - `def _get_accounts_holding_asset()`: ✅ 해당 자산이 거래된 계좌 목록을 조회
  - `def _get_first_transaction_date()`: ✅ (asset_id, account_id)의 최초 거래일을 조회합니다.
  - `def rebuild_snapshots_for_updated_assets()`: ✅ 가격 업데이트 후 스냅샷 자동 리빌드

## backend\services\snapshot_frame.py

- `def _flatten_rows()`: Supabase(PostgREST) 응답 rows는 join/select에 따라 중첩 dict가 섞일 수 있음.
- `def _to_yyyy_mm_dd()`: 
- `def _strict_numeric()`: 숫자 변환은 '조용히 0으로 덮지 않는다'.
- `def to_snapshot_df()`: daily_snapshots rows -> 정규화된 DataFrame

## backend\services\transaction_service.py

- `class CreateTransactionRequest`
- `class TransactionService`
  - `def _normalize_currency()`: 
  - `def _iso_date()`: 
  - `def _to_date()`: Supabase에서 내려오는 날짜 타입을 date로 통일한다.
  - `def _chunk()`: 
  - `def _is_manual_asset()`: ✅ 수동평가 자산 여부 판단
  - `def _get_asset_cash_flag()`: CASH_KRW / CASH_USD 여부 판단:
  - `def validate_request()`: 
  - `def create_transaction()`: transactions insert (단일)
  - `def rebuild_realized_pnl_for_asset()`: 특정 자산의 모든 거래를 순서대로 순회하며 realized_pnl을 계산하여 DB를 업데이트한다.
  - `def rebuild_daily_snapshots_for_asset()`: (account_id, asset_id, date range)에 대해 daily_snapshots를 리빌드한다.
  - `def _get_asset_currency()`: ✅ 원자산의 통화를 조회하여, 어떤 CASH 자산을 움직여야 하는지 결정합니다.
  - `def _get_account_currency()`: ✅ 계좌에 통화 컬럼이 있는 경우, 그 값을 우선 사용합니다.
  - `def _get_cash_asset_id_by_currency()`: ✅ 통화에 맞는 CASH 자산을 찾습니다.
  - `def _build_cash_mirror_request()`: BUY/SELL 거래를 CASH 입출금으로 미러링한다.
  - `def _find_auto_cash_transactions()`: AUTO CASH 미러 거래를 찾는다.
  - `def get_transaction_by_id()`: 
  - `def create_transaction_and_rebuild()`: ✅ 거래 입력 단일 진입점(확장)
  - `def update_transaction_and_rebuild()`: 거래 수정 + 스냅샷 리빌드
  - `def delete_transaction_and_rebuild()`: 거래 삭제 + 스냅샷 리빌드

## dashboard\app.py

- `def _inject_mobile_redirect()`: 
- `def render_login_page()`: Renders the login page.
- `def render_main_dashboard()`: Renders the main dashboard after user is logged in.

## dashboard\asset_editor.py

- `def _load_asset_price_source()`: ✅ asset_price_sources에서 특정 자산의 설정을 가져옵니다.
- `def _upsert_asset_price_source()`: ✅ asset_price_sources 업서트
- `def _load_latest_holding_asset_ids_global()`: Load asset_ids held on the latest snapshot date across all accounts.
- `def render_asset_editor()`: 

## dashboard\data.py

- `def load_assets_lookup()`: asset_id → 자산명(name_kr) 매핑용 lookup 로드
- `def get_usdkrw_rate()`: USD/KRW 환율을 Streamlit 세션 캐시에서 반환.
- `def get_historical_usdkrw_rate()`: 지정된 기간의 일일 USD/KRW 환율 이력을 가져옵니다.

## dashboard\price_updater.py

- `def _render_auto_updater()`: 
- `def _render_manual_updater()`: 
- `def render_price_updater()`: 

## dashboard\recurring_order_editor.py

- `def _load_accounts_df()`: 
- `def _load_assets_df()`: 
- `def render_recurring_order_editor()`: 

## dashboard\render.py

- `def load_portfolio_return_series_cached()`: cached wrapper for get_portfolio_return_series
- `def load_asset_grouping_summary()`: 자산 분류 기준(자산 유형/기초자산 클래스)별 평가금액 합계를 가져옵니다.
- `def render_asset_grouping_pie_section()`: 
- `def render_kpi_section()`: 
- `def render_period_performance_section()`: 기간별 성과 분석 (Cash Flow 고려)
- `def render_portfolio_trend_chart()`: 
- `def render_benchmark_comparison_section()`: 
- `def render_asset_return_section()`: 
- `def render_latest_snapshot_table()`: 
- `def render_account_selector()`: 
- `def _get_min_snapshot_date()`: daily_snapshots의 최소 날짜를 조회한다.
- `def resolve_date_range()`: 기간 코드("오늘", "일주일", "한달", "3달(1분기)", "YTD(올해)", "ALL")를
- `def render_period_selector()`: 
- `def render_target_vs_actual_weight_section()`: 
- `def render_asset_weight_section()`: 
- `def render_asset_contribution_section()`: 
- `def render_asset_contribution_stacked_area()`: 
- `def render_portfolio_treemap()`: 
- `def render_asset_contribution_section_full()`: 
- `def render_realized_pnl_charts()`: 
- `def render_transactions_table_section()`: 
- `def render_asset_transaction_history()`: 보유 중인 자산을 선택하여 해당 자산의 전체 거래 내역을 조회합니다.
- `def render_holding_period_section()`: 

## dashboard\snapshot_editor.py

- `def _load_manual_assets_df()`: 
- `def _load_snapshots_for_date_multi()`: ✅ 여러 계좌에 대해 (date=고정) 스냅샷 로드
- `def _upsert_snapshots()`: 
- `def _upsert_asset_prices()`: 수동자산 평가 입력 시점에 asset_prices도 함께 저장한다.
- `def render_snapshot_editor()`: 
- `def _load_existing_pairs_for_manual_assets()`: ✅ 멀티 편집에서 '해당 계좌에 실제로 존재하는 자산'만 보여주기 위한 (account_id, asset_id) pair 조회

## dashboard\transaction_editor.py

- `def _load_accounts_df()`: 
- `def _load_assets_df()`: 
- `def _load_latest_holding_asset_ids()`: 
- `def _find_cash_asset_id()`: 
- `def render_transaction_editor()`: 

## dashboard\transaction_importer.py

- `class PreparedTransaction`
- `def _normalize_column_key()`: 컬럼명을 비교하기 위해 공백/특수문자를 제거하고 소문자로 통일한다.
- `def _normalize_trade_type()`: 매수/매도 표기를 BUY/SELL로 표준화한다.
- `def _normalize_currency()`: 통화 표기를 KRW/USD 중심으로 표준화한다.
- `def _normalize_market()`: 시장 구분을 내부 표준값으로 맞춘다.
- `def _parse_numeric()`: 문자열에 포함된 쉼표 등 서식을 제거하고 숫자로 변환한다.
- `def _map_columns()`: 업로드된 컬럼을 표준 컬럼명으로 매핑한다.
- `def _get_account_id_by_name()`: 계좌명을 account_id로 매칭하고, 문제 발생 시 오류 메시지를 돌려준다.
- `def _get_asset_row_by_ticker()`: 
- `def _find_existing_duplicate()`: 기존 transactions 테이블에 같은 거래가 있는지 확인한다.
- `def _render_required_fields_table()`: 
- `def _render_account_reference_table()`: 
- `def _get_latest_transaction_dates()`: 계좌별 최근 거래일을 조회해 중복 입력을 예방하도록 돕는다.
- `def _clean_dataframe_strings()`: 모든 값을 문자열로 읽은 DataFrame에서 NaN 관련 문자열이나 공백만 있는 셀을 빈 문자열로 정제하고 빈 행을 삭제한다.
- `def _read_uploaded_file()`: 
- `def _read_pasted_text()`: 
- `def _prepare_trade_rows()`: 거래 행을 검증해 (준비된 거래 목록, 오류 목록, 중복 거래 목록)을 반환한다.
- `def _prepare_dividend_rows()`: 배당금 행을 검증해 (준비된 거래 목록, 오류 목록, 중복 거래 목록)을 반환한다.
- `def _load_google_sheet()`: 구글 스프레드시트 링크를 바탕으로 데이터를 CSV 형태로 읽어온다.
- `def _execute_upload()`: 검증이 끝난 거래를 실제로 insert한다.
- `def render_transaction_importer()`: 

## mobile\app.py

- `def _is_mobile_user_agent()`: User-Agent 문자열로 모바일 접속 여부를 추정합니다.
- `def _get_streamlit_url()`: 데스크톱 접속 시 리다이렉트할 Streamlit URL을 가져옵니다.
- `def _read_index_html()`: 모바일 React 페이지 HTML을 읽어 반환합니다.
- `def get_app()`: 외부에서 FastAPI 앱을 가져갈 때 사용하는 헬퍼 함수입니다.

## mobile\data.py

- `def _date_range_from_days()`: 최근 n일 범위를 (start, end) 문자열로 반환합니다.
- `def _json_safe_records()`: NaN/NaT 값을 JSON에서 이해 가능한 None으로 치환합니다.
- `def list_accounts()`: 계좌 목록을 조회합니다.
- `def load_assets_lookup()`: 자산 정보 lookup을 조회합니다.
- `def get_kpi_summary()`: 전체 포트폴리오 KPI를 계산합니다.
- `def get_latest_snapshot_table()`: 가장 최신 스냅샷 테이블 데이터를 반환합니다.
- `def get_recent_transactions()`: 최근 n일 동안의 거래 내역을 조회합니다.
- `def get_top_contributions()`: 최근 n일 누적 기여도 기준 Top K 종목을 반환합니다.
- `def get_portfolio_treemap()`: 포트폴리오 Treemap용 데이터를 반환합니다.

