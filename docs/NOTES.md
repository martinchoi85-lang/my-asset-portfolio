“dashboard는 렌더링/사용자 입력”

“backend는 데이터 조회/계산”

“backend는 dashboard를 import하지 않는다”

valuation_price ← 현재가
purchase_price ← 평균매입가
valuation_amount ← 평가 금액
purchase_amount ← 매입 금액

---

## 🗑️ 미사용 코드 (Dead Code) 및 정리 대상 (발견일: 2026-03-11)

바로 삭제하지 않고 리팩토링 시 일괄 정리할 대상입니다.

1. **`src/asset_portfolio/backend/services/portfolio_calculator.py`**
   - `calculate_asset_return_series_from_snapshots` 함수 내부에 있는 대량의 주석 처리된 레거시 순회 로직 (Line 312 ~ 339 부근). pandas 벡터화 연산으로 대체된 이후 방치되어 있습니다.
   
2. **`src/asset_portfolio/backend/services/daily_snapshot_generator.py`**
   - 상단 import 구문이 중복되어 있습니다. `from datetime import date`, `get_supabase_client`, `calculate_daily_snapshots_for_asset` 코드가 파일 최상단에 두 번 반복해서 작성되어 있습니다 (Line 1~5 와 8~12).

3. **`src/asset_portfolio/dashboard` 내 일부 유틸리티 함수 파편화**
   - `fx_utils.py` 와 `data.py`가 대시보드 내에 존재하나, 데이터 페칭이나 캐싱 등의 백엔드 성격 로직이 혼재되어 있습니다. 향후 `backend/services/` 내부로 통합 및 구조화가 권장됩니다.

---

## 🛠️ 잠재적 로직 이슈 및 성능 개선 가능 사항

1. **스냅샷 재생성 시 전체 트랜잭션 로드 비효율 (`portfolio_calculator.py`)**
   - `calculate_daily_snapshots_for_asset()` 실행 시 `start_date`부터 `end_date` 구간의 스냅샷을 만들기 위해, 해당 자산의 **'과거 모든 거래 내역'**을 불러와서 누적 잔고를 계산합니다.
   - 단기적으로는 정확도를 보장하는 가장 훌륭한 방법이지만, 거래량이 1,000건, 10,000건 단위로 넘어갈 경우 성능 저하가 우려됩니다. `start_date - 1일` 시점의 스냅샷을 초기 기준점(Base)으로 불러오는 방식의 로직 개선을 검토해 볼 수 있습니다.

2. **과거 스냅샷의 현재가(Current Price) 오염 문제 (`portfolio_calculator.py`)**
   - 스냅샷 생성 중 `price_history`에 과거 일자 가격이 없을 때 `assets.current_price`를 Fallback으로 가져다 쓰는 로직이 존재합니다.
   - 이로 인해 가격 히스토리가 누락된 과거 일자의 평가금액이 **최신 가격**으로 계산되어 수익률이 왜곡될 여지가 있습니다. 과거 데이터 생성을 위해서는 `current_price`보다 오히려 매입단가(`purchase_price`)를 Fallback으로 사용하는 것이 논리상 '미래 가격 참조(Look-ahead bias)'를 막을 수 있습니다.

3. **Multi-currency 연산의 책임 분산 이슈**
   - 현재 환율 변환 로직이 대시보드 렌더링 계층(`render.py` -> `fx_utils.py`)에 강하게 결합되어 있어, 백엔드 계산 모듈 자체는 자산의 원본 통화만 신경 쓰고 있습니다.
   - 향후 USD 자산과 KRW 자산 혼합 포트폴리오의 정확한 TWR, 기여도 계산을 백엔드에서 수행하려면 백엔드 계산기에서 `fx_service`를 주입받아 베이스 통화(KRW 등)로 통일하는 작업을 직접 수행하도록 구조가 변경되어야 합니다.