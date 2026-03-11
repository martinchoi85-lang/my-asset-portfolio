# 🛠️ 향후 리팩토링 계획 (Refactoring Plan)

이 문서는 프로젝트가 더 커지기 전에 **안정성(Stability), 수월한 유지보수성(Maintainability), 그리고 확장 가능성(Scalability)**을 확보하기 위해 수행해야 할 리팩토링 목표와 단계별 계획을 정의합니다. 현재 발견된 병목 지점, 데드 코드, 그리고 잠재적 로직 버그를 기반으로 작성되었습니다.

---

## 🛑 현재 아키텍처의 주요 문제점 (Pain Points)

1. **Snapshot 계산 로직의 비효율성 및 평가금 왜곡 가능성**
   - 매번 자산의 Snapshots을 리빌드할 때마다 '과거 모든 트랜잭션'을 불러와 처음부터 잔고를 계산합니다. (O(N) 성능 저하 요인)
   - 과거 날짜의 매일매일 평가금(valuation_amount)을 계산할 때, 해당 일자의 가격 히스토리가 DB에 없으면 **`assets.current_price` (최신 가격)를 사용**하여 과거의 평가금액을 왜곡시킵니다.
   
2. **Multi-Currency(다중 통화) 책임 분산**
   - 현재 백엔드의 `portfolio_calculator`는 개별 통화 기반으로 계산하고, 환율 변환(USD -> KRW)은 대시보드(`fx_utils.py` 및 `render.py`) 계층에서 처리되고 있습니다. 이는 향후 포트폴리오全体の TWR(Time-Weighted Return) 등 정교한 백테스팅 및 수익률 복합 계산을 어렵게 만듭니다.

3. **파편화된 유틸리티와 남겨진 데드 코드**
   - `portfolio_calculator.py`에 더 이상 쓰이지 않는 주석 블록, `daily_snapshot_generator.py`의 중복 임포트, 그리고 대시보드 쪽에 남겨진 데이터 조작 패키지(`data.py`) 등 정리가 필요한 코드가 혼재합니다.

---

## 📍 리팩토링 마일스톤 (Milestones)

### Phase 1: 로직 결함 수정 및 데드 코드 대청소 (Clean-up & High Priority Fixes)
- **Dead Code 삭제**: `NOTES.md`에 기록된 주석 처리된 레거시 함수 및 중복된 Import 구문을 모두 삭제.
- **파일 재배치(Re-structure)**: `dashboard` 디렉터리 하단에 있는 `fx_utils.py`와 `data.py` 내부 로직 중 백엔드 코어에 속해야 할 로직을 `backend/services`나 `backend/utils` 하위로 이동.
- **Snapshot 가격 Fallback 로직 수정**: `portfolio_calculator.calculate_daily_snapshots_for_asset` 내부의 Fallback을 `current_price`에서 `purchase_price`(매입원가)로 변경하여 과거 데이터가 미래 가격으로 부풀려지는 것을 차단.

### Phase 2: Snapshot 성능 최적화 (Performance Optimization)
- **점진적 스냅샷 빌드 (Incremental Build) 도입**:
  - 기존처럼 시작일부터 전체 역사를 다시 순회(O(N))하는 대신, 시작일 바로 전날(start_date - 1일)의 Snapshot 데이터를 조회.
  - 해당 데이터를 기반(Base State)으로 삼아 이후의 트랜잭션만 연산하도록 `calculate_daily_snapshots_for_asset` 재설계.

### Phase 3: Multi-Currency 아키텍처 서버 사이드 이관 (Domain Consistency)
- **환율 적용 계층 변경**: 대시보드의 `render.py`에서 FX 변환을 없애고, 백엔드의 `portfolio_service.py`나 `daily_snapshot_generator.py` 호출 결과물 자체에 Base Currency (예: KRW) 기준의 `valuation_amount_krw` 등을 포함하여 제공토록 설계.
- **FxService 고도화**: 트랜잭션 저장 시 해당 일자의 환율(FX Rate)을 함께 보관하는 필드 추가 (추가적인 DB Schema 변경 필요). 이를 바탕으로 진정한 '환차익'과 '자본수익'을 분별하는 기초 뼈대 마련.

### Phase 4: 테스트 코드 작성 및 검증 자동화 (Test Automation)
- 핵심이 되는 순수 함수 모듈(`portfolio_calculator.py` 등)부터 `pytest`를 활용한 Unit Test 구축.
- BUY/SELL 거래 누적에 따른 평균단가, 실현손익, 스냅샷 평가금액 검증 시나리오 작성.

---

> **비고**: 본 리팩토링은 한 번에 큰 규모로 진행하기보다, 기능 개발과 병행하여 **안전한 Phase 단위로 (특히 Phase 1 -> Phase 2 순서)** 진행하는 것을 권장합니다. 본 문서에 작성된 계획에 따라 향후 작업을 실행할 수 있습니다.
