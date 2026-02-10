# Project Roadmap

## ✅ Completed / In Progress
- [x] **기본 데이터 구조 설계**: Accounts, Assets, Transactions, Daily Snapshots 테이블
- [x] **대시보드 초기 구현**: 주요 자산 현황판, 도넛 차트
- [x] **데이터 입력/수정**:
    - [x] 거래 내역 추가/수정/삭제 (`transaction_editor.py`)
    - [x] 자산 정보 관리 (`asset_editor.py`)
    - [x] 스냅샷 수동 생성 및 조회 (`snapshot_editor.py`)
- [x] **정기 매수 관리**:
    - [x] 정기 매수 등록/수정/삭제 (`recurring_order_editor.py`)
    - [x] (Fix) 자산 정보 연동 에러 수정
- [ ] **가격 업데이트**:
    - [ ] `price_updater.py` (기본 구현 됨, 안정화 필요)

---

## 🚀 Upcoming Features (Priority High)

### 1. 📈 총 평가금액 추세(Trend) 차트
- **목표**: 자산의 시계열 변화를 한눈에 파악
- **상세**:
    - `daily_snapshots` 기반 라인 차트 구현
    - 전체/계좌별/자산별 필터링 지원
    - 기간 설정 (1M, 3M, 6M, 1Y, YTD, All)

### 2. 🗓️ 기간별 성과 분석 (Period Analysis)
- **목표**: 특정 기간 동안의 자산 증감 분석
- **상세**:
    - 기간 선택기(Date Range Picker) 도입
    - `Start Date` vs `End Date`의 Net Worth 비교
    - 해당 기간 동안의 입출금(Cash Flow) 반영하여 실제 투자 손익 추정

### 3. 💰 실현손익(Realized P&L) 계산
- **목표**: 매도 확정된 손익을 정확히 계산
- **상세**:
    - **FIFO/Average Cost** 로직 결정 필요 (우선 Average Cost 권장)
    - 매도(SELL) 거래 발생 시, 해당 자산의 평단가 대비 차익 계산 후 저장
    - `realized_gains` 테이블 또는 `transactions` 테이블에 컬럼 추가 검토

### 4. 🧩 동적 그룹화 차트 개선 (Dynamic Grouping)
- **목표**: 다양한 카테고리 기준으로 자산 현황 조회
- **상세**:
    - 기존 도넛 차트/트리맵 강화
    - Group By 옵션 다양화: 자산군(Type), 통화(Currency), 섹터(Sector), 지역(Region), 계좌(Account) 등
    - Drill-down 기능 (예: 주식 -> 미국주식 -> 기술주)

### 5. 🔍 TDF/펀드 세부 자산 분해 (Look-through)
- **목표**: ETF/펀드 내의 실제 보유 자산까지 분석
- **상세**:
    - 펀드/ETF의 구성 종목(Holdings) 데이터 모델링 필요
    - 예: TDF2050 보유 시 -> 주식 80%, 채권 20%로 분해하여 전체 포트폴리오 비중 계산

### 6. ⏳ 자산별 보유 기간(Holding Period) 분석
- **목표**: 장기/단기 투자 성향 분석 및 세금 최적화 기초 자료
- **상세**:
    - 최초 매수일(First Buy Date) 추적
    - 가중 평균 보유 기간(Weighted Average Holding Period) 계산
    - "Long-term vs Short-term" 비중 시각화

---

## 🔮 Future Backlog (Nice to Have)

### 7. 금액 기준 정기 매수 (Amount-based Recurring Buy)
- **목표**: "매월 50만원 매수"와 같은 금액 기준 설정
- **상세**:
    - 실행 시점의 현재가(Price)를 조회하여 수량(Quantity) 역산
    - `Quantity = Amount / Current Price` (소수점 처리 로직 필요)
    - 오차 보정 기능 (매수 후 실제 체결 수량/금액 보정)

### 8. 편의성 개선
- **로그인 세션 유지**: 모바일 환경에서 Refresh 시 로그아웃 되는 문제 해결 (Cookie/LocalStorage 활용)
- **배당 관리**: 배당금 입력 및 배당 수익률 시각화
- **환율(FX) 효과 분리**: 환차익과 자산 수익 분리하여 표시

---

## 🛠️ Refactoring & System Stability
1. **스냅샷 생성 로직 안정화**: 스케줄러 도입 (GitHub Actions or Cron)
2. **데이터 무결성 검증**: 마이너스 잔고 방지, 중복 거래 방지 로직 강화
3. **테스트 코드 작성**: 주요 계산 로직(수익률, 평단가) 단위 테스트