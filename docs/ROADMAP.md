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
- [x] **시스템 안정성 개선**:
    - [x] **대규모 데이터 조회 최적화**: Supabase 1000행 제한 해결을 위한 Pagination 로직 구현 (`query.py`)
- [x] **차트 고도화**:
    - [x] **자산별 수익률/가격 비교**: Plotly Dual-Axis 차트 적용 (수익률 vs 가격 동시 표시)

---

## 🚀 Upcoming Features (Priority High)

### 1. 📈 총 평가금액 추세(Trend) 차트
- **목표**: 자산의 시계열 변화를 한눈에 파악
- **상세**:
    - `daily_snapshots` 기반 라인 차트 구현
    - 전체/계좌별/자산별 필터링 지원
    - 기간 설정 (1M, 3M, 6M, 1Y, YTD, All)

### 2. 🗓️ 기간별 성과 분석 (Period Analysis)
- **[x] 완료**: 특정 기간 동안의 자산 증감 분석 기능 구현
    - [x] 기간 선택기(Date Range Picker) 도입
    - [x] `Start Date` vs `End Date`의 Net Worth 비교
    - [x] 해당 기간 동안의 입출금(Cash Flow) 반영하여 실제 투자 손익 추정 (Modified Dietz)

### 3. 💰 실현손익(Realized P&L) 계산
- **[x] 완료**: 매도(SELL) 거래 발생 시 이동평균법(Average Cost) 기준 실현손익 계산 및 대시보드 표시 구현
    - [x] 매도 거래 발생 시 실현손익을 기록하도록 `transactions` 테이블에 `realized_pnl` 컬럼 추가 및 로직 연동
    - [x] 수수료(fee) 및 세금(tax)을 매수 원가 및 매도 실현손익에 반영 (`portfolio_calculator.py`)
    - [x] 과거 거래 추가/수정/삭제 시 실현손익 자동 재계산 및 정합성 보장 로직 구현 (`transaction_service.py`)
    - [x] 대시보드 내 전체 KPI 카드 및 거래 내역 탭에 실현손익 표시 추가 (`render.py`)
    - [x] 성과 및 이력 탭에 자산별(기여도 Top 10) / 월별 누적 실현손익 차트 추가 완료

### 4. 🧩 동적 그룹화 차트 개선 (Dynamic Grouping)
- **[x] 완료**: 다양한 카테고리 기준으로 자산 현황 조회 (1차 구현)
    - [x] 기존 도넛 차트/트리맵 강화
    - [x] Group By 옵션 다양화: 자산군(Type), 통화(Currency), 시장(Market) 등
    - [ ] Drill-down 기능 (예: 주식 -> 미국주식 -> 기술주) (추후 과제)

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

### 9. 자산 가격 업데이트 시 현재 로그인한 사용자의 자산만 업데이트 되도록 수정
- **문제**: 자산 가격 업데이트 시 현재는 현재 로그인한 사용자의 자산 이외에 DB assets 테이블의 모든 자산이 업데이트 
- **제한사항**: 해당 내용을 구현함에 있어 DB CRUD나 연산량이 크게 증가해서 app performance에 영향을 많이 준다면 보류

### 10. User 로그인 이후 세션 유지
- **문제1**: 반복적인 DB Create가 필요한 동작(예. transaction 입력)을 수행할 때 소요시간이 너무 오래 걸림
- **문제2**: 로그인 후 refresh(F5키 누름)하면 로그아웃 됨
- **해결방안**: 로그인을 하면 일정 시간 동안 세션을 유지하고, transaction 입력은 다수의 건을 입력 후에 한 번에 DB에 저장하는 방식으로 변경

---

## 🛠️ Refactoring & System Stability
1. **스냅샷 생성 로직 안정화**: 스케줄러 도입 (GitHub Actions or Cron)
2. **데이터 무결성 검증**: 마이너스 잔고 방지, 중복 거래 방지 로직 강화
3. **테스트 코드 작성**: 주요 계산 로직(수익률, 평단가) 단위 테스트