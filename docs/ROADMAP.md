# Project Roadmap

## ✅ Completed / In Progress
- [x] **기본 데이터 구조 설계**: Accounts, Assets, Transactions, Daily Snapshots 테이블
- [x] **대시보드 초기 구현**: 주요 자산 현황판, 도넛 차트
- [x] **데이터 입력/수정**:
    - [x] 거래 내역 개별 추가/수정/삭제 (`transaction_editor.py`)
    - [x] 거래 내역 대량 업로드 (CSV/Excel/클립보드 붙여넣기) 및 파싱 고도화 (`transaction_importer.py`)
    - [x] 자산 정보 관리 (`asset_editor.py`)
    - [x] 스냅샷 수동 생성 및 조회 (`snapshot_editor.py`)
- [x] **정기 매수 관리**:
    - [x] 정기 매수 등록/수정/삭제 (`recurring_order_editor.py`)
    - [x] (Fix) 자산 정보 연동 에러 수정
- [x] **가격 업데이트**:
    - [x] `price_updater.py` (자동/수동 가격 업데이트 통합 및 분기 처리 완료)
- [x] **시스템 안정성 개선**:
    - [x] **대규모 데이터 조회 최적화**: Supabase 1000행 제한 해결을 위한 Pagination 로직 구현 (`query.py`)
- [x] **차트 고도화**:
    - [x] **자산별 수익률/가격 비교**: Plotly Dual-Axis 차트 적용 (수익률 vs 가격 동시 표시)

---

## 🚀 Upcoming Features (Priority High)

### 1. 📈 총 평가금액 추세(Trend) 차트
- **[x] 완료**: 자산의 시계열 변화 및 원금 대비 변동성 시각화
    - [x] `daily_snapshots` 기반 라인 차트 구현 및 계좌별 조회 지원
    - [x] '총 평가금액(기본축)'과 '투자원금(보조축)'을 분리하여 스케일 격차 문제 해소 및 변동 추세 비교 강화
    - [x] 최근 30일(일간), 최근 12개월(월간), 최근 5년(연간) 기준의 자산 등락폭 바 차트 제공

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

### 5. ⏳ 자산별 보유 기간(Holding Period) 분석
- **[x] 완료**: 장기/단기 투자 성향 분석 및 세금 최적화 기초 자료 제공
    - [x] 트랜잭션 내역 대상 FIFO(선입선출) 로직 적용으로 잔여 수량 분리
    - [x] 잔여 로트별 최초 매수일(First Buy Date) 추적
    - [x] 가중 평균 보유 기간(Weighted Average Holding Period) 계산
    - [x] "Long-term vs Short-term" (1년 기준) 수량 비중 표 및 누적 막대 차트(Stacked Bar) 시각화 ('이력' 탭 하단)

### 6. 🔍 TDF/펀드 세부 자산 분해 (Look-through)
- **목표**: ETF/펀드 내의 실제 보유 자산까지 분석
- **상세**:
    - 펀드/ETF의 구성 종목(Holdings) 데이터 모델링 필요
    - 예: TDF2050 보유 시 -> 주식 80%, 채권 20%로 분해하여 전체 포트폴리오 비중 계산

---

---

## 🔮 Future Backlog (Phased Roadmap)

### Phase 1: 기반 아키텍처 및 도메인 정렬 (Backend Refactoring) [✅ 완료]
- **[x] Auto / Manual 자산의 논리적 완벽 분리**: DB 스키마 노출 방지 및 다형성 객체 기반 파이프라인 정립.
- **[x] 매뉴얼 자산 매도시 수익률 계산 방법 확정**: 예적금 부분 매도 및 전체 해지 시 원금 비례 차감 공식 설계/적용 완료.

### Phase 2: 대시보드 뷰어 및 사용자 맞춤 UI 개편 [✅ 완료]
- **[x] 메뉴 아키텍처 분리 (Two-Track UI)**: 동적인 시장 연동 자산과 정적 자산을 별도의 메뉴로 이원화.
- **[x] 맞춤형 액션 제공**: 예적금 전액 출금, 이자 수령, 납입원금 직접 수정 기능 제공 및 현금 미러링 지원.

### Phase 3: Transaction Importer 자동화 및 유연화
- **[ ] 통합 업로드 처리 파이프라인 완성**: 엑셀/CSV 한 장 안에 주식 매수와 예적금이 섞여 있어도 엔진이 알아서 분류.
- **[ ] Smart 파서 강화**: 누락된 Account 도출, Ticker 없을 경우 Fuzzy 매칭 보강.

### Phase 3.1: 시스템 관리 기능 확장
- **[x] 계좌 관리 및 HTS 템플릿 프로파일 관리 UI 도입**: 동적 파싱을 위한 시스템 환경설정 DB화.
- **[ ] Asset 추가 메뉴 기능 강화 (Asset Editor V2)**: HTS 의존성 없이 유연한 수동 자산 신규 편입 보장.

### Phase 4: 시스템 안정화 및 편의성 강화 (Performance & Monitoring)
- **[ ] 로그인 세션 분실 방지 (유지)**: 모바일 접속 등에서 앱 Refresh(F5) 시 로그아웃되는 세션 증발 문제 해결 (쿠키 및 LocalStorage 활용).
- **[ ] 일괄 처리(Batching) 기능 도입 (트랜잭션 지연 해소)**: 입력된 트랜잭션을 한 번에 Bulk Insert 하여 대량 생성에 걸리는 지연 시간 개선.
- **[ ] 금액 기준 정기 매수 (Amount-based Recurring Buy)**: '매월 50만 원' 등 금액을 설정하면, 현재가(Price)를 역산하여 수량을 구하고 소수점 오차 보정 기능을 도입.
- **[ ] 스냅샷 생성 / 가격 업데이트 오류 시 UI 알람 표출**: 업데이트 병목 시 크래시나는 대신 직관적인 경고 알람 제공.

### Phase 5: 복합 자산 및 뷰어 고도화 (Advanced Modeling)
- **[ ] TDF/펀드류 세부 자산 분해 (Look-through) 분석**: ETF, 퇴직연금펀드 등의 구성자산(주식/채권 비중) 데이터를 분해, 전체 포트폴리오 비중에 체인(Chain)으로 투과하여 표시.
- **[ ] 대시보드 : 투자 참고 프레임워크 (매크로 표출)**: 미국/한국 기준금리, 인플레이션율 등 주요 거시 매크로 데이터를 스냅샷 타임라인에 오버레이시켜 분석 지원.
- **[ ] 현재 사용자 자산 한정 업데이트 최적화 방안 조사**: 자산 가격 일괄 업데이트 시 속도 저하를 피하기 위해, 로그인한 사용자에게 귀속된 자산만 선택적 갱신(Performance Trade-off 판단 후).

### Phase 6: 지능적 AI 피드백 및 알리미 (Future AI Expansion)
- **[ ] 각종 지표 기반 매도 시점 알리미**: 환율 상단, 평가 수익률 N% 도달 등 지표 연계 알람 넛지 시스템 도입.
- **[ ] 데이터 인터랙션 / 오픈 AI 에이전트 브릿지**: MCP 등을 도입해 대시보드의 포트폴리오를 Markdown/JSON 변환 후 AI 프롬프트와 쉽게 연동.

---

## 🛠️ Refactoring & System Stability (Ongoing)
1. **[x] 스냅샷 생성(Daily Snapshot) 증분 빌드 고도화 완료**
   - 과거 스냅샷 기준 Incremental Build 도입으로 수명형 비효율 완전 해방.
2. **[x] 다중 통화(Multi-currency) 아키텍처 재설계 및 과거 환율 연동 반영 완료**
3. **[ ] 데이터 무결성 검증 강화**: 트랜잭션 추가 시 마이너스 잔고 검증 및 미인식 거래 방지 가드레일 확충.
4. **[ ] 단위/통합 테스트 커버리지 확대**: 수익률 산출, 평균 매입단가 계산 로직 테스트 작성.
5. **[ ] 배포 스케줄러 연동(Cron)**: 가격 자동 수집 스크립트 실행 스케줄 도입.