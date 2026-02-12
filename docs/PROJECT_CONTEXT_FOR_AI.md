이 문서는 **새로운 AI 세션에서 이 파일 하나만 읽어도 프로젝트 전체 맥락을 이해하도록 설계**되었습니다.

> **참고**: Supabase DB 스키마(DDL)는 [docs/DB_SCHEMA.md](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/docs/DB_SCHEMA.md)에 별도로 관리됩니다.

---

# 📌 Asset Portfolio App – AI Context Document

> 목적:
> 이 문서는 새로운 AI 세션(ChatGPT / Codex 등)이 본 프로젝트의
> **철학, 구조, 데이터 흐름, 정책, 현재 상태, 주요 이슈, 향후 로드맵**을
> 한 번에 이해하기 위한 컨텍스트 문서이다.

---

# 1️⃣ 프로젝트 개요

## 프로젝트명

Personal Asset Portfolio Tracker

## 핵심 목적

* 개인 투자 포트폴리오의

  * 자산 구성
  * 평가금액
  * 투자원금
  * 수익금액
  * 수익률
  * 자산 기여도(TWR 기반)
* 를 **정확하고 일관된 계산 로직**으로 추적

---

## 핵심 원칙

### 1. Single Source of Truth = Transactions

모든 데이터의 유입/변경은 **Transaction을 통해서만 발생한다.**

❌ Snapshot 직접 수정 금지
❌ 평가금액 직접 수정 금지
❌ 수량 직접 수정 금지

✔ 매수
✔ 매도
✔ 입금(DEPOSIT)
✔ 출금(WITHDRAW)

을 통해 상태가 변해야 한다.

---

### 2. Snapshot은 결과 테이블이다

`daily_snapshots`는:

* 상태를 저장하는 테이블
* 계산 결과를 저장하는 테이블
* 거래 기반으로 재생성 가능한 테이블

즉:

> Snapshot은 "진실"이 아니라 "계산 결과"이다.

---

### 3. TWR(Time-Weighted Return) 기반 기여도 계산

본 프로젝트는 단순 평가손익이 아니라:

* 자산별 기여도
* 기간 수익률
* TWR 기반 계산

을 목표로 한다.

---

# 2️⃣ 시스템 구조

## 전체 아키텍처

### 앱 구조

```
src/asset_portfolio/
  ├── backend/           # 백엔드 로직 계층
  │   ├── infra/         # DB 연결 등 인프라 계층
  │   └── services/      # 비즈니스 로직 서비스
  ├── dashboard/         # 데스크톱/웹 대시보드 앱 (Streamlit)
  └── mobile/            # 모바일 전용 간소화 앱 (Streamlit)
```

### 데이터 흐름

```
User Input (Dashboard/Mobile)
      ↓
TransactionService (backend/services)
      ↓
transactions 테이블 (Supabase)
      ↓
Daily Snapshot Generator
      ↓
PortfolioCalculator / PriceUpdaterService
      ↓
daily_snapshots 테이블
      ↓
PortfolioService / WeightService / BenchmarkService
      ↓
Dashboard UI / Mobile UI
```

### Backend 서비스 계층

**Infrastructure (`backend/infra/`):**
- Supabase 연결 관리
- DB 세션 관리

**Services (`backend/services/`):**
- `transaction_service.py`: 거래 추가/수정/삭제 로직
- `asset_service.py`: 자산 메타데이터 관리
- `daily_snapshot_generator.py`: 스냅샷 생성 엔진
- `portfolio_calculator.py`: 포트폴리오 계산 로직 (보유량, 평단가, 수익률)
- `portfolio_service.py`: 포트폴리오 조회 서비스
- `portfolio_weight_service.py`: 자산 비중 계산
- `price_updater_service.py`: 가격 업데이트 로직
- `krx_price_fetcher.py`: KRX 데이터 수집
- `manual_cost_basis_service.py`: Manual 자산 원금 관리
- `benchmark_service.py`: 벤치마크 비교 분석
- `fx_service.py`: 환율 처리
- `snapshot_frame.py`: 스냅샷 데이터프레임 처리

### 앱별 특징

**Dashboard (`dashboard/`):**
- 데스크톱/웹 브라우저용 풀 버전
- 모든 편집 기능 포함 (거래, 자산, 스냅샷, 정기매수)
- 상세 차트 및 분석
- 파일: `app.py`, `render.py`, `transaction_editor.py`, `asset_editor.py`, `snapshot_editor.py`, `recurring_order_editor.py`, `price_updater.py`, `transaction_importer.py`

**Mobile (`mobile/`):**
- 모바일 전용 간소화 UI
- 조회 중심 (편집 기능 최소화)
- 빠른 로딩과 간결한 인터페이스
- 파일: `app.py`, `data.py`

---

# 3️⃣ 주요 테이블 개념

> **상세 스키마**: 전체 DB DDL은 [docs/DB_SCHEMA.md](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/docs/DB_SCHEMA.md)를 참조하세요.
> 아래는 주요 테이블에 대한 개념 설명입니다.

## 1. assets

* 자산 정의
* auto_update 여부
* 자산 타입 (STOCK, ETF, CASH, MANUAL 등)

---

## 2. transactions

* 모든 변화의 출발점
* BUY
* SELL
* DEPOSIT
* WITHDRAW

---

## 3. daily_snapshots

* 날짜별 자산 상태
* quantity
* valuation_price
* valuation_amount
* purchase_amount

---

## 4. asset_price_history (추가됨)

도입 이유:

* 과거 거래 수정 가능성
* 과거 snapshot 재생성 필요
* 과거 가격 보존 필요

> 가격 히스토리가 없으면 과거 snapshot은 왜곡된다.

---

## 5. manual_asset_cost_basis_events

## 6. manual_asset_cost_basis_current

manual 자산의 원금 추적을 위한 구조

manual 자산은:

* 수량 개념이 없음
* 평가금액 수기 입력
* 원금은 별도 이벤트로 관리

---

# 4️⃣ 자산 유형 정책

| 자산 유형  | quantity 의미 | valuation_price | purchase_price |
| ------ | ----------- | --------------- | -------------- |
| STOCK  | 실제 수량       | 현재 주가           | 매수 단가          |
| ETF    | 실제 수량       | 현재 주가           | 매수 단가          |
| CASH   | 평가금액        | 1               | 1              |
| MANUAL | 1 고정        | 현재 평가금액         | 1 (원금은 별도 관리)  |

---

# 5️⃣ Daily Snapshot 설계 철학

## 실행 방식

Windows Task Scheduler

* 매일 07:10
* run_daily_job.py 실행

---

## 기존 문제

* 최근 60일을 매일 재생성
* current_price를 과거에도 적용
* 결과적으로 과거 snapshot이 왜곡

---

## 수정 방향

✔ 가격 히스토리 기반 스냅샷 생성
✔ 과거 날짜는 해당 날짜의 price 사용
✔ 과거 데이터는 덮어쓰지 않음

---

# 6️⃣ Manual Asset 정책

Manual 자산은:

* 시장 가격 API 없음
* 수기 가격 입력
* snapshot editor로 수정 가능

### 중요한 구분

* 현금 유입 ≠ 특정 자산 원금 증가
* 배당은 현금 유입
* 추가 매수는 자산 원금 증가

---

# 7️⃣ 미국 주식 환율 이슈

현재 구조:

* KRW 기준 계산
* 매수 시 환율 저장하지 않음

문제:

* 환율 변동 수익률 반영 불가

향후 필요:

* transaction에 fx_rate 저장
* USD 계좌 개념 도입
* 통화별 snapshot 구조 고려

---

# 8️⃣ 가격 수집 전략

## 현재 구현

* **yfinance**: 해외 주식/ETF 가격 수집
* **KRX Price Fetcher**: 국내 주식/ETF 가격 수집 (구현 완료)
  - `krx_price_fetcher.py` 서비스 추가
  - KRX 공식 데이터 활용

## 향후 개선

* 자동 가격 수집 플러그형 구조
* UI에서 데이터 소스 등록 가능 구조 설계 필요
* 가격 히스토리 보존 강화

---

# 9️⃣ 현재까지 발생한 주요 구조적 이슈

### 1. Snapshot 재생성 문제

과거 가격 왜곡

### 2. Manual 자산 원금 추적 문제

수익률 계산 불가

### 3. Transaction으로 스냅샷 보정 어려움

오랜 기간 왜곡된 데이터 수정 난이도 높음

### 4. Cash vs Asset 원금 개념 혼동

---

# 🔟 향후 예상되는 이슈

* 과거 거래 수정
* 다중 통화
* 세금 계산
* 배당 처리
* 리밸런싱
* 성과 attribution 정교화
* 성능 문제 (snapshot 증가 시)
* 트랜잭션 동시성

---

# 11️⃣ 리팩토링 필요성

현재 코드 문제:

* 로직 중복
* manual 자산 예외처리 다수
* snapshot rebuild 경로 복잡
* validation 분산

필요 작업:

* Transaction → Event sourcing 구조 정리
* Snapshot 계산 모듈화
* PriceProvider 추상화
* 통화 계층 분리

---

# 12️⃣ 앞으로의 설계 방향 (중요)

## 장기 목표

* 이벤트 기반 구조
* 가격 히스토리 완전 분리
* 통화 독립 구조
* 자산 유형 확장 가능 구조

---

# 13️⃣ AI에게 요청할 때 고려사항

AI는 다음을 반드시 이해해야 한다:

1. Snapshot은 결과 테이블
2. Transactions만 상태 변경 가능
3. 과거 거래 수정은 항상 발생할 수 있음
4. 가격 히스토리는 필수
5. manual 자산은 특수 케이스
6. 통화 문제는 아직 완성되지 않음

---

# 14️⃣ 현재 상태 요약

✔ Snapshot 자동 생성
✔ Price history 도입
✔ Manual cost basis 구조 도입
✔ KRX 가격 수집 구현 완료
✔ Dashboard / Mobile 앱 분리
✔ Backend 구조 개선 (infra/services 분리)
✔ 정기 매수(Recurring Order) 기능 구현
✔ 벤치마크 비교 기능 추가
✔ FX(환율) 서비스 추가
⚠️ 구조 리팩토링 필요

---

# 15️⃣ 향후 로드맵

## 구현 완료 항목

* ✅ 기본 데이터 구조 (Accounts, Assets, Transactions, Snapshots)
* ✅ Dashboard 초기 구현 (도넛 차트, 자산 현황판)
* ✅ 거래 내역 관리 (추가/수정/삭제)
* ✅ 자산 정보 관리
* ✅ 스냅샷 수동 생성 및 조회
* ✅ 정기 매수 관리 (등록/수정/삭제)
* ✅ KRX 가격 수집
* ✅ 모바일 앱 분리

## Phase 1: 데이터 시각화 및 분석 강화 (우선순위 높음)

* 📈 총 평가금액 추세(Trend) 차트
* 🗓️ 기간별 성과 분석 (Period Analysis)
* 💰 실현손익(Realized P&L) 계산
* 🧩 동적 그룹화 차트 개선 (자산군/통화/섹터별)
* 🔍 TDF/펀드 세부 자산 분해 (Look-through)
* ⏳ 자산별 보유 기간 분석

## Phase 2: Multi-currency 및 수익률 고도화

* Multi-currency 구조 확립
* USD 계좌 개념 도입
* 환율 반영 수익률 계산
* TWR(Time-Weighted Return) 기반 기여도 계산

## Phase 3: 고급 분석 및 최적화

* 성과 attribution 고도화
* 세금 반영
* 리밸런싱 분석
* 배당 관리 및 시각화

## Phase 4: 시스템 안정화

* PriceUpdater 안정화
* 데이터 무결성 검증 강화
* 테스트 코드 작성
* 스케줄러 도입 (GitHub Actions or Cron)

---

# 16️⃣ AI 세션 시작 시 안내 문구

새 AI 세션에서:

> 이 프로젝트는 Transaction 기반 포트폴리오 시스템이며 Snapshot은 계산 결과 테이블이다.
> PROJECT_CONTEXT_FOR_AI.md를 기준으로 구조를 이해하고 제안하라.

---

# 17️⃣ 이 문서의 목적

이 문서는:

* 채팅창이 길어질 때
* 새로운 AI 세션 시작 시
* 리팩토링 시작 시
* 구조 검토 요청 시

항상 동일한 철학과 맥락을 유지하기 위한 기준 문서이다.

---

# ✅ 결론

이 프로젝트는 단순한 자산 조회 앱이 아니라:

> **이벤트 기반 포트폴리오 계산 엔진을 지향한다.**

현재는 중간 단계이며,
구조적 정리가 필요한 시점이다.