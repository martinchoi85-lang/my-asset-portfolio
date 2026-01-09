# Asset Portfolio Management Web App

거래(Transaction) 중심의 개인 자산 포트폴리오 관리 웹앱입니다.
부부(향후 자녀 포함)의 여러 계좌와 자산을 하나의 포트폴리오로 통합하고,
**“얼마를 가지고 있는가”가 아니라 “어떻게 벌었는가”**를 분석하는 것을 목표로 합니다.

---

## 1. 프로젝트 철학 (Core Principles)

### 1) 모든 데이터의 시작은 Transaction

* 자산의 수량, 평균매입가, 평가금액은 **항상 거래의 누적 결과**
* 자산 상태를 직접 수정하는 기능은 제공하지 않음
* 예외: 수동평가 자산(manual assets)은 Snapshot Editor를 통해 날짜별 평가값을 명시적으로 입력

> **상태(state)는 저장하지 않는다.
> 상태는 항상 거래로부터 계산된다.**

---

### 2) DB는 저장소, 계산은 Python

* PostgreSQL(Supabase)은 데이터 저장소 역할
* 보유량, 평균단가, 수익률, 기여도, 비중 계산은 모두 Python 서비스 레이어에서 수행
* DB Trigger / 복잡한 SQL 집계는 사용하지 않음

---

### 3) daily_snapshots는 캐시된 계산 결과

* daily_snapshots는 원본 데이터가 아님
* 언제든 transactions로부터 재생성 가능해야 함
* 잘못되면 “수정”이 아니라 “리빌드”

---

## 2. 전체 아키텍처 개요

```
User Input
  ↓
Transaction Editor
  ↓
transactions (원본 데이터)
  ↓
portfolio_calculator / daily_snapshot_generator
  ↓
daily_snapshots (계산 결과 캐시)
  ↓
portfolio_service / weight / contribution services
  ↓
Streamlit Dashboard
```

---

## 3. 주요 테이블 설명

### transactions

* 모든 자산 변동의 유일한 원본
* BUY / SELL / INIT / DEPOSIT / WITHDRAW
* BUY/SELL 시 현금 자동 반영(auto_cash) 가능

### assets

* 자산 메타데이터
* ticker, currency, asset_type, price_source
* current_price는 “현재가”만 보유 (과거 가격 히스토리 없음)

### daily_snapshots

* (date, asset_id, account_id) 단위 스냅샷
* quantity, valuation_price, valuation_amount, purchase_price
* 계산 결과 캐시

---

## 4. Snapshot 생성 로직

* 특정 계좌 + 자산에 대해
* 거래일 ~ end_date까지 날짜 단위로 순회
* 해당 날짜까지의 거래를 누적 적용
* 결과를 daily_snapshots에 upsert

⚠️ 현재 버전(V1.1)에서는:

* 과거 가격 히스토리가 없어
* 모든 날짜에 동일한 current_price가 적용됨
  → 과거 날짜의 평가금액이 동일한 것은 **의도된 설계**

---

## 5. Price Updater

* 외부(yfinance)에서 자산의 현재가(current_price)만 업데이트
* manual 자산은 업데이트 대상 제외
* 가격 업데이트 실패는 앱 전체 실패로 이어지지 않아야 함

---

## 6. Dashboard 구성 원칙

* 기본 화면은 **“오늘 기준(as-of)”**
* 1M / 3M / YTD는 분석용 보조 수단
* total_amount = 0 인 날짜는 대부분의 시계열 분석에서 제외

---

## 7. 현금(auto_cash) 처리

* BUY → 현금 WITHDRAW
* SELL → 현금 DEPOSIT
* cash는 quantity=금액, price=1 고정
* 현금도 일반 자산과 동일하게 snapshot 생성

⚠️ SELL 시 보유량 초과 매도는 반드시 차단해야 함

---

## 8. 현재 설계의 한계 (의도된 제약)

* 과거 가격 히스토리 없음
* FX 손익 분해 미구현
* 대규모 리빌드 성능 최적화 미완

→ 단계적 개선 대상

---

## 9. 향후 로드맵 (요약)

* 거래 수정/삭제
* SELL 보유량 검증 강화
* snapshot 증분 리빌드
* 가격 히스토리 테이블 도입
* FX 손익 분해
* 배당(cash flow) 분리
* 정기 매수(Recurring Buy)

---

## 10. 핵심 문장

> **이 앱에서 “상태”는 존재하지 않는다.
> 상태는 항상 거래로부터 계산된다.**