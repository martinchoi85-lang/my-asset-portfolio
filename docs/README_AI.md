이 문서는 **새로운 AI 세션에서 이 파일 하나만 읽어도 프로젝트 전체 맥락을 이해하도록 설계**되었습니다.

> [!IMPORTANT]
> 프로젝트의 상세 진행 상황과 최신 파일 구조는 아래 문서를 함께 참조하세요.
> - **개발 로드맵**: [docs/ROADMAP.md](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/docs/ROADMAP.md)
> - **파일 및 함수 구조**: [docs/ARCHITECTURE.md](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/docs/ARCHITECTURE.md)
> - **DB 스키마(DDL)**: [docs/DB_SCHEMA.md](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/docs/DB_SCHEMA.md)
> - **개발 시 주의사항**: [docs/GUIDE-RAIL.md](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/docs/GUIDE-RAIL.md)

---

# 📌 Asset Portfolio App – AI Context Document

> **목적**: 새로운 AI 세션이 본 프로젝트의 **철학, 구조, 데이터 흐름, 정책, 현재 상태, 주요 이슈, 향후 로드맵**을 한 번에 파악하여 일관된 코드 작성을 돕기 위함입니다.

---

# 1️⃣ 프로젝트 개요 및 철학

## 프로젝트명
**Personal Asset Portfolio Tracker** (개인 자산 포트폴리오 관리 시스템)

## 핵심 목적
개인 투자 포트폴리오의 **자산 구성, 평가금액, 투자원금, 수익금액, 수익률(TWR/Modified Dietz), 자산 기여도**를 정확하고 일관된 로직으로 추적합니다.

## 핵심 원칙 (Core Philosophy)
1. **Single Source of Truth = Transactions**: 모든 데이터의 유입과 변경은 오직 거래(Transaction)를 통해서만 발생합니다. (수량/평가액 직접 수정 금지)
2. **Snapshot은 결과 테이블이다**: `daily_snapshots`는 거래를 기반으로 언제든 재생성 가능한 "계산 결과"입니다.
3. **TWR(Time-Weighted Return) & FIFO 기반**: 단순 수익률이 아니라 시간 가중 수익률과 선입선출(FIFO) 방식의 보유 기간 분석을 지향합니다.
4. **Phenomenon-First Logic**: 버그 리포트나 기능 설명 시 "현상"을 먼저 기술하고, 그에 따른 소프트웨어 원칙(SoC, DRY 등)을 적용합니다.

---

# 2️⃣ 시스템 아키텍처 및 기술 스택

## 기술 스택
- **Core**: Python 3.10+, Streamlit (Dashboard)
- **Backend**: Supabase (PostgreSQL), `yfinance` (Market Data), `pandas` (Data Processing)
- **Architecture**: Clean Architecture 기반의 Domain/Service 분리
- **Mobile**: FastAPI + React 기반 간소화 뷰어

## 디렉토리 구조 (요약)
```
src/asset_portfolio/
├── backend/           # 핵심 비즈니스 로직 및 인프라 계층
│   ├── infra/         # Supabase 연결 및 로우 쿼리 (query.py, supabase_client.py)
│   └── services/      # 도메인 서비스 (Transaction, Portfolio, Asset, FX 등)
├── dashboard/         # Streamlit 기반 메인 UI (관리 및 분석 풀 버전)
├── mobile/            # FastAPI/React 기반 모바일 최적화 뷰어
└── utils/             # 공통 유틸리티
```

---

# 3️⃣ 주요 도메인 로직 및 정책

## 자산 유형별 처리 방식
| 자산 유형 | Quantity | Valuation Price | Cost Basis 관리 |
| :--- | :--- | :--- | :--- |
| **STOCK/ETF** | 실제 보유 수량 | 시장가 (yfinance/KRX) | 매수 거래 기반 평단가 |
| **CASH** | 금액 자체 | 1.0 고정 | 1.0 고정 (원금=금액) |
| **MANUAL** | 1.0 고정 | 사용자가 입력한 평가액 | `manual_cost_basis` 테이블에서 별도 관리 |

## 핵심 서비스 정책
- **Transaction Mirroring**: BUY/SELL 거래 발생 시 해당 계좌의 CASH 자산이 자동으로 증감하도록 미러링 트랜잭션을 생성합니다. (내부 흐름과 외부 자본 흐름 `is_external_flow` 구분)
- **Snapshot Incremental Build**: 모든 스냅샷을 매번 재계산하지 않고, 변경된 시점 이후만 업데이트하는 증분 빌드 방식을 사용합니다.
- **As-Of Lookup**: 실시간 지표 계산 시 각 자산별로 "특정 시점의 최신 상태"를 찾아 합산하는 로직을 통일하여 사용합니다.

---

# 4️⃣ 현재 개발 상태 (Current Milestone)

## 최근 완료된 주요 기능
- **실현손익(Realized P&L)**: 이동평균법 기반 매도 수익 계산 및 기간별 누적 차트 구현.
- **기간 성과 분석**: Modified Dietz 공식을 적용하여 순입출금이 반영된 정확한 기간 수익률 산출.
- **보유 기간 분석**: FIFO 기반의 장기/단기 보유 비중 및 가중 평균 보유일 시각화.
- **동적 그룹화**: 전략 유형, 자산군, 통화 등 다양한 기준으로 포트폴리오를 분해하는 파이 차트/트리맵.
- **HTS 임포터 고도화**: 증권사별 파싱 규칙을 DB에서 관리(Profile Editor)하고 클립보드 붙여넣기 지원.
- **시스템 최적화**: Supabase 1,000행 제한 해결을 위한 Pagination 적용 및 스냅샷 생성 속도 개선.

## 진행 중인 과제
- **Look-through 분석**: ETF/펀드 내부 구성 종목을 분해하여 실제 자산 비중에 투과하는 기능.
- **매크로 지표 오버레이**: 기준금리, 환율 등 거시 지표와 포트폴리오 추세 결합.
- **데이터 무결성 가드**: 마이너스 잔고 방지 및 미인식 거래 자동 매핑 강화.

---

# 5️⃣ AI를 위한 작업 가이드라인

1. **로직 수정 시**: 항상 `backend/services` 계층의 해당 서비스를 먼저 수정하고, 필요시 `dashboard`나 `mobile`의 UI를 업데이트하세요.
2. **데이터 조회 시**: `infra/query.py`의 기존 함수를 재사용하거나, 대량 데이터의 경우 반드시 페이지네이션을 고려하세요.
3. **비즈니스 로직 주석**: 복잡한 Flow 변환이나 비동기 로직에 대해서만 간결한 **한국어 주석**을 작성하세요.
4. **시각화 요청 시**: Mermaid 다이어그램이나 시퀀스 다이어그램을 먼저 제시하여 구조적 합의를 본 뒤 코드를 작성하세요.
5. **리소스 효율성**: Mini PC 환경을 고려하여 메모리 집약적인 작업은 `pandas` 벡터화 연산을 우선 사용하세요.

---

# 6️⃣ 결론 및 비전
이 프로젝트는 단순한 자산 기록장을 넘어 **"이벤트 기반의 포트폴리오 계산 엔진"**을 지향합니다. 모든 제안은 확장성과 데이터 정합성을 최우선으로 고려해야 합니다.