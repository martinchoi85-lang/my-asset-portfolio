# Phase 5: 미래 확장 파이프라인 구축 – 구현 계획서

## 배경 및 목표

Phase 1~4를 통해 백엔드 도메인 분리, Two-Track UI, 캐싱 아키텍처, 세션 고도화가 완성되었습니다.  
Phase 5는 이 기반 위에서 **분석 기능의 질적 상승**과 **AI 연동 통로 개방**을 목표로 합니다.

4가지 과제를 우선순위 순서대로 구현합니다:

1. 🔬 복합 자산 Look-through 분석  
2. 🌐 거시 지표 오버레이  
3. ⚖️ 리밸런싱 시뮬레이터  
4. 🤖 AI 리포트(MCP) 인터페이스  

---

> [!NOTE]
> **확정된 설계 결정사항**
> - **과제 3 목표 비중 저장**: `user_rebalancing_targets` DB 테이블 신설, 영구 저장 (Option B)
> - **과제 1 Look-through 아키텍처**: 서비스 레이어 헬퍼 함수(`lookthrough_service.py`) 방식 — 영향받는 컴포넌트 2개(파이 차트, 리밸런싱)에서만 명시적 호출

---

## 과제 1: 복합 자산 Look-through 분석

### 배경

`asset_segments` 테이블은 이미 DB에 존재 (`asset_id + segment_asset_class + weight`)하지만,  
현재 UI나 백엔드 서비스에서 전혀 활용되지 않고 있습니다.  
TDF·펀드 자산의 내부 자산군 배분(예: 주식 70%, 채권 30%)을 수동 입력 받고,  
이를 기존 `동적 그룹화 파이 차트`에 반영하는 것이 목표입니다.

### 아키텍처 결정: 서비스 레이어 헬퍼 함수 방식

**Look-through가 실제로 필요한 컴포넌트:**
| 컴포넌트 | 필요 여부 | 이유 |
|---|---|---|
| KPI, 스냅샷 테이블, 추세 차트 | ❌ | 개별 자산 단위 또는 단순 합산 |
| **동적 그룹화 파이 차트** | ✅ | 자산군 집계 필요 |
| **리밸런싱 시뮬레이터** | ✅ | 목표 vs 현재 자산군 비중 비교 |

→ **영향 컴포넌트가 2개에 불과**하므로, 단일 헬퍼 함수를 만들어 해당 컴포넌트에서만 명시적 호출

### 변경 파일 계획

#### [NEW] [lookthrough_service.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/backend/services/lookthrough_service.py)
```python
def apply_lookthrough_to_grouping_df(df: pd.DataFrame, supabase) -> pd.DataFrame:
    """
    자산군 집계 DataFrame에서 lookthrough_available=True 자산을
    asset_segments 비중으로 분해(explode)합니다.
    
    - lookthrough 미적용 자산: 원래 자산군으로 그대로 집계
    - lookthrough 적용 자산: valuation_amount를 비중대로 쪼개서 여러 행으로 분해
    """
```

#### [MODIFY] [asset_editor.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/dashboard/asset_editor.py)
- `render_asset_editor()` 함수 내, `lookthrough_available = True`인 자산(펀드/TDF) 편집 시
- **Look-through 세그먼트 편집 UI 추가**: 기존 자산 속성 폼 하단에 `st.data_editor` 삽입
  - 컬럼: `자산군(segment_asset_class)`, `비중(%)(weight)` 
  - 비중 합계 ≠ 100% 시 저장 불가 경고
  - `저장` 클릭 시 `asset_segments` 테이블에 UPSERT

#### [MODIFY] [query.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/backend/infra/query.py)
- `def get_asset_segments(asset_id)`: 특정 자산의 세그먼트 조회
- `def upsert_asset_segments(asset_id, segments)`: 전체 삭제 후 재삽입

#### [MODIFY] [render.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/dashboard/render.py)
- `load_asset_grouping_summary()` 에서 DB 조회 직후 `apply_lookthrough_to_grouping_df()` 호출
- 나머지 컴포넌트는 변경 없음
- 예시: TDF(시가 1,000만) + {주식 70%, 채권 30%} → 주식 700만 + 채권 300만으로 분해 후 기존 집계에 합산

### UI 흐름

```
[자산 정보 수정] 메뉴
  → 자산 선택 드롭다운
  → 자산이 fund/tdf이고 lookthrough_available=True면
    → "📊 세그먼트 구성 (Look-through)" 섹션 노출
    → 자산군 / 비중(%) 2열 편집 테이블
    → [저장] 버튼
```

### 주의사항
- `asset_segments` 미입력 자산은 `asset_type` 기준으로 그대로 집계 (이전 동작 유지)
- `lookthrough_available` 컬럼이 아직 `False`인 자산은 해당 UI 미노출

---

## 과제 2: 거시 지표 오버레이

### 배경

포트폴리오 추세 차트(`render_portfolio_trend_chart`) 내 혹은 성과 탭 하단에  
미국 10년물 국채 금리(^TNX), 달러-원 환율(USDKRW=X) 등의 거시 지표를 오버레이하여  
자산 흐름과의 상관관계를 시각적으로 파악할 수 있도록 합니다.

### 변경 파일 계획

#### [NEW] [macro_service.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/backend/services/macro_service.py)
```python
MACRO_INDICATORS = {
    "미국 10년물 국채 금리": "^TNX",
    "달러-원 환율": "KRW=X",
    "S&P 500": "^GSPC",
    "KOSPI": "^KS11",
}

@st.cache_data(ttl=3600)
def load_macro_series(indicator_key: str, start_date: str, end_date: str) -> pd.DataFrame:
    """yfinance로 거시 지표 시계열 데이터를 가져옵니다."""
    ...
```

#### [MODIFY] [render.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/dashboard/render.py)
- `render_portfolio_trend_chart()` 하단에 **"🌐 거시 지표 오버레이"** 섹션 추가
  - `st.multiselect`로 지표 1~2개 선택
  - 선택된 지표를 secondary y-axis(우축)에 오버레이
  - 포트폴리오 평가금액은 좌축(기존), 거시 지표는 우축
  - 데이터 없음(주말/휴일) 시 forward-fill 처리

### UI 흐름

```
[성과 (Performance)] 탭 → 자산 추세 (Trend) 섹션
  → 기존 차트 하단에 구분선
  → "🌐 거시 지표 오버레이" 섹션
    → multiselect: [미국 10년물 국채 금리, 달러-원 환율, S&P 500, KOSPI]
    → 선택 지표가 동일 기간 내 우측 Y축으로 오버레이 표시
```

### 주의사항
- 거시 지표는 단순 **참고 정보**이므로, 없어도 대시보드 오류가 발생하지 않도록 방어 처리
- yfinance 호출 실패 시 `st.warning("거시 지표를 불러오지 못했습니다.")` 처리

---

## 과제 3: 리밸런싱 시뮬레이터

### 배경

사용자가 자산군별 목표 비중(%)을 설정하면,  
현재 실제 비중과 비교하여 **매수/매도 권고 금액**을 자동으로 계산해 줍니다.  
`render_target_vs_actual_weight_section`이 이미 구현되어 있으므로, 이를 확장합니다.

> [!NOTE]
> **확정**: DB 테이블 신설(Option B)로 진행합니다. 사용자가 설정한 목표 비중은 `user_rebalancing_targets` 테이블에 영구 저장됩니다.

### 변경 파일 계획

#### [NEW] DB 테이블: `user_rebalancing_targets`
```sql
CREATE TABLE user_rebalancing_targets (
  id bigint PRIMARY KEY GENERATED ALWAYS AS IDENTITY,
  user_id uuid NOT NULL REFERENCES users(id),
  account_id uuid NOT NULL REFERENCES accounts(id),
  asset_class text NOT NULL,         -- 자산군 (예: 'Equity', 'Fixed Income')
  target_weight numeric NOT NULL,     -- 목표 비중 (0~100)
  updated_at timestamptz DEFAULT now(),
  UNIQUE (user_id, account_id, asset_class)
);
```

#### [NEW] [rebalancing_simulator.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/dashboard/rebalancing_simulator.py)
```python
def render_rebalancing_simulator(user_id: str, account_id: str):
    """목표 비중 설정 및 리밸런싱 시뮬레이션 UI"""

    # 1) 현재 자산군별 평가금액 조회 (load_asset_grouping_summary + apply_lookthrough 재활용)
    # 2) DB에서 저장된 목표 비중 불러오기 (없으면 현재 비중으로 초기화)
    # 3) 자산군별 목표 비중 슬라이더 (합계 100% 실시간 검증)
    # 4) [저장] 버튼 → user_rebalancing_targets 테이블에 UPSERT
    # 5) [시뮬레이션 실행]: 현재 금액 × 목표 비중 = 목표 금액 → Delta 계산
    # 6) 결과 테이블 (매수: green, 매도: red)
```

#### [MODIFY] [app.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/dashboard/app.py)
- `menu_items`의 `📊 통합 대시보드` 하위 항목에 `"리밸런싱 시뮬레이터"` 추가
- 해당 페이지 라우팅: `render_rebalancing_simulator(user_id, account_id)` 호출

#### [MODIFY] [render.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/dashboard/render.py)
- `render_target_vs_actual_weight_section()`: 현재 목표 비중 표시 기능을  
  시뮬레이터 화면으로 이동하는 링크/안내 문구로 대체하거나,  
  기존 static 목표 biweight (하드코딩)를 제거하고 시뮬레이터로 일원화

### UI 흐름

```
[📊 통합 대시보드] → [리밸런싱 시뮬레이터]
  → 계좌 선택기
  → 현재 자산군별 평가금액 및 비중 표시
  → 자산군별 목표 비중(%) 슬라이더 (합계 100% 검증 실시간)
  → [시뮬레이션 실행] 버튼
  → 결과 테이블: [자산군 | 현재금액 | 현재비중% | 목표비중% | 목표금액 | 차이금액(매수/매도)]
  → 하단: "총 매수 예상 금액 / 총 매도 예상 금액" 요약
```

### 계산 공식

```
target_amount = total_portfolio_value × target_weight_pct / 100
delta = target_amount - current_amount
delta > 0 → 매수 권고
delta < 0 → 매도 권고
```

---

## 과제 4: AI 연동 준비 (MCP 인터페이스)

### 배경

사용자의 포트폴리오 현황을 **Markdown 형태의 컨텍스트 리포트**로 추출하여,  
Claude/GPT 등의 AI와 대화 시 그대로 붙여넣을 수 있도록 합니다.  
복잡한 API 서버 구현 대신, **Streamlit UI 내 "복사" 버튼** 방식으로 MVP를 구현합니다.

### 변경 파일 계획

#### [NEW] [ai_context_service.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/backend/services/ai_context_service.py)
```python
def build_ai_context_report(user_id: str, account_id: str) -> str:
    """
    현재 포트폴리오 상태를 AI가 이해하기 쉬운 Markdown으로 생성합니다.
    
    포함 내용:
    - 조회 시점, 계좌 정보
    - 총 평가금액 / 투자원금 / 평가손익
    - 자산군별 비중 (상위 10개)
    - 보유 종목 리스트 (자산명, 평가금액, 수익률)
    - 최근 30일 거래 내역 요약
    """
    ...
```

#### [MODIFY] [app.py](file:///c:/Users/MartinChoi/Documents/WorkSpace/asset-portfolio/src/asset_portfolio/dashboard/app.py)
- `menu_items`의 `🛠️ 시스템 관리` 하위에 `"AI 리포트 추출"` 추가
- 라우팅: `render_ai_context_report(user_id, account_id)` 호출

#### [NEW] ai_context_report 렌더링 (render.py 내 함수 추가 or 별도 파일)
```python
def render_ai_context_report(user_id: str, account_id: str):
    st.subheader("🤖 AI 리포트 추출")
    st.caption("아래 리포트를 복사하여 ChatGPT/Claude 등에 붙여넣으세요.")
    
    account_id = render_account_selector(user_accounts)  # 계좌 선택
    report = build_ai_context_report(user_id, account_id)
    
    st.code(report, language="markdown")  # 선택/복사 가능한 코드 블록으로 표시
    st.download_button("📥 파일로 저장", data=report, file_name="portfolio_context.md")
```

### UI 흐름

```
[🛠️ 시스템 관리] → [AI 리포트 추출]
  → 계좌 선택기
  → [리포트 생성] 버튼
  → 결과: Markdown 형태 코드 블록 (선택 후 복사 가능)
  → [📥 .md 파일로 저장] 버튼
```

### 리포트 예시 내용

```markdown
# 포트폴리오 현황 리포트
조회 시점: 2026-03-19 | 계좌: 키움_메인

## KPI 요약
- 총 평가금액: ₩ 108,432,000
- 투자원금: ₩ 100,000,000
- 평가손익: ₩ 8,432,000 (+8.43%)

## 자산군 비중
| 자산군 | 평가금액 | 비중 |
|--------|---------|------|
| 주식(ETF) | 65,000,000 | 59.9% |
| 예적금 | 30,000,000 | 27.7% |
...

## 보유 자산 현황
| 자산명 | 평가금액 | 수익률 |
...

## 최근 30일 거래 요약
- 2026-03-15 TIGER미국나스닥100 BUY 10주 @ 102,000
...
```

---

## 구현 순서 및 우선순위

| 순서 | 과제 | 이유 |
|------|------|------|
| 1 | AI 리포트(과제 4) | 가장 독립적이고 단순. 즉시 실용적 가치 창출 |
| 2 | 거시 지표(과제 2) | 기존 차트 확장 형태. 리스크 낮음 |
| 3 | 리밸런싱(과제 3) | 기존 `load_asset_grouping_summary` 재활용 가능 |
| 4 | Look-through(과제 1) | `asset_editor` UI 변경 + 집계 로직 변경 포함으로 가장 복잡 |

---

## 검증 계획

### 자동화 테스트
- 현재 테스트 파일: `backend/services/tests/` 디렉토리 확인 필요
- 신규 서비스(`ai_context_service.py`, `macro_service.py`)에 대한 유닛 테스트 작성 계획

### 수동 검증 항목

| 과제 | 검증 방법 |
|------|----------|
| Look-through | TDF/펀드 자산 세그먼트 입력 후, 동적 그룹화 파이 차트에서 해당 자산이 분해되어 집계되는지 확인 |
| 거시 지표 | 성과 탭의 추세 차트에서 지표 선택 후, 해당 기간의 실제 지표 값과 대조 |
| 리밸런싱 | 슬라이더로 목표 비중 변경 시 매수/매도 권고 금액이 실시간으로 정확히 계산되는지 확인 |
| AI 리포트 | 리포트 생성 → 내용이 현재 KPI/스냅샷 데이터와 일치하는지 직접 확인 |

### 검증 실행 방법
```
# 앱 실행
streamlit run .\src\asset_portfolio\dashboard\app.py

# 각 기능은 개발 후 Streamlit 앱에서 직접 내비게이션하여 확인
```
