# 🚀 Project Guide-rail for AI Coding Assistant

이 문서는 AI가 본 프로젝트의 코드를 생성하거나 수정할 때 **반드시 준수해야 할 기술적 제약 사항과 빈번한 실수 방지 가이드**를 담고 있습니다. 모든 코드 변경 전에 이 내용을 먼저 숙지하십시오.

---

## 1. Supabase & DB Integrity

### ⚠️ BigInt & Null Safety (Error: 22P02)
*   **현상**: `id` 컬럼(BigInt/UUID)에 `None` 또는 `""`(빈 문자열)이 들어간 상태로 `.eq()` 필터를 걸면 PostgREST API 에러가 발생함.
*   **가이드**:
    *   DB 조회를 수행하기 전, ID 변수가 `None`이 아닌지 반드시 먼저 검증하라.
    *   특히 **"신규 등록 모드"**에서는 아직 ID가 생성되지 않았으므로, ID 기반의 연관 데이터 조회(예: `_load_asset_price_source`)를 건너뛰거나 기본값을 반환하도록 예외 처리하라.
    ```python
    # Bad
    res = supabase.table("table").select("*").eq("id", some_id).execute()
    
    # Good
    if some_id:
        res = supabase.table("table").select("*").eq("id", some_id).execute()
    else:
        res = [] # 또는 적절한 기본값
    ```

---

## 2. Streamlit UI Components

### ⚠️ Deprecation Awareness (Streamlit 1.40+)
*   **현상**: `use_container_width=True` 매개변수는 향후 삭제될 예정(Deprecation)이며 경고를 발생시킴.
*   **가이드**:
    *   `st.dataframe`, `st.plotly_chart`, `st.form_submit_button` 등에서 너비를 조절할 때 `use_container_width=True` 대신 **`width='stretch'`**를 사용하라.
    *   고정 폭이 필요한 경우 `width='content'`를 고려하라.

---

## 3. Dashboard Editor Patterns

### 🔄 Mode-Awareness (Edit vs Add)
*   **현상**: 하나의 에디터에서 수정과 등록을 동시에 처리할 때, 수정 전용 로직(ID 기반 조회)이 등록 모드에서 실행되어 충돌함.
*   **가이드**:
    *   `is_edit_mode` 플래그를 명확히 정의하고, 이에 따라 UI 컴포넌트의 `disabled` 속성과 초기값(`row` 데이터)을 분기 처리하라.
    *   티커(Ticker)와 같이 수정 불가능한 필드는 `is_edit_mode=True`일 때 `disabled=True`로 설정하라.

### 🎯 Metadata Suggestion Dropdowns
*   **현상**: 자유 텍스트 입력은 데이터 파편화(오타 등)를 유발함.
*   **가이드**:
    *   자산 메타데이터(`asset_type`, `vehicle_type` 등) 입력 시, 기존 DB에 적재된 Unique Set을 드롭다운으로 먼저 제안하라.
    *   단, 확장성을 위해 항상 **`[직접 입력]`** 옵션을 포함하고, 이를 선택했을 때만 `st.text_input`이 나타나도록 구현하라.

---

## 4. Resource Efficiency (Mini PC Environment)

*   **가이드**:
    *   불필요한 반복 조회를 피하기 위해 `st.cache_data`를 적극 활용하라 (단, 데이터 변경 후에는 `st.cache_data.clear()` 필수).
    *   조회 쿼리 시 필요한 컬럼만 명시적으로 `select()` 하여 네트워크 부하를 최소화하라.

---

> **Note**: 새로운 패턴의 버그나 경고가 발견될 경우, 이 가이드라인을 최우선으로 업데이트하여 지식 자산으로 관리할 것.
