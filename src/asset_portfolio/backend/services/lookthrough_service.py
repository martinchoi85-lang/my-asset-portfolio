import pandas as pd

def apply_lookthrough_to_grouping_df(df: pd.DataFrame, supabase) -> pd.DataFrame:
    """
    자산군 집계 DataFrame에서 Look-through 대상 자산(펀드, TDF 등)을 
    내부 세그먼트 비중으로 분해(Explode)하여 재집계하는 함수입니다.

    수행 로직 예시:
    1. 입력받은 df에서 lookthrough_available=True 인 자산을 식별합니다.
    2. 해당 자산들의 asset_id를 기준으로 'asset_segments' 테이블에서 
       내부 자산군(segment_asset_class)과 비중(weight) 정보를 조회합니다.
    3. Look-through 적용 자산의 'valuation_amount'를 각 세그먼트 비중만큼 곱하여 여러 행으로 분해합니다.
       - 예: TDF(1,000만원, 주식 70%/채권 30%) -> 주식(700만원), 채권(300만원)
    4. 분해된 데이터와 Look-through 미적용 자산 데이터를 합칩니다.
    5. 최종적으로 'asset_class' 또는 'underlying_asset_class' 기준으로 다시 GroupBy 연산을 수행하여 
       보정된 집계 DataFrame을 반환합니다.

    Args:
        df (pd.DataFrame): 원본 자산 데이터 (valuation_amount, asset_id 등 포함)
        supabase: DB 조회를 위한 Supabase 클라이언트 객체

    Returns:
        pd.DataFrame: Look-through가 반영된 자산군별 합계 데이터
    """
    # TODO: [Phase 5] 실제 구현부
    # 1. Look-through 대상 자산 필터링
    # 2. asset_segments 테이블에서 비중 데이터 fetch
    # 3. 비중 계산 및 데이터프레임 분해 (Explode)
    # 4. 최종 재집계 (GroupBy)
    
    """
    자산군 집계 DataFrame에서 Look-through 대상 자산(펀드, TDF 등)을 
    내부 세그먼트 비중으로 분해(Explode)하여 재집계하는 함수입니다.
    """
    # 원본 데이터 복사 (원본 훼손 방지)
    work_df = df.copy()

    # 1. Look-through 대상 자산 필터링
    # lookthrough_available 컬럼이 True인 대상만 추출
    target_mask = work_df['lookthrough_available'] == True
    lt_targets = work_df[target_mask]
    non_lt_assets = work_df[~target_mask]

    if lt_targets.empty:
        return work_df

    # 2. asset_segments 테이블에서 비중 데이터 fetch (Batch 조회)
    target_asset_ids = lt_targets['asset_id'].unique().tolist()
    
    response = supabase.table("asset_segments") \
        .select("asset_id, segment_asset_class, weight") \
        .in_("asset_id", target_asset_ids) \
        .execute()

    segments_df = pd.DataFrame(response.data)

    if segments_df.empty:
        # 비중 데이터가 없으면 원본 그대로 반환
        return work_df

    # 3. 비중 계산 및 데이터프레임 분해 (Explode)
    # [Merge] 대상 자산과 세그먼트 정보를 asset_id 기준으로 결합
    exploded_df = lt_targets.merge(segments_df, on='asset_id', how='left', suffixes=('', '_seg'))

    # [Calculate] 비중(weight)을 적용하여 valuation_amount 재계산
    # weight가 0~1 사이 소수점이라고 가정 (0.7 = 70%)
    exploded_df['valuation_amount'] = exploded_df['valuation_amount'] * exploded_df['weight']
    
    # [Update Asset Class] 기존 asset_class를 세그먼트의 자산군으로 교체
    exploded_df['asset_class'] = exploded_df['segment_asset_class']

    # 4. 최종 재집계 (GroupBy)
    # 룩스루가 적용된 데이터와 미적용 데이터를 합침
    final_combined = pd.concat([non_lt_assets, exploded_df], ignore_index=True)

    # 'asset_class'를 기준으로 다시 그룹화하여 금액 합산
    # (필요에 따라 underlying_asset_class 등 추가 컬럼을 기준으로 할 수 있습니다)
    result_df = final_combined.groupby('asset_class', as_index=False)['valuation_amount'].sum()

    return result_df

"""
[호출 예시 - src/asset_portfolio/dashboard/render.py]

def load_asset_grouping_summary(user_id: str, account_id: str) -> pd.DataFrame:
    # 1) 기존 DB 조회 로직 (daily_snapshots + assets JOIN)
    # ... (기존 코드 생략) ...
    df = snapshot_query.execute().data
    
    # 2) Look-through 보정 적용
    from asset_portfolio.backend.services.lookthrough_service import apply_lookthrough_to_grouping_df
    df_lookthrough = apply_lookthrough_to_grouping_df(df, supabase)
    
    # 3) 최종 그룹화 및 반환
    # return df_lookthrough.groupby(...)
"""
