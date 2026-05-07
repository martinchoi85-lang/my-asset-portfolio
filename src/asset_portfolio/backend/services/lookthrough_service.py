import pandas as pd
import logging

logger = logging.getLogger(__name__)

def apply_lookthrough_to_grouping_df(df: pd.DataFrame, supabase) -> pd.DataFrame:
    """
    자산군 집계 DataFrame에서 Look-through 대상 자산(펀드, TDF 등)을 
    내부 세그먼트 비중으로 분해(Explode)하여 재계산하는 함수입니다.

    Args:
        df (pd.DataFrame): 원본 자산 데이터 ('asset_id'와 금액 컬럼 포함)
        supabase: DB 조회를 위한 Supabase 클라이언트 객체

    Returns:
        pd.DataFrame: Look-through가 반영된 전체 자산 데이터프레임
    """
    if df.empty or 'asset_id' not in df.columns:
        return df
        
    # 금액 컬럼 찾기 (rename 전후 호환성)
    amount_col = 'total_valuation_amount' if 'total_valuation_amount' in df.columns else 'valuation_amount'
    
    # 1. 대상 자산들의 ID 추출
    asset_ids = df['asset_id'].dropna().unique().tolist()
    if not asset_ids:
        return df
        
    # 2. asset_segments 테이블에서 비중 데이터 조회
    try:
        # lookthrough_available 여부를 assets에서 확인하기보다, 
        # asset_segments에 데이터가 있으면 적용 대상으로 간주하는 것이 훨씬 안전하고 효율적입니다.
        response = supabase.table("asset_segments").select("asset_id, segment_asset_class, weight").in_("asset_id", asset_ids).execute()
        segments_data = response.data
    except Exception as e:
        logger.error(f"Look-through 세그먼트 데이터 조회 실패: {e}")
        return df
        
    if not segments_data:
        return df # 세그먼트 정보가 없으면 원본 그대로 반환
        
    segments_df = pd.DataFrame(segments_data)
    # 백분율(%)을 비율(0~1)로 변환
    segments_df['weight'] = pd.to_numeric(segments_df['weight'], errors='coerce') / 100.0 
    
    # 3. 비중 계산 및 데이터프레임 분해 (Explode)
    lookthrough_asset_ids = segments_df['asset_id'].unique()
    
    # 원본 데이터프레임을 Look-through 적용 대상과 미대상으로 분리
    df_non_lookthrough = df[~df['asset_id'].isin(lookthrough_asset_ids)].copy()
    df_lookthrough_target = df[df['asset_id'].isin(lookthrough_asset_ids)].copy()
    
    # 대상 자산과 세그먼트 데이터 병합 (1:N 분해)
    df_exploded = df_lookthrough_target.merge(segments_df, on='asset_id', how='left')
    
    # 평가금액을 세그먼트 비중만큼 쪼개기
    if amount_col in df_exploded.columns:
        df_exploded[amount_col] = pd.to_numeric(df_exploded[amount_col], errors='coerce') * df_exploded['weight']
    
    # 자산군 컬럼 덮어쓰기 (rename 전: 'assets.underlying_asset_class', 후: 'underlying_asset_class')
    class_col = 'underlying_asset_class' if 'underlying_asset_class' in df.columns else 'assets.underlying_asset_class'
    
    if class_col in df_exploded.columns:
        # 분해된 데이터의 자산군을 세그먼트 자산군으로 덮어쓰기
        df_exploded[class_col] = df_exploded['segment_asset_class']
        
    # 불필요한 임시 컬럼 제거
    df_exploded = df_exploded.drop(columns=['segment_asset_class', 'weight'])
    
    # 4. 분해된 데이터와 기존 데이터 병합하여 최종 반환
    df_final = pd.concat([df_non_lookthrough, df_exploded], ignore_index=True)
    
    return df_final
