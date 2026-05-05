import pandas as pd
from typing import List, Dict
from asset_portfolio.backend.infra.supabase_client import get_supabase_client

class RebalancingService:
    @staticmethod
    def get_target_weights(user_id: str, account_id: str, grouping_criteria: str) -> List[Dict]:
        """
        특정 사용자와 그룹핑 기준에 대한 목표 비중을 조회합니다.
        account_id가 "__ALL__" 이면 전체 포트폴리오 기준(NULL)으로 조회합니다.
        """
        supabase = get_supabase_client()
        query = (
            supabase.table("portfolio_target_weights")
            .select("target_category, target_weight")
            .eq("user_id", user_id)
            .eq("grouping_criteria", grouping_criteria)
        )
        if account_id and account_id != "__ALL__":
            query = query.eq("account_id", account_id)
        else:
            query = query.is_("account_id", "null")
            
        response = query.execute()
        return response.data or []

    @staticmethod
    def save_target_weights_bulk(user_id: str, account_id: str, grouping_criteria: str, weights_data: List[Dict]) -> bool:
        """
        목표 비중을 일괄 저장합니다 (기존 기준 삭제 후 새로 삽입).
        weights_data: [{"target_category": "주식", "target_weight": 60}, ...]
        """
        supabase = get_supabase_client()
        db_account_id = None if not account_id or account_id == "__ALL__" else account_id
        
        # 1. 기존 데이터 삭제 (Graceful fail if table doesn't exist)
        try:
            delete_query = supabase.table("portfolio_target_weights").delete().eq("user_id", user_id).eq("grouping_criteria", grouping_criteria)
            if db_account_id:
                delete_query = delete_query.eq("account_id", db_account_id)
            else:
                delete_query = delete_query.is_("account_id", "null")
            delete_query.execute()
        except Exception as e:
            print(f"Warning on delete: {e}")
            pass
            
        if not weights_data:
            return True

        # 2. 신규 데이터 삽입
        insert_data = []
        for w in weights_data:
            insert_data.append({
                "user_id": user_id,
                "account_id": db_account_id,
                "grouping_criteria": grouping_criteria,
                "target_category": w.get("target_category", ""),
                "target_weight": float(w.get("target_weight", 0.0))
            })
            
        try:
            supabase.table("portfolio_target_weights").insert(insert_data).execute()
            return True
        except Exception as e:
            print(f"Failed to save target weights: {e}")
            return False

    @staticmethod
    def calculate_rebalancing_gap(current_df: pd.DataFrame, target_weights: List[Dict], grouping_criteria: str) -> pd.DataFrame:
        """
        현재 비중과 목표 비중을 비교하여 차이(Gap)를 계산합니다.
        current_df: load_asset_grouping_summary 에서 반환된 DataFrame
        반환 DataFrame 주요 컬럼: target_category, current_amount, current_weight, target_weight, target_amount, amount_gap, weight_gap
        """
        if current_df.empty:
            return pd.DataFrame()
            
        total_val = current_df["total_valuation_amount"].sum()
        if total_val <= 0:
            return pd.DataFrame()

        if grouping_criteria not in current_df.columns:
            return pd.DataFrame()
            
        # 그룹화하여 현재 금액 집계
        grouped = current_df.groupby(grouping_criteria, as_index=False)["total_valuation_amount"].sum()
        grouped.rename(columns={grouping_criteria: "target_category", "total_valuation_amount": "current_amount"}, inplace=True)
        grouped["current_weight"] = (grouped["current_amount"] / total_val) * 100.0
        
        # 목표 비중 DataFrame
        target_df = pd.DataFrame(target_weights)
        if target_df.empty:
            target_df = pd.DataFrame(columns=["target_category", "target_weight"])
        else:
            target_df["target_weight"] = pd.to_numeric(target_df["target_weight"], errors="coerce").fillna(0.0)

        # 병합 (목표에만 있거나 현재에만 있는 범주 모두 포함)
        merged = pd.merge(grouped[["target_category", "current_amount", "current_weight"]], 
                          target_df, on="target_category", how="outer").fillna(0)
                          
        # 범주명이 없는 경우 처리
        merged["target_category"] = merged["target_category"].replace("", "미분류")
        
        # 갭 계산
        merged["target_amount"] = (merged["target_weight"] / 100.0) * total_val
        merged["amount_gap"] = merged["target_amount"] - merged["current_amount"]
        merged["weight_gap"] = merged["target_weight"] - merged["current_weight"]
        
        return merged
