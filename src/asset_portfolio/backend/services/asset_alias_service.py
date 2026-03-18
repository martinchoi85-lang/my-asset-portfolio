import logging
from typing import List, Optional
from asset_portfolio.backend.infra.supabase_client import get_supabase_client

logger = logging.getLogger(__name__)

class AssetAliasService:
    @staticmethod
    def get_alias_map(user_id: str) -> dict:
        """
        사용자의 모든 자산 별칭(Alias)을 조회하여 {alias_name: asset_id} 딕셔너리로 반환합니다.
        """
        supabase = get_supabase_client()
        response = supabase.table("asset_aliases") \
            .select("alias_name, asset_id") \
            .eq("user_id", user_id) \
            .execute()
        
        return {item["alias_name"]: item["asset_id"] for item in response.data}

    @staticmethod
    def add_alias(user_id: str, alias_name: str, asset_id: int) -> bool:
        """
        새로운 자산 별칭을 등록하거나 기존 별칭을 업데이트합니다.
        """
        supabase = get_supabase_client()
        try:
            # upsert 사용 (user_id, alias_name 유니크 제약 조건 활용)
            data = {
                "user_id": user_id,
                "alias_name": alias_name,
                "asset_id": asset_id
            }
            supabase.table("asset_aliases").upsert(
                data, on_conflict="user_id, alias_name"
            ).execute()
            logger.info(f"Alias registered: {alias_name} -> asset_id {asset_id}")
            return True
        except Exception as e:
            logger.error(f"Failed to add alias: {e}")
            return False

    @staticmethod
    def delete_alias(user_id: str, alias_name: str) -> bool:
        """
        자산 별칭을 삭제합니다.
        """
        supabase = get_supabase_client()
        try:
            supabase.table("asset_aliases") \
                .delete() \
                .eq("user_id", user_id) \
                .eq("alias_name", alias_name) \
                .execute()
            return True
        except Exception as e:
            logger.error(f"Failed to delete alias: {e}")
            return False
