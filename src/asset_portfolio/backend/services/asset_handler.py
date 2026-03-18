from __future__ import annotations

import abc
from datetime import date
from typing import Dict, Any, List

from asset_portfolio.backend.infra.supabase_client import get_supabase_client


class AssetHandler(abc.ABC):
    """자산의 유형별(수동/자동 등) 트랜잭션, 스냅샷 리빌드 등 공통 인터페이스"""

    def __init__(self, asset_id: int):
        self.asset_id = asset_id

    @abc.abstractmethod
    def is_manual(self) -> bool:
        pass

    @abc.abstractmethod
    def rebuild_snapshots(self, account_id: str, start_date: date, end_date: date, delete_first: bool = True) -> int:
        """이 자산에 대한 스냅샷 리빌드 정책 실행"""
        pass

    @abc.abstractmethod
    def calculate_snapshots(self, account_id: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """이 자산에 대한 스냅샷 데이터를 계산"""
        pass


class AutoAssetHandler(AssetHandler):
    """일반적인 시장/자동 평가 자산 핸들러"""

    def is_manual(self) -> bool:
        return False

    def rebuild_snapshots(self, account_id: str, start_date: date, end_date: date, delete_first: bool = True) -> int:
        # 이 부분은 추후 transaction_service나 portfolio_calculator에서 옮겨올 로직
        # 일단은 기존 TransactionService가 호출하던 calculate_daily_snapshots_for_asset를 
        # 그대로 래핑하는 방식으로 구현하거나 위임
        from asset_portfolio.backend.services.portfolio_calculator import calculate_daily_snapshots_for_asset
        
        supabase = get_supabase_client()
        snapshots = calculate_daily_snapshots_for_asset(
            asset_id=self.asset_id,
            account_id=account_id,
            start_date=start_date,
            end_date=end_date,
        )

        if not snapshots:
            if delete_first:
                (
                    supabase.table("daily_snapshots")
                    .delete()
                    .eq("account_id", account_id)
                    .eq("asset_id", self.asset_id)
                    .gte("date", start_date.isoformat())
                    .lte("date", end_date.isoformat())
                    .execute()
                )
            return 0

        for r in snapshots:
            if isinstance(r.get("date"), date):
                r["date"] = r["date"].isoformat()

        if delete_first:
            (
                supabase.table("daily_snapshots")
                .delete()
                .eq("account_id", account_id)
                .eq("asset_id", self.asset_id)
                .gte("date", start_date.isoformat())
                .lte("date", end_date.isoformat())
                .execute()
            )

        inserted = 0
        from asset_portfolio.backend.services.transaction_service import TransactionService
        for chunk in TransactionService._chunk(snapshots, size=500):
            supabase.table("daily_snapshots").upsert(
                chunk,
                on_conflict="date,asset_id,account_id",
            ).execute()
            inserted += len(chunk)

        return inserted

    def calculate_snapshots(self, account_id: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        from asset_portfolio.backend.services.portfolio_calculator import _calculate_auto_snapshots_for_asset
        return _calculate_auto_snapshots_for_asset(self.asset_id, account_id, start_date, end_date)


class ManualAssetHandler(AssetHandler):
    """수동 기입 자산(예적금, 부동산 등) 핸들러"""

    def is_manual(self) -> bool:
        return True

    def rebuild_snapshots(self, account_id: str, start_date: date, end_date: date, delete_first: bool = True) -> int:
        """
        수동 자산은 Snapshot Editor가 관리하므로
        자동 리빌드로 삭제/재생성하면 사용자가 입력한 평가금액이 날아감.
        따라서 리빌드를 수행하지 않습니다.
        """
        return 0

    def calculate_snapshots(self, account_id: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        수동 자산용 스냅샷 계산기 (추후 Step 4에서 구현)
        """
        return []

    def calculate_withdrawal_cost_delta(self, current_cost: float, current_valuation: float, withdraw_amount: float) -> float:
        """
        수동 자산의 인출 시 비례 차감될 원금(delta)을 계산한다.
        예: 원금 100, 평가금 150에서 15 인출 시, 인출 비율(15/150=10%)만큼 원금(10)에서 차감 (반환값: -10)
        """
        if current_valuation <= 0:
            return -withdraw_amount
        ratio = withdraw_amount / current_valuation
        return -(current_cost * ratio)


class AssetManager:
    """자산 ID의 메타데이터를 기반으로 적절한 AssetHandler를 반환하는 팩토리"""
    
    _MANUAL_PRICE_SOURCES = {"manual"}

    @classmethod
    def get_handler(cls, asset_id: int) -> AssetHandler:
        supabase = get_supabase_client()
        row = (
            supabase.table("assets")
            .select("price_source, asset_type")
            .eq("id", asset_id)
            .single()
            .execute()
            .data
        ) or {}
        
        price_source = (row.get("price_source") or "").lower().strip()
        
        if price_source in cls._MANUAL_PRICE_SOURCES:
            return ManualAssetHandler(asset_id)
        else:
            return AutoAssetHandler(asset_id)
