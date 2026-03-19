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
        수동 자산은 사용자 입력 값을 우선하되, 
        입력이 없는 구간은 트랜잭션에 따른 수량 변화와 직전 평가액을 Carry Forward 한다.
        """
        supabase = get_supabase_client()
        
        # 1. 계산 수행
        snapshots = self.calculate_snapshots(account_id, start_date, end_date)
        if not snapshots:
            return 0
            
        # 2. ISO 포맷 변환
        prepared = []
        for s in snapshots:
            row = dict(s)
            if isinstance(row.get("date"), date):
                row["date"] = row["date"].isoformat()
            prepared.append(row)
            
        # 3. UPSERT 실행 (on_conflict 처리)
        # 삭제 후 삽입이 아닌 UPSERT를 사용하여 사용자가 Snapshot Editor로 입력한 특정 컬럼(예: valuation_amount)을 
        # 어느 정도 보존할 여지를 둡니다. (단, 현재 schema 상 전체 row가 덮어씌워짐)
        # TODO: 사용자의 수동 입력 값을 별도 컬럼으로 보호하거나, select 후 merge 하는 방식 고려
        
        inserted = 0
        from asset_portfolio.backend.services.transaction_service import TransactionService
        for chunk in TransactionService._chunk(prepared, size=500):
            supabase.table("daily_snapshots").upsert(chunk, on_conflict="date,asset_id,account_id").execute()
            inserted += len(chunk)
            
        return inserted

    def calculate_snapshots(self, account_id: str, start_date: date, end_date: date) -> List[Dict[str, Any]]:
        """
        수동 자산용 스냅샷 계산기:
        - 트랜잭션 기반 수량/원금 추적
        - 평가액은 직전일 스냅샷의 valuation_price를 그대로 계승 (Carry Forward)
        """
        from asset_portfolio.backend.services.portfolio_calculator import _calculate_auto_snapshots_for_asset
        # 수령/가격/금액 추적 로직은 _calculate_auto_snapshots_for_asset와 유사하되
        # 가격이 없는 경우(last_price is None) current_price가 아닌 '마지막 스냅샷의 가격'을 쓰도록 함
        # 현재는 우선 공통 로직을 재사용하여 기본적인 시계열을 만듭니다.
        return _calculate_auto_snapshots_for_asset(self.asset_id, account_id, start_date, end_date)

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
        
        asset_type = (row.get("asset_type") or "").lower().strip()
        price_source = (row.get("price_source") or "").lower().strip()
        
        # ✅ 예수금(cash)은 입출금 트랜잭션 추적이 핵심이므로 자동 핸들러 사용
        if asset_type == "cash":
            return AutoAssetHandler(asset_id)
            
        if price_source in cls._MANUAL_PRICE_SOURCES:
            return ManualAssetHandler(asset_id)
        else:
            return AutoAssetHandler(asset_id)
