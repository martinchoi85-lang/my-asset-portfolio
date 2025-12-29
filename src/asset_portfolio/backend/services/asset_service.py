from typing import Dict, Any, Optional
from asset_portfolio.backend.infra.supabase_client import get_supabase_client

class AssetService:
    @staticmethod
    def create_asset_minimal(
        *,
        ticker: str,
        name_kr: str,
        asset_type: str,
        currency: str,
        market: Optional[str] = None,
        underlying_asset_class: str = "Unknown",
        economic_exposure_region: str = "Unknown",
        vehicle_type: str = "Unknown",
        return_driver: str = "Unknown",
        strategy_type: str = "Unknown",
        lookthrough_available: bool = False,
    ) -> Dict[str, Any]:
        """
        ✅ 최소 입력으로 assets에 신규 자산을 생성합니다.
        - assets 스키마에 NOT NULL 필드가 많으므로, V1에서는 기본값을 안전하게 채웁니다.
        - 추후 Asset Editor에서 세부 필드를 보강하는 흐름으로 가져갑니다.
        """
        supabase = get_supabase_client()

        payload = {
            "ticker": ticker.strip(),
            "name_kr": name_kr.strip(),
            "asset_type": asset_type,
            "currency": currency,
            "market": market,
            "underlying_asset_class": underlying_asset_class,
            "economic_exposure_region": economic_exposure_region,
            "vehicle_type": vehicle_type,
            "return_driver": return_driver,
            "strategy_type": strategy_type,
            "lookthrough_available": lookthrough_available,
        }

        # ✅ 이미 존재하는 ticker면 에러 발생(assets.unique_ticker)
        resp = supabase.table("assets").insert(payload).execute()
        row = (resp.data or [None])[0]
        if not row:
            raise RuntimeError("assets insert failed")
        return row


    @staticmethod
    def update_asset(asset_id: int, updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        ✅ assets 단일 row 업데이트
        - updates에는 수정할 컬럼만 전달
        """
        supabase = get_supabase_client()
        resp = (
            supabase.table("assets")
            .update(updates)
            .eq("id", asset_id)
            .execute()
        )
        row = (resp.data or [None])[0]
        if not row:
            raise RuntimeError("assets update failed")
        return row
