import asyncio
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
supabase = get_supabase_client()
res1 = supabase.table('asset_prices').delete().eq('asset_id', 7).eq('source', 'manual_snapshot').eq('close_price', 1.0).execute()
print('Deleted asset_prices:', res1.data)
res2 = supabase.table('daily_snapshots').delete().eq('asset_id', 7).eq('valuation_price', 1.0).execute()
print('Deleted daily_snapshots:', res2.data)
