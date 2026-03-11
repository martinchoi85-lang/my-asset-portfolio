import asyncio
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
supabase = get_supabase_client()
res = supabase.table('assets').update({'price_source': 'manual_price'}).eq('id', 7).execute()
print('Updated KRX 금현물 to manual_price:', res.data)
