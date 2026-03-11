import asyncio
from asset_portfolio.backend.infra.supabase_client import get_supabase_client
supabase = get_supabase_client()
print('--- assets ---')
print(supabase.table('assets').select('*').eq('id', 7).execute().data)
print('--- asset_prices ---')
print(supabase.table('asset_prices').select('*').eq('asset_id', 7).order('price_date', desc=True).limit(5).execute().data)
