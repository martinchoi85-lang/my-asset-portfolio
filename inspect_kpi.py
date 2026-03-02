import sys
import os
sys.path.append(os.path.join(os.getcwd(), "src"))

from asset_portfolio.backend.services.portfolio_service import get_portfolio_return_series

user_id = '7f472a2b-952b-4d26-a833-de1d1b760d75'
acc_id = "__ALL__"

df_1m = get_portfolio_return_series(user_id, acc_id, "2025-01-25", "2025-02-25")

print("1M DF length:", len(df_1m))
if not df_1m.empty:
    print(df_1m.head(2))
    print(df_1m.tail(2))
    
    s_ret = df_1m.iloc[0]["portfolio_return"]
    e_ret = df_1m.iloc[-1]["portfolio_return"]
    print("start return (iloc 0):", s_ret)
    print("end return (iloc -1):", e_ret)
    print("1M metric:", ((1+e_ret)/(1+s_ret)-1)*100)
