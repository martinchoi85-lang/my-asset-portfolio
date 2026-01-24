from __future__ import annotations

import os
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles

from asset_portfolio.mobile.data import (
    ALL_ACCOUNT_TOKEN,
    get_kpi_summary,
    get_latest_snapshot_table,
    get_portfolio_treemap,
    get_recent_transactions,
    get_top_contributions,
    list_accounts,
)


BASE_DIR = Path(__file__).resolve().parent
STATIC_DIR = BASE_DIR / "static"


def _is_mobile_user_agent(user_agent: str) -> bool:
    """User-Agent 문자열로 모바일 접속 여부를 추정합니다."""
    # 초보자용 설명:
    # - User-Agent는 브라우저/기기 정보를 담은 문자열입니다.
    # - 모바일 기기에서 자주 등장하는 키워드를 포함하면 모바일로 판단합니다.
    mobile_keywords = [
        "iphone",
        "android",
        "ipad",
        "ipod",
        "mobile",
        "opera mini",
        "blackberry",
        "iemobile",
    ]
    ua = (user_agent or "").lower()
    return any(keyword in ua for keyword in mobile_keywords)


def _get_streamlit_url() -> str:
    """데스크톱 접속 시 리다이렉트할 Streamlit URL을 가져옵니다."""
    return os.environ.get("STREAMLIT_URL", "http://localhost:8501")


def _read_index_html() -> str:
    """모바일 React 페이지 HTML을 읽어 반환합니다."""
    index_path = STATIC_DIR / "index.html"
    return index_path.read_text(encoding="utf-8")


app = FastAPI()
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def root(request: Request):
    """모바일 접속이면 React 페이지, 아니면 Streamlit으로 이동합니다."""
    force_desktop = request.query_params.get("force_desktop") == "1"
    force_mobile = request.query_params.get("force_mobile") == "1"
    user_agent = request.headers.get("user-agent", "")

    if force_desktop:
        return RedirectResponse(_get_streamlit_url())

    if force_mobile or _is_mobile_user_agent(user_agent):
        return HTMLResponse(_read_index_html())

    return RedirectResponse(_get_streamlit_url())


@app.get("/mobile")
async def mobile_page():
    """모바일 페이지를 명시적으로 열고 싶을 때 사용하는 엔드포인트입니다."""
    return HTMLResponse(_read_index_html())


@app.get("/api/accounts")
async def api_accounts():
    """모바일 화면에서 사용할 계좌 목록을 제공합니다."""
    accounts = list_accounts()
    return JSONResponse({
        "accounts": [
            {"id": ALL_ACCOUNT_TOKEN, "label": "전체 계좌 (ALL)"},
            *[
                {
                    "id": acc["id"],
                    "label": f"{acc['brokerage']} | {acc['name']} ({acc['owner']})",
                }
                for acc in accounts
            ],
        ]
    })


@app.get("/api/kpi")
async def api_kpi(account_id: str = ALL_ACCOUNT_TOKEN, days: int = 30):
    """KPI 카드 데이터"""
    data = get_kpi_summary(account_id, days)
    return JSONResponse({"kpi": data, "days": days})


@app.get("/api/latest-snapshot")
async def api_latest_snapshot(account_id: str = ALL_ACCOUNT_TOKEN):
    """최신 스냅샷 테이블 데이터"""
    data = get_latest_snapshot_table(account_id)
    return JSONResponse(data)


@app.get("/api/transactions")
async def api_transactions(account_id: str = ALL_ACCOUNT_TOKEN, days: int = 30):
    """최근 거래 내역"""
    data = get_recent_transactions(account_id, days)
    return JSONResponse({"rows": data, "days": days})


@app.get("/api/top-contributions")
async def api_top_contributions(
    account_id: str = ALL_ACCOUNT_TOKEN,
    days: int = 30,
    top_k: int = 5,
):
    """수익률 기여 Top K"""
    data = get_top_contributions(account_id, days, top_k)
    return JSONResponse({"rows": data, "days": days, "top_k": top_k})


@app.get("/api/treemap")
async def api_treemap(account_id: str = ALL_ACCOUNT_TOKEN, days: int = 30):
    """Treemap 데이터"""
    data = get_portfolio_treemap(account_id, days)
    return JSONResponse(data)


def get_app() -> FastAPI:
    """외부에서 FastAPI 앱을 가져갈 때 사용하는 헬퍼 함수입니다."""
    return app
