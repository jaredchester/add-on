from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any, Dict

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse

# Paths and defaults
DATA_PATH = Path("/data/state.json")
OPTIONS_PATH = Path("/data/options.json")
FEED_LOG_PATH = Path("/config/www/charles_feed.log")
DEFAULT_PROMPT = (
    "You are CHARLES – the Chester House Automated Residential Liaison & Executive System – "
    "a sardonic, witty butler. Reply in one short sentence."
)

app = FastAPI(title="CHARLES Hub API", openapi_url=None, docs_url=None)
state_lock = asyncio.Lock()
state: Dict[str, Any] = {}


def read_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def write_json(path: Path, data: Dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def default_state(options: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "persona_prompt": options.get("persona_prompt") or DEFAULT_PROMPT,
        "route_default": "both",
        "throttle_seconds": 300,
        "feed_enabled": True,
        "notifications_enabled": True,
        "notify_service": "notify.mobile_app_pixel_9",
        "notify_tag": "charles_stream",
        "notify_title": "CHARLES says",
        "conversation_agent_id": "conversation.openai_conversation",
        "categories": {
            "weather": True,
            "system": True,
            "vacuum": True,
            "lighting": True,
            "arrivals": True,
            "people": True,
            "musings": True,
            "jokes": True,
        },
        "last_entry_key": "",
        "last_entry_time": 0.0,
        "unread_count": 0,
        "unread_log": [],
    }


async def load_state() -> None:
    global state
    options = read_json(OPTIONS_PATH)
    existing = read_json(DATA_PATH)
    merged = default_state(options)
    merged.update(existing)
    state = merged
    write_json(DATA_PATH, state)


async def persist_state() -> None:
    write_json(DATA_PATH, state)


@app.on_event("startup")
async def startup() -> None:
    await load_state()


@app.get("/api/health")
async def health() -> Dict[str, Any]:
    return {"status": "ok"}


@app.get("/api/state")
async def get_state() -> Dict[str, Any]:
    async with state_lock:
        return state


@app.post("/api/state")
async def update_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    async with state_lock:
        for k, v in payload.items():
            state[k] = v
        await persist_state()
        return state


@app.get("/api/feed")
async def get_feed() -> Dict[str, Any]:
    if not FEED_LOG_PATH.exists():
        return {"entries": []}
    entries = [ln.strip() for ln in FEED_LOG_PATH.read_text(encoding="utf-8").splitlines() if ln.strip()]
    return {"entries": entries}


@app.post("/api/emit")
async def emit(payload: Dict[str, Any]) -> JSONResponse:
    # Placeholder emit endpoint; Phase 2 will call HA.
    topic = payload.get("topic", "general")
    category = payload.get("category", "system")
    context = payload.get("context", "All systems nominal.")
    route = payload.get("route", "default")
    async with state_lock:
        last = state.get("last_entry_key", "")
        last_ts = float(state.get("last_entry_time", 0.0))
        now_ts = 0.0  # placeholder; Phase 2 will use real timestamps/throttle
        state["last_entry_key"] = f"{category}|{topic}|{context}"
        state["last_entry_time"] = now_ts
        await persist_state()
    return JSONResponse(
        {
            "status": "ok",
            "route": route,
            "topic": topic,
            "category": category,
            "context": context,
        }
    )


@app.post("/api/mark_read")
async def mark_read() -> Dict[str, Any]:
    async with state_lock:
        state["unread_count"] = 0
        state["unread_log"] = []
        await persist_state()
    return {"status": "ok"}
