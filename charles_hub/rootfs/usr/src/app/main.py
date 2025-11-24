from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import time
from typing import Any, Dict, Optional

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

# Paths and defaults
DATA_PATH = Path("/data/state.json")
OPTIONS_PATH = Path("/data/options.json")
FEED_LOG_PATH = Path("/config/www/charles_feed.log")
STATIC_DIR = Path(__file__).parent / "static"
DEFAULT_PROMPT = (
    "You are CHARLES – the Chester House Automated Residential Liaison & Executive System – "
    "a sardonic, witty butler. Reply in one short sentence."
)

app = FastAPI(title="CHARLES Hub API", openapi_url=None, docs_url=None)
if STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")
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


async def call_conversation(
    prompt: str,
    topic: str,
    context: str,
    conversation_id: str,
    agent_id: str,
) -> str:
    token = os.getenv("SUPERVISOR_TOKEN")
    if not token:
        return context
    payload = {
        "text": f"{prompt}\n\nTopic: {topic}\n\nContext: {context}",
        "conversation_id": conversation_id,
        "agent_id": agent_id,
    }
    url = "http://supervisor/core/api/conversation/process"
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            res = await client.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload)
            res.raise_for_status()
            data = res.json()
            return (
                data.get("response", {})
                .get("speech", {})
                .get("plain", {})
                .get("speech", context)
            )
    except Exception as err:
        print(f"[charles] conversation call failed: {err}")
        return context


async def call_notify(service: str, title: str, message: str, tag: str) -> None:
    token = os.getenv("SUPERVISOR_TOKEN")
    if not token or "." not in service:
        return
    domain, srv = service.split(".", 1)
    url = f"http://supervisor/core/api/services/{domain}/{srv}"
    payload = {
        "title": title,
        "message": message,
        "data": {
            "tag": tag,
            "group": tag,
            "notification_icon": "mdi:robot",
            "url": "/lovelace/home_dash/charles",
        },
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.post(url, headers={"Authorization": f"Bearer {token}"}, json=payload)
            res.raise_for_status()
    except Exception as err:
        print(f"[charles] notify call failed: {err}")


def category_allowed(category: str) -> bool:
    key = category.lower()
    return bool(state.get("categories", {}).get(key, True))


def append_feed_line(ts_iso: str, topic: str, message: str) -> str:
    FEED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    clean_msg = " ".join(message.split())
    line = f"{ts_iso} · {topic} · {clean_msg}"
    try:
        with FEED_LOG_PATH.open("a", encoding="utf-8") as fp:
            fp.write(line + "\n")
    except Exception as err:
        print(f"[charles] feed write failed: {err}")
    return line


@app.on_event("startup")
async def startup() -> None:
    await load_state()


@app.get("/", response_class=HTMLResponse)
async def root_page() -> HTMLResponse:
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(status_code=404, detail="UI not found")
    return HTMLResponse(index.read_text(encoding="utf-8"))


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
    topic = payload.get("topic", "general")
    category = payload.get("category", "system")
    context = payload.get("context", "All systems nominal.")
    route_raw = payload.get("route", "default")
    notify_service_override: Optional[str] = payload.get("notify_service")
    notify_tag_override: Optional[str] = payload.get("notify_tag")
    notify_title_override: Optional[str] = payload.get("notify_title")
    prompt_override: Optional[str] = payload.get("prompt_override")
    conversation_id: str = payload.get("conversation_id", "charles_general")

    async with state_lock:
        default_route = state.get("route_default", "both")
        throttle_seconds = int(state.get("throttle_seconds", 0))
        last_key = state.get("last_entry_key", "")
        last_ts = float(state.get("last_entry_time", 0.0))
        resolved_route = default_route if route_raw == "default" else route_raw
        now_ts = time.time()
        current_key = f"{category.lower()}|{topic.lower()}|{context.strip()}"
        if current_key == last_key and (now_ts - last_ts) < throttle_seconds:
            return JSONResponse({"status": "throttled", "route": resolved_route}, status_code=202)

    agent_prompt = prompt_override or state.get("persona_prompt", DEFAULT_PROMPT)
    agent_id = state.get("conversation_agent_id", "conversation.openai_conversation")

    message_text = await call_conversation(
        prompt=agent_prompt,
        topic=topic,
        context=context,
        conversation_id=conversation_id,
        agent_id=agent_id,
    )

    ts_iso = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now_ts))
    send_feed = state.get("feed_enabled", True) and resolved_route in {"feed", "both"}
    send_notify = (
        state.get("notifications_enabled", True)
        and resolved_route in {"notify", "both"}
        and category_allowed(category)
    )

    feed_line = None
    if send_feed:
        feed_line = append_feed_line(ts_iso, topic, message_text)

    if send_notify:
        notify_service = notify_service_override or state.get("notify_service", "notify.mobile_app_pixel_9")
        notify_tag = notify_tag_override or state.get("notify_tag", "charles_stream")
        notify_title = notify_title_override or state.get("notify_title", "CHARLES says")
        await call_notify(
            service=notify_service,
            title=notify_title,
            message=message_text,
            tag=notify_tag,
        )

    async with state_lock:
        state["last_entry_key"] = current_key
        state["last_entry_time"] = now_ts
        if send_notify:
            unread_entry = message_text[:140]
            existing = state.get("unread_log", [])
            state["unread_log"] = ([unread_entry] + existing)[:5]
            state["unread_count"] = int(state.get("unread_count", 0)) + 1
        await persist_state()

    return JSONResponse(
        {
            "status": "ok",
            "route": resolved_route,
            "topic": topic,
            "category": category,
            "context": context,
            "message": message_text,
            "feed_line": feed_line,
            "notified": bool(send_notify),
        }
    )


@app.post("/api/mark_read")
async def mark_read() -> Dict[str, Any]:
    async with state_lock:
        state["unread_count"] = 0
        state["unread_log"] = []
        await persist_state()
    return {"status": "ok"}
