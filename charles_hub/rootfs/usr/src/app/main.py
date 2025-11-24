from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import time
import random
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
ROOT_PATH = os.getenv("CHARLES_ROOT_PATH", "")
DEFAULT_PROMPT = (
    "You are CHARLES – the Chester House Automated Residential Liaison & Executive System – "
    "a sardonic, witty butler. Reply in one short sentence."
)

app = FastAPI(title="CHARLES Hub API", root_path=ROOT_PATH, openapi_url=None, docs_url=None)
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
        "feed_categories": {
            "weather": True,
            "system": True,
            "vacuum": True,
            "lighting": True,
            "arrivals": True,
            "people": True,
            "musings": True,
            "jokes": True,
        },
        "quiet_hours_start": options.get("quiet_hours_start", "22:00"),
        "quiet_hours_end": options.get("quiet_hours_end", "07:00"),
        "quiet_exempt_categories": options.get("quiet_exempt_categories", ["weather", "system"]),
        "weather_min_gap": options.get("weather_min_gap", 3600),
        "musings_interval_min": options.get("musings_interval_min", 60),
        "musings_interval_max": options.get("musings_interval_max", 180),
        "musings_daily_cap": options.get("musings_daily_cap", 4),
        "jokes_interval_min": options.get("jokes_interval_min", 120),
        "jokes_interval_max": options.get("jokes_interval_max", 240),
        "jokes_daily_cap": options.get("jokes_daily_cap", 3),
        "musing_pool": options.get("musing_pool", []),
        "joke_pool": options.get("joke_pool", []),
        "news_urls": options.get("news_urls", []),
        "last_musing_time": 0.0,
        "last_joke_time": 0.0,
        "musing_count_date": "",
        "joke_count_date": "",
        "musing_count_today": 0,
        "joke_count_today": 0,
        "last_weather_time": 0.0,
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


def feed_category_allowed(category: str) -> bool:
    key = category.lower()
    return bool(state.get("feed_categories", {}).get(key, True))


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
    asyncio.create_task(scheduled_loop("musings"))
    asyncio.create_task(scheduled_loop("jokes"))


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


async def process_emit(
    topic: str,
    category: str,
    context: str,
    route_raw: str,
    conversation_id: str,
    notify_service_override: Optional[str] = None,
    notify_tag_override: Optional[str] = None,
    notify_title_override: Optional[str] = None,
    prompt_override: Optional[str] = None,
) -> Dict[str, Any]:
    async with state_lock:
        default_route = state.get("route_default", "both")
        throttle_seconds = int(state.get("throttle_seconds", 0))
        last_key = state.get("last_entry_key", "")
        last_ts = float(state.get("last_entry_time", 0.0))
        resolved_route = default_route if route_raw == "default" else route_raw
        now_ts = time.time()
        if category.lower() == "weather":
            min_gap = int(state.get("weather_min_gap", 0))
            last_weather = float(state.get("last_weather_time", 0.0))
            if min_gap > 0 and (now_ts - last_weather) < min_gap:
                return {"status": "throttled", "route": resolved_route}
        current_key = f"{category.lower()}|{topic.lower()}|{context.strip()}"
        if current_key == last_key and (now_ts - last_ts) < throttle_seconds:
            return {"status": "throttled", "route": resolved_route}

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
    send_feed = (
        state.get("feed_enabled", True)
        and resolved_route in {"feed", "both"}
        and feed_category_allowed(category)
    )
    quiet = in_quiet_hours(time.time())
    quiet_exempt = [c.lower() for c in state.get("quiet_exempt_categories", [])]
    quiet_allows_notify = quiet and (category.lower() in quiet_exempt)
    send_notify = (
        state.get("notifications_enabled", True)
        and resolved_route in {"notify", "both"}
        and category_allowed(category)
        and (not quiet or quiet_allows_notify)
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
        if category.lower() == "weather":
            state["last_weather_time"] = now_ts
        if send_notify:
            unread_entry = message_text[:140]
            existing = state.get("unread_log", [])
            state["unread_log"] = ([unread_entry] + existing)[:5]
            state["unread_count"] = int(state.get("unread_count", 0)) + 1
        await persist_state()

    return {
        "status": "ok",
        "route": resolved_route,
        "topic": topic,
        "category": category,
        "context": context,
        "message": message_text,
        "feed_line": feed_line,
        "notified": bool(send_notify),
    }


@app.post("/api/emit")
async def emit(payload: Dict[str, Any]) -> JSONResponse:
    result = await process_emit(
        topic=payload.get("topic", "general"),
        category=payload.get("category", "system"),
        context=payload.get("context", "All systems nominal."),
        route_raw=payload.get("route", "default"),
        conversation_id=payload.get("conversation_id", "charles_general"),
        notify_service_override=payload.get("notify_service"),
        notify_tag_override=payload.get("notify_tag"),
        notify_title_override=payload.get("notify_title"),
        prompt_override=payload.get("prompt_override"),
    )
    if result.get("status") == "throttled":
        return JSONResponse(result, status_code=202)
    return JSONResponse(result)


@app.post("/api/mark_read")
async def mark_read() -> Dict[str, Any]:
    async with state_lock:
        state["unread_count"] = 0
        state["unread_log"] = []
        await persist_state()
    return {"status": "ok"}


def in_quiet_hours(now_ts: float) -> bool:
    start = state.get("quiet_hours_start", "22:00")
    end = state.get("quiet_hours_end", "07:00")
    try:
        sh, sm = [int(x) for x in start.split(":")]
        eh, em = [int(x) for x in end.split(":")]
    except Exception:
        return False
    now = time.localtime(now_ts)
    minutes_now = now.tm_hour * 60 + now.tm_min
    start_minutes = sh * 60 + sm
    end_minutes = eh * 60 + em
    if start_minutes <= end_minutes:
        return start_minutes <= minutes_now < end_minutes
    return minutes_now >= start_minutes or minutes_now < end_minutes


async def scheduled_loop(kind: str) -> None:
    while True:
        try:
            async with state_lock:
                interval_min = int(state.get(f"{kind}_interval_min", 0))
                interval_max = int(state.get(f"{kind}_interval_max", max(interval_min, 60)))
            delay_minutes = random.randint(interval_min, max(interval_min, interval_max)) if interval_min > 0 else 60
            await asyncio.sleep(delay_minutes * 60)

            async with state_lock:
                enabled_feed = state.get("feed_enabled", True)
                enabled_notify = state.get("notifications_enabled", True)
                category_allowed_flag = category_allowed(kind)
                feed_allowed_flag = feed_category_allowed(kind)
                if (not enabled_feed and not enabled_notify) or (not category_allowed_flag and not feed_allowed_flag):
                    continue
                if in_quiet_hours(time.time()):
                    continue

                today = time.strftime("%Y-%m-%d", time.localtime())
                cap = int(state.get(f"{kind}_daily_cap", 0))
                count_date_key = f"{kind}_count_date"
                count_key = f"{kind}_count_today"
                if state.get(count_date_key) != today:
                    state[count_date_key] = today
                    state[count_key] = 0
                if cap > 0 and state.get(count_key, 0) >= cap:
                    continue

                pool = state.get(f"{kind}_pool", [])
                prompt = random.choice(pool) if pool else f"Share a {kind[:-1]} update."
            result = await process_emit(
                topic=kind,
                category=kind,
                context=prompt,
                route_raw="default",
                conversation_id=f"charles_{kind}",
            )
            if result.get("status") != "throttled":
                async with state_lock:
                    state[count_key] = state.get(count_key, 0) + 1
                    state[f"last_{kind}_time"] = time.time()
                    await persist_state()
        except Exception as err:
            print(f"[charles] scheduler error ({kind}): {err}")
            await asyncio.sleep(60)
