from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import time
import random
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple, List
import difflib
import re

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
DEFAULT_RECENT_LIMIT = 10
ADDON_VERSION = os.getenv("ADDON_VERSION", "0.9.54")

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
    recent_limit = int(options.get("recent_limit", DEFAULT_RECENT_LIMIT))
    notify_cap_chars = int(options.get("notify_cap_chars", 240))
    return {
        "persona_prompt": options.get("persona_prompt") or DEFAULT_PROMPT,
        "route_default": "both",
        "throttle_seconds": 300,
        "feed_enabled": True,
        "notifications_enabled": True,
        "notify_service": "notify.mobile_app_pixel_9",
        "notify_tag": "charles_stream",
        "notify_title": "CHARLES says",
        "notify_cap_chars": notify_cap_chars,
        "notify_caps": options.get("notify_caps", {}),
        "conversation_agent_id": "conversation.openai_conversation",
    "categories": {
        "weather": True,
        "trivia": True,
        "calendar": True,
        "system": True,
        "vacuum": True,
        "lighting": True,
        "arrivals": True,
        "people": True,
        "musings": True,
        "jokes": True,
        "brief": True,
    },
    "feed_categories": {
        "weather": True,
        "trivia": True,
        "calendar": True,
        "system": True,
        "vacuum": True,
        "lighting": True,
        "arrivals": True,
        "people": True,
        "musings": True,
        "jokes": True,
        "brief": True,
    },
        "quiet_hours_start": options.get("quiet_hours_start", "22:00"),
        "quiet_hours_end": options.get("quiet_hours_end", "07:00"),
        "quiet_feed_suppress": options.get("quiet_feed_suppress", False),
        "weather_min_gap": options.get("weather_min_gap", 3600),
        "weather_temp_delta": options.get("weather_temp_delta", 5),
        "weather_condition_change": options.get("weather_condition_change", True),
        "weather_feed_only_minor": options.get("weather_feed_only_minor", False),
        "weather_poll_route": options.get("weather_poll_route", "feed"),
        "recent_limit": recent_limit,
        "weather_entity": options.get("weather_entity", "weather.home"),
        "weather_poll_interval": options.get("weather_poll_interval", 300),
    "last_weather_payload": {},
    "calendar_entities": options.get("calendar_entities", []),
    "calendar_lead_minutes": options.get("calendar_lead_minutes", 60),
    "calendar_poll_interval": options.get("calendar_poll_interval", 300),
        "calendar_announced": options.get("calendar_announced", []),
        "last_calendar_poll": 0.0,
    "musings_interval_min": options.get("musings_interval_min", 60),
    "musings_interval_max": options.get("musings_interval_max", 180),
    "musings_daily_cap": options.get("musings_daily_cap", 4),
        "jokes_interval_min": options.get("jokes_interval_min", 120),
        "jokes_interval_max": options.get("jokes_interval_max", 240),
        "jokes_daily_cap": options.get("jokes_daily_cap", 3),
        "brief_enabled_morning": options.get("brief_enabled_morning", True),
        "brief_enabled_evening": options.get("brief_enabled_evening", False),
        "brief_time_morning": options.get("brief_time_morning", "07:45"),
        "brief_time_evening": options.get("brief_time_evening", "21:15"),
        "brief_morning_sent": "",
        "brief_evening_sent": "",
        "brief_include_weather": options.get("brief_include_weather", True),
        "brief_include_calendar": options.get("brief_include_calendar", True),
        "brief_include_unread": options.get("brief_include_unread", True),
        "brief_seed_source": options.get("brief_seed_source", "musing"),
    "musing_pool": options.get("musing_pool", []),
    "joke_pool": options.get("joke_pool", []),
    "trivia_pool": options.get("trivia_pool", []),
    "trivia_urls": options.get("trivia_urls", []),
    "trivia_cache": [],
    "trivia_cache_ts": 0.0,
    "news_urls": options.get("news_urls", []),
    "pauses": {},
    "paused_all_until": 0.0,
    "last_musing_time": 0.0,
    "last_joke_time": 0.0,
    "last_trivia_time": 0.0,
    "musing_count_date": "",
    "joke_count_date": "",
    "musing_count_today": 0,
    "joke_count_today": 0,
    "trivia_count_date": "",
    "trivia_count_today": 0,
        "trivia_interval_min": options.get("trivia_interval_min", 180),
        "trivia_interval_max": options.get("trivia_interval_max", 360),
        "trivia_daily_cap": options.get("trivia_daily_cap", 3),
        "arrival_emit_delay": options.get("arrival_emit_delay", 0),
        "arrival_combine_window": options.get("arrival_combine_window", 300),
        "last_weather_time": 0.0,
        "next_musing_time": 0.0,
        "next_joke_time": 0.0,
        "next_trivia_time": 0.0,
        "people_interval_min": options.get("people_interval_min", 120),
        "people_interval_max": options.get("people_interval_max", 360),
        "people_daily_cap": options.get("people_daily_cap", 4),
        "people_entities": options.get("people_entities", []),
        "presence_zones": options.get("presence_zones", []),
        "presence_auto_arrivals": options.get("presence_auto_arrivals", True),
        "presence_poll_interval": options.get("presence_poll_interval", 60),
        "presence_last_states": {},
        "next_people_time": 0.0,
        "last_people_time": 0.0,
        "people_count_date": "",
        "people_count_today": 0,
        "arrival_groups": {},
        "arrival_pending": {},
        "last_entry_key": "",
        "last_entry_time": 0.0,
        "unread_count": 0,
        "unread_log": [],
    "last_emit": {},
    "last_error": "",
        "recent_seeds": {
            "jokes": [],
            "musings": [],
            "trivia": [],
        },
        "recent_outputs": {
            "jokes": [],
            "musings": [],
            "trivia": [],
        },
        "recent_outputs_general": [],
}


async def load_state() -> None:
    global state
    options = read_json(OPTIONS_PATH)
    existing = read_json(DATA_PATH)
    merged = default_state(options)
    merged.update(existing)
    rl = int(merged.get("recent_limit", DEFAULT_RECENT_LIMIT))
    # trim recents to limit
    rec_seeds = merged.get("recent_seeds", {})
    rec_outputs = merged.get("recent_outputs", {})
    rec_outputs_general = merged.get("recent_outputs_general", [])
    for key in ["jokes", "musings", "trivia"]:
        if key in rec_seeds:
            rec_seeds[key] = rec_seeds[key][:rl]
        if key in rec_outputs:
            rec_outputs[key] = rec_outputs[key][:rl]
    rec_outputs_general = rec_outputs_general[:rl]
    merged["recent_seeds"] = rec_seeds
    merged["recent_outputs"] = rec_outputs
    merged["recent_outputs_general"] = rec_outputs_general
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


def is_muted(category: str) -> bool:
    return False


def is_paused(category: str) -> bool:
    try:
        pauses = state.get("pauses", {})
        until = float(pauses.get(category.lower(), 0))
        return until > time.time()
    except Exception:
        return False


def is_global_paused() -> bool:
    try:
        until = float(state.get("paused_all_until", 0))
        return until > time.time()
    except Exception:
        return False


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


def replace_last_feed_line(expected: str, new_line: str) -> str:
    try:
        if not FEED_LOG_PATH.exists():
            FEED_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
            FEED_LOG_PATH.write_text(new_line + "\n", encoding="utf-8")
            return new_line
        lines = [ln for ln in FEED_LOG_PATH.read_text(encoding="utf-8").splitlines()]
        if lines and lines[-1].strip() == expected.strip():
            lines[-1] = new_line
            FEED_LOG_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")
            return new_line
        # Fallback append
        with FEED_LOG_PATH.open("a", encoding="utf-8") as fp:
            fp.write(new_line + "\n")
        return new_line
    except Exception as err:
        print(f"[charles] feed replace failed: {err}")
        return append_feed_line(time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(time.time())), "arrivals", new_line)


def build_notify_summary(current: str, existing_unread: List[str]) -> str:
    entries = [current] + (existing_unread or [])
    entries = [e for e in entries if e][:5]
    if len(entries) == 1:
        out = entries[0]
    parts = [f"{idx+1}) {txt}" for idx, txt in enumerate(entries)]
    out = f"{len(entries)} updates: " + " | ".join(parts)
    cap = int(state.get("notify_cap_chars", 240) or 240)
    if cap > 0 and len(out) > cap:
        out = out[: max(0, cap - 3)] + "..."
    return out


def normalize_pool(val: Any) -> List[str]:
    if isinstance(val, list):
        items: List[str] = []
        for item in val:
            if isinstance(item, str):
                parts = [p.strip() for p in item.splitlines() if p.strip()]
                items.extend(parts)
        return items
    if isinstance(val, str):
        return [p.strip() for p in val.splitlines() if p.strip()]
    return []


async def build_people_context() -> str:
    token = os.getenv("SUPERVISOR_TOKEN")
    entities = state.get("people_entities", []) or []
    try:
        people: List[Dict[str, Any]] = []
        if token:
            url = "http://supervisor/core/api/states"
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.get(url, headers={"Authorization": f"Bearer {token}"})
                res.raise_for_status()
                data = res.json()
                for item in data:
                    ent_id = str(item.get("entity_id", ""))
                    if not ent_id.startswith("person."):
                        continue
                    if entities and ent_id not in entities:
                        continue
                    name = item.get("attributes", {}).get("friendly_name") or ent_id
                    state_val = item.get("state", "unknown")
                    last_changed = item.get("last_changed")
                    people.append({"name": name, "state": state_val, "last_changed": last_changed})
        if not people:
            return "Share a quick household presence update in one sentence."
        home = [p["name"] for p in people if p["state"] == "home"]
        away = [p["name"] for p in people if p["state"] != "home"]
        parts = []
        if home:
            parts.append("Home: " + ", ".join(home))
        if away:
            parts.append("Away: " + ", ".join(away))
        return "; ".join(parts) if parts else "Presence update: no data."
    except Exception as err:
        print(f"[charles] people context failed: {err}")
        return "Share a quick household presence update in one sentence."


def parse_time_str(val: str) -> Optional[int]:
    try:
        hh, mm = [int(x) for x in val.split(":")]
        return hh * 60 + mm
    except Exception:
        return None


def parse_iso_ts(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    try:
        if isinstance(raw, dict):
            if "dateTime" in raw:
                raw = raw["dateTime"]
            elif "date" in raw:
                raw = raw["date"]
        s = str(raw)
        if len(s) == 10:  # YYYY-MM-DD
            dt = datetime.fromisoformat(s)
        else:
            if s.endswith("Z"):
                s = s.replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.timestamp()
    except Exception:
        return None


def choose_seed(category: str, pool: List[str]) -> Optional[str]:
    return choose_seed_ex(category, pool, None)


def choose_seed_ex(category: str, pool: List[str], excluded: Optional[List[str]]) -> Optional[str]:
    if not pool:
        return None
    recent = state.get("recent_seeds", {}).get(category, [])
    excluded_set = set(excluded or [])
    candidates = [p for p in pool if p not in recent and p not in excluded_set]
    if not candidates:
        candidates = [p for p in pool if p not in excluded_set]
    return random.choice(candidates or pool)


async def load_trivia_pool(force: bool = False) -> List[str]:
    async with state_lock:
        cache = state.get("trivia_cache", [])
        cache_ts = float(state.get("trivia_cache_ts", 0.0))
        urls = [u.strip() for u in state.get("trivia_urls", []) or [] if u.strip()]
        base_pool = state.get("trivia_pool", []) or []
    if cache and not force:
        return cache
    seeds: List[str] = list(base_pool)
    # Fetch remote text files (one fact per line)
    for url in urls:
        try:
            async with httpx.AsyncClient(timeout=8.0) as client:
                res = await client.get(url)
                res.raise_for_status()
                txt = res.text
                for ln in txt.splitlines():
                    val = ln.strip()
                    if val:
                        seeds.append(val)
        except Exception as err:
            print(f"[charles] trivia url fetch failed ({url}): {err}")
    # Local fallback files
    for path_str in ["/config/charles_trivia.txt", "/usr/src/app/charles_trivia.txt"]:
        p = Path(path_str)
        if p.exists():
            try:
                for ln in p.read_text(encoding="utf-8", errors="ignore").splitlines():
                    val = ln.strip()
                    if val:
                        seeds.append(val)
            except Exception as err:
                print(f"[charles] trivia file read failed ({path_str}): {err}")
    # Deduplicate while preserving order
    seen = set()
    uniq = []
    for v in seeds:
        if v not in seen:
            seen.add(v)
            uniq.append(v)
    async with state_lock:
        state["trivia_cache"] = uniq
        state["trivia_cache_ts"] = time.time()
        await persist_state()
    return uniq


async def build_quick_context(category: str, excluded_seeds: Optional[List[str]] = None) -> Tuple[str, str, Optional[str]]:
    cat = category.lower()
    async with state_lock:
        st = dict(state)
    if cat == "weather":
        payload = st.get("last_weather_payload") or await poll_weather_entity()
        if payload:
            cond = payload.get("condition", "?")
            temp = payload.get("temperature", "?")
            return (f"Weather update: {cond} at {temp}°.", "weather", None)
        return ("Share the current weather briefly.", "weather", None)
    if cat == "calendar":
        return ("Share today's and tomorrow's upcoming calendar items briefly.", "calendar", None)
    if cat == "musings":
        pool: List[str] = normalize_pool(st.get("musing_pool") or [])
        seed = choose_seed_ex("musings", pool, excluded_seeds)
        if seed:
            return (f"Seed: {seed}\nRespond with one short, wry musing inspired by this seed. Keep it to one or two sentences. Avoid repeating earlier musings today.", "musings", seed)
        return ("Share one short, wry house musing in one or two sentences. Avoid repeating earlier musings today.", "musings", None)
    if cat == "jokes":
        pool: List[str] = st.get("joke_pool") or []
        seed = choose_seed_ex("jokes", pool, excluded_seeds)
        if seed:
            return (f"Seed: {seed}\nTell exactly one short joke based on this seed. Avoid repeating earlier jokes today. Return only one short joke.", "jokes", seed)
        return ("Tell exactly one short joke. Avoid repeating earlier jokes today. Return only one short joke.", "jokes", None)
    if cat == "trivia":
        pool = await load_trivia_pool()
        seed = choose_seed_ex("trivia", pool, excluded_seeds)
        if seed:
            return (f"Seed: {seed}\nShare exactly one short trivia fact based on this seed. Avoid repeating earlier trivia today. One sentence only.", "trivia", seed)
        return ("Share exactly one short trivia fact. Avoid repeating earlier trivia today. One sentence only.", "trivia", None)
    if cat == "lighting":
        return ("Summarize current lighting status and any recent changes in one sentence.", "lighting", None)
    if cat == "arrivals":
        return ("Share the latest arrival/departure update in one sentence.", "arrivals", None)
    if cat == "people":
        ctx = await build_people_context()
        return (ctx, "people", None)
    if cat == "vacuum":
        return ("Share the current vacuum status in one sentence.", "vacuum", None)
    if cat == "system":
        return ("System check-in and recent notable events in one sentence.", "system", None)
    if cat == "brief":
        summary = await build_brief()
        return (summary, "brief", None)
    return (f"Share a quick {cat} update in one sentence.", cat, None)


async def build_brief() -> str:
    pieces = []
    include_weather = bool(state.get("brief_include_weather", True))
    include_calendar = bool(state.get("brief_include_calendar", True))
    include_unread = bool(state.get("brief_include_unread", True))
    seed_source = state.get("brief_seed_source", "musing")

    if include_weather:
        payload = state.get("last_weather_payload", {}) or await poll_weather_entity() or {}
        cond = payload.get("condition", "?")
        temp = payload.get("temperature", "?")
        pieces.append(f"Weather: {cond} at {temp}°.")
    if include_calendar:
        try:
            token = os.getenv("SUPERVISOR_TOKEN")
            cals = state.get("calendar_entities", []) or []
            cal_snips = []
            if token and cals:
                now_utc = datetime.now(timezone.utc)
                end_utc = now_utc + timedelta(hours=36)
                for ent in cals[:5]:
                    events = await fetch_calendar_events(ent, now_utc.isoformat(), end_utc.isoformat())
                    if events:
                        ev = events[0]
                        title = ev.get("summary") or ev.get("title") or "Event"
                        start_ts = parse_iso_ts(ev.get("start"))
                        if start_ts:
                            tstr = datetime.fromtimestamp(start_ts).strftime("%I:%M%p").lower().lstrip("0")
                            cal_snips.append(f"{title} at {tstr}")
            if cal_snips:
                pieces.append("Calendar: " + "; ".join(cal_snips[:3]))
            else:
                pieces.append("Calendar: no key events.")
        except Exception:
            pieces.append("Calendar: unavailable.")
    if include_unread:
        unread = state.get("unread_log", [])
        if unread:
            pieces.append(f"Unread: {len(unread)} pending.")

    seed = ""
    if seed_source in {"musing", "both"}:
        pool = normalize_pool(state.get("musing_pool") or [])
        if pool:
            seed = random.choice(pool)
    if not seed and seed_source in {"trivia", "both"}:
        pool = normalize_pool(state.get("trivia_pool") or [])
        if pool:
            seed = random.choice(pool)
    if seed:
        pieces.append(f"Seed: {seed}")
    return " ".join(pieces)


def weather_significant(previous: Dict[str, Any], current: Dict[str, Any], temp_delta_req: float, condition_change_required: bool) -> Tuple[bool, str]:
    prev_cond = str(previous.get("condition", "")).lower()
    new_cond = str(current.get("condition", "")).lower()
    prev_temp = previous.get("temperature")
    new_temp = current.get("temperature")
    condition_changed = (prev_cond != new_cond) if (prev_cond or new_cond) else False
    temp_delta = 0.0
    if isinstance(prev_temp, (int, float)) and isinstance(new_temp, (int, float)):
        temp_delta = abs(new_temp - prev_temp)
    significant = True
    reason = "significant"
    if condition_change_required and not condition_changed and temp_delta < temp_delta_req:
        significant = False
        reason = "minor"
    elif not condition_change_required and temp_delta < temp_delta_req and not condition_changed:
        significant = False
        reason = "minor"
    return significant, reason


def format_clock(ts: float) -> str:
    try:
        formatted = time.strftime("%I:%M %p", time.localtime(ts)).lower()
        return formatted.lstrip("0")
    except Exception:
        return ""


def push_recent(category: str, seed: Optional[str], message: str) -> None:
    if category not in {"jokes", "musings", "trivia"}:
        return
    recent_seeds = state.setdefault("recent_seeds", {"jokes": [], "musings": [], "trivia": []})
    recent_outputs = state.setdefault("recent_outputs", {"jokes": [], "musings": [], "trivia": []})
    limit = int(state.get("recent_limit", DEFAULT_RECENT_LIMIT))
    if seed:
        arr = recent_seeds.setdefault(category, [])
        arr.insert(0, seed)
        del arr[limit:]
    if message:
        arr = recent_outputs.setdefault(category, [])
        arr.insert(0, message[:200])
        del arr[limit:]


async def fetch_calendar_events(entity_id: str, start_iso: str, end_iso: str) -> list:
    token = os.getenv("SUPERVISOR_TOKEN")
    if not token or not entity_id:
        return []
    url = f"http://supervisor/core/api/calendars/{entity_id}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.get(
                url,
                headers={"Authorization": f"Bearer {token}"},
                params={"start": start_iso, "end": end_iso},
            )
            res.raise_for_status()
            return res.json() or []
    except Exception as err:
        print(f"[charles] calendar poll failed for {entity_id}: {err}")
        return []


async def calendar_poll_loop() -> None:
    while True:
        try:
            async with state_lock:
                interval = max(60, int(state.get("calendar_poll_interval", 300)))
                entities = [e.strip() for e in state.get("calendar_entities", []) if e.strip()]
                lead = int(state.get("calendar_lead_minutes", 60))
                announced = set(state.get("calendar_announced", []))
            if not entities:
                await asyncio.sleep(interval)
                continue

            now_ts = time.time()
            now_utc = datetime.fromtimestamp(now_ts, tz=timezone.utc)
            start_iso = now_utc.isoformat()
            end_iso = (now_utc + timedelta(hours=36)).isoformat()
            today = time.strftime("%Y-%m-%d", time.localtime(now_ts))
            tomorrow_ts = now_ts + 86400
            tomorrow = time.strftime("%Y-%m-%d", time.localtime(tomorrow_ts))

            today_events = []
            tomorrow_events = []

            for ent in entities:
                events = await fetch_calendar_events(ent, start_iso, end_iso)
                for ev in events:
                    summary = ev.get("summary") or ev.get("title") or "Calendar event"
                    start_ts = parse_iso_ts(ev.get("start")) or parse_iso_ts(ev.get("start_time"))
                    if not start_ts:
                        continue
                    event_date = time.strftime("%Y-%m-%d", time.localtime(start_ts))
                    key = f"{ent}|{int(start_ts)}|{summary}"
                    # Lead-time alert
                    if (start_ts - now_ts) >= 0 and (start_ts - now_ts) <= (lead * 60) and key not in announced:
                        ctx = f'"{summary}" starts at {format_clock(start_ts)} ({ent}).'
                        result = await process_emit(
                            topic="calendar",
                            category="calendar",
                            context=ctx,
                            route_raw="default",
                            conversation_id="charles_calendar_event",
                        )
                        if result.get("status") != "throttled":
                            announced.add(key)
                    if event_date == today:
                        today_events.append((start_ts, summary, ent))
                    elif event_date == tomorrow:
                        tomorrow_events.append((start_ts, summary, ent))

            def summarize(events: list) -> str:
                if not events:
                    return ""
                events_sorted = sorted(events, key=lambda x: x[0])
                parts = []
                for st, summ, ent in events_sorted[:8]:
                    parts.append(f"{format_clock(st)} — {summ} ({ent})")
                return "; ".join(parts)

            async with state_lock:
                state["calendar_announced"] = list(announced)[-100:]
                state["last_calendar_poll"] = now_ts
                await persist_state()
            await asyncio.sleep(interval)
        except Exception as err:
            print(f"[charles] calendar poll loop error: {err}")
            await asyncio.sleep(120)


@app.on_event("startup")
async def startup() -> None:
    await load_state()
    asyncio.create_task(scheduled_loop("musings"))
    asyncio.create_task(scheduled_loop("jokes"))
    asyncio.create_task(scheduled_loop("trivia"))
    asyncio.create_task(scheduled_people_loop())
    asyncio.create_task(presence_poll_loop())
    asyncio.create_task(brief_loop())
    asyncio.create_task(weather_poll_loop())
    asyncio.create_task(calendar_poll_loop())
    asyncio.create_task(arrival_pending_loop())


@app.get("/", response_class=HTMLResponse)
async def root_page() -> HTMLResponse:
    index = STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(status_code=404, detail="UI not found")
    return HTMLResponse(index.read_text(encoding="utf-8"))


@app.get("/api/health")
async def health() -> Dict[str, Any]:
    async with state_lock:
        quiet_active = in_quiet_hours(time.time())
        return {
            "status": "ok",
            "last_emit": state.get("last_emit", {}),
            "last_error": state.get("last_error", ""),
            "paused_all_until": state.get("paused_all_until", 0),
            "next_musing_time": state.get("next_musing_time", 0),
            "next_joke_time": state.get("next_joke_time", 0),
            "next_trivia_time": state.get("next_trivia_time", 0),
            "next_people_time": state.get("next_people_time", 0),
            "presence_poll_interval": state.get("presence_poll_interval", 60),
            "last_calendar_poll": state.get("last_calendar_poll", 0),
            "last_weather_time": state.get("last_weather_time", 0),
            "last_weather_payload": state.get("last_weather_payload", {}),
            "weather_min_gap": state.get("weather_min_gap", 0),
            "weather_temp_delta": state.get("weather_temp_delta", 5),
            "weather_condition_change": state.get("weather_condition_change", True),
            "weather_feed_only_minor": state.get("weather_feed_only_minor", False),
            "weather_poll_route": state.get("weather_poll_route", "feed"),
            "quiet_active": quiet_active,
            "quiet_feed_suppress": state.get("quiet_feed_suppress", False),
        }


@app.get("/api/state")
async def get_state() -> Dict[str, Any]:
    async with state_lock:
        merged = dict(state)
        merged["addon_version"] = ADDON_VERSION
        return merged


@app.post("/api/state")
async def update_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    async with state_lock:
        # merge incoming payload with type safety and defaults
        cats_in = payload.get("categories") or {}
        feed_in = payload.get("feed_categories") or {}
        all_cats = ["weather","trivia","calendar","system","vacuum","lighting","arrivals","people","musings","jokes","brief"]
        state["categories"] = {c: bool(cats_in.get(c, state.get("categories", {}).get(c, True))) for c in all_cats}
        state["feed_categories"] = {c: bool(feed_in.get(c, state.get("feed_categories", {}).get(c, True))) for c in all_cats}

        def intval(key: str, default: int) -> int:
            try:
                return int(payload.get(key, state.get(key, default)) or 0)
            except Exception:
                return default

        def floatval(key: str, default: float) -> float:
            try:
                return float(payload.get(key, state.get(key, default)) or 0)
            except Exception:
                return default

        state["persona_prompt"] = payload.get("persona_prompt", state.get("persona_prompt", DEFAULT_PROMPT))
        state["route_default"] = payload.get("route_default", state.get("route_default", "both"))
        state["throttle_seconds"] = intval("throttle_seconds", 0)
        state["feed_enabled"] = bool(payload.get("feed_enabled", state.get("feed_enabled", True)))
        state["notifications_enabled"] = bool(payload.get("notifications_enabled", state.get("notifications_enabled", True)))
        state["notify_service"] = payload.get("notify_service", state.get("notify_service", "notify.mobile_app_pixel_9"))
        state["notify_tag"] = payload.get("notify_tag", state.get("notify_tag", "charles_stream"))
        state["notify_title"] = payload.get("notify_title", state.get("notify_title", "CHARLES says"))
        state["notify_cap_chars"] = intval("notify_cap_chars", 240) or 240
        caps_in = payload.get("notify_caps") or state.get("notify_caps") or {}
        clean_caps = {}
        for key, val in caps_in.items():
            try:
                ival = int(val)
                if ival > 0:
                    clean_caps[key] = ival
            except Exception:
                continue
        state["notify_caps"] = clean_caps
        state["conversation_agent_id"] = payload.get("conversation_agent_id", state.get("conversation_agent_id", "conversation.openai_conversation"))
        state["quiet_hours_start"] = payload.get("quiet_hours_start", state.get("quiet_hours_start", "22:00"))
        state["quiet_hours_end"] = payload.get("quiet_hours_end", state.get("quiet_hours_end", "07:00"))
        state["quiet_feed_suppress"] = bool(payload.get("quiet_feed_suppress", state.get("quiet_feed_suppress", False)))
        state["weather_min_gap"] = intval("weather_min_gap", 0)
        state["weather_temp_delta"] = floatval("weather_temp_delta", 5)
        state["weather_condition_change"] = bool(payload.get("weather_condition_change", state.get("weather_condition_change", True)))
        state["weather_feed_only_minor"] = bool(payload.get("weather_feed_only_minor", state.get("weather_feed_only_minor", False)))
        state["weather_entity"] = payload.get("weather_entity", state.get("weather_entity", "weather.home"))
        state["weather_poll_interval"] = intval("weather_poll_interval", 300)
        state["weather_poll_route"] = payload.get("weather_poll_route", state.get("weather_poll_route", "feed"))
        state["trivia_interval_min"] = intval("trivia_interval_min", 0)
        state["trivia_interval_max"] = intval("trivia_interval_max", 0)
        state["trivia_daily_cap"] = intval("trivia_daily_cap", 0)
        state["arrival_emit_delay"] = intval("arrival_emit_delay", 0)
        state["arrival_combine_window"] = intval("arrival_combine_window", 300)
        state["brief_enabled_morning"] = bool(payload.get("brief_enabled_morning", state.get("brief_enabled_morning", True)))
        state["brief_enabled_evening"] = bool(payload.get("brief_enabled_evening", state.get("brief_enabled_evening", False)))
        state["brief_time_morning"] = payload.get("brief_time_morning", state.get("brief_time_morning", "07:45"))
        state["brief_time_evening"] = payload.get("brief_time_evening", state.get("brief_time_evening", "21:15"))
        state["brief_include_weather"] = bool(payload.get("brief_include_weather", state.get("brief_include_weather", True)))
        state["brief_include_calendar"] = bool(payload.get("brief_include_calendar", state.get("brief_include_calendar", True)))
        state["brief_include_unread"] = bool(payload.get("brief_include_unread", state.get("brief_include_unread", True)))
        state["brief_seed_source"] = payload.get("brief_seed_source", state.get("brief_seed_source", "musing"))
        state["calendar_lead_minutes"] = intval("calendar_lead_minutes", 60)
        state["calendar_poll_interval"] = intval("calendar_poll_interval", 300)
        state["musing_pool"] = payload.get("musing_pool", state.get("musing_pool", []))
        state["joke_pool"] = payload.get("joke_pool", state.get("joke_pool", []))
        state["trivia_pool"] = payload.get("trivia_pool", state.get("trivia_pool", []))
        state["trivia_urls"] = payload.get("trivia_urls", state.get("trivia_urls", []))
        state["news_urls"] = payload.get("news_urls", state.get("news_urls", []))
        state["calendar_entities"] = payload.get("calendar_entities", state.get("calendar_entities", []))
        state["people_entities"] = payload.get("people_entities", state.get("people_entities", []))
        state["presence_zones"] = payload.get("presence_zones", state.get("presence_zones", []))
        state["presence_poll_interval"] = intval("presence_poll_interval", 60)
        state["presence_auto_arrivals"] = bool(payload.get("presence_auto_arrivals", state.get("presence_auto_arrivals", True)))
        state["musings_interval_min"] = intval("musings_interval_min", 0)
        state["musings_interval_max"] = intval("musings_interval_max", 0)
        state["musings_daily_cap"] = intval("musings_daily_cap", 0)
        state["jokes_interval_min"] = intval("jokes_interval_min", 0)
        state["jokes_interval_max"] = intval("jokes_interval_max", 0)
        state["jokes_daily_cap"] = intval("jokes_daily_cap", 0)
        state["people_interval_min"] = intval("people_interval_min", 0)
        state["people_interval_max"] = intval("people_interval_max", 0)
        state["people_daily_cap"] = intval("people_daily_cap", 0)
        state["recent_limit"] = intval("recent_limit", DEFAULT_RECENT_LIMIT) or DEFAULT_RECENT_LIMIT

        # trim recents on save
        rec_seeds = state.get("recent_seeds", {})
        rec_outputs = state.get("recent_outputs", {})
        limit = int(state.get("recent_limit", DEFAULT_RECENT_LIMIT))
        for key in ["jokes", "musings", "trivia"]:
            if key in rec_seeds:
                rec_seeds[key] = rec_seeds[key][:limit]
            if key in rec_outputs:
                rec_outputs[key] = rec_outputs[key][:limit]
        state["recent_outputs_general"] = (state.get("recent_outputs_general", []) or [])[:limit]
        state["recent_seeds"] = rec_seeds
        state["recent_outputs"] = rec_outputs
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
    weather_payload: Optional[Dict[str, Any]] = None,
    use_raw_context: bool = False,
    seed_tag: Optional[str] = None,
    message_override: Optional[str] = None,
    group_key: Optional[str] = None,
    combine_window: Optional[int] = None,
    arrival_name: Optional[str] = None,
    arrival_location: Optional[str] = None,
    force: bool = False,
) -> Dict[str, Any]:
    async with state_lock:
        default_route = state.get("route_default", "both")
        throttle_seconds = int(state.get("throttle_seconds", 0))
        last_key = state.get("last_entry_key", "")
        last_ts = float(state.get("last_entry_time", 0.0))
        paused_all_until = float(state.get("paused_all_until", 0.0))
        resolved_route = default_route if route_raw == "default" else route_raw
        now_ts = time.time()

        # Capture weather state upfront
        weather_last_time = float(state.get("last_weather_time", 0.0))
        weather_last_payload = state.get("last_weather_payload", {}) or {}
        weather_min_gap = int(state.get("weather_min_gap", 0))
        weather_temp_delta = float(state.get("weather_temp_delta", 5))
        weather_cond_change = bool(state.get("weather_condition_change", True))
        weather_feed_only_minor = bool(state.get("weather_feed_only_minor", False))

    minor_weather = False
    combined_feed_prev = None
    combined_names: List[str] = []
    combine_mode = False
    if category.lower() == "weather" and not force:
        if weather_min_gap > 0 and (now_ts - weather_last_time) < weather_min_gap:
            return {"status": "throttled", "route": resolved_route}
        current_payload = weather_payload or {}
        if current_payload:
            significant, reason = weather_significant(
                weather_last_payload,
                current_payload,
                weather_temp_delta,
                weather_cond_change,
            )
            if not significant:
                if weather_feed_only_minor:
                    minor_weather = True
                    resolved_route = "feed"
                else:
                    return {"status": "throttled", "route": resolved_route}
    if category.lower() == "arrivals" and group_key:
        awin = combine_window if combine_window is not None else 300
        arrivals = state.get("arrival_groups", {})
        prev = arrivals.get(group_key)
        if prev and (now_ts - prev.get("ts", 0)) <= awin:
            combine_mode = True
            combined_names = list(prev.get("names", []))
            if arrival_name:
                combined_names.append(arrival_name)
            combined_names = sorted(set([n for n in combined_names if n]))
            loc = arrival_location or prev.get("location") or topic
            context = f"{', '.join(combined_names)} arrived at {loc} within the last few minutes."
            combined_feed_prev = prev.get("feed_line")
        else:
            if arrival_name:
                combined_names = [arrival_name]
            loc = arrival_location or topic
            context = context or f"{arrival_name or 'Someone'} arrived at {loc}."
    current_key = f"{category.lower()}|{topic.lower()}|{context.strip()}"
    if not force and current_key == last_key and (now_ts - last_ts) < throttle_seconds:
        return {"status": "throttled", "route": resolved_route}

    if not force and paused_all_until > now_ts:
        return {"status": "paused", "route": resolved_route, "paused_all_until": paused_all_until}

    if not force and is_paused(category):
        return {"status": "paused", "route": resolved_route}

    if category.lower() not in {"jokes", "musings", "trivia"} and not force:
        if await is_similar_general(context):
            return {"status": "throttled", "route": resolved_route, "reason": "similar"}

    agent_prompt = prompt_override or state.get("persona_prompt", DEFAULT_PROMPT)
    agent_id = state.get("conversation_agent_id", "conversation.openai_conversation")

    if message_override is not None:
        message_text = message_override
    elif use_raw_context:
        message_text = context
    else:
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
    if quiet and bool(state.get("quiet_feed_suppress", False)):
        send_feed = False
    send_notify = (
        state.get("notifications_enabled", True)
        and resolved_route in {"notify", "both"}
        and category_allowed(category)
        and (not quiet)
    )
    if category.lower() == "weather" and minor_weather:
        send_notify = False

    feed_line = None
    if send_feed:
        line_content = f"{ts_iso} · {topic} · {' '.join(message_text.split())}"
        if combine_mode and combined_feed_prev:
            feed_line = replace_last_feed_line(combined_feed_prev, line_content)
        else:
            feed_line = append_feed_line(ts_iso, topic, message_text)

    if send_notify:
        existing_unread = state.get("unread_log", [])
        notify_message = build_notify_summary(message_text, existing_unread)
        caps = state.get("notify_caps", {})
        cap_override = caps.get(category.lower())
        if cap_override:
            try:
                cap_val = int(cap_override)
                if cap_val > 0 and len(notify_message) > cap_val:
                    notify_message = notify_message[: max(0, cap_val - 3)] + "..."
            except Exception:
                pass
        notify_service = notify_service_override or state.get("notify_service", "notify.mobile_app_pixel_9")
        notify_tag = notify_tag_override or state.get("notify_tag", "charles_stream")
        notify_title = notify_title_override or state.get("notify_title", "CHARLES says")
        await call_notify(
            service=notify_service,
            title=notify_title,
            message=notify_message,
            tag=notify_tag,
        )

    async with state_lock:
        state["last_entry_key"] = current_key
        state["last_entry_time"] = now_ts
        if category.lower() == "weather":
            state["last_weather_time"] = now_ts
            state["last_weather_payload"] = weather_payload or {}
        if send_notify:
            unread_entry = message_text[:140]
            existing = state.get("unread_log", [])
            state["unread_log"] = ([unread_entry] + existing)[:5]
            state["unread_count"] = int(state.get("unread_count", 0)) + 1
        if category.lower() in {"jokes", "musings", "trivia"}:
            push_recent(category.lower(), seed_tag, message_text)
        else:
            limit = int(state.get("recent_limit", DEFAULT_RECENT_LIMIT))
            ro = state.setdefault("recent_outputs_general", [])
            ro.insert(0, message_text[:200])
            del ro[limit:]
        if category.lower() in {"arrivals", "people"} and group_key:
            arrivals = state.setdefault("arrival_groups", {})
            arrivals[group_key] = {
                "ts": now_ts,
                "names": combined_names or ([arrival_name] if arrival_name else []),
                "location": arrival_location or topic,
                "feed_line": feed_line,
            }
        state["last_emit"] = {
            "time": now_ts,
            "topic": topic,
            "category": category,
            "route": resolved_route,
            "status": "ok",
            "notified": bool(send_notify),
        }
        state["last_error"] = ""
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


def normalize_text(txt: str) -> str:
    txt = txt.lower()
    txt = re.sub(r"[^a-z0-9\s]", " ", txt)
    return " ".join(txt.split())


async def is_similar_output(category: str, candidate: str) -> bool:
    cat = category.lower()
    norm = normalize_text(candidate)
    async with state_lock:
        recents = list(state.get("recent_outputs", {}).get(cat, []))
    for recent in recents:
        rnorm = normalize_text(recent)
        if not rnorm:
            continue
        ratio = difflib.SequenceMatcher(None, norm, rnorm).ratio()
        if ratio >= 0.72:
            return True
        tokens = set(norm.split())
        rtokens = set(rnorm.split())
        if tokens and rtokens:
            overlap = len(tokens & rtokens) / max(len(tokens), len(rtokens))
            if overlap >= 0.6:
                return True
    return False


async def is_similar_general(candidate: str) -> bool:
    norm = normalize_text(candidate)
    async with state_lock:
        recents = list(state.get("recent_outputs_general", []))
    for recent in recents:
        rnorm = normalize_text(recent)
        if not rnorm:
            continue
        ratio = difflib.SequenceMatcher(None, norm, rnorm).ratio()
        if ratio >= 0.9:
            return True
    return False


async def emit_with_retry(category: str, conversation_id: str, topic: Optional[str] = None, max_attempts: int = 3) -> Dict[str, Any]:
    attempts = 0
    tried_seeds: List[str] = []
    last_result = None
    while attempts < max_attempts:
        context, tp, seed = await build_quick_context(category, excluded_seeds=tried_seeds)
        attempts += 1
        if seed:
            tried_seeds.append(seed)
        # Generate message via persona first
        agent_prompt = state.get("persona_prompt", DEFAULT_PROMPT)
        agent_id = state.get("conversation_agent_id", "conversation.openai_conversation")
        message = await call_conversation(
            prompt=agent_prompt,
            topic=tp,
            context=context,
            conversation_id=conversation_id,
            agent_id=agent_id,
        )
        if category.lower() in {"jokes", "musings", "trivia"}:
            if await is_similar_output(category, message):
                continue
        result = await process_emit(
            topic=topic or tp,
            category=category,
            context=context,
            route_raw="default",
            conversation_id=conversation_id,
            seed_tag=seed,
            message_override=message,
        )
        return result
    # Fallback: return last attempted result if any; otherwise throttled
    return last_result or {"status": "throttled", "route": "default"}


@app.post("/api/emit")
async def emit(payload: Dict[str, Any]) -> JSONResponse:
    # Arrivals can be queued for debounce
    if payload.get("category", "").lower() == "arrivals":
        result = await handle_arrival_emit(payload)
        code = 202 if result.get("status") == "pending" else 200
        return JSONResponse(result, status_code=code)
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
        force=bool(payload.get("force", False)),
        weather_payload={
            "temperature": payload.get("weather_temperature"),
            "condition": payload.get("weather_condition"),
        },
    )
    if result.get("status") == "throttled":
        return JSONResponse(result, status_code=202)
    return JSONResponse(result)


@app.post("/api/trigger")
async def trigger(payload: Dict[str, Any]) -> JSONResponse:
    category = payload.get("category", "system")
    force = bool(payload.get("force", False))
    if category.lower() in {"jokes", "musings", "trivia"}:
        result = await emit_with_retry(category, conversation_id=f"charles_trigger_{category}")
    else:
        context, topic, seed = await build_quick_context(category)
        result = await process_emit(
            topic=topic,
            category=category,
            context=context,
            route_raw="default",
            conversation_id=f"charles_trigger_{category}",
            use_raw_context=False,
            seed_tag=seed,
            force=force,
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


@app.post("/api/trivia/reload")
async def reload_trivia() -> Dict[str, Any]:
    pool = await load_trivia_pool(force=True)
    return {"status": "ok", "count": len(pool), "loaded_at": time.time()}


@app.post("/api/preview")
async def preview(payload: Dict[str, Any]) -> Dict[str, Any]:
    topic = payload.get("topic", "general")
    category = payload.get("category", "system")
    context = payload.get("context", "All systems nominal.")
    agent_prompt = state.get("persona_prompt", DEFAULT_PROMPT)
    agent_id = state.get("conversation_agent_id", "conversation.openai_conversation")
    message = await call_conversation(
        prompt=agent_prompt,
        topic=topic,
        context=context,
        conversation_id=payload.get("conversation_id", "charles_preview"),
        agent_id=agent_id,
    )
    return {"status": "ok", "message": message, "category": category, "topic": topic, "context": context}


@app.get("/api/preview/brief")
async def preview_brief() -> Dict[str, Any]:
    brief = await build_brief()
    agent_prompt = state.get("persona_prompt", DEFAULT_PROMPT)
    agent_id = state.get("conversation_agent_id", "conversation.openai_conversation")
    message = await call_conversation(
        prompt=agent_prompt,
        topic="brief",
        context=brief,
        conversation_id="charles_preview_brief",
        agent_id=agent_id,
    )
    return {"status": "ok", "context": brief, "message": message}


@app.post("/api/mute")
async def mute(payload: Dict[str, Any]) -> Dict[str, Any]:
    raise HTTPException(status_code=410, detail="mute support removed")


@app.post("/api/pause")
async def pause(payload: Dict[str, Any]) -> Dict[str, Any]:
    category = payload.get("category")
    minutes = float(payload.get("minutes", 0))
    if not category:
        raise HTTPException(status_code=400, detail="category required")
    until = time.time() + (minutes * 60) if minutes > 0 else 0
    async with state_lock:
        if category.lower() == "all":
            state["paused_all_until"] = until
        else:
            pauses = state.setdefault("pauses", {})
            if until > 0:
                pauses[category.lower()] = until
            else:
                pauses.pop(category.lower(), None)
        await persist_state()
    return {"status": "ok", "category": category, "paused_until": until}


@app.get("/api/entities/{domain}")
async def list_entities(domain: str) -> Dict[str, Any]:
    domain = domain.lower()
    if domain not in {"weather", "calendar", "person", "zone"}:
        raise HTTPException(status_code=400, detail="domain must be weather, calendar, person, or zone")
    token = os.getenv("SUPERVISOR_TOKEN")
    if not token:
        raise HTTPException(status_code=500, detail="Missing supervisor token")
    try:
        async with httpx.AsyncClient(timeout=8.0) as client:
            res = await client.get("http://supervisor/core/api/states", headers={"Authorization": f"Bearer {token}"})
            res.raise_for_status()
            data = res.json()
            items = [
                {"entity_id": item.get("entity_id"), "name": item.get("attributes", {}).get("friendly_name", "")}
                for item in data
                if str(item.get("entity_id", "")).startswith(f"{domain}.")
            ]
            return {"status": "ok", "entities": items}
    except Exception as err:
        raise HTTPException(status_code=500, detail=f"Failed to list entities: {err}")


async def handle_arrival_emit(payload: Dict[str, Any]) -> Dict[str, Any]:
    name = payload.get("name") or payload.get("arrival_name") or payload.get("person")
    location = payload.get("location") or payload.get("arrival_location") or payload.get("topic", "home")
    group_key = (payload.get("group_key") or location or "people").strip().lower()
    context = payload.get("context") or f"{name or 'Someone'} arrived at {location}."
    category_override = payload.get("category_override")
    topic_val = payload.get("topic") or "people"
    delay = int(state.get("arrival_emit_delay", 0))
    combine_window = int(state.get("arrival_combine_window", 300))
    now_ts = time.time()
    if delay <= 0:
        return await process_emit(
            topic=topic_val,
            category=category_override or "arrivals",
            context=context,
            route_raw=payload.get("route", "default"),
            conversation_id=payload.get("conversation_id", "charles_arrivals"),
            arrival_name=name,
            arrival_location=location,
            group_key=group_key,
            combine_window=combine_window,
            force=bool(payload.get("force", False)),
        )
    async with state_lock:
        pending = state.setdefault("arrival_pending", {})
        entry = pending.get(group_key, {"names": []})
        names = entry.get("names", [])
        if name and name not in names:
            names.append(name)
        entry.update({
            "ts": now_ts,
            "names": names,
            "location": location,
            "context": context,
            "route": payload.get("route", "default"),
            "conversation_id": payload.get("conversation_id", "charles_arrivals"),
            "combine_window": combine_window,
            "category_override": category_override or "arrivals",
        })
        pending[group_key] = entry
        await persist_state()
    return {"status": "pending", "route": "default"}


async def arrival_pending_loop() -> None:
    while True:
        try:
            delay = int(state.get("arrival_emit_delay", 0))
            if delay <= 0:
                await asyncio.sleep(20)
                continue
            now_ts = time.time()
            emit_list = []
            async with state_lock:
                pending = state.get("arrival_pending", {})
                to_remove = []
                for key, entry in pending.items():
                    ts = entry.get("ts", 0)
                    if (now_ts - ts) >= delay:
                        emit_list.append((key, entry))
                        to_remove.append(key)
                for k in to_remove:
                    pending.pop(k, None)
                if to_remove:
                    state["arrival_pending"] = pending
                    await persist_state()
            for key, entry in emit_list:
                names = entry.get("names", [])
                location = entry.get("location", "home")
                ctx = entry.get("context") or f"{', '.join(names) if names else 'Someone'} arrived at {location}."
                if len(names) > 1:
                    ctx = f"{', '.join(names)} arrived at {location} within the last few minutes."
                await process_emit(
                    topic="people",
                    category=entry.get("category_override") or "arrivals",
                    context=ctx,
                    route_raw=entry.get("route", "default"),
                    conversation_id=entry.get("conversation_id", "charles_arrivals"),
                    group_key=key,
                    combine_window=entry.get("combine_window", 300),
                    arrival_name=names[-1] if names else None,
                    arrival_location=location,
                )
            await asyncio.sleep(20)
        except Exception as err:
            print(f"[charles] arrival pending loop error: {err}")
            await asyncio.sleep(30)

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
            next_fire = time.time() + (delay_minutes * 60)
            async with state_lock:
                state[f"next_{kind}_time"] = next_fire
                await persist_state()
            await asyncio.sleep(delay_minutes * 60)

            async with state_lock:
                enabled_feed = state.get("feed_enabled", True)
                enabled_notify = state.get("notifications_enabled", True)
                category_allowed_flag = category_allowed(kind)
                feed_allowed_flag = feed_category_allowed(kind)
                if (not enabled_feed and not enabled_notify) or (not category_allowed_flag and not feed_allowed_flag):
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
                seed = choose_seed(kind, pool)
                if seed:
                    context = (
                        f"Seed: {seed}\n"
                        f"{'Tell exactly one short joke based on this seed. Avoid repeating earlier jokes today. Return only one short joke.' if kind=='jokes' else 'Respond with one short, wry musing inspired by this seed. Keep it to one or two sentences. Avoid repeating earlier musings today.'}"
                    )
                else:
                    context = f"Share one short {kind[:-1]} update in one sentence."
            if kind in {"jokes", "musings", "trivia"}:
                result = await emit_with_retry(kind, conversation_id=f"charles_{kind}")
            else:
                result = await process_emit(
                    topic=kind,
                    category=kind,
                    context=context,
                    route_raw="default",
                    conversation_id=f"charles_{kind}",
                    seed_tag=seed,
                )
            if result.get("status") != "throttled":
                async with state_lock:
                    state[count_key] = state.get(count_key, 0) + 1
                    now_val = time.time()
                    state[f"last_{kind}_time"] = now_val
                    state[f"next_{kind}_time"] = now_val + (delay_minutes * 60)
                    await persist_state()
        except Exception as err:
            print(f"[charles] scheduler error ({kind}): {err}")
            await asyncio.sleep(60)


async def scheduled_people_loop() -> None:
    while True:
        try:
            async with state_lock:
                interval_min = int(state.get("people_interval_min", 120))
                interval_max = int(state.get("people_interval_max", max(interval_min, 120)))
            delay_minutes = random.randint(interval_min, max(interval_min, interval_max)) if interval_min > 0 else 120
            next_fire = time.time() + (delay_minutes * 60)
            async with state_lock:
                state["next_people_time"] = next_fire
                await persist_state()
            await asyncio.sleep(delay_minutes * 60)

            async with state_lock:
                enabled_feed = state.get("feed_enabled", True)
                enabled_notify = state.get("notifications_enabled", True)
                category_allowed_flag = category_allowed("people")
                feed_allowed_flag = feed_category_allowed("people")
                if (not enabled_feed and not enabled_notify) or (not category_allowed_flag and not feed_allowed_flag):
                    continue

                today = time.strftime("%Y-%m-%d", time.localtime())
                cap = int(state.get("people_daily_cap", 0))
                if state.get("people_count_date") != today:
                    state["people_count_date"] = today
                    state["people_count_today"] = 0
                if cap > 0 and state.get("people_count_today", 0) >= cap:
                    continue

            ctx = await build_people_context()
            result = await process_emit(
                topic="people",
                category="people",
                context=ctx,
                route_raw="default",
                conversation_id="charles_people",
            )
            if result.get("status") != "throttled":
                async with state_lock:
                    state["people_count_today"] = state.get("people_count_today", 0) + 1
                    now_val = time.time()
                    state["last_people_time"] = now_val
                    state["next_people_time"] = now_val + (delay_minutes * 60)
                    await persist_state()
        except Exception as err:
            print(f"[charles] scheduler error (people): {err}")
            await asyncio.sleep(60)


async def presence_poll_loop() -> None:
    """Poll person entities to auto-detect arrivals into selected zones/home."""
    while True:
        try:
            interval = max(20, int(state.get("presence_poll_interval", 60)))
            auto_on = bool(state.get("presence_auto_arrivals", True))
            if not auto_on:
                await asyncio.sleep(interval)
                continue
            token = os.getenv("SUPERVISOR_TOKEN")
            if not token:
                await asyncio.sleep(interval)
                continue
            persons_filter = [p.strip() for p in state.get("people_entities", []) if p.strip()]
            zones_filter_raw = [z.strip().lower() for z in state.get("presence_zones", []) if z.strip()]
            zones_filter: set[str] = set(zones_filter_raw)
            for z in zones_filter_raw:
                if z.startswith("zone."):
                    zones_filter.add(z.replace("zone.", "", 1))
            if "zone.home" in zones_filter_raw:
                zones_filter.add("home")
            async with httpx.AsyncClient(timeout=10.0) as client:
                res = await client.get("http://supervisor/core/api/states", headers={"Authorization": f"Bearer {token}"})
                res.raise_for_status()
                data = res.json()
            # build lookup of last states
            async with state_lock:
                last_states = dict(state.get("presence_last_states", {}))
            updates = []
            for item in data:
                ent_id = str(item.get("entity_id", ""))
                if not ent_id.startswith("person."):
                    continue
                if persons_filter and ent_id not in persons_filter:
                    continue
                zone = str(item.get("state", "unknown"))
                # ignore not_home / unknown transitions
                if zone in {"not_home", "unknown", "unavailable"}:
                    continue
                if zones_filter and zone.lower() not in zones_filter:
                    continue
                prev = last_states.get(ent_id)
                if prev == zone:
                    continue
                name = item.get("attributes", {}).get("friendly_name") or ent_id
                updates.append((ent_id, name, zone))
                last_states[ent_id] = zone
            if updates:
                async with state_lock:
                    state["presence_last_states"] = last_states
                    await persist_state()
                for ent_id, name, zone in updates:
                    ctx = f"{name} arrived at {zone}."
                    payload = {
                        "name": name,
                        "location": zone,
                        "group_key": zone,
                        "context": ctx,
                        "category_override": "people",
                        "route": "default",
                        "topic": "people",
                        "combine_window": int(state.get("arrival_combine_window", 300) or 300),
                    }
                    await handle_arrival_emit(payload)
            await asyncio.sleep(interval)
        except Exception as err:
            print(f"[charles] presence poll loop error: {err}")
            await asyncio.sleep(60)
async def poll_weather_entity() -> Optional[Dict[str, Any]]:
    token = os.getenv("SUPERVISOR_TOKEN")
    entity_id = state.get("weather_entity", "weather.home")
    if not token or not entity_id:
        return None
    url = f"http://supervisor/core/api/states/{entity_id}"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            res = await client.get(url, headers={"Authorization": f"Bearer {token}"})
            res.raise_for_status()
            data = res.json()
            return {
                "condition": data.get("state"),
                "temperature": data.get("attributes", {}).get("temperature"),
            }
    except Exception as err:
        print(f"[charles] weather poll failed: {err}")
        return None


async def brief_loop() -> None:
    while True:
        try:
            now = time.localtime()
            today = time.strftime("%Y-%m-%d", now)
            async with state_lock:
                enabled_m = bool(state.get("brief_enabled_morning", True))
                enabled_e = bool(state.get("brief_enabled_evening", False))
                tm = state.get("brief_time_morning", "07:45") or "07:45"
                te = state.get("brief_time_evening", "21:15") or "21:15"
                sent_m = state.get("brief_morning_sent", "")
                sent_e = state.get("brief_evening_sent", "")
            def minutes(val: str) -> int:
                try:
                    h, m = [int(x) for x in val.split(":")]
                    return h*60+m
                except Exception:
                    return 0
            now_min = now.tm_hour*60 + now.tm_min
            tasks = []
            if enabled_m and sent_m != today and now_min >= minutes(tm):
                tasks.append("morning")
            if enabled_e and sent_e != today and now_min >= minutes(te):
                tasks.append("evening")
            for which in tasks:
                brief = await build_brief()
                res = await process_emit(
                    topic="brief",
                    category="brief",
                    context=brief,
                    route_raw="default",
                    conversation_id=f"charles_brief_{which}",
                )
                if res.get("status") != "throttled":
                    async with state_lock:
                        if which == "morning":
                            state["brief_morning_sent"] = today
                        else:
                            state["brief_evening_sent"] = today
                        await persist_state()
            await asyncio.sleep(60)
        except Exception as err:
            print(f"[charles] brief loop error: {err}")
            await asyncio.sleep(120)


async def weather_poll_loop() -> None:
    while True:
        try:
            interval = max(60, int(state.get("weather_poll_interval", 300)))
            payload = await poll_weather_entity()
            if payload and payload.get("condition") not in (None, "unknown", "unavailable"):
                context = f"Weather update: {payload.get('condition','?')} at {payload.get('temperature','?')}°."
                route = state.get("weather_poll_route", "feed")
                result = await process_emit(
                    topic="weather",
                    category="weather",
                    context=context,
                    route_raw=route if route in {"feed", "notify", "both"} else "feed",
                    conversation_id="charles_weather_poll",
                    weather_payload=payload,
                )
                if result.get("status") == "ok":
                    async with state_lock:
                        state["last_emit"] = {
                            "time": time.time(),
                            "topic": "weather",
                            "category": "weather",
                            "route": "feed",
                            "status": "ok",
                            "notified": result.get("notified", False),
                        }
                        state["last_error"] = ""
                        await persist_state()
            await asyncio.sleep(interval)
        except Exception as err:
            print(f"[charles] weather poll loop error: {err}")
            await asyncio.sleep(120)
