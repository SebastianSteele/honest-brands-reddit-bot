import os
import re
import json
import asyncio
import random
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv
import aiohttp
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

load_dotenv()

TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CLICKUP_TOKEN = os.getenv("CLICKUP_TOKEN")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")
CLICKUP_MEMBER_DB_LIST_ID = "901516122313"
EXPORT_WEBHOOK_URL = os.getenv("EXPORT_WEBHOOK_URL", "")
# Optional: exact name of the weekly-hours custom field on the check-in list (see CANONICAL_WEEKLY_HOURS_FIELD_NAMES).
CLICKUP_WEEKLY_HOURS_FIELD_NAME = (os.getenv("CLICKUP_WEEKLY_HOURS_FIELD_NAME") or "").strip()
# Optional: force this field UUID on the check-in list (skips list-field discovery).
CLICKUP_CI_FIELD_WEEKLY_HOURS_BAND = (os.getenv("CLICKUP_CI_FIELD_WEEKLY_HOURS_BAND") or "").strip()
# Display name used when auto-creating the Number field via POST /v2/list/{list_id}/field.
CLICKUP_WEEKLY_HOURS_FIELD_DISPLAY_NAME = (
    (os.getenv("CLICKUP_WEEKLY_HOURS_FIELD_DISPLAY_NAME") or "Weekly Number of Hours").strip()
)
# When true (default), create that field on the check-in list if it is missing.
CLICKUP_AUTO_CREATE_WEEKLY_HOURS_FIELD = os.getenv(
    "CLICKUP_AUTO_CREATE_WEEKLY_HOURS_FIELD", "true",
).lower() not in ("0", "false", "no", "off")

# --- Validate required env vars at import time ---
_missing = [k for k, v in {
    "DISCORD_TOKEN": DISCORD_TOKEN,
    "CLICKUP_TOKEN": CLICKUP_TOKEN,
    "CLICKUP_LIST_ID": CLICKUP_LIST_ID,
}.items() if not v]
if _missing:
    raise RuntimeError(f"Missing required env vars: {', '.join(_missing)}. Check your .env file.")

# ClickUp Member Database field IDs
CU_FIELD_DISCORD_USERNAME = "1aad9b55-223b-40f9-96e6-9388386b5ed2"
CU_FIELD_LAST_ACTIVITY_DATE = "7d31a36c-eccc-43e0-8311-861d82202850"
CU_FIELD_LAST_ACTIVITY = "245ff4b2-fbb0-446c-b398-5e2a75f57d21"
CU_FIELD_MILESTONE = "d02fa014-856a-4f55-ba3e-4ec57a21b002"
CU_FIELD_WEEKS_IN_STAGE = "7771170b-f862-4435-89e6-11a149a51646"
CU_FIELD_BLOCKER = "84fe7f3d-716c-4cd2-98c6-1a088c32d104"
CU_FIELD_WHAT_WOULD_HELP = "074c35ab-2ad6-466c-ab8e-685aea688d86"
CU_FIELD_NEXT_STEPS = "414d79b2-d1ab-47b8-981e-428b55f7533a"

# Last Weekly Check-in Date — date field on the Member Database
CU_FIELD_LAST_CHECKIN_DATE = "b504e08a-086f-402b-a76f-f5b158896b4c"

# ClickUp Program Name field (dropdown) — used to identify Accelerate members
CU_FIELD_PROGRAM_NAME = "d44e9584-d751-40fb-9b52-0cb7fb9d80aa"
CU_PROGRAM_ACCELERATE_INDEX = 1  # orderindex for "Accelerate" in the dropdown

# ClickUp Member Database — Coach field (users type)
CU_FIELD_COACH = "3c4c9ce5-07f5-4aa3-a0bf-1dbca6c9efe3"

# Program Name dropdown options (orderindex → name)
PROGRAM_NAMES = {0: "Core", 1: "Accelerate", 2: "Scale", 3: "Velocity"}

# ClickUp Check-in List field IDs (populated on each task)
CI_FIELD_BLOCKER = "84fe7f3d-716c-4cd2-98c6-1a088c32d104"
CI_FIELD_DATE = "f60d63b8-924b-42a5-84df-8f612656fbf2"
CI_FIELD_MEMBER = "7a6a1a07-2e70-44ad-bb93-5e807ea7035c"
CI_FIELD_NEXT_STEPS = "414d79b2-d1ab-47b8-981e-428b55f7533a"
CI_FIELD_STAGE = "2e00e59d-ac4a-401e-b632-b90ec44962b2"
CI_FIELD_WEEK = "7160ff5a-8278-4d17-8c71-b9c13f04a1a6"
CI_FIELD_WEEKS_IN_STAGE = "2710fa28-d9bd-4462-b9c6-b8e346144518"
CI_FIELD_WHAT_WOULD_HELP = "074c35ab-2ad6-466c-ab8e-685aea688d86"

# Map bot stages to ClickUp Milestone dropdown options
STAGE_TO_MILESTONE = {
    "1. Finding a product": "1. Select a Product",
    "2. Building a store": "2. Build Site",
    "3. Creating ads": "3. Make Ads",
    "4. Getting sales": "4. First Sale",
    "5. Scaling": "5. Scaling",
}

# Only DM members who joined within this many months
MEMBER_MAX_AGE_MONTHS = 7

# Total weekly DMs in the new-member sequence (overridden by NEW_MEMBER_TOTAL_STEPS env var in testing)
NEW_MEMBER_TOTAL_STEPS = int(os.getenv("NEW_MEMBER_TOTAL_STEPS", "4"))

# Persistent state directory.
#
# On Railway/Heroku/etc. the container filesystem is ephemeral — every redeploy
# wipes any file written next to bot.py. That used to silently reset
# pending_checkins.json, known_accelerate.json, dm_blocked.json, AND
# checkin_data.json (this last one was even committed to the repo so each
# `git pull` during deploy clobbered live state with the snapshot from the
# last commit).
#
# Set STATE_DIR=/data in production (with a Railway volume mounted at /data)
# to keep all bot state across deploys. Unset locally to fall back to the
# script directory — local dev keeps working unchanged.
_STATE_DIR_OVERRIDE = (os.getenv("STATE_DIR") or "").strip()
STATE_DIR = _STATE_DIR_OVERRIDE or os.path.dirname(__file__)
if _STATE_DIR_OVERRIDE:
    try:
        os.makedirs(STATE_DIR, exist_ok=True)
        print(f"[STATE] persistent state directory: {STATE_DIR}")
    except Exception as _e:
        print(f"[STATE] could NOT create {STATE_DIR}: {_e} — falling back to script dir")
        STATE_DIR = os.path.dirname(__file__)

# File to persist pending new joiners awaiting their first check-in
PENDING_FILE = os.path.join(STATE_DIR, "pending_checkins.json")

# File to track weekly check-in submissions
CHECKIN_DATA_FILE = os.path.join(STATE_DIR, "checkin_data.json")

# File to track users who have DMs disabled (skip them instead of retrying)
DM_BLOCKED_FILE = os.path.join(STATE_DIR, "dm_blocked.json")

# Stages where follow-up DMs stop (from check-in form selection)
ADVANCED_STAGES = {"4. Getting sales", "5. Scaling"}

# File to track which Accelerate members have been seen (so only NEW ones get the onboarding sequence)
KNOWN_MEMBERS_FILE = os.path.join(STATE_DIR, "known_accelerate.json")

# DM pacing: send in batches to avoid spam detection
DM_DELAY_MIN = 8   # minimum seconds between DMs
DM_DELAY_MAX = 15  # maximum seconds between DMs
DM_BATCH_SIZE = 20  # pause after this many DMs
DM_BATCH_PAUSE = 60  # seconds to pause between batches


# --- ClickUp-based Accelerate member lookup (cached) ---
_accelerate_cache: dict = {"usernames": set(), "last_fetched": None}
_CACHE_TTL = timedelta(hours=1)


async def fetch_accelerate_usernames() -> set:
    """Query ClickUp Member Database and return a set of lowercased Discord usernames
    whose Program Name is 'Accelerate'.  Results are cached for 1 hour."""
    now = datetime.now()
    if (_accelerate_cache["last_fetched"] is not None
            and now - _accelerate_cache["last_fetched"] < _CACHE_TTL):
        return _accelerate_cache["usernames"]

    headers = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}
    usernames = set()
    page = 0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    f"https://api.clickup.com/api/v2/list/{CLICKUP_MEMBER_DB_LIST_ID}/task",
                    params={"include_closed": "true", "subtasks": "true", "page": page},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        print(f"[CLICKUP] Failed to fetch members: {resp.status}")
                        return _accelerate_cache["usernames"]  # return stale cache
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"[CLICKUP] Network error: {e}")
                return _accelerate_cache["usernames"]

            task_list = data.get("tasks", [])
            if not task_list:
                break

            for task in task_list:
                program_name_val = None
                discord_username = None
                for cf in task.get("custom_fields", []):
                    if cf.get("id") == CU_FIELD_PROGRAM_NAME:
                        program_name_val = cf.get("value")
                    elif cf.get("id") == CU_FIELD_DISCORD_USERNAME:
                        discord_username = (cf.get("value") or "").strip()
                if (program_name_val is not None
                        and int(program_name_val) == CU_PROGRAM_ACCELERATE_INDEX
                        and discord_username):
                    usernames.add(discord_username.lower())
            page += 1

    _accelerate_cache["usernames"] = usernames
    _accelerate_cache["last_fetched"] = now
    print(f"[CLICKUP] Refreshed Accelerate cache: {len(usernames)} members")
    return usernames


def is_within_join_window(member: discord.Member) -> bool:
    """Return True if the member joined within MEMBER_MAX_AGE_MONTHS."""
    if member.joined_at is None:
        return False
    cutoff = datetime.now(timezone.utc) - timedelta(days=MEMBER_MAX_AGE_MONTHS * 30)
    return member.joined_at >= cutoff


# --- ClickUp-based Stage 4/5 exclusion (checks submitted check-ins) ---
_exclusion_cache: dict = {"user_ids": set(), "last_fetched": None}


async def fetch_excluded_user_ids() -> set:
    """Query the ClickUp check-in list and return a set of Discord user IDs
    whose most recent check-in has Stage 4 or 5.  Cached for 1 hour."""
    now = datetime.now()
    if (_exclusion_cache["last_fetched"] is not None
            and now - _exclusion_cache["last_fetched"] < _CACHE_TTL):
        return _exclusion_cache["user_ids"]

    headers = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}
    excluded = set()
    page = 0

    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task",
                    params={"include_closed": "true", "page": page},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        print(f"[CLICKUP] Failed to fetch check-ins: {resp.status}")
                        return _exclusion_cache["user_ids"]
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"[CLICKUP] Network error fetching check-ins: {e}")
                return _exclusion_cache["user_ids"]

            task_list = data.get("tasks", [])
            if not task_list:
                break

            for task in task_list:
                stage = None
                for cf in task.get("custom_fields", []):
                    if cf.get("id") == CI_FIELD_STAGE:
                        stage = (cf.get("value") or "").strip()
                if stage and stage in ADVANCED_STAGES:
                    # Extract Discord user ID from uid: tag
                    for tag in task.get("tags", []):
                        tag_name = tag.get("name", "")
                        if tag_name.startswith("uid:"):
                            excluded.add(tag_name[4:])
            page += 1

    _exclusion_cache["user_ids"] = excluded
    _exclusion_cache["last_fetched"] = now
    print(f"[CLICKUP] Refreshed exclusion cache: {len(excluded)} members in Stage 4/5")
    return excluded


def is_advanced_stage(user_id, excluded_ids: set) -> bool:
    """Return True if the user's ID appears in the ClickUp-based exclusion set."""
    return str(user_id) in excluded_ids

# --- Discord setup ---
intents = discord.Intents.default()
intents.members = True
intents.message_content = True

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)

# Stage options matching the website exactly
STAGE_OPTIONS = [
    ("1. Finding a product", "1. Finding a product"),
    ("2. Building a store", "2. Building a store"),
    ("3. Creating ads", "3. Creating ads"),
    ("4. Getting sales", "4. Getting sales"),
    ("5. Scaling", "5. Scaling"),
]

HOURS_OPTIONS = [
    ("Less than an hour", "Less than an hour"),
    ("Two to four hours", "Two to four hours"),
    ("Five to ten hours", "Five to ten hours"),
    ("10+ hours", "10+ hours"),
]

# Band for number fields / exports: 1 = <1h … 4 = 10+h
HOURS_LABEL_TO_BAND = {value: i for i, (_, value) in enumerate(HOURS_OPTIONS, start=1)}

CANONICAL_WEEKLY_HOURS_FIELD_NAMES = frozenset({
    "weekly number of hours",
    "hours spent this week",
    "weekly hours",
    "hours this week",
    "weekly hours (band)",
})


def weekly_hours_band_for_label(label: str):
    """Return 1–4 for a known hours label, else None."""
    return HOURS_LABEL_TO_BAND.get(label)


# --- Weekly hours ClickUp field on CHECKIN list (CLICKUP_LIST_ID) ---
_wh_hours_field_lock = asyncio.Lock()
_wh_hours_field_cache: dict = {"ready": False, "meta": None}


def _forced_weekly_hours_meta() -> dict | None:
    if not CLICKUP_CI_FIELD_WEEKLY_HOURS_BAND:
        return None
    return {
        "id": CLICKUP_CI_FIELD_WEEKLY_HOURS_BAND,
        "name": "(CLICKUP_CI_FIELD_WEEKLY_HOURS_BAND)",
        "type": "number",
        "type_config": {},
    }


def _pick_weekly_hours_field(fields: list) -> dict | None:
    """Pick the weekly-hours field; avoids the existing numeric **Week** (calendar week) column."""
    if CLICKUP_WEEKLY_HOURS_FIELD_NAME:
        for f in fields:
            if (f.get("name") or "").strip() == CLICKUP_WEEKLY_HOURS_FIELD_NAME:
                return f
        print(f"[CLICKUP] CLICKUP_WEEKLY_HOURS_FIELD_NAME={CLICKUP_WEEKLY_HOURS_FIELD_NAME!r} not on list")

    for f in fields:
        n = (f.get("name") or "").strip().lower()
        if n in CANONICAL_WEEKLY_HOURS_FIELD_NAMES:
            ty = f.get("type") or ""
            if ty in ("number", "drop_down", "short_text", "text"):
                return f

    candidates = []
    for f in fields:
        ty = f.get("type") or ""
        if ty not in ("number", "drop_down", "short_text", "text"):
            continue
        n = (f.get("name") or "").strip().lower()
        if n == "week":
            continue
        if "hour" not in n:
            continue
        if any(k in n for k in ("week", "band", "spent", "number")):
            candidates.append(f)
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        names = ", ".join((c.get("name") or "") for c in candidates)
        print(f"[CLICKUP] Multiple weekly-hours field candidates ({names}) — using first. "
              f"Add a field named 'Weekly Number of Hours' or set CLICKUP_WEEKLY_HOURS_FIELD_NAME.")
        return candidates[0]
    return None


async def _try_create_weekly_hours_number_field(
    session: aiohttp.ClientSession,
    existing_fields: list,
) -> dict | None:
    """
    ClickUp supports POST /v2/list/{list_id}/field to add a list-level custom field.
    Creates a Number field for bands 1–4 unless a field with the same name already exists.
    """
    if not CLICKUP_AUTO_CREATE_WEEKLY_HOURS_FIELD:
        return None
    want = CLICKUP_WEEKLY_HOURS_FIELD_DISPLAY_NAME.strip().lower()
    for f in existing_fields:
        if (f.get("name") or "").strip().lower() == want:
            return f
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/field"
    payload = {
        "name": CLICKUP_WEEKLY_HOURS_FIELD_DISPLAY_NAME,
        "type": "number",
        "type_config": {},
    }
    try:
        async with session.post(
            url,
            json=payload,
            headers={"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"},
            timeout=aiohttp.ClientTimeout(total=15),
        ) as resp:
            body = await resp.text()
            if resp.status == 200:
                try:
                    data = json.loads(body)
                except json.JSONDecodeError:
                    print(f"[CLICKUP] Auto-create weekly hours: invalid JSON: {body[:300]}")
                    return None
                field = data.get("field")
                if field:
                    print(f"[CLICKUP] Created weekly hours field {field.get('name')!r} id={field.get('id')}")
                    return field
            print(f"[CLICKUP] Auto-create weekly hours field failed: {resp.status} {body[:500]}")
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"[CLICKUP] Auto-create weekly hours field error: {e}")
    return None


async def get_weekly_hours_field_meta(session: aiohttp.ClientSession) -> dict | None:
    """GET /v2/list/{CLICKUP_LIST_ID}/field — cached per process."""
    forced = _forced_weekly_hours_meta()
    if forced:
        return forced
    if _wh_hours_field_cache["ready"]:
        return _wh_hours_field_cache["meta"]
    async with _wh_hours_field_lock:
        if _wh_hours_field_cache["ready"]:
            return _wh_hours_field_cache["meta"]
        url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/field"
        try:
            async with session.get(
                url,
                headers={"Authorization": CLICKUP_TOKEN},
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    print(f"[CLICKUP] List fields fetch {resp.status}: {body[:400]}")
                    _wh_hours_field_cache["ready"] = True
                    _wh_hours_field_cache["meta"] = None
                    return None
                data = await resp.json()
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"[CLICKUP] List fields fetch error: {e}")
            _wh_hours_field_cache["ready"] = True
            _wh_hours_field_cache["meta"] = None
            return None
        fields = data.get("fields") or []
        meta = _pick_weekly_hours_field(fields)
        if not meta:
            want = CLICKUP_WEEKLY_HOURS_FIELD_DISPLAY_NAME.strip().lower()
            for f in fields:
                if (f.get("name") or "").strip().lower() == want:
                    meta = f
                    break
        if not meta:
            meta = await _try_create_weekly_hours_number_field(session, fields)
        if meta:
            print(f"[CLICKUP] Weekly hours field: {meta.get('name')!r} id={meta.get('id')} type={meta.get('type')}")
        else:
            print(
                "[CLICKUP] No weekly hours field — set CLICKUP_AUTO_CREATE_WEEKLY_HOURS_FIELD=true "
                "(default) or add a Number / Dropdown / Text field on the check-in list.",
            )
        _wh_hours_field_cache["ready"] = True
        _wh_hours_field_cache["meta"] = meta
        return meta


def _dropdown_option_id_for_label(field_meta: dict, label: str) -> str | None:
    opts = (field_meta.get("type_config") or {}).get("options") or []
    want = (label or "").strip().lower()
    for o in opts:
        if (o.get("name") or "").strip().lower() == want:
            oid = o.get("id")
            return str(oid) if oid is not None else None
    return None


def _band_from_task_weekly_hours_cf(field_meta: dict, raw) -> int | None:
    if raw is None or raw == "":
        return None
    ty = field_meta.get("type") or ""
    if ty == "number":
        try:
            n = int(float(raw))
            if 1 <= n <= 4:
                return n
        except (TypeError, ValueError):
            return None
    if ty in ("short_text", "text"):
        s = str(raw).strip()
        b = weekly_hours_band_for_label(s)
        if b is not None:
            return b
        try:
            n = int(float(s))
            if 1 <= n <= 4:
                return n
        except (TypeError, ValueError):
            return None
        return None
    if ty != "drop_down":
        return None
    opts = (field_meta.get("type_config") or {}).get("options") or []
    sraw = str(raw)
    for o in opts:
        if str(o.get("id")) == sraw:
            return weekly_hours_band_for_label((o.get("name") or "").strip())
    try:
        idx = int(float(raw))
    except (TypeError, ValueError):
        idx = None
    if idx is not None:
        for o in opts:
            if o.get("orderindex") == idx:
                return weekly_hours_band_for_label((o.get("name") or "").strip())
    return None


def weekly_hours_custom_field_entry(field_meta: dict | None, band: int | None, label: str) -> dict | None:
    """Value for create-task custom_fields."""
    if field_meta is None or band is None:
        return None
    fid = field_meta.get("id")
    if not fid:
        return None
    ty = field_meta.get("type") or ""
    if ty == "number":
        return {"id": fid, "value": band}
    if ty == "drop_down":
        oid = _dropdown_option_id_for_label(field_meta, label)
        if oid:
            return {"id": fid, "value": oid}
        print(f"[CLICKUP] Dropdown weekly hours field has no option matching {label!r}")
        return None
    if ty in ("short_text", "text"):
        return {"id": fid, "value": label}
    return None


def _weekly_hours_band_from_task(task: dict, field_meta: dict | None = None):
    if field_meta and field_meta.get("id"):
        for cf in task.get("custom_fields") or []:
            if cf.get("id") != field_meta["id"]:
                continue
            b = _band_from_task_weekly_hours_cf(field_meta, cf.get("value"))
            if b is not None:
                return b
            break
    desc = task.get("description") or ""
    m = re.search(r"\*\*Hours Spent This Week:\*\*\s*(.+?)(?:\n|$)", desc, re.IGNORECASE)
    if m:
        return weekly_hours_band_for_label(m.group(1).strip())
    return None


# --- Weekly check-in tracking ---
def _get_week_start():
    """Monday 00:00 US/Eastern of current week as ISO string."""
    _et = ZoneInfo("America/New_York")
    now = datetime.now(_et)
    monday = now - timedelta(days=now.weekday())
    return monday.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None).isoformat()


def _load_checkin_data() -> dict:
    if os.path.exists(CHECKIN_DATA_FILE):
        with open(CHECKIN_DATA_FILE, "r") as f:
            return json.load(f)
    return {"checkins": {}, "week_start": None}


def _save_checkin_data(data: dict):
    with open(CHECKIN_DATA_FILE, "w") as f:
        json.dump(data, f, indent=2)


def _ensure_current_week(data: dict) -> dict:
    """Reset tracking if a new week has started."""
    current = _get_week_start()
    if data.get("week_start") != current:
        data["checkins"] = {}
        data["week_start"] = current
        _save_checkin_data(data)
    return data


def has_checked_in(user_id) -> bool:
    data = _ensure_current_week(_load_checkin_data())
    return str(user_id) in data["checkins"]


def record_checkin(user_id):
    data = _ensure_current_week(_load_checkin_data())
    _et = ZoneInfo("America/New_York")
    data["checkins"][str(user_id)] = datetime.now(_et).isoformat()
    _save_checkin_data(data)


# --- DM-blocked user tracking ---
def _load_dm_blocked() -> dict:
    if os.path.exists(DM_BLOCKED_FILE):
        with open(DM_BLOCKED_FILE, "r") as f:
            return json.load(f)
    return {}


def _save_dm_blocked(data: dict):
    with open(DM_BLOCKED_FILE, "w") as f:
        json.dump(data, f, indent=2)


def mark_dm_blocked(user_id):
    """Mark a user as having DMs disabled — skip them in future sends."""
    data = _load_dm_blocked()
    data[str(user_id)] = datetime.now().isoformat()
    _save_dm_blocked(data)


def unmark_dm_blocked(user_id):
    """Remove a user from the blocked list (e.g. they successfully checked in)."""
    data = _load_dm_blocked()
    data.pop(str(user_id), None)
    _save_dm_blocked(data)


def is_dm_blocked(user_id) -> bool:
    return str(user_id) in _load_dm_blocked()


# --- Pending check-ins persistence ---
def load_pending() -> dict:
    if os.path.exists(PENDING_FILE):
        with open(PENDING_FILE, "r") as f:
            return json.load(f)
    return {}


def save_pending(data: dict):
    with open(PENDING_FILE, "w") as f:
        json.dump(data, f)


# --- ClickUp Member Database integration (async with aiohttp) ---
async def find_member_by_discord(discord_username: str):
    """Search the ClickUp Member Database for a member by Discord username."""
    headers = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}
    page = 0
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    f"https://api.clickup.com/api/v2/list/{CLICKUP_MEMBER_DB_LIST_ID}/task",
                    params={"include_closed": "true", "subtasks": "true", "page": page},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        print(f"[CLICKUP] Failed to fetch members: {resp.status}")
                        return None
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"[CLICKUP] Network error fetching members: {e}")
                return None

            task_list = data.get("tasks", [])
            if not task_list:
                break

            for task in task_list:
                for cf in task.get("custom_fields", []):
                    if cf.get("id") == CU_FIELD_DISCORD_USERNAME:
                        val = (cf.get("value") or "").strip().lower()
                        if val == discord_username.lower():
                            return task
            page += 1
    return None


async def update_member_profile(task_id: str, stage: str,
                                weeks: str = "", blocker: str = "",
                                what_would_help: str = "", next_steps: str = ""):
    """Update a member's ClickUp profile after a check-in submission."""
    headers = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}
    now_ms = int(datetime.now().timestamp() * 1000)

    errors = []

    async with aiohttp.ClientSession() as session:
        async def _set_field(field_id, value, label):
            try:
                async with session.post(
                    f"https://api.clickup.com/api/v2/task/{task_id}/field/{field_id}",
                    json={"value": value},
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status != 200:
                        body = await r.text()
                        errors.append(f"{label}: {r.status} {body}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                errors.append(f"{label}: network error — {e}")

        # Update Last Activity Date
        await _set_field(CU_FIELD_LAST_ACTIVITY_DATE, now_ms, "Last Activity Date")

        # Update Last Weekly Check-in Date (separate date column)
        await _set_field(CU_FIELD_LAST_CHECKIN_DATE, now_ms, "Last Weekly Check-in Date")

        # Update Last Activity text
        await _set_field(CU_FIELD_LAST_ACTIVITY, "Weekly Check-in", "Last Activity")

        # Update Weeks in Stage (number field)
        if weeks:
            try:
                await _set_field(CU_FIELD_WEEKS_IN_STAGE, float(weeks), "Weeks in Stage")
            except ValueError:
                errors.append(f"Weeks in Stage: invalid number '{weeks}'")

        # Update Blocker
        if blocker:
            await _set_field(CU_FIELD_BLOCKER, blocker, "Blocker")

        # Update What Would Help
        if what_would_help:
            await _set_field(CU_FIELD_WHAT_WOULD_HELP, what_would_help, "What Would Help")

        # Update Next Steps
        if next_steps:
            await _set_field(CU_FIELD_NEXT_STEPS, next_steps, "Next Steps")

        # Map stage to milestone and update
        milestone_name = STAGE_TO_MILESTONE.get(stage)
        if milestone_name:
            try:
                async with session.get(
                    f"https://api.clickup.com/api/v2/list/{CLICKUP_MEMBER_DB_LIST_ID}/field",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as field_resp:
                    if field_resp.status == 200:
                        resp_data = await field_resp.json()
                        for field in resp_data.get("fields", []):
                            if field["id"] == CU_FIELD_MILESTONE:
                                for opt in field.get("type_config", {}).get("options", []):
                                    if opt["name"] == milestone_name:
                                        await _set_field(CU_FIELD_MILESTONE, opt["orderindex"], "Milestone")
                                        break
                                break
                    else:
                        errors.append(f"Milestone: field fetch failed {field_resp.status}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                errors.append(f"Milestone: network error — {e}")

    if errors:
        for e in errors:
            print(f"[CLICKUP] Field update error on {task_id}: {e}")
    else:
        print(f"[CLICKUP] Updated member profile: {task_id}")


# --- Public check-in confirmation in 1-1 ticket channels ---
# Channel names follow "<ticket#>-<discord_username>", e.g. "69-michaelralston92".
_TICKET_CHANNEL_NAME_RE = re.compile(r"^(\d+)-(.+)$")


def _ticket_channels_for_username(guild: discord.Guild, username_lower: str) -> list[discord.TextChannel]:
    found = []
    for ch in guild.text_channels:
        m = _TICKET_CHANNEL_NAME_RE.match(ch.name.strip())
        if m and m.group(2).lower() == username_lower:
            found.append(ch)
    return found


def _pick_ticket_channel_for_confirmation(channels: list[discord.TextChannel]) -> discord.TextChannel | None:
    """Prefer a channel not under a 'Closed' category; if multiple, prefer highest ticket prefix."""
    if not channels:
        return None

    def ticket_prefix(ch: discord.TextChannel) -> int:
        m = _TICKET_CHANNEL_NAME_RE.match(ch.name.strip())
        return int(m.group(1)) if m else 0

    def is_closed_category(ch: discord.TextChannel) -> bool:
        cat = ch.category.name if ch.category else ""
        return "closed" in cat.lower()

    open_like = [c for c in channels if not is_closed_category(c)]
    pool = open_like if open_like else channels
    return max(pool, key=ticket_prefix)


def _coach_assignee_labels(member_task: dict) -> list[str]:
    """Coach custom field + ClickUp task assignees (CSM often appears as assignee)."""
    labels: list[str] = []
    seen: set[str] = set()

    _, coaches = _extract_member_info(member_task)
    for c in coaches:
        s = (c or "").strip()
        if s and s.lower() not in seen:
            seen.add(s.lower())
            labels.append(s)

    if os.getenv("CHECKIN_TAG_ASSIGNEES", "true").lower() not in ("0", "false", "no", "off"):
        for a in member_task.get("assignees") or []:
            name = (a.get("username") or "").strip()
            if name and name.lower() not in seen:
                seen.add(name.lower())
                labels.append(name)

    return labels


def _score_name_match(member: discord.Member, label_low: str, tokens: list[str]) -> int:
    """Higher = better match for fuzzy coach resolution."""
    if member.bot:
        return -1
    dn = (member.display_name or "").lower()
    gn = (getattr(member, "global_name", None) or "").lower()
    un = member.name.lower()
    surfaces = [dn, gn, un, f"{dn} {gn}".strip(), f"{gn} {dn}".strip()]
    best = 0
    for s in surfaces:
        if not s:
            continue
        if s == label_low:
            best = max(best, 100)
        elif label_low in s or s in label_low:
            best = max(best, 80)
        elif all(t in s for t in tokens):
            best = max(best, 60)
        elif tokens and tokens[0] in s:
            best = max(best, 40)
    return best


async def _resolve_coach_mentions_async(guild: discord.Guild, coach_labels: list[str]) -> str:
    """Match ClickUp names to guild members (cache + gateway query_members + fuzzy scoring)."""
    if not coach_labels:
        return ""

    seen_ids: set[int] = set()
    mentions: list[str] = []

    for raw in coach_labels:
        label = (raw or "").strip()
        if not label:
            continue

        label_low = label.lower()
        tokens = [t for t in label_low.split() if t]

        member = guild.get_member_named(label)

        if member is None:
            for m in guild.members:
                if _score_name_match(m, label_low, tokens) >= 100:
                    member = m
                    break

        if member is None:
            low = label_low
            for m in guild.members:
                if m.bot:
                    continue
                dn = (m.display_name or "").lower()
                gn = (getattr(m, "global_name", None) or "").lower()
                if m.name.lower() == low or dn == low or gn == low:
                    member = m
                    break

        if member is None and len(tokens) >= 2:
            member = guild.get_member_named(f"{tokens[0]} {tokens[-1]}".title())
            if member is None:
                member = guild.get_member_named(tokens[0])

        if member is None and tokens:
            q = tokens[0][:31]
            try:
                queried = await guild.query_members(query=q, limit=30)
            except (discord.HTTPException, TypeError, ValueError) as e:
                print(f"[TICKET] query_members({q!r}): {e}")
                queried = []

            best_m = None
            best_score = 0
            for m in queried:
                sc = _score_name_match(m, label_low, tokens)
                if sc > best_score:
                    best_score = sc
                    best_m = m
            if best_m is not None and best_score >= 40:
                member = best_m

        if member is None and tokens:
            best_m = None
            best_score = 0
            for m in guild.members:
                sc = _score_name_match(m, label_low, tokens)
                if sc > best_score:
                    best_score = sc
                    best_m = m
            if best_m is not None and best_score >= 60:
                member = best_m

        if member is not None and member.id not in seen_ids:
            seen_ids.add(member.id)
            mentions.append(member.mention)
        else:
            print(f"[TICKET] Could not resolve coach/CSM to Discord member: {label!r}")

    return " ".join(mentions)


async def post_public_checkin_confirmation(client: discord.Client, user: discord.User) -> None:
    """Post a short, non-sensitive confirmation in the member's 1-1 ticket channel (visible to everyone there).

    Tags coaches from the ClickUp Member Database Coach field when they match a member of the guild (CSM ping).
    """
    flag = os.getenv("CHECKIN_TICKET_CONFIRM", "true").lower()
    if flag in ("0", "false", "no", "off"):
        return

    guild_id_raw = (os.getenv("DISCORD_GUILD_ID") or "").strip()
    try:
        if guild_id_raw:
            guild = client.get_guild(int(guild_id_raw))
        elif len(client.guilds) == 1:
            guild = client.guilds[0]
        else:
            print("[TICKET] Multiple guilds connected — set DISCORD_GUILD_ID for ticket confirmations.")
            return

        if guild is None:
            print("[TICKET] Guild not found for ticket confirmation.")
            return

        candidates = _ticket_channels_for_username(guild, user.name.lower())
        channel = _pick_ticket_channel_for_confirmation(candidates)
        if channel is None:
            print(f"[TICKET] No ticket channel matching username {user.name!r}")
            return

        coach_ping = ""
        member_task = await find_member_by_discord(user.name)
        if member_task:
            labels = _coach_assignee_labels(member_task)
            coach_ping = await _resolve_coach_mentions_async(guild, labels)

        body = f"{user.mention} **Check-in received** — thanks! Your weekly update was logged."
        if coach_ping:
            body = f"{coach_ping}\n{body}"

        await channel.send(body)
        print(f"[TICKET] Posted confirmation in #{channel.name}")
    except discord.Forbidden:
        print(f"[TICKET] Missing permission to post in ticket channel for {user.name!r}")
    except discord.HTTPException as e:
        print(f"[TICKET] Discord HTTP error posting confirmation: {e}")
    except Exception as e:
        print(f"[TICKET] Error posting confirmation: {e}")


# --- Check-in Modal (the popup form) ---
class CheckInModal(discord.ui.Modal, title="Weekly Accountability Check-in"):
    def __init__(self, selected_stage: str, weekly_hours: str):
        super().__init__()
        self.selected_stage = selected_stage
        self.weekly_hours = weekly_hours

    weeks = discord.ui.TextInput(
        label="How many weeks have you been in this stage?",
        placeholder="e.g., 3",
        style=discord.TextStyle.short,
        max_length=10,
    )
    blocker = discord.ui.TextInput(
        label="Main thing slowing you down right now?",
        placeholder="What's the biggest obstacle right now?",
        style=discord.TextStyle.paragraph,
        max_length=1000,
    )
    help_needed = discord.ui.TextInput(
        label="What would help you progress faster?",
        placeholder="What support, resources, or changes would make a difference?",
        style=discord.TextStyle.paragraph,
        max_length=1000,
    )
    next_steps = discord.ui.TextInput(
        label="Steps you'll take this week to move forward?",
        placeholder="What specific actions will you commit to this week?",
        style=discord.TextStyle.paragraph,
        max_length=1000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        # Build ClickUp task
        today = datetime.now().strftime("%b %d, %Y")
        hours_band = weekly_hours_band_for_label(self.weekly_hours)
        headers = {
            "Authorization": CLICKUP_TOKEN,
            "Content-Type": "application/json",
        }
        base_custom_fields = [
            {"id": CI_FIELD_MEMBER, "value": interaction.user.display_name},
            {"id": CI_FIELD_DATE, "value": today},
            {"id": CI_FIELD_STAGE, "value": self.selected_stage},
            {"id": CI_FIELD_WEEKS_IN_STAGE, "value": self.weeks.value},
            {"id": CI_FIELD_WEEK, "value": datetime.now().isocalendar()[1]},
            {"id": CI_FIELD_BLOCKER, "value": self.blocker.value},
            {"id": CI_FIELD_WHAT_WOULD_HELP, "value": self.help_needed.value},
            {"id": CI_FIELD_NEXT_STEPS, "value": self.next_steps.value},
        ]

        # Respond to Discord immediately (must be within 3 seconds)
        await interaction.response.defer(ephemeral=True, thinking=True)

        cu_status = None
        checkin_task_id = None
        try:
            async with aiohttp.ClientSession() as session:
                wh_meta = await get_weekly_hours_field_meta(session)
                custom_fields = list(base_custom_fields)
                wh_entry = weekly_hours_custom_field_entry(
                    wh_meta, hours_band, self.weekly_hours,
                )
                if wh_entry:
                    custom_fields.append(wh_entry)
                task_data = {
                    "name": f"Check-in — {interaction.user.display_name} — {today}",
                    "description": (
                        f"**Member:** {interaction.user.display_name}\n"
                        f"**Discord Username:** {interaction.user.name}\n"
                        f"**Date:** {today}\n\n"
                        f"---\n\n"
                        f"**Stage:** {self.selected_stage}\n\n"
                        f"**Hours Spent This Week:** {self.weekly_hours}\n\n"
                        f"**Weeks in Stage:** {self.weeks.value}\n\n"
                        f"**Blocker:** {self.blocker.value}\n\n"
                        f"**What Would Help:** {self.help_needed.value}\n\n"
                        f"**Next Steps:** {self.next_steps.value}"
                    ),
                    "priority": 3,
                    "tags": ["check-in", interaction.user.name],
                    "custom_fields": custom_fields,
                }
                async with session.post(
                    f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task",
                    json=task_data,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    cu_status = resp.status
                    if cu_status == 200:
                        resp_data = await resp.json()
                        checkin_task_id = resp_data.get("id")
                    else:
                        body = await resp.text()
                        print(f"[ERROR] ClickUp API: {cu_status} — {body}")

            if cu_status == 200:
                # Record that this user checked in this week
                record_checkin(interaction.user.id)
                # Clear DM-blocked flag if they managed to check in
                unmark_dm_blocked(interaction.user.id)

                await interaction.followup.send(
                    "✅ **Check-in submitted!** Your progress has been logged. Have a great week!",
                    ephemeral=True,
                )
                print(f"[OK] Check-in from {interaction.user.display_name}")

                # Update member profile + enrich check-in task in background
                asyncio.create_task(_update_member_after_checkin(
                    discord_username=interaction.user.name,
                    display_name=interaction.user.display_name,
                    stage=self.selected_stage,
                    weeks=self.weeks.value,
                    blocker=self.blocker.value,
                    help_needed=self.help_needed.value,
                    next_steps=self.next_steps.value,
                    checkin_task_id=checkin_task_id,
                ))
                # Short public note in their 1-1 ticket channel (no form details)
                asyncio.create_task(post_public_checkin_confirmation(
                    interaction.client,
                    interaction.user,
                ))
            else:
                await interaction.followup.send(
                    "⚠️ Check-in received but there was an issue saving it. The team has been notified.",
                    ephemeral=True,
                )
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"[ERROR] Request failed: {e}")
            await interaction.followup.send(
                "⚠️ Something went wrong. Please try again in a moment.",
                ephemeral=True,
            )


async def _update_member_after_checkin(discord_username, display_name, stage,
                                       weeks, blocker, help_needed, next_steps,
                                       checkin_task_id=None):
    """Background task to update ClickUp member profile and enrich check-in task."""
    try:
        member_task = await find_member_by_discord(discord_username)
        if member_task:
            await update_member_profile(
                member_task["id"], stage,
                weeks=weeks, blocker=blocker,
                what_would_help=help_needed, next_steps=next_steps,
            )
            print(f"[CLICKUP] Member profile updated for {display_name}")

            # Enrich check-in task with program and coach info
            if checkin_task_id:
                await _enrich_checkin_task(checkin_task_id, member_task, discord_username)
        else:
            print(f"[CLICKUP] No matching member for {display_name} (username: {discord_username})")
    except Exception as e:
        print(f"[CLICKUP] Error updating member {display_name}: {e}")


def _extract_member_info(member_task):
    """Extract program name and coach names from a member database task."""
    program = None
    coaches = []
    for cf in member_task.get("custom_fields", []):
        if cf.get("id") == CU_FIELD_PROGRAM_NAME and cf.get("value") is not None:
            try:
                program = PROGRAM_NAMES.get(int(cf["value"]))
            except (ValueError, TypeError):
                pass
        elif cf.get("id") == CU_FIELD_COACH and cf.get("value"):
            coaches = [u.get("username", "") for u in cf["value"] if u.get("username")]
    return program, coaches


async def _enrich_checkin_task(checkin_task_id, member_task, discord_username):
    """Add program, coach, and Discord username tags + update description on a check-in task."""
    headers = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}
    program, _ = _extract_member_info(member_task)
    # Use _coach_assignee_labels to pull coaches from BOTH the Coach custom
    # field AND the task assignees so members without the Coach field still
    # get their CSM/coach tagged on the check-in task.
    coaches = _coach_assignee_labels(member_task)

    # Build tags to add
    tags = []
    if program:
        tags.append(program.lower())
    for coach in coaches:
        tags.append(coach.lower())

    async with aiohttp.ClientSession() as session:
        # Add tags
        for tag in tags:
            try:
                async with session.post(
                    f"https://api.clickup.com/api/v2/task/{checkin_task_id}/tag/{tag}",
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as r:
                    if r.status != 200:
                        body = await r.text()
                        print(f"[CLICKUP] Failed to add tag '{tag}': {r.status} {body}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"[CLICKUP] Error adding tag '{tag}': {e}")

        # Update description to include program and coach
        try:
            async with session.get(
                f"https://api.clickup.com/api/v2/task/{checkin_task_id}",
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as r:
                if r.status == 200:
                    task_data = await r.json()
                    old_desc = task_data.get("description", "")
                    extra_lines = []
                    # Add full name from member database task name
                    member_full_name = (member_task.get("name") or "").strip()
                    if member_full_name:
                        extra_lines.append(f"**Full Name:** {member_full_name}")
                    if program:
                        extra_lines.append(f"**Program:** {program}")
                    if coaches:
                        extra_lines.append(f"**Coach:** {', '.join(coaches)}")
                    if extra_lines:
                        # Insert after the Date line
                        new_desc = old_desc.replace(
                            "\n\n---",
                            "\n" + "\n".join(extra_lines) + "\n\n---",
                            1,
                        )
                        async with session.put(
                            f"https://api.clickup.com/api/v2/task/{checkin_task_id}",
                            json={"description": new_desc},
                            headers=headers,
                            timeout=aiohttp.ClientTimeout(total=10),
                        ) as r2:
                            if r2.status == 200:
                                print(f"[CLICKUP] Enriched check-in {checkin_task_id} "
                                      f"(program={program}, coaches={coaches})")
                            else:
                                body = await r2.text()
                                print(f"[CLICKUP] Failed to update description: {r2.status} {body}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"[CLICKUP] Error enriching check-in task: {e}")


# --- Stage Select Menu (dropdown before modal) ---
class StageSelect(discord.ui.Select):
    def __init__(self):
        options = [
            discord.SelectOption(label=label, value=value)
            for label, value in STAGE_OPTIONS
        ]
        super().__init__(
            placeholder="Which stage are you currently at?",
            options=options,
            custom_id="stage_select",
        )

    async def callback(self, interaction: discord.Interaction):
        selected_stage = self.values[0]
        await interaction.response.send_message(
            "**Step 2 of 2 — Hours this week**\n"
            "Choose roughly how many hours you’ve spent on the business this week, "
            "then the check-in form will open.",
            view=HoursSelectView(selected_stage=selected_stage),
            ephemeral=True,
        )


class StageSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(StageSelect())


class HoursSelect(discord.ui.Select):
    def __init__(self, selected_stage: str):
        self.selected_stage = selected_stage
        options = [
            discord.SelectOption(label=label, value=value)
            for label, value in HOURS_OPTIONS
        ]
        super().__init__(
            placeholder="How many hours this week?",
            options=options,
            custom_id="hours_select",
        )

    async def callback(self, interaction: discord.Interaction):
        selected_hours = self.values[0]
        await interaction.response.send_modal(
            CheckInModal(
                selected_stage=self.selected_stage,
                weekly_hours=selected_hours,
            ),
        )


class HoursSelectView(discord.ui.View):
    def __init__(self, selected_stage: str):
        super().__init__(timeout=300)
        self.add_item(HoursSelect(selected_stage=selected_stage))


# --- Button that opens the stage select ---
class CheckInButton(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    @discord.ui.button(
        label="Start Check-in",
        style=discord.ButtonStyle.green,
        emoji="📋",
        custom_id="checkin_button",
    )
    async def start_checkin(self, interaction: discord.Interaction, button: discord.ui.Button):
        view = StageSelectView()
        await interaction.response.send_message(
            "**Step 1 of 2 — Stage**\n"
            "Pick the stage you’re in. **Next**, you’ll pick **hours spent this week**, then the form opens.",
            view=view,
            ephemeral=True,
        )


# --- Slash command: /checkin ---
@tree.command(name="checkin", description="Submit your weekly accountability check-in")
async def checkin_command(interaction: discord.Interaction):
    view = StageSelectView()
    await interaction.response.send_message(
        "**Step 1 of 2 — Stage**\n"
        "Pick the stage you’re in. **Next**, you’ll pick **hours spent this week**, then the form opens.",
        view=view,
        ephemeral=True,
    )


# --- Admin command: trigger check-in DMs now ---
@tree.command(name="trigger_checkins", description="[Admin] Send check-in DMs to all eligible members now")
@app_commands.default_permissions(administrator=True)
async def trigger_checkins(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        await _send_checkin_dms(
            "manual_trigger",
            "**📋 Weekly Check-in Time!**\n\n"
            "It's time for your weekly accountability check-in.\n"
            "Click the button below to submit your progress update.\n\n"
            "*This helps us keep your support aligned each week.*",
        )
        await interaction.followup.send("✅ Check-in DMs sent!", ephemeral=True)
    except Exception as e:
        print(f"[ERROR] trigger_checkins: {e}")
        await interaction.followup.send(f"⚠️ Error: {e}", ephemeral=True)


# --- Admin command: show eligibility status for all Accelerate members ---
@tree.command(name="checkin_status", description="[Admin] Show which Accelerate members are eligible for check-in DMs")
@app_commands.default_permissions(administrator=True)
async def checkin_status(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    accelerate_usernames = await fetch_accelerate_usernames()
    excluded_ids = await fetch_excluded_user_ids()
    cutoff = datetime.now(timezone.utc) - timedelta(days=MEMBER_MAX_AGE_MONTHS * 30)
    lines = []
    for member in interaction.guild.members:
        if member.bot:
            continue
        if member.name.lower() not in accelerate_usernames:
            continue
        joined = member.joined_at
        joined_str = joined.strftime("%b %d, %Y") if joined else "unknown"
        reasons = []
        eligible = True
        if joined and joined < cutoff:
            reasons.append(f"joined {joined_str} (>{MEMBER_MAX_AGE_MONTHS} months ago)")
            eligible = False
        if has_checked_in(member.id):
            reasons.append("already checked in this week")
            eligible = False
        if is_advanced_stage(member.id, excluded_ids):
            reasons.append("Stage 4/5")
            eligible = False
        if is_dm_blocked(member.id):
            reasons.append("DMs blocked")
            eligible = False
        status = "✅" if eligible else "❌"
        reason_text = f" — {', '.join(reasons)}" if reasons else ""
        lines.append(f"{status} **{member.display_name}** (joined {joined_str}){reason_text}")

    if not lines:
        await interaction.followup.send(
            f"No Accelerate members found in Discord.\n"
            f"ClickUp has {len(accelerate_usernames)} Accelerate usernames: {', '.join(sorted(accelerate_usernames)) or 'none'}",
            ephemeral=True,
        )
        return

    msg = f"**Accelerate Members — Eligibility Report**\n(Source: ClickUp Program Name | Filter: joined within {MEMBER_MAX_AGE_MONTHS} months)\n\n" + "\n".join(lines)
    if len(msg) > 1900:
        msg = msg[:1900] + "\n... (truncated)"
    await interaction.followup.send(msg, ephemeral=True)


@tree.command(
    name="hai_scrape_now",
    description="[Admin] Force-run the HonestAI FAQ scrape right now",
)
@app_commands.default_permissions(administrator=True)
async def hai_scrape_now(interaction: discord.Interaction):
    """Kicks off a one-shot scrape of #ask-honestai and ships it to the
    Apps Script Web App. Replies ephemerally with a short summary.
    """
    await interaction.response.defer(ephemeral=True, thinking=True)

    try:
        import faq_scraper
    except Exception as e:
        await interaction.followup.send(
            f"⚠️ FAQ scraper module not available: {e}",
            ephemeral=True,
        )
        return

    try:
        result = await faq_scraper.run_once(client)
    except Exception as e:
        await interaction.followup.send(
            f"❌ Scrape errored: `{e}`",
            ephemeral=True,
        )
        return

    if not result or not result.get("ok"):
        err = (result or {}).get("error", "unknown error")
        await interaction.followup.send(
            f"❌ Scrape failed: `{err}`",
            ephemeral=True,
        )
        return

    scanned = result.get("scanned", 0)
    shipped = result.get("shipped", 0)
    watermark = result.get("watermark") or "(unchanged)"

    if scanned == 0:
        body = (
            "✅ Scrape finished — no new messages since last run.\n"
            f"Watermark: `{watermark}`\n\n"
            "Run **HonestAI FAQ → Run analysis now** in the sheet to classify "
            "anything pending."
        )
    else:
        body = (
            f"✅ Scrape finished\n"
            f"• scanned: **{scanned}** new questions\n"
            f"• shipped: **{shipped}** to Apps Script\n"
            f"• watermark: `{watermark}`\n\n"
            "Next: **HonestAI FAQ → Run analysis now** in the sheet to "
            "classify + render the dashboard."
        )
    await interaction.followup.send(body, ephemeral=True)


# --- Periodic scan: detect new Accelerate members from ClickUp ---
@tasks.loop(hours=6)
async def scan_new_accelerate_members():
    """Check ClickUp for NEW Accelerate members not yet seen by the bot.

    First run: marks all existing members as 'known' WITHOUT adding them to
    the onboarding pending queue — they get weekly broadcasts instead.
    Subsequent runs: only truly new members are added to pending.
    """
    accelerate_usernames = await fetch_accelerate_usernames()
    if not accelerate_usernames:
        return

    # Load known members (already-seen Accelerate members)
    known = {}
    if os.path.exists(KNOWN_MEMBERS_FILE):
        with open(KNOWN_MEMBERS_FILE, "r") as f:
            known = json.load(f)

    first_run = len(known) == 0
    pending = load_pending()
    added = 0
    newly_known = 0

    for guild in client.guilds:
        for member in guild.members:
            if member.bot:
                continue
            if member.name.lower() not in accelerate_usernames:
                continue
            if not is_within_join_window(member):
                continue
            user_key = str(member.id)
            if user_key in known:
                continue

            # Mark as known
            known[user_key] = {"username": member.name, "seen_at": datetime.now().isoformat()}
            newly_known += 1

            if first_run:
                # First run — don't add existing members to onboarding queue
                continue

            # Truly new member — add to onboarding pending queue
            if user_key not in pending and not has_checked_in(member.id):
                pending[user_key] = {
                    "guild_id": guild.id,
                    "added_at": datetime.now().isoformat(),
                    "step": 1,
                }
                added += 1
                print(f"[PENDING] {member.display_name} added via ClickUp scan — first check-in in 7 days")

    with open(KNOWN_MEMBERS_FILE, "w") as f:
        json.dump(known, f, indent=2)

    if added:
        save_pending(pending)

    if first_run:
        # Clear any incorrectly added pending entries from before this fix
        save_pending({})
        print(f"[SCAN] First run — registered {newly_known} existing Accelerate members (no onboarding DMs)")
    else:
        print(f"[SCAN] Checked {len(accelerate_usernames)} Accelerate members, {newly_known} newly seen, {added} added to onboarding")


@scan_new_accelerate_members.before_loop
async def before_scan():
    await client.wait_until_ready()


# Messages for each step of the new-member check-in sequence
_NEW_MEMBER_MESSAGES = {
    1: (
        "**📋 Welcome to your first Check-in!**\n\n"
        "You've been with us for a week now — time for your first accountability check-in.\n"
        "Click the button below to submit your progress update.\n\n"
        "*This helps us keep your support aligned each week.*"
    ),
    2: (
        "**📋 Week 2 Check-in**\n\n"
        "Two weeks in — great to have you here! Time to log your progress.\n"
        "Click the button below to submit your weekly check-in.\n\n"
        "*Your CSM uses this to tailor support for you.*"
    ),
    3: (
        "**📋 Week 3 Check-in**\n\n"
        "You're three weeks in — keep the momentum going!\n"
        "Click the button below to submit your weekly check-in.\n\n"
        "*Consistent check-ins = faster progress.*"
    ),
    4: (
        "**📋 Week 4 Check-in**\n\n"
        "Four weeks with us — incredible progress so far!\n"
        "Click the button below to submit your weekly check-in.\n\n"
        "*Keep it up — consistency is everything.*"
    ),
    5: (
        "**📋 Week 5 Check-in**\n\n"
        "Five weeks in — you're building great habits!\n"
        "Click the button below to submit your weekly check-in.\n\n"
        "*Your CSM uses this to tailor support for you.*"
    ),
    6: (
        "**📋 Week 6 Check-in**\n\n"
        "Six weeks in — stay focused on your next milestone.\n"
        "Click the button below to submit your weekly check-in.\n\n"
        "*Consistent check-ins = faster progress.*"
    ),
    7: (
        "**📋 Week 7 Check-in**\n\n"
        "Seven weeks in — you're doing amazing!\n"
        "Click the button below to submit your weekly progress update.\n\n"
        "*We're here to support you every step of the way.*"
    ),
    8: (
        "**📋 Week 8 Check-in**\n\n"
        "Two months in — keep pushing toward your goals!\n"
        "Click the button below to submit your weekly check-in.\n\n"
        "*Your progress matters — let's track it together.*"
    ),
    9: (
        "**📋 Week 9 Check-in**\n\n"
        "Nine weeks in — every check-in brings you closer.\n"
        "Click the button below to submit your weekly progress update.\n\n"
        "*Consistency is the key to results.*"
    ),
    10: (
        "**📋 Week 10 Check-in**\n\n"
        "Ten weeks in — outstanding commitment!\n"
        "Click the button below to submit your weekly check-in.\n\n"
        "*Your CSM reviews every submission to better support you.*"
    ),
    11: (
        "**📋 Week 11 Check-in**\n\n"
        "Almost at the finish line — week 11 check-in time!\n"
        "Click the button below to submit your progress update.\n\n"
        "*One more week after this — keep going!*"
    ),
    12: (
        "**📋 Week 12 Check-in — Final!**\n\n"
        "You've reached week 12 — congratulations on an incredible run!\n"
        "Click the button below to submit your final check-in.\n\n"
        "*After this you'll move to the regular weekly check-in schedule.*"
    ),
}


# --- Background task: send new-member check-in sequence (4 DMs, weekly) ---
@tasks.loop(hours=6)
async def check_pending_members():
    pending = load_pending()
    if not pending:
        return

    excluded_ids = await fetch_excluded_user_ids()

    now = datetime.now()
    to_remove = []

    for user_id, info in list(pending.items()):
        step = info.get("step", 1)
        added_at = datetime.fromisoformat(info["added_at"])
        # Each step fires 7 days after the previous one (step 1 = day 7, step 2 = day 14, ...)
        last_sent_at = (
            datetime.fromisoformat(info["last_sent_at"])
            if info.get("last_sent_at")
            else added_at
        )
        next_send = last_sent_at + timedelta(days=7)

        if now < next_send:
            continue

        guild = client.get_guild(info["guild_id"])
        if not guild:
            to_remove.append(user_id)
            continue
        member = guild.get_member(int(user_id))
        if not member:
            to_remove.append(user_id)
            continue

        if is_advanced_stage(int(user_id), excluded_ids):
            to_remove.append(user_id)
            print(f"[SKIP] {member.display_name} is in advanced stage — removing from sequence")
            continue

        message = _NEW_MEMBER_MESSAGES.get(step, _NEW_MEMBER_MESSAGES[4])
        try:
            view = CheckInButton()
            await member.send(message, view=view)
            print(f"[DM] New-member step {step} sent to {member.display_name}")
            await asyncio.sleep(random.uniform(DM_DELAY_MIN, DM_DELAY_MAX))

            if step >= NEW_MEMBER_TOTAL_STEPS:
                to_remove.append(user_id)
            else:
                pending[user_id]["step"] = step + 1
                pending[user_id]["last_sent_at"] = now.isoformat()

        except discord.Forbidden:
            mark_dm_blocked(int(user_id))
            to_remove.append(user_id)
            print(f"[SKIP] Can't DM {member.display_name} (DMs disabled — marked blocked)")
        except discord.HTTPException as e:
            if e.status == 429:
                retry_after = getattr(e, "retry_after", 60)
                print(f"[RATE] 429 hit — backing off {retry_after}s")
                await asyncio.sleep(retry_after)
                # Don't advance step — retry next cycle
            else:
                print(f"[ERROR] DM to {member.display_name}: {e}")
                to_remove.append(user_id)

    for uid in to_remove:
        pending.pop(uid, None)
    save_pending(pending)


# --- Auto-DM tasks (for existing members with accelerate/core roles) ---
et = ZoneInfo("America/New_York")
monday_time = datetime.now(et).replace(hour=9, minute=0, second=0).timetz()
wednesday_time = datetime.now(et).replace(hour=12, minute=0, second=0).timetz()


async def _send_checkin_dms(label: str, message: str):
    """Shared logic: DM members who haven't checked in this week.

    Anti-spam measures:
    - Random jitter between DMs (DM_DELAY_MIN to DM_DELAY_MAX seconds)
    - Batch pausing (DM_BATCH_PAUSE seconds every DM_BATCH_SIZE messages)
    - Skip users with DMs disabled (persistent tracking)
    - Exponential backoff on 429 rate limits
    - Cross-guild deduplication
    """
    accelerate_usernames = await fetch_accelerate_usernames()
    excluded_ids = await fetch_excluded_user_ids()
    pending = load_pending()
    sent = 0
    skipped = 0
    dm_blocked = 0
    ineligible = 0
    seen_users = set()  # Dedupe across guilds

    for guild in client.guilds:
        for member in guild.members:
            if member.bot or member.id in seen_users:
                continue
            seen_users.add(member.id)
            # Only DM members in ClickUp Accelerate program + within join window
            if member.name.lower() not in accelerate_usernames:
                continue
            if not is_within_join_window(member):
                ineligible += 1
                continue
            if str(member.id) in pending:
                continue
            if is_advanced_stage(member.id, excluded_ids):
                continue
            if has_checked_in(member.id):
                skipped += 1
                continue
            if is_dm_blocked(member.id):
                dm_blocked += 1
                continue
            try:
                view = CheckInButton()
                await member.send(message, view=view)
                sent += 1
                print(f"[DM] Sent {label} to {member.display_name}")
                # Batch pause: longer break every N messages
                if sent % DM_BATCH_SIZE == 0:
                    print(f"[PACE] Batch pause after {sent} DMs ({DM_BATCH_PAUSE}s)")
                    await asyncio.sleep(DM_BATCH_PAUSE)
                else:
                    # Random jitter between DMs to look natural
                    await asyncio.sleep(random.uniform(DM_DELAY_MIN, DM_DELAY_MAX))
            except discord.Forbidden:
                mark_dm_blocked(member.id)
                dm_blocked += 1
                print(f"[SKIP] Can't DM {member.display_name} (DMs disabled — marked blocked)")
            except discord.HTTPException as e:
                if e.status == 429:
                    retry_after = getattr(e, "retry_after", 60)
                    print(f"[RATE] 429 hit — backing off {retry_after}s")
                    await asyncio.sleep(retry_after)
                else:
                    print(f"[ERROR] DM to {member.display_name}: {e}")
            except Exception as e:
                print(f"[ERROR] DM to {member.display_name}: {e}")

    print(f"[{label.upper()}] Sent: {sent}, Skipped (checked in): {skipped}, DM-blocked: {dm_blocked}, Join-date filtered: {ineligible}")


@tasks.loop(time=monday_time)
async def weekly_reminder():
    if datetime.now(et).weekday() != 0:
        return
    await _send_checkin_dms(
        "weekly",
        "**\U0001f4cb Weekly Check-in Time!**\n\n"
        "It's time for your weekly accountability check-in.\n"
        "Click the button below to submit your progress update.\n\n"
        "*This helps us keep your support aligned each week.*",
    )


@tasks.loop(time=wednesday_time)
async def midweek_reminder():
    if datetime.now(et).weekday() != 2:
        return
    await _send_checkin_dms(
        "midweek",
        "**\U0001f514 Midweek Reminder**\n\n"
        "You haven't submitted your check-in yet this week.\n"
        "Click below to log your progress \u2014 it only takes a minute.\n\n"
        "*Your CSM uses this to support you better.*",
    )


@weekly_reminder.before_loop
async def before_weekly_reminder():
    await client.wait_until_ready()


@midweek_reminder.before_loop
async def before_midweek_reminder():
    await client.wait_until_ready()


@check_pending_members.before_loop
async def before_check_pending():
    await client.wait_until_ready()


# --- Monthly check-in data export ---
@tasks.loop(hours=24)
async def monthly_export():
    """On the 1st of each month, export all check-in tasks from ClickUp for AI analysis."""
    now_est = datetime.now(ZoneInfo("America/New_York"))
    if now_est.day != 1:
        return

    month_label = now_est.strftime("%Y-%m")
    month_start_ms = int(now_est.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    # Go back one full month
    if now_est.month == 1:
        prev_month = now_est.replace(year=now_est.year - 1, month=12, day=1)
    else:
        prev_month = now_est.replace(month=now_est.month - 1, day=1)
    prev_month_ms = int(prev_month.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)

    headers = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}
    all_tasks = []
    page = 0

    async with aiohttp.ClientSession() as session:
        wh_meta = await get_weekly_hours_field_meta(session)
        while True:
            try:
                async with session.get(
                    f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task",
                    params={
                        "include_closed": "true",
                        "date_created_gt": prev_month_ms,
                        "date_created_lt": month_start_ms,
                        "page": page,
                    },
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=20),
                ) as resp:
                    if resp.status != 200:
                        print(f"[EXPORT] ClickUp fetch failed: {resp.status}")
                        return
                    data = await resp.json()
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"[EXPORT] Network error: {e}")
                return

            batch = data.get("tasks", [])
            if not batch:
                break
            all_tasks.extend(batch)
            page += 1

    export = {
        "export_month": month_label,
        "generated_at": now_est.isoformat(),
        "total_checkins": len(all_tasks),
        "checkins": [
            {
                "name": t.get("name"),
                "created_at": t.get("date_created"),
                "description": t.get("description", ""),
                "tags": [tag.get("name") for tag in t.get("tags", [])],
                "weekly_hours_band": _weekly_hours_band_from_task(t, wh_meta),
            }
            for t in all_tasks
        ],
    }

    export_json = json.dumps(export, indent=2)
    print(f"[EXPORT] {month_label}: {len(all_tasks)} check-ins exported")

    if EXPORT_WEBHOOK_URL:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    EXPORT_WEBHOOK_URL,
                    json=export,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status < 300:
                        print(f"[EXPORT] Sent to webhook successfully")
                    else:
                        print(f"[EXPORT] Webhook returned {resp.status}")
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            print(f"[EXPORT] Webhook error: {e}")
    else:
        # No webhook — write to file as fallback (under STATE_DIR so the
        # exports survive redeploys when a volume is mounted).
        export_dir = os.path.join(STATE_DIR, "exports")
        os.makedirs(export_dir, exist_ok=True)
        export_path = os.path.join(export_dir, f"checkins_{month_label}.json")
        with open(export_path, "w") as f:
            f.write(export_json)
        print(f"[EXPORT] Written to {export_path}")


@monthly_export.before_loop
async def before_monthly_export():
    await client.wait_until_ready()


# --- Bot ready ---
_synced = False


async def _prefetch_weekly_hours_field():
    """Resolve weekly-hours field once at startup so first check-in is faster."""
    try:
        async with aiohttp.ClientSession() as session:
            await get_weekly_hours_field_meta(session)
    except Exception as e:
        print(f"[CLICKUP] Weekly hours field prefetch: {e}")


@client.event
async def on_ready():
    global _synced

    # Register persistent views (must happen every reconnect)
    client.add_view(CheckInButton())

    # Only sync slash commands once per process to avoid 429s
    if not _synced:
        try:
            # Copy commands to guild scope for instant availability
            for guild in client.guilds:
                tree.copy_global_to(guild=guild)
                await tree.sync(guild=guild)
                print(f"[SYNC] Commands synced to {guild.name}")

            # Clear global scope to remove duplicates (takes up to 1hr to propagate)
            tree.clear_commands(guild=None)
            await tree.sync()

            _synced = True
        except discord.HTTPException as e:
            print(f"[SYNC] Failed to sync commands: {e}")

    print(f"Bot online: {client.user}")
    print(f"Connected to {len(client.guilds)} server(s)")
    if TEST_MODE:
        print("[TEST MODE] DMs and scheduled tasks are disabled")

    # Start background tasks if not already running (skip in test mode)
    if not TEST_MODE:
        if not weekly_reminder.is_running():
            weekly_reminder.start()
        if not midweek_reminder.is_running():
            midweek_reminder.start()
        if not check_pending_members.is_running():
            check_pending_members.start()
        if not scan_new_accelerate_members.is_running():
            scan_new_accelerate_members.start()
        if not monthly_export.is_running():
            monthly_export.start()
        asyncio.create_task(_prefetch_weekly_hours_field())

        # HonestAI FAQ scraper — daily scrape of #ask-honestai that
        # ships to the Apps Script Web App. Runs here (not in Apps
        # Script) because Discord blocks GAS's outbound IPs on guild
        # endpoints. Module lives in faq_scraper.py.
        try:
            import faq_scraper
            faq_scraper.register(client)
        except Exception as e:
            print(f"[HAI] FAQ scraper registration failed: {e}")


client.run(DISCORD_TOKEN)
