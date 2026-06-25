import os
import re
import json
import time
import asyncio
import random
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv
import aiohttp
from aiohttp import web
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

load_dotenv()

TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CLICKUP_TOKEN = os.getenv("CLICKUP_TOKEN")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")
CLICKUP_MEMBER_DB_LIST_ID = "901516122313"
EXPORT_WEBHOOK_URL = os.getenv("EXPORT_WEBHOOK_URL", "")

# --- Manual "send check-in now" HTTP endpoint -----------------------------
# Lets the HonestBrands HQ dashboard fire a one-off 1:1 check-in nudge for a
# specific member (CSM clicks "Send check-in now"). Disabled unless
# CHECKIN_API_SECRET is set. Binds to $PORT (Railway web service) or
# CHECKIN_API_PORT, default 8080.
CHECKIN_API_SECRET = (os.getenv("CHECKIN_API_SECRET") or "").strip()
CHECKIN_API_PORT = int(os.getenv("PORT") or os.getenv("CHECKIN_API_PORT") or "8080")
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

# Member Database fields written when a member submits product info via the bot.
# Listings Reviewed is a numeric counter (incremented +1 per submission); Latest
# Call Topic is a text field overwritten with "Product: <name> — <link>" so
# coaches see the freshest product info at a glance on the contact page.
CU_FIELD_LISTINGS_REVIEWED = "22bdd321-6b51-4324-83d0-4878e9ddc3b8"
CU_FIELD_LATEST_CALL_TOPIC = "fdebf899-a6b3-4216-81f1-f40e26602d54"

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
# Number field on the check-in list — which step of the roadmap checklist the
# member says they're on this week (free-text answer parsed to an int).
CI_FIELD_ROADMAP_STEP = "3d8f6bbd-18bb-4e24-91cb-0cbf93f6b2c9"
# ClickUp made this a Space-level field, so the SAME id is attached to the
# Member Database list too — the member's current step mirrors onto their
# contact card via update_member_profile().
CU_FIELD_ROADMAP_STEP = CI_FIELD_ROADMAP_STEP

# Map bot stages to ClickUp Milestone dropdown options. Until a "Launched Ads"
# milestone is added in ClickUp, "4. Launched Ads" maps to "3. Make Ads".
STAGE_TO_MILESTONE = {
    "1. Finding a Product": "1. Select a Product",
    "2. Building a Store": "2. Build Site",
    "3. Creating Ads": "3. Make Ads",
    "4. Launched Ads": "3. Make Ads",
    "5. Making Sales": "4. First Sale",
    "6. Scaling Brand": "5. Scaling",
}

# Eligibility — only DM Accelerate members who joined Discord on/after this
# date AND are within their first CHECKIN_WEEKS_CAP weeks. After 12 weeks the
# member rolls off the DM list automatically.
MEMBER_JOIN_CUTOFF = datetime(2026, 3, 1, tzinfo=timezone.utc)
CHECKIN_WEEKS_CAP = 12

# Total weekly DMs in the new-member sequence (overridden by NEW_MEMBER_TOTAL_STEPS env var in testing)
NEW_MEMBER_TOTAL_STEPS = int(os.getenv("NEW_MEMBER_TOTAL_STEPS", "12"))

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


def _state_diagnostic() -> None:
    """Print every state file at boot with its existence + size, plus a
    write-probe of STATE_DIR so we can tell at a glance whether the
    Railway volume is actually attached and persisting writes between
    redeploys.

    If you ever see '[STATE] write-probe FAILED' or every state file
    showing 'missing' on a redeploy after the bot had previously run,
    the Railway volume mount path doesn't match STATE_DIR (case-
    sensitive!) and writes are landing on ephemeral disk.
    """
    targets = {
        "pending_checkins.json":   os.path.join(STATE_DIR, "pending_checkins.json"),
        "checkin_data.json":       os.path.join(STATE_DIR, "checkin_data.json"),
        "dm_blocked.json":         os.path.join(STATE_DIR, "dm_blocked.json"),
        "known_accelerate.json":   os.path.join(STATE_DIR, "known_accelerate.json"),
        "member_product_info.json": os.path.join(STATE_DIR, "member_product_info.json"),
        "faq_scraper_state.json":  os.path.join(STATE_DIR, "faq_scraper_state.json"),
    }
    print(f"[STATE] dir exists: {os.path.isdir(STATE_DIR)}  path: {STATE_DIR}")
    for label, path in targets.items():
        if os.path.exists(path):
            try:
                size = os.path.getsize(path)
                mtime = datetime.fromtimestamp(os.path.getmtime(path))
                print(f"[STATE]   {label}: {size} bytes  mtime={mtime.isoformat(timespec='seconds')}")
            except Exception as _se:
                print(f"[STATE]   {label}: present but stat() failed: {_se}")
        else:
            print(f"[STATE]   {label}: missing")
    probe = os.path.join(STATE_DIR, ".state_probe")
    try:
        with open(probe, "w") as _f:
            _f.write(datetime.now().isoformat())
        with open(probe, "r") as _f:
            _ = _f.read()
        os.remove(probe)
        print(f"[STATE] write-probe OK ({STATE_DIR} is writable)")
    except Exception as _e:
        print(f"[STATE] write-probe FAILED: {_e}")


_state_diagnostic()

# File to persist pending new joiners awaiting their first check-in
PENDING_FILE = os.path.join(STATE_DIR, "pending_checkins.json")

# File to track weekly check-in submissions
CHECKIN_DATA_FILE = os.path.join(STATE_DIR, "checkin_data.json")

# File to track users who have DMs disabled (skip them instead of retrying)
DM_BLOCKED_FILE = os.path.join(STATE_DIR, "dm_blocked.json")

# File to record which scheduled reminders have fired today — used by
# reminder_dispatcher to be idempotent across bot restarts. Maps
# {"weekly": "YYYY-MM-DD", "midweek": "YYYY-MM-DD"}.
REMINDER_FIRES_FILE = os.path.join(STATE_DIR, "reminder_fires.json")

# Stages where follow-up DMs stop (from check-in form selection).
# Both the new 6-stage labels and the legacy 5-stage labels are listed so
# previously submitted check-ins still mark the member as advanced.
ADVANCED_STAGES = {
    # New 6-stage system
    "5. Making Sales",
    "6. Scaling Brand",
    # Legacy 5-stage labels (still present on older check-in tasks)
    "4. Getting sales",
    "5. Scaling",
}

# File to track which Accelerate members have been seen (so only NEW ones get the onboarding sequence)
KNOWN_MEMBERS_FILE = os.path.join(STATE_DIR, "known_accelerate.json")

# File to cache each member's product name + link so we only ask once
PRODUCT_INFO_FILE = os.path.join(STATE_DIR, "member_product_info.json")

# DM pacing: send in batches to avoid spam detection
DM_DELAY_MIN = 8   # minimum seconds between DMs
DM_DELAY_MAX = 15  # maximum seconds between DMs
DM_BATCH_SIZE = 20  # pause after this many DMs
DM_BATCH_PAUSE = 60  # seconds to pause between batches


# --- ClickUp-based Accelerate member lookup (cached) ---
# `missing_username` holds Accelerate members whose Discord-username field is
# blank.  Without this, those members are silently dropped from the DM loop
# (the eligibility filter `member.name.lower() not in accelerate_usernames`
# fails closed) and we only find out when someone files a "I never got the
# check-in DM" ticket weeks later.  Surfacing the list both at refresh time
# and through /checkin_status makes the failure mode loud.
_accelerate_cache: dict = {
    "usernames":        set(),
    "missing_username": [],   # list of {"name": str, "task_id": str, "status": str}
    "last_fetched":     None,
}
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
    missing_username: list[dict] = []
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
                is_accelerate = (
                    program_name_val is not None
                    and int(program_name_val) == CU_PROGRAM_ACCELERATE_INDEX
                )
                if is_accelerate and discord_username:
                    usernames.add(discord_username.lower())
                elif is_accelerate and not discord_username:
                    missing_username.append({
                        "name": task.get("name") or "(unnamed)",
                        "task_id": task.get("id") or "",
                        "status": (task.get("status") or {}).get("status", ""),
                    })
            page += 1

    _accelerate_cache["usernames"] = usernames
    _accelerate_cache["missing_username"] = missing_username
    _accelerate_cache["last_fetched"] = now
    print(f"[CLICKUP] Refreshed Accelerate cache: {len(usernames)} members")
    if missing_username:
        # Loud warning so this shows up in Heroku/Railway logs the moment a
        # new Accelerate member is created without a Discord handle.
        print(
            f"[CLICKUP] WARN: {len(missing_username)} Accelerate member(s) have a "
            f"BLANK Discord username and will NOT receive check-in DMs:"
        )
        for entry in missing_username[:20]:
            print(
                f"          - {entry['name']!r} "
                f"(status={entry['status']}, task=https://app.clickup.com/t/{entry['task_id']})"
            )
        if len(missing_username) > 20:
            print(f"          ... and {len(missing_username) - 20} more (run /checkin_status to see all)")
    return usernames


def get_accelerate_missing_username() -> list[dict]:
    """Return the cached list of Accelerate members with a blank Discord username.

    Read-only accessor for /checkin_status — the cache is populated as a side
    effect of fetch_accelerate_usernames(), so callers must call that first
    (or rely on a recent prior refresh) to get current data.
    """
    return list(_accelerate_cache.get("missing_username") or [])


def is_within_join_window(member: discord.Member) -> bool:
    """Return True if the member is in their first CHECKIN_WEEKS_CAP weeks AND
    joined Discord on or after MEMBER_JOIN_CUTOFF.

    The cohort scope is intentionally tight — coaching check-ins target newer
    Accelerate members through their first 12 weeks. After that they roll off
    automatically.
    """
    if member.joined_at is None:
        return False
    if member.joined_at < MEMBER_JOIN_CUTOFF:
        return False
    weeks_since_join = (datetime.now(timezone.utc) - member.joined_at).days / 7
    return weeks_since_join < CHECKIN_WEEKS_CAP


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

# Stage options — 6-stage funnel from the spec
STAGE_OPTIONS = [
    ("1. Finding a Product", "1. Finding a Product"),
    ("2. Building a Store", "2. Building a Store"),
    ("3. Creating Ads", "3. Creating Ads"),
    ("4. Launched Ads", "4. Launched Ads"),
    ("5. Making Sales", "5. Making Sales"),
    ("6. Scaling Brand", "6. Scaling Brand"),
]

# Stages where we ask for product name + link (one-time capture).
# Anyone past "Finding a Product" should have something to point at.
PRODUCT_INFO_STAGES = {
    "2. Building a Store",
    "3. Creating Ads",
    "4. Launched Ads",
    "5. Making Sales",
    "6. Scaling Brand",
}


def _stage_requires_product_info(stage: str) -> bool:
    return stage in PRODUCT_INFO_STAGES


HOURS_OPTIONS = [
    ("Didn't have much time", "Didn't have much time"),
    ("1–4 hours", "1–4 hours"),
    ("5–10 hours", "5–10 hours"),
    ("10+ hours", "10+ hours"),
]

# Mood / progress confidence — final dropdown before the form opens
FEELING_OPTIONS = [
    ("Locked in", "Locked in"),
    ("Confident I'll make progress", "Confident I'll make progress"),
    ("A bit stuck", "A bit stuck"),
    ("Overwhelmed", "Overwhelmed"),
    ("Completely blocked", "Completely blocked"),
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


# --- In-flight check-in lock ---
# Prevents a user from starting two parallel flows (e.g. clicking the channel
# button AND running /checkin in DM at the same time). Per-process in-memory:
# a bot restart drops the locks and the user can simply start again. Together
# with the per-week has_checked_in() guard and the re-check inside submit, this
# gives three layers of duplicate protection.
#
# Locks carry a TTL so a member who abandons mid-flow (closes the modal, ignores
# the DM, etc.) isn't permanently blocked — they roll off after CHECKIN_LOCK_TTL
# seconds. The conversational flow releases explicitly via release_checkin_lock
# in its finally block; the modal flow releases on successful submit. The TTL is
# the safety net for everything else.
CHECKIN_LOCK_TTL = 1800  # 30 minutes
_inflight_checkins: dict[int, float] = {}


def acquire_checkin_lock(user_id: int) -> bool:
    """Try to claim the in-flight slot for this user. Returns False only if
    a fresh (within TTL) flow is already running."""
    now = time.time()
    started = _inflight_checkins.get(user_id)
    if started is not None and now - started < CHECKIN_LOCK_TTL:
        return False
    _inflight_checkins[user_id] = now
    return True


def release_checkin_lock(user_id: int) -> None:
    _inflight_checkins.pop(user_id, None)


def is_checkin_in_flight(user_id: int) -> bool:
    started = _inflight_checkins.get(user_id)
    if started is None:
        return False
    if time.time() - started >= CHECKIN_LOCK_TTL:
        _inflight_checkins.pop(user_id, None)
        return False
    return True


# --- Reminder fire tracking (persistent, restart-safe) ---
def _load_reminder_fires() -> dict:
    if os.path.exists(REMINDER_FIRES_FILE):
        try:
            with open(REMINDER_FIRES_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return {}
    return {}


def _save_reminder_fires(data: dict) -> None:
    with open(REMINDER_FIRES_FILE, "w") as f:
        json.dump(data, f, indent=2)


def already_fired_today(kind: str, date_iso: str) -> bool:
    """True if `kind` ('weekly' or 'midweek') was already fired on `date_iso`."""
    return _load_reminder_fires().get(kind) == date_iso


def mark_fired_today(kind: str, date_iso: str) -> None:
    data = _load_reminder_fires()
    data[kind] = date_iso
    _save_reminder_fires(data)


# --- Member product info persistence ---
# Once a member tells us their product name + link, we cache it locally and
# never ask again. Future check-ins include the saved info in the task
# description so coaches can see what each person is working on without having
# to re-ask.
def _load_product_info() -> dict:
    if os.path.exists(PRODUCT_INFO_FILE):
        with open(PRODUCT_INFO_FILE, "r") as f:
            return json.load(f)
    return {}


def _save_product_info(data: dict):
    with open(PRODUCT_INFO_FILE, "w") as f:
        json.dump(data, f, indent=2)


def get_product_info(discord_username: str) -> dict | None:
    return _load_product_info().get((discord_username or "").lower())


def has_product_info(discord_username: str) -> bool:
    return get_product_info(discord_username) is not None


def save_member_product_info(discord_username: str, product_name: str, product_link: str):
    data = _load_product_info()
    data[(discord_username or "").lower()] = {
        "product_name": (product_name or "").strip(),
        "product_link": (product_link or "").strip(),
        "captured_at": datetime.now().isoformat(),
    }
    _save_product_info(data)


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
                                what_would_help: str = "", next_steps: str = "",
                                roadmap_step: str = ""):
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

        # Update Roadmap Step (number field) — member's current step this week
        step_num = _parse_step_number(roadmap_step)
        if step_num is not None:
            await _set_field(CU_FIELD_ROADMAP_STEP, step_num, "Roadmap Step")

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


async def save_product_info_to_member_db(discord_username: str,
                                         product_name: str,
                                         product_link: str) -> None:
    """When a member shares their product via the bot, bump 'Listings Reviewed'
    +1 and overwrite 'Latest Call Topic' with the product info so coaches see
    it on the contact page."""
    try:
        member_task = await find_member_by_discord(discord_username)
    except Exception as e:
        print(f"[CLICKUP] Product info — member lookup error for {discord_username}: {e}")
        return
    if not member_task:
        print(f"[CLICKUP] Product info — no member match for {discord_username!r}")
        return

    task_id = member_task["id"]

    current_count = 0
    for cf in member_task.get("custom_fields", []):
        if cf.get("id") == CU_FIELD_LISTINGS_REVIEWED:
            try:
                current_count = int(float(cf.get("value") or 0))
            except (TypeError, ValueError):
                current_count = 0
            break

    pname = (product_name or "").strip()
    plink = (product_link or "").strip()
    topic = (
        f"Product: {pname} — {plink}".strip(" —")
        if (pname or plink)
        else ""
    )

    headers = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}
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
                        print(f"[CLICKUP] Product info — {label} update {r.status}: {body[:300]}")
            except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                print(f"[CLICKUP] Product info — {label} network error: {e}")

        await _set_field(CU_FIELD_LISTINGS_REVIEWED, current_count + 1, "Listings Reviewed")
        if topic:
            await _set_field(CU_FIELD_LATEST_CALL_TOPIC, topic, "Latest Call Topic")

    print(
        f"[CLICKUP] Product info saved to member {task_id} "
        f"(Listings Reviewed: {current_count} → {current_count + 1})"
    )


# --- Public check-in confirmation in 1-1 ticket channels ---
# Channel names follow "<ticket#>-<discord_username>", e.g. "69-michaelralston92".
_TICKET_CHANNEL_NAME_RE = re.compile(r"^(\d+)-(.+)$")


def _normalize_handle(s: str) -> str:
    """Reduce a Discord username / channel-name segment to a comparable core:
    lowercase, keep only [a-z0-9]. Discord channel names can't contain '.',
    spaces or most punctuation, so a username like 'john.doe' becomes the
    channel segment 'johndoe' (or 'john-doe'). Normalizing both sides lets them
    match instead of silently failing — which previously meant no 1-1 ticket
    post and no reminder routed for anyone with punctuation in their handle."""
    return re.sub(r"[^a-z0-9]", "", (s or "").lower())


def _ticket_channels_for_username(guild: discord.Guild, username_lower: str) -> list[discord.TextChannel]:
    target_exact = (username_lower or "").lower()
    target_norm = _normalize_handle(username_lower)
    found = []
    for ch in guild.text_channels:
        m = _TICKET_CHANNEL_NAME_RE.match(ch.name.strip())
        if not m:
            continue
        seg = m.group(2)
        if seg.lower() == target_exact or (target_norm and _normalize_handle(seg) == target_norm):
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


async def post_checkin_to_ticket_channel(
    client: discord.Client,
    user: discord.User,
    *,
    answers: dict | None = None,
    completed_in_channel_id: int | None = None,
) -> None:
    """Post a check-in summary in the member's 1-1 ticket channel.

    - If the check-in was completed in the ticket channel itself
      (`completed_in_channel_id` matches), post a short confirmation since the
      raw answers are already visible above.
    - Otherwise (DM modal flow, or /checkin run from a non-ticket channel),
      post the FULL raw answers so the coach sees what the member wrote.

    Tags coaches from the ClickUp Member Database Coach field when they match
    a member of the guild.
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

        if completed_in_channel_id == channel.id:
            # Conversational flow happened right here — coach already saw the
            # whole thing scroll by. Just give them a ping + done marker.
            body = (
                f"{user.mention} ✅ **Check-in saved to ClickUp.** "
                "Your coaching team will review this to help you make progress."
            )
        elif answers:
            # DM modal (or external-channel) flow — surface the raw answers so
            # the coach sees what the member wrote.
            product = get_product_info(user.name)
            product_lines = ""
            if product and (product.get("product_name") or product.get("product_link")):
                product_lines = (
                    f"**Product:** {product.get('product_name') or '—'}\n"
                    f"**Product Link:** {product.get('product_link') or '—'}\n\n"
                )
            body = (
                f"{user.mention} **Weekly check-in submitted**\n\n"
                f"**Stage:** {answers['stage']}\n"
                f"**Roadmap step:** {answers.get('roadmap_step') or '—'}\n"
                f"**Hours last week:** {answers['weekly_hours']}\n"
                f"**Feeling:** {answers['feeling']}\n"
                f"**Weeks in stage:** {answers['weeks']}\n\n"
                f"{product_lines}"
                f"**Blocker:** {answers['blocker']}\n\n"
                f"**Support that would help:** {answers['help_needed']}\n\n"
                f"**ONE key thing this week:** {answers['next_steps']}"
            )
        else:
            # No answers provided and not posted from this channel — fall back
            # to the legacy short confirmation.
            body = (
                f"{user.mention} **Check-in received** — thanks! Your coaching "
                "team will review this to help you make progress."
            )

        if coach_ping:
            body = f"{coach_ping}\n{body}"

        # Body may exceed Discord's 2000-char per-message limit if a member
        # writes a novel. Split conservatively on paragraph boundaries.
        for chunk in _split_for_discord(body):
            await channel.send(chunk)
        print(f"[TICKET] Posted check-in summary in #{channel.name}")
    except discord.Forbidden:
        print(f"[TICKET] Missing permission to post in ticket channel for {user.name!r}")
    except discord.HTTPException as e:
        print(f"[TICKET] Discord HTTP error posting confirmation: {e}")
    except Exception as e:
        print(f"[TICKET] Error posting confirmation: {e}")


def _split_for_discord(text: str, limit: int = 1900) -> list[str]:
    """Split a long message into chunks under Discord's 2000-char limit,
    preferring paragraph boundaries."""
    if len(text) <= limit:
        return [text]
    chunks: list[str] = []
    remaining = text
    while len(remaining) > limit:
        # Find last paragraph break before the limit
        cut = remaining.rfind("\n\n", 0, limit)
        if cut < limit // 2:
            cut = remaining.rfind("\n", 0, limit)
        if cut < limit // 2:
            cut = limit
        chunks.append(remaining[:cut].rstrip())
        remaining = remaining[cut:].lstrip()
    if remaining:
        chunks.append(remaining)
    return chunks


# Backwards-compat alias for any callers still referring to the old name.
post_public_checkin_confirmation = post_checkin_to_ticket_channel


# --- Check-in Modal (the popup form) ---
class CheckInModal(discord.ui.Modal, title="Weekly Coach Check-in"):
    def __init__(self, selected_stage: str, weekly_hours: str, feeling: str):
        super().__init__()
        self.selected_stage = selected_stage
        self.weekly_hours = weekly_hours
        self.feeling = feeling

    roadmap_step = discord.ui.TextInput(
        label="What step # on the roadmap checklist?",
        placeholder="Just the number — e.g. 1.7",
        style=discord.TextStyle.short,
        max_length=100,
    )
    weeks = discord.ui.TextInput(
        label="How many weeks have you been in this stage?",
        placeholder="e.g., 3",
        style=discord.TextStyle.short,
        max_length=10,
    )
    blocker = discord.ui.TextInput(
        label="What's blocking your progress right now?",
        placeholder="Be specific.",
        style=discord.TextStyle.paragraph,
        max_length=1000,
    )
    help_needed = discord.ui.TextInput(
        label="What kind of support would help you most?",
        placeholder="Be specific.",
        style=discord.TextStyle.paragraph,
        max_length=1000,
    )
    next_steps = discord.ui.TextInput(
        label="The ONE key thing to get done this week?",
        placeholder="Be specific.",
        style=discord.TextStyle.paragraph,
        max_length=1000,
    )

    async def on_submit(self, interaction: discord.Interaction):
        # Respond to Discord immediately (must be within 3 seconds)
        await interaction.response.defer(ephemeral=True, thinking=True)

        try:
            # Idempotency: if a parallel flow already recorded a check-in
            # (e.g. user somehow opened both DM modal and a channel
            # conversational flow), bail before creating a duplicate ClickUp
            # task.
            if has_checked_in(interaction.user.id):
                await interaction.followup.send(
                    "Looks like you already checked in this week — no duplicate created.",
                    ephemeral=True,
                )
                return

            ok, _task_id, err = await submit_checkin(
                user=interaction.user,
                stage=self.selected_stage,
                roadmap_step=self.roadmap_step.value,
                weekly_hours=self.weekly_hours,
                feeling=self.feeling,
                weeks=self.weeks.value,
                blocker=self.blocker.value,
                help_needed=self.help_needed.value,
                next_steps=self.next_steps.value,
            )

            if not ok:
                print(f"[ERROR] modal submit failed: {err}")
                await interaction.followup.send(
                    "⚠️ Something went wrong saving your check-in. Please try again in a moment.",
                    ephemeral=True,
                )
                return

            await interaction.followup.send(
                "Thanks for checking in — clarity creates momentum 💪\n"
                "Your coaching team will review this to help you make progress 👊",
                ephemeral=True,
            )
            print(f"[OK] Check-in from {interaction.user.display_name}")

            # DM modal flow: post the FULL raw answers in the user's 1-1
            # ticket channel so the coach sees what they wrote (not just a
            # generic "received" ping).
            asyncio.create_task(post_checkin_to_ticket_channel(
                interaction.client,
                interaction.user,
                answers={
                    "stage": self.selected_stage,
                    "roadmap_step": self.roadmap_step.value,
                    "weekly_hours": self.weekly_hours,
                    "feeling": self.feeling,
                    "weeks": self.weeks.value,
                    "blocker": self.blocker.value,
                    "help_needed": self.help_needed.value,
                    "next_steps": self.next_steps.value,
                },
            ))
        finally:
            release_checkin_lock(interaction.user.id)


async def _find_checkin_task_by_name(session, headers, task_name):
    """Return the id of an existing check-in task with this exact name created
    today, else None. Lets us avoid creating a duplicate when a slow ClickUp
    response timed out on our side *after* the task was actually saved."""
    midnight = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    since_ms = int(midnight.timestamp() * 1000)
    try:
        async with session.get(
            f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task",
            params={"include_closed": "true", "subtasks": "false", "date_created_gt": since_ms},
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=15),
        ) as r:
            if r.status != 200:
                return None
            data = await r.json()
    except (aiohttp.ClientError, asyncio.TimeoutError):
        return None
    target = (task_name or "").strip()
    for t in data.get("tasks", []):
        if (t.get("name") or "").strip() == target:
            return t.get("id")
    return None


async def _create_checkin_task(session, headers, task_data):
    """POST a check-in task to ClickUp, hardened against the failure mode that
    showed members 'something went wrong' even though the task actually saved:

    - 30s timeout (ClickUp occasionally responds slowly; 10s was too tight),
    - retries on transient errors (timeout / connection / 429 / 5xx),
    - idempotency: before re-POSTing, and once more at the end, check whether a
      prior attempt already created the task so we never duplicate a check-in.

    Returns (task_id, error_str)."""
    url = f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task"
    task_name = task_data.get("name", "")
    max_attempts = 3
    last_err = None
    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            existing = await _find_checkin_task_by_name(session, headers, task_name)
            if existing:
                print(f"[CHECKIN] Prior attempt had saved it — reusing {existing} (no duplicate)")
                return existing, None
        try:
            async with session.post(
                url, json=task_data, headers=headers,
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("id"), None
                body = await resp.text()
                last_err = f"ClickUp returned {resp.status}"
                print(f"[ERROR] ClickUp API (attempt {attempt}/{max_attempts}): {resp.status} — {body[:300]}")
                if resp.status not in (429, 500, 502, 503, 504):
                    return None, last_err  # non-transient (auth/bad field) — retry won't help
                ra = resp.headers.get("Retry-After", "")
                await asyncio.sleep(float(ra) if ra.replace(".", "", 1).isdigit() else 2 * attempt)
        except (aiohttp.ClientError, asyncio.TimeoutError) as e:
            last_err = str(e) or type(e).__name__
            print(f"[ERROR] submit_checkin request failed (attempt {attempt}/{max_attempts}): {last_err}")
            if attempt < max_attempts:
                await asyncio.sleep(2 * attempt)
    # Out of attempts: the final POST may have saved before the response timed out.
    recovered = await _find_checkin_task_by_name(session, headers, task_name)
    if recovered:
        print(f"[CHECKIN] Task saved despite errors — recovered {recovered}")
        return recovered, None
    return None, last_err or "save failed after retries"


def _parse_step_number(raw: str):
    """Pull the first number — integer OR decimal — out of a free-text
    roadmap-step answer so it can go in the numeric ClickUp field. The roadmap
    uses decimal steps (e.g. '1.7'), so '1.7' → 1.7, 'step 7' → 7, '7-8' → 7.
    Returns None when there's no number, in which case the field is left unset
    rather than erroring (the raw answer is still kept in the task description)."""
    if not raw:
        return None
    m = re.search(r"\d+(?:\.\d+)?", str(raw))
    if not m:
        return None
    val = m.group()
    return float(val) if "." in val else int(val)


async def submit_checkin(
    *,
    user,
    stage: str,
    roadmap_step: str = "",
    weekly_hours: str,
    feeling: str,
    weeks: str,
    blocker: str,
    help_needed: str,
    next_steps: str,
):
    """Build + POST a ClickUp check-in task. Records the check-in and schedules
    the background member-profile enrichment.

    Returns (ok: bool, checkin_task_id: str | None, error: str | None).
    Shared by the modal flow (DM) and the conversational flow (1-1 channel).
    """
    today = datetime.now().strftime("%b %d, %Y")
    hours_band = weekly_hours_band_for_label(weekly_hours)
    headers = {
        "Authorization": CLICKUP_TOKEN,
        "Content-Type": "application/json",
    }
    display_name = getattr(user, "display_name", None) or user.name

    base_custom_fields = [
        {"id": CI_FIELD_MEMBER, "value": display_name},
        {"id": CI_FIELD_DATE, "value": today},
        {"id": CI_FIELD_STAGE, "value": stage},
        {"id": CI_FIELD_WEEKS_IN_STAGE, "value": weeks},
        {"id": CI_FIELD_WEEK, "value": datetime.now().isocalendar()[1]},
        {"id": CI_FIELD_BLOCKER, "value": blocker},
        {"id": CI_FIELD_WHAT_WOULD_HELP, "value": help_needed},
        {"id": CI_FIELD_NEXT_STEPS, "value": next_steps},
    ]

    step_num = _parse_step_number(roadmap_step)
    if step_num is not None:
        base_custom_fields.append({"id": CI_FIELD_ROADMAP_STEP, "value": step_num})

    product = get_product_info(user.name)
    product_lines = ""
    if product:
        pname = product.get("product_name") or ""
        plink = product.get("product_link") or ""
        if pname or plink:
            product_lines = (
                f"**Product:** {pname}\n\n"
                f"**Product Link:** {plink}\n\n"
            )

    try:
        async with aiohttp.ClientSession() as session:
            wh_meta = await get_weekly_hours_field_meta(session)
            custom_fields = list(base_custom_fields)
            wh_entry = weekly_hours_custom_field_entry(wh_meta, hours_band, weekly_hours)
            if wh_entry:
                custom_fields.append(wh_entry)
            task_data = {
                "name": f"Check-in — {display_name} — {today}",
                "description": (
                    f"**Member:** {display_name}\n"
                    f"**Discord Username:** {user.name}\n"
                    f"**Date:** {today}\n\n"
                    f"---\n\n"
                    f"**Stage:** {stage}\n\n"
                    f"**Roadmap Step:** {roadmap_step or '—'}\n\n"
                    f"{product_lines}"
                    f"**Hours Spent This Week:** {weekly_hours}\n\n"
                    f"**Weeks in Stage:** {weeks}\n\n"
                    f"**Feeling About Progress:** {feeling}\n\n"
                    f"**Blocker:** {blocker}\n\n"
                    f"**Support That Would Help:** {help_needed}\n\n"
                    f"**ONE Key Thing This Week:** {next_steps}"
                ),
                "priority": 3,
                "tags": ["check-in", user.name],
                "custom_fields": custom_fields,
            }
            checkin_task_id, err = await _create_checkin_task(session, headers, task_data)
            if not checkin_task_id:
                return False, None, err
    except (aiohttp.ClientError, asyncio.TimeoutError) as e:
        print(f"[ERROR] submit_checkin session error: {e}")
        return False, None, str(e)

    record_checkin(user.id)
    unmark_dm_blocked(user.id)

    asyncio.create_task(_update_member_after_checkin(
        discord_username=user.name,
        display_name=display_name,
        stage=stage,
        roadmap_step=roadmap_step,
        weeks=weeks,
        blocker=blocker,
        help_needed=help_needed,
        next_steps=next_steps,
        checkin_task_id=checkin_task_id,
    ))
    return True, checkin_task_id, None


async def _update_member_after_checkin(discord_username, display_name, stage,
                                       weeks, blocker, help_needed, next_steps,
                                       roadmap_step="", checkin_task_id=None):
    """Background task to update ClickUp member profile and enrich check-in task."""
    try:
        member_task = await find_member_by_discord(discord_username)
        if member_task:
            await update_member_profile(
                member_task["id"], stage,
                weeks=weeks, blocker=blocker,
                what_would_help=help_needed, next_steps=next_steps,
                roadmap_step=roadmap_step,
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


# --- Product Info Modal (first-time capture for stage 2+) ---
class ProductInfoModal(discord.ui.Modal, title="Tell us about your product"):
    """Asked once per member, only when they pick stage 2+ for the first time.
    Saves to the local product info cache; future check-ins read from there and
    skip this modal entirely."""

    def __init__(self, selected_stage: str, weekly_hours: str, feeling: str):
        super().__init__()
        self.selected_stage = selected_stage
        self.weekly_hours = weekly_hours
        self.feeling = feeling

    product_name = discord.ui.TextInput(
        label="What is your product called?",
        placeholder="e.g., GlowSerum Pro",
        style=discord.TextStyle.short,
        max_length=200,
    )
    product_link = discord.ui.TextInput(
        label="Can you share a link?",
        placeholder="e.g., yourstore.com/product",
        style=discord.TextStyle.short,
        max_length=500,
        required=False,
    )

    async def on_submit(self, interaction: discord.Interaction):
        save_member_product_info(
            interaction.user.name,
            self.product_name.value,
            self.product_link.value,
        )
        asyncio.create_task(save_product_info_to_member_db(
            interaction.user.name,
            self.product_name.value,
            self.product_link.value,
        ))
        view = ContinueCheckinView(
            selected_stage=self.selected_stage,
            weekly_hours=self.weekly_hours,
            feeling=self.feeling,
        )
        await interaction.response.send_message(
            "✅ Got it — product saved.\n\n**Last step — open your check-in:**",
            view=view,
            ephemeral=True,
        )


class ContinueCheckinView(discord.ui.View):
    """Intermediate button shown after product info is captured. Clicking it
    opens the main CheckInModal — needed because Discord won't let us push two
    modals back-to-back without a user interaction in between."""

    def __init__(self, selected_stage: str, weekly_hours: str, feeling: str):
        super().__init__(timeout=300)
        self.selected_stage = selected_stage
        self.weekly_hours = weekly_hours
        self.feeling = feeling

    @discord.ui.button(
        label="Continue Check-in",
        style=discord.ButtonStyle.green,
        emoji="📋",
    )
    async def continue_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_modal(
            CheckInModal(
                selected_stage=self.selected_stage,
                weekly_hours=self.weekly_hours,
                feeling=self.feeling,
            ),
        )


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
            "**Step 2 of 3 — Hours last week**\n"
            "Choose roughly how much time you dedicated last week.",
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
            placeholder="How much time did you dedicate last week?",
            options=options,
            custom_id="hours_select",
        )

    async def callback(self, interaction: discord.Interaction):
        selected_hours = self.values[0]
        await interaction.response.send_message(
            "**Step 3 of 3 — How are you feeling?**\n"
            "Pick the mood that fits your progress this week, then the form opens.",
            view=FeelingSelectView(
                selected_stage=self.selected_stage,
                weekly_hours=selected_hours,
            ),
            ephemeral=True,
        )


class HoursSelectView(discord.ui.View):
    def __init__(self, selected_stage: str):
        super().__init__(timeout=300)
        self.add_item(HoursSelect(selected_stage=selected_stage))


class FeelingSelect(discord.ui.Select):
    def __init__(self, selected_stage: str, weekly_hours: str):
        self.selected_stage = selected_stage
        self.weekly_hours = weekly_hours
        options = [
            discord.SelectOption(label=label, value=value)
            for label, value in FEELING_OPTIONS
        ]
        super().__init__(
            placeholder="How are you feeling about progress this week?",
            options=options,
            custom_id="feeling_select",
        )

    async def callback(self, interaction: discord.Interaction):
        feeling = self.values[0]
        needs_product = (
            _stage_requires_product_info(self.selected_stage)
            and not has_product_info(interaction.user.name)
        )
        if needs_product:
            await interaction.response.send_modal(
                ProductInfoModal(
                    selected_stage=self.selected_stage,
                    weekly_hours=self.weekly_hours,
                    feeling=feeling,
                ),
            )
        else:
            await interaction.response.send_modal(
                CheckInModal(
                    selected_stage=self.selected_stage,
                    weekly_hours=self.weekly_hours,
                    feeling=feeling,
                ),
            )


class FeelingSelectView(discord.ui.View):
    def __init__(self, selected_stage: str, weekly_hours: str):
        super().__init__(timeout=300)
        self.add_item(FeelingSelect(selected_stage=selected_stage, weekly_hours=weekly_hours))


# --- Conversational check-in flow (1-1 ticket channel + DM fallback) ---
# Instead of a modal popup, the bot asks each question as its own message in
# the channel. Structured fields use dropdowns; free-text fields wait for the
# member's next message. Coach reads the whole conversation in real time.

_CANCEL_TOKENS = {"cancel", "stop", "abort", "quit"}
_CONVO_TIMEOUT_SECONDS = 1800  # 30 min per question; whole flow can sit idle


class _SingleSelectView(discord.ui.View):
    """Posts one dropdown, captures the picked value, stops the view. Only the
    target user can interact."""

    def __init__(self, *, user_id: int, options: list, placeholder: str):
        super().__init__(timeout=_CONVO_TIMEOUT_SECONDS)
        self.user_id = user_id
        self.value: str | None = None
        self.add_item(_AskSelect(options=options, placeholder=placeholder))

    async def interaction_check(self, interaction: discord.Interaction) -> bool:
        if interaction.user.id != self.user_id:
            await interaction.response.send_message(
                "This check-in isn't for you — type `/checkin` to start your own.",
                ephemeral=True,
            )
            return False
        return True


class _AskSelect(discord.ui.Select):
    def __init__(self, *, options: list, placeholder: str):
        super().__init__(
            placeholder=placeholder,
            options=[discord.SelectOption(label=l, value=v) for l, v in options],
            min_values=1,
            max_values=1,
        )

    async def callback(self, interaction: discord.Interaction):
        self.view.value = self.values[0]
        await interaction.response.defer()
        self.view.stop()


async def _ask_select(
    *, user: discord.User, channel, prompt: str, options: list, placeholder: str,
) -> str | None:
    """Post a dropdown question and wait for the user to pick. Returns the
    value, or None if they timed out."""
    view = _SingleSelectView(user_id=user.id, options=options, placeholder=placeholder)
    msg = await channel.send(prompt, view=view)
    timed_out = await view.wait()
    if timed_out or view.value is None:
        try:
            await msg.edit(content=f"{prompt}\n*(Timed out — run `/checkin` to start again.)*", view=None)
        except discord.HTTPException:
            pass
        return None
    # Lock in the picked value so it can't be re-selected.
    try:
        await msg.edit(content=f"{prompt}\n**You picked:** {view.value}", view=None)
    except discord.HTTPException:
        pass
    return view.value


async def _ask_text(
    *,
    client: discord.Client,
    user: discord.User,
    channel,
    prompt: str,
    max_length: int | None = None,
    required: bool = True,
) -> str | None:
    """Post a free-text question and wait for the user's next message in this
    channel. Returns the text, '' if optional + skipped, or None if cancelled /
    timed out."""
    suffix = "\n*Reply here — type `cancel` to abort, or `skip` to leave blank.*" if not required \
        else "\n*Reply here — type `cancel` to abort.*"
    await channel.send(f"{prompt}{suffix}")
    while True:
        try:
            msg = await client.wait_for(
                "message",
                check=lambda m: m.author.id == user.id and m.channel.id == channel.id,
                timeout=_CONVO_TIMEOUT_SECONDS,
            )
        except asyncio.TimeoutError:
            await channel.send(
                f"{user.mention} ⏰ Check-in timed out. Run `/checkin` when you're ready."
            )
            return None
        text = (msg.content or "").strip()
        low = text.lower()
        if low in _CANCEL_TOKENS:
            await channel.send(f"{user.mention} Check-in cancelled. Run `/checkin` whenever.")
            return None
        if not required and low in {"skip", "n/a", "none"}:
            return ""
        if not text:
            if not required:
                return ""
            await channel.send("That came through empty — try again.")
            continue
        if max_length and len(text) > max_length:
            await channel.send(
                f"That's {len(text)} characters — try keeping it under {max_length}."
            )
            continue
        return text


async def run_conversational_checkin(
    *,
    client: discord.Client,
    user: discord.User,
    channel,
) -> None:
    """Walk the user through Stage → Hours → Feeling → (Product?) → Weeks →
    Blocker → Help → Next-steps in `channel`. Posts to ClickUp on success.

    The caller (`_dispatch_checkin_entry`) MUST have already acquired the
    in-flight lock for this user. We release it unconditionally in `finally`.
    """
    try:
        await channel.send(
            f"👋 {user.mention} **Weekly coach check-in — let's go.**\n"
            "I'll ask 8 quick questions. You can type `cancel` any time to bail out."
        )

        stage = await _ask_select(
            user=user, channel=channel,
            prompt="**1 / 8 — Which stage are you currently at?**",
            options=STAGE_OPTIONS,
            placeholder="Pick your stage",
        )
        if stage is None:
            return

        roadmap_step = await _ask_text(
            client=client, user=user, channel=channel,
            prompt="**2 / 8 — What step number on the roadmap checklist are you at?**\n*Just the number — e.g. `1.7`*",
            max_length=100,
        )
        if roadmap_step is None:
            return

        weekly_hours = await _ask_select(
            user=user, channel=channel,
            prompt="**3 / 8 — How much time did you dedicate last week?**",
            options=HOURS_OPTIONS,
            placeholder="Pick your hours",
        )
        if weekly_hours is None:
            return

        feeling = await _ask_select(
            user=user, channel=channel,
            prompt="**4 / 8 — How are you feeling about progress?**",
            options=FEELING_OPTIONS,
            placeholder="Pick the closest match",
        )
        if feeling is None:
            return

        # One-time product capture for stages 2+ if we haven't cached it yet.
        if _stage_requires_product_info(stage) and not has_product_info(user.name):
            product_name = await _ask_text(
                client=client, user=user, channel=channel,
                prompt="**Quick one-time question — what is your product called?**",
                max_length=200,
            )
            if product_name is None:
                return
            product_link = await _ask_text(
                client=client, user=user, channel=channel,
                prompt="**Can you share a link?**  *(Optional — type `skip` if not yet.)*",
                max_length=500,
                required=False,
            )
            if product_link is None:
                return
            save_member_product_info(user.name, product_name, product_link)
            asyncio.create_task(save_product_info_to_member_db(
                user.name, product_name, product_link,
            ))

        weeks = await _ask_text(
            client=client, user=user, channel=channel,
            prompt="**5 / 8 — How many weeks have you been in this stage?**\n*e.g. `3`*",
            max_length=10,
        )
        if weeks is None:
            return

        blocker = await _ask_text(
            client=client, user=user, channel=channel,
            prompt="**6 / 8 — What's blocking your progress right now?**\n*Be specific.*",
            max_length=1000,
        )
        if blocker is None:
            return

        help_needed = await _ask_text(
            client=client, user=user, channel=channel,
            prompt="**7 / 8 — What kind of support would help you most?**\n*Be specific.*",
            max_length=1000,
        )
        if help_needed is None:
            return

        next_steps = await _ask_text(
            client=client, user=user, channel=channel,
            prompt="**8 / 8 — The ONE key thing to get done this week?**\n*Be specific.*",
            max_length=1000,
        )
        if next_steps is None:
            return

        # Race re-check: another flow (DM modal) may have submitted in parallel.
        if has_checked_in(user.id):
            await channel.send(
                f"{user.mention} Looks like a check-in already landed for you this "
                "week — skipping to avoid a duplicate."
            )
            return

        await channel.send("Saving your check-in… 📋")
        ok, _task_id, err = await submit_checkin(
            user=user,
            stage=stage,
            roadmap_step=roadmap_step,
            weekly_hours=weekly_hours,
            feeling=feeling,
            weeks=weeks,
            blocker=blocker,
            help_needed=help_needed,
            next_steps=next_steps,
        )
        if not ok:
            print(f"[CONVO] submit failed: {err}")
            await channel.send(
                f"{user.mention} ⚠️ Something went wrong saving your check-in. "
                "Try `/checkin` again in a moment."
            )
            return

        await channel.send(
            f"{user.mention} ✅ **Thanks for checking in — clarity creates momentum.** 💪"
        )
        print(f"[OK] Conversational check-in from {user.name}")

        # Post the wrap-up to the 1-1 ticket channel. If the convo happened in
        # that same channel, this just adds a coach @mention + done marker.
        # If the convo happened elsewhere (DM fallback, /checkin in random
        # channel), this posts the full raw answers there.
        asyncio.create_task(post_checkin_to_ticket_channel(
            client,
            user,
            answers={
                "stage": stage,
                "weekly_hours": weekly_hours,
                "feeling": feeling,
                "weeks": weeks,
                "blocker": blocker,
                "help_needed": help_needed,
                "next_steps": next_steps,
            },
            completed_in_channel_id=channel.id,
        ))
    finally:
        release_checkin_lock(user.id)


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
        await _dispatch_checkin_entry(interaction)


def _is_dm_channel(channel) -> bool:
    """True if `channel` is a Discord DM (private or group-DM)."""
    if channel is None:
        return False
    return channel.type in (discord.ChannelType.private, discord.ChannelType.group)


async def _dispatch_checkin_entry(interaction: discord.Interaction) -> None:
    """Shared entry point for both the button click and the /checkin slash
    command. Routes by where the interaction happened:

      - DM         → existing modal flow (3 ephemeral selects + popup form)
      - Any guild  → conversational flow in that channel

    This is the ONLY place that acquires the in-flight lock for a fresh flow.
    Both downstream flows are responsible for releasing it in a `finally`.
    """
    # Already submitted this week?
    if has_checked_in(interaction.user.id):
        await interaction.response.send_message(
            "You've already checked in this week — see you next Monday. 👊",
            ephemeral=True,
        )
        return

    # Single lock acquire — closes the double-click race for both paths.
    if not acquire_checkin_lock(interaction.user.id):
        await interaction.response.send_message(
            "You've already got a check-in open — finish that one first.",
            ephemeral=True,
        )
        return

    try:
        if _is_dm_channel(interaction.channel):
            # DM path: existing modal flow. Lock is released by
            # CheckInModal.on_submit (or by TTL if the user abandons mid-flow).
            await interaction.response.send_message(
                "**Step 1 of 3 — Stage**\n"
                "Pick the stage you're at. Next you'll pick **hours** and **how you're feeling**, "
                "then the form opens.",
                view=StageSelectView(),
                ephemeral=True,
            )
            return

        # Channel path: ack ephemerally to clear the button's loading state,
        # then kick off the public conversation. run_conversational_checkin
        # releases the lock in its own finally.
        await interaction.response.send_message(
            "Starting your check-in below — answer each question right here. 👇",
            ephemeral=True,
        )
    except Exception:
        # If we acquired the lock but couldn't even send the ack, release the
        # lock so the user can retry. (Don't swallow the exception itself.)
        release_checkin_lock(interaction.user.id)
        raise

    asyncio.create_task(run_conversational_checkin(
        client=interaction.client,
        user=interaction.user,
        channel=interaction.channel,
    ))


# --- Slash command: /checkin ---
@tree.command(name="checkin", description="Submit your weekly coach check-in")
async def checkin_command(interaction: discord.Interaction):
    await _dispatch_checkin_entry(interaction)


# --- Admin command: trigger check-in DMs now ---
@tree.command(name="trigger_checkins", description="[Admin] Send check-in reminders to all eligible members now")
@app_commands.default_permissions(administrator=True)
async def trigger_checkins(interaction: discord.Interaction):
    await interaction.response.defer(ephemeral=True)
    try:
        await _send_checkin_dms(
            "manual_trigger",
            "{mention} 👋 **Weekly coach check-in.**\n"
            "Hit **Start Check-in** below (or type `/checkin`) and I'll walk you "
            "through 8 quick questions right here. Takes about 2 minutes.\n\n"
            "*Your coaching team uses this to help you make progress.*",
            "**📋 Weekly Coach Check-in**\n\n"
            "Time for your weekly coach check-in. Click **Start Check-in** below "
            "to share where you're at — takes about 2 minutes.\n\n"
            "*Your coaching team uses this to help you make progress.*",
        )
        await interaction.followup.send("✅ Check-in reminders sent!", ephemeral=True)
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
    weeks_cutoff = datetime.now(timezone.utc) - timedelta(weeks=CHECKIN_WEEKS_CAP)
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
        if joined and joined < MEMBER_JOIN_CUTOFF:
            reasons.append(
                f"joined {joined_str} (before {MEMBER_JOIN_CUTOFF.strftime('%b %d, %Y')} cutoff)"
            )
            eligible = False
        if joined and joined < weeks_cutoff:
            reasons.append(f"joined {joined_str} (>{CHECKIN_WEEKS_CAP} weeks ago)")
            eligible = False
        if has_checked_in(member.id):
            reasons.append("already checked in this week")
            eligible = False
        if is_advanced_stage(member.id, excluded_ids):
            reasons.append("Making Sales / Scaling Brand")
            eligible = False
        if is_dm_blocked(member.id):
            reasons.append("DMs blocked")
            eligible = False
        status = "✅" if eligible else "❌"
        reason_text = f" — {', '.join(reasons)}" if reasons else ""
        lines.append(f"{status} **{member.display_name}** (joined {joined_str}){reason_text}")

    # Surface Accelerate members in ClickUp who are silently filtered out
    # because their Discord username field is blank.  These never appear in
    # the eligibility loop above because they're missing from
    # `accelerate_usernames`, so without this section the operator has no
    # way to know they exist short of opening every ClickUp row by hand.
    missing_dc = get_accelerate_missing_username()
    missing_block = ""
    if missing_dc:
        missing_lines = []
        for entry in missing_dc[:25]:
            url = f"https://app.clickup.com/t/{entry['task_id']}" if entry.get("task_id") else ""
            status = entry.get("status") or "?"
            missing_lines.append(f"⚠️ **{entry['name']}** (status={status}) — {url}")
        more = ""
        if len(missing_dc) > 25:
            more = f"\n... and {len(missing_dc) - 25} more"
        missing_block = (
            f"\n\n**Accelerate members with BLANK Discord username "
            f"(silently skipped — fix in ClickUp):** {len(missing_dc)}\n"
            + "\n".join(missing_lines)
            + more
        )

    if not lines:
        body = (
            f"No Accelerate members found in Discord.\n"
            f"ClickUp has {len(accelerate_usernames)} Accelerate usernames: "
            f"{', '.join(sorted(accelerate_usernames)) or 'none'}"
            f"{missing_block}"
        )
        if len(body) > 1900:
            body = body[:1900] + "\n... (truncated)"
        await interaction.followup.send(body, ephemeral=True)
        return

    msg = (
        f"**Accelerate Members — Eligibility Report**\n"
        f"(Source: ClickUp Program Name | Filter: joined "
        f"≥ {MEMBER_JOIN_CUTOFF.strftime('%b %d, %Y')} "
        f"and within first {CHECKIN_WEEKS_CAP} weeks)\n\n"
        + "\n".join(lines)
        + missing_block
    )
    if len(msg) > 1900:
        msg = msg[:1900] + "\n... (truncated)"
    await interaction.followup.send(msg, ephemeral=True)


@tree.command(
    name="hai_reset_watermark",
    description="[Admin] Reset HonestAI scraper watermark — next scrape rewalks from the channel start",
)
@app_commands.default_permissions(administrator=True)
async def hai_reset_watermark(interaction: discord.Interaction):
    """Delete the FAQ scraper's watermark file so the next /hai_scrape_now
    starts at the oldest message in #ask-honestai.

    Use this when you've just enabled HAI_SIBLING_SCAN and want to
    backfill answers for every previously-cached question — once the bot
    re-ships a message with new answer text, the GAS cache's
    mergeForUpsert_ will auto-clear the prior analysis for that row, the
    next analyzer pass will reclassify it with the actual answer, and
    answer_status will flip from "unanswered" to "answered".
    """
    await interaction.response.defer(ephemeral=True, thinking=True)
    try:
        import faq_scraper
        cfg = faq_scraper._cfg()
        path = cfg["state_path"]
        existed = path.exists()
        if existed:
            path.unlink()
        await interaction.followup.send(
            (
                f"✅ Watermark cleared (`{path}`).\n\n"
                if existed else
                f"ℹ️ No watermark file at `{path}` — already empty.\n\n"
            ) + (
                "Next `/hai_scrape_now` will rewalk from the channel start.\n"
                "Reminder: enable `HAI_SIBLING_SCAN=true` in Railway env "
                "vars first if you want the rescrape to detect inline "
                "answers (otherwise this will just re-ship the same "
                "answerless rows)."
            ),
            ephemeral=True,
        )
    except Exception as e:
        await interaction.followup.send(
            f"❌ Failed to reset watermark: `{e}`",
            ephemeral=True,
        )


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


# Messages for each step of the new-member coach check-in sequence.
# After step 12 the member rolls off and stops receiving DMs (12-week program).
_NEW_MEMBER_MESSAGES = {
    1: (
        "**📋 Welcome to your first Coach Check-in!**\n\n"
        "You've been with us for a week — time for your first coach check-in.\n"
        "Click the button below to share where you're at.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    2: (
        "**📋 Week 2 Coach Check-in**\n\n"
        "Two weeks in — let's see where you're at.\n"
        "Click the button below to share your update.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    3: (
        "**📋 Week 3 Coach Check-in**\n\n"
        "Three weeks in — keep the momentum going.\n"
        "Click the button below to share your update.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    4: (
        "**📋 Week 4 Coach Check-in**\n\n"
        "One month in — share where you're at this week.\n"
        "Click the button below to submit your check-in.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    5: (
        "**📋 Week 5 Coach Check-in**\n\n"
        "Five weeks in — you're building real habits.\n"
        "Click the button below to share your update.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    6: (
        "**📋 Week 6 Coach Check-in**\n\n"
        "Halfway through your 12-week program — keep going.\n"
        "Click the button below to share your update.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    7: (
        "**📋 Week 7 Coach Check-in**\n\n"
        "Seven weeks in — stay focused on your next milestone.\n"
        "Click the button below to submit your check-in.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    8: (
        "**📋 Week 8 Coach Check-in**\n\n"
        "Two months in — share where you're at this week.\n"
        "Click the button below to submit your check-in.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    9: (
        "**📋 Week 9 Coach Check-in**\n\n"
        "Nine weeks in — every check-in adds up.\n"
        "Click the button below to share your update.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    10: (
        "**📋 Week 10 Coach Check-in**\n\n"
        "Ten weeks in — outstanding commitment.\n"
        "Click the button below to share your update.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    11: (
        "**📋 Week 11 Coach Check-in**\n\n"
        "One week left in your 12-week program — keep pushing.\n"
        "Click the button below to share your update.\n\n"
        "*Your coaching team uses this to help you make progress.*"
    ),
    12: (
        "**📋 Week 12 Coach Check-in — Final**\n\n"
        "You've reached week 12 — congrats on an incredible run.\n"
        "Click the button below to submit your final check-in.\n\n"
        "*After this you'll roll off the weekly coach check-in sequence.*"
    ),
}


# --- Background task: send new-member coach check-in sequence (12 DMs, weekly) ---
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

# Scheduled reminder slots, evaluated by reminder_dispatcher every 5 min in ET.
# Polling design avoids the discord.py + ZoneInfo `time.utcoffset() is None`
# subtlety that made the previous `@tasks.loop(time=...)` schedule hard to debug.
REMINDER_WEEKLY_WEEKDAY  = 0   # Monday
REMINDER_WEEKLY_HOUR     = 9   # 9 AM ET
REMINDER_MIDWEEK_WEEKDAY = 2   # Wednesday
REMINDER_MIDWEEK_HOUR    = 12  # 12 PM ET


async def _send_checkin_dms(label: str, channel_message: str, dm_message: str | None = None):
    """Post the weekly / midweek check-in reminder to each eligible member.

    Routing: post in the member's 1-1 ticket channel (with @mention + the
    Start Check-in button) when one exists; otherwise fall back to DM. The
    ticket channel is the same one the bot already posts confirmations into —
    `<ticket#>-<discord_username>`, e.g. `69-michaelralston92`.

    `channel_message` is the body posted in the ticket channel (uses @mention
    for notification). `dm_message` is the body posted in DM fallback — if
    omitted, channel_message is reused.

    Anti-spam measures (DM path only):
    - Random jitter between DMs (DM_DELAY_MIN to DM_DELAY_MAX seconds)
    - Batch pausing (DM_BATCH_PAUSE seconds every DM_BATCH_SIZE messages)
    - Skip users with DMs disabled (persistent tracking)
    - Exponential backoff on 429 rate limits
    - Cross-guild deduplication
    """
    dm_message = dm_message or channel_message
    accelerate_usernames = await fetch_accelerate_usernames()
    excluded_ids = await fetch_excluded_user_ids()
    pending = load_pending()
    sent_channel = 0
    sent_dm = 0
    skipped = 0
    dm_blocked = 0
    ineligible = 0
    no_channel = 0
    seen_users = set()  # Dedupe across guilds

    for guild in client.guilds:
        for member in guild.members:
            if member.bot or member.id in seen_users:
                continue
            seen_users.add(member.id)
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

            # 1-1 ticket channel preferred. Coach sees the reminder land and
            # the conversational flow happens right there.
            candidates = _ticket_channels_for_username(guild, member.name.lower())
            ticket_channel = _pick_ticket_channel_for_confirmation(candidates)

            if ticket_channel is not None:
                try:
                    body = channel_message.format(mention=member.mention)
                    await ticket_channel.send(body, view=CheckInButton())
                    sent_channel += 1
                    print(f"[CHANNEL] Sent {label} to #{ticket_channel.name} for {member.display_name}")
                    # Channel posts have their own per-channel rate limit; a
                    # short pause is enough.
                    await asyncio.sleep(1.0)
                    continue
                except discord.Forbidden:
                    print(f"[CHANNEL] No permission to post in #{ticket_channel.name} — falling back to DM")
                except discord.HTTPException as e:
                    print(f"[CHANNEL] HTTP error posting in #{ticket_channel.name}: {e} — falling back to DM")
                except Exception as e:
                    print(f"[CHANNEL] Error posting in #{ticket_channel.name}: {e} — falling back to DM")

            # DM fallback.
            no_channel += 1
            if is_dm_blocked(member.id):
                dm_blocked += 1
                continue
            try:
                await member.send(dm_message, view=CheckInButton())
                sent_dm += 1
                print(f"[DM] Sent {label} to {member.display_name}")
                if sent_dm % DM_BATCH_SIZE == 0:
                    print(f"[PACE] Batch pause after {sent_dm} DMs ({DM_BATCH_PAUSE}s)")
                    await asyncio.sleep(DM_BATCH_PAUSE)
                else:
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

    print(
        f"[{label.upper()}] Channel: {sent_channel}, DM: {sent_dm} "
        f"(of {no_channel} with no ticket channel), "
        f"Skipped (checked in): {skipped}, DM-blocked: {dm_blocked}, "
        f"Join-date filtered: {ineligible}"
    )


# --- Reminder copy (channel + DM variants) ---
_WEEKLY_CHANNEL_MSG = (
    "{mention} 👋 **Weekly coach check-in.**\n"
    "Hit **Start Check-in** below (or type `/checkin`) and I'll walk you "
    "through 8 quick questions right here. Takes about 2 minutes.\n\n"
    "*Your coaching team uses this to help you make progress.*"
)
_WEEKLY_DM_MSG = (
    "**📋 Weekly Coach Check-in**\n\n"
    "Time for your weekly coach check-in. Click **Start Check-in** below "
    "to share where you're at — takes about 2 minutes.\n\n"
    "*Your coaching team uses this to help you make progress.*"
)
_MIDWEEK_CHANNEL_MSG = (
    "{mention} 🔔 **Still need your check-in this week.**\n"
    "Hit **Start Check-in** below or type `/checkin` — 2 minutes and your "
    "coach has what they need.\n\n"
    "*Your coaching team uses this to help you make progress.*"
)
_MIDWEEK_DM_MSG = (
    "**🔔 Midweek Reminder**\n\n"
    "You haven't submitted your coach check-in yet this week. Click "
    "**Start Check-in** below to share your update — only takes a minute.\n\n"
    "*Your coaching team uses this to help you make progress.*"
)


@tasks.loop(minutes=5)
async def reminder_dispatcher():
    """Single dispatcher for weekly + midweek reminders.

    Polls every 5 min, checks the current weekday + hour in ET, and fires
    the appropriate reminder once per day (idempotent via persistent
    fire-tracker). Replaces the previous tasks.loop(time=...) pair, which
    depended on tz-aware time objects whose utcoffset() returns None for
    ZoneInfo — making fire timing hard to reason about.

    With this design:
      - bot restarts during the firing hour still trigger the reminder
      - bot restarts AFTER firing don't re-fire (persistent state)
      - every fire writes a [REMINDER] Firing X at <ts> log line so the
        actual decision time is visible in production logs
    """
    now_et = datetime.now(et)
    date_iso = now_et.date().isoformat()
    weekday = now_et.weekday()
    hour = now_et.hour

    if weekday == REMINDER_WEEKLY_WEEKDAY and hour == REMINDER_WEEKLY_HOUR:
        if not already_fired_today("weekly", date_iso):
            mark_fired_today("weekly", date_iso)
            print(f"[REMINDER] Firing WEEKLY at {now_et.isoformat(timespec='seconds')}")
            await _send_checkin_dms("weekly", _WEEKLY_CHANNEL_MSG, _WEEKLY_DM_MSG)

    if weekday == REMINDER_MIDWEEK_WEEKDAY and hour == REMINDER_MIDWEEK_HOUR:
        if not already_fired_today("midweek", date_iso):
            mark_fired_today("midweek", date_iso)
            print(f"[REMINDER] Firing MIDWEEK at {now_et.isoformat(timespec='seconds')}")
            await _send_checkin_dms("midweek", _MIDWEEK_CHANNEL_MSG, _MIDWEEK_DM_MSG)


@reminder_dispatcher.before_loop
async def before_reminder_dispatcher():
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


# --- Manual "send check-in now" — single member -------------------------------
def _find_guild_member(*, user_id: str | None = None, username: str | None = None):
    """Locate a (guild, member) pair across every guild the bot is in.

    Matches by Discord user id first (exact), then by lowercased username.
    Returns (None, None) when nothing matches.
    """
    uname = (username or "").strip().lstrip("@").lower() or None
    uid = (user_id or "").strip() or None
    for guild in client.guilds:
        if uid:
            try:
                m = guild.get_member(int(uid))
            except (TypeError, ValueError):
                m = None
            if m is not None:
                return guild, m
        if uname:
            for m in guild.members:
                if (m.name or "").lower() == uname:
                    return guild, m
    return None, None


async def send_checkin_to_member(member: discord.Member, guild: discord.Guild, kind: str = "weekly") -> dict:
    """Send a single coach check-in nudge to one member, on demand.

    Mirrors the scheduled job's routing: post in the member's 1-1 ticket
    channel (with @mention + Start Check-in button) when one exists, else fall
    back to a DM. This is a manual CSM override, so it intentionally bypasses
    the weekly-eligibility filters (join window / advanced stage / already
    checked in) — the only thing it still respects is DM-blocked on the DM
    fallback path. Returns a small result dict for the API response.
    """
    if kind == "midweek":
        channel_msg, dm_msg = _MIDWEEK_CHANNEL_MSG, _MIDWEEK_DM_MSG
    else:
        channel_msg, dm_msg = _WEEKLY_CHANNEL_MSG, _WEEKLY_DM_MSG

    candidates = _ticket_channels_for_username(guild, (member.name or "").lower())
    ticket_channel = _pick_ticket_channel_for_confirmation(candidates)
    if ticket_channel is not None:
        try:
            await ticket_channel.send(channel_msg.format(mention=member.mention), view=CheckInButton())
            print(f"[SEND-NOW] {kind} -> #{ticket_channel.name} for {member.display_name}")
            return {"ok": True, "via": "channel", "channel": ticket_channel.name}
        except (discord.Forbidden, discord.HTTPException) as e:
            print(f"[SEND-NOW] channel post failed ({e}) — falling back to DM")

    if is_dm_blocked(member.id):
        return {"ok": False, "error": "dm_blocked"}
    try:
        await member.send(dm_msg, view=CheckInButton())
        print(f"[SEND-NOW] {kind} -> DM for {member.display_name}")
        return {"ok": True, "via": "dm"}
    except discord.Forbidden:
        mark_dm_blocked(member.id)
        return {"ok": False, "error": "dm_blocked"}
    except discord.HTTPException as e:
        return {"ok": False, "error": f"discord_http_{e.status}"}


async def _handle_send_checkin(request: web.Request) -> web.Response:
    """POST /send-checkin — fire a manual 1:1 check-in nudge.

    Auth: shared secret via `X-Api-Secret` header (or `?secret=`).
    Body (JSON): { "username"?: str, "user_id"?: str, "kind"?: "weekly"|"midweek" }
    One of username / user_id is required.
    """
    secret = request.headers.get("X-Api-Secret") or request.query.get("secret") or ""
    if not CHECKIN_API_SECRET or secret != CHECKIN_API_SECRET:
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)

    try:
        body = await request.json()
    except Exception:
        body = {}
    username = str(body.get("username") or "").strip() or None
    user_id = str(body.get("user_id") or "").strip() or None
    kind = (str(body.get("kind") or "weekly").lower())
    if kind not in ("weekly", "midweek"):
        kind = "weekly"
    if not username and not user_id:
        return web.json_response({"ok": False, "error": "username_or_user_id_required"}, status=400)

    guild, member = _find_guild_member(user_id=user_id, username=username)
    if member is None:
        return web.json_response({"ok": False, "error": "member_not_found"}, status=404)

    result = await send_checkin_to_member(member, guild, kind=kind)
    result["member"] = {
        "id": str(member.id),
        "username": member.name,
        "display_name": member.display_name,
    }
    return web.json_response(result, status=200 if result.get("ok") else 502)


async def _handle_api_health(request: web.Request) -> web.Response:
    return web.json_response({"ok": True, "bot": str(client.user) if client.user else None})


_api_started = False


async def start_api_server() -> None:
    """Run the aiohttp endpoint alongside the Discord client."""
    app = web.Application()
    app.router.add_get("/healthz", _handle_api_health)
    app.router.add_post("/send-checkin", _handle_send_checkin)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", CHECKIN_API_PORT)
    await site.start()
    print(f"[API] send-checkin endpoint listening on :{CHECKIN_API_PORT}")


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
    global _synced, _api_started

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
        if not reminder_dispatcher.is_running():
            reminder_dispatcher.start()
        if not check_pending_members.is_running():
            check_pending_members.start()
        if not scan_new_accelerate_members.is_running():
            scan_new_accelerate_members.start()
        if not monthly_export.is_running():
            monthly_export.start()
        asyncio.create_task(_prefetch_weekly_hours_field())
        # Start the manual "send check-in now" HTTP endpoint
        if CHECKIN_API_SECRET and not _api_started:
            _api_started = True
            asyncio.create_task(start_api_server())

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
