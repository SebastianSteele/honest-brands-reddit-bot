"""
HonestAI FAQ scraper — daily task module for the HonestBrands Discord bot.

Runs inside the existing discord.py bot process. Once a day, it:
  1. Fetches new messages from #ask-honestai since the last-shipped
     watermark (stored in faq_scraper_state.json).
  2. For each question, picks a "best answer" from the thread (if any)
     or from the next 30 sibling messages (inline replies).
  3. POSTs the payload as JSON to the HonestAI FAQ Apps Script Web App
     endpoint, authenticating with a shared secret.

Why this lives in the bot: Discord blocks Google Apps Script's outbound
IP range for guild/channel endpoints (confirmed 2026-04-30 via a direct
diagnostic). The bot's Heroku IPs are not blocked, so the scrape has
to run here.

Integration:

    import faq_scraper
    faq_scraper.register(client)

in bot.py's on_ready (after the other scheduled tasks start). Nothing
else in bot.py needs to change.

Environment variables (added to the bot's .env):

    HAI_WEBHOOK_URL        Apps Script Web App /exec URL
    HAI_WEBHOOK_SECRET     shared secret, same as Script Property in GAS
    HAI_CHANNEL_ID         (optional) default 1402210105960693810
    HAI_SCRAPE_HOUR_LOCAL  (optional) hour-of-day in HAI_SCRAPE_TZ. Default 7
                           (= 7am every morning, DST-aware).
    HAI_SCRAPE_TZ          (optional) IANA tz name. Default America/New_York.
    HAI_SCRAPE_HOUR_UTC    (optional, legacy) raw UTC hour. Only used if
                           HAI_SCRAPE_HOUR_LOCAL is unset. Doesn't follow DST.
    HAI_MAX_MESSAGES       (optional) cap per run, default 2000
"""

from __future__ import annotations

import json
import os
import sys
from datetime import time as dtime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
import discord
from discord.ext import tasks


# Use print() with a [HAI] prefix for Railway visibility — matches the
# rest of the bot's logging conventions ([SYNC], [CLICKUP], [SCAN], ...).
# Python's `logging` module is configured at WARNING by default in this
# project, which silenced our earlier log.info() calls.
def _log(msg: str) -> None:
    print(f"[HAI] {msg}", flush=True)
    sys.stdout.flush()


DEFAULT_CHANNEL_ID = "1402210105960693810"
DEFAULT_STATE_FILENAME = "faq_scraper_state.json"
DEFAULT_SCRAPE_TZ = "America/New_York"
DEFAULT_SCRAPE_HOUR_LOCAL = 7


# ---------- config ----------

def _cfg() -> dict[str, Any]:
    # Scheduling: prefer HAI_SCRAPE_HOUR_LOCAL + HAI_SCRAPE_TZ (DST-aware,
    # matches the rest of bot.py). HAI_SCRAPE_HOUR_UTC is kept as a legacy
    # escape hatch — used only when the new var is unset and the user has
    # explicitly opted into the old UTC behaviour.
    hour_local_raw = os.getenv("HAI_SCRAPE_HOUR_LOCAL", "").strip()
    hour_utc_raw = os.getenv("HAI_SCRAPE_HOUR_UTC", "").strip()
    if hour_local_raw:
        scrape_tz = (os.getenv("HAI_SCRAPE_TZ", "").strip() or DEFAULT_SCRAPE_TZ)
        scrape_hour_local = int(hour_local_raw)
    elif hour_utc_raw:
        scrape_tz = "UTC"
        scrape_hour_local = int(hour_utc_raw)
    else:
        scrape_tz = (os.getenv("HAI_SCRAPE_TZ", "").strip() or DEFAULT_SCRAPE_TZ)
        scrape_hour_local = DEFAULT_SCRAPE_HOUR_LOCAL

    return {
        "channel_id": int(os.getenv("HAI_CHANNEL_ID", DEFAULT_CHANNEL_ID)),
        "webhook_url": os.getenv("HAI_WEBHOOK_URL", "").strip(),
        "webhook_secret": os.getenv("HAI_WEBHOOK_SECRET", "").strip(),
        "scrape_tz": scrape_tz,
        "scrape_hour_local": scrape_hour_local,
        # Default capped at 500 messages per run. All-time backfill
        # completes over multiple days' daily runs — this keeps any
        # single run fast enough to live inside Discord's rate-limit
        # budgets without thrashing.
        "max_messages": int(os.getenv("HAI_MAX_MESSAGES", "500")),
        # Sibling-scan reads the next 30 messages after every non-threaded
        # question to find inline answers. It's accurate but blows the
        # API-call budget up by ~30x. Off by default; turn on only once
        # the initial backfill has completed.
        "sibling_scan": os.getenv("HAI_SIBLING_SCAN", "false").lower() in ("1", "true", "yes", "on"),
        "state_path": Path(os.getenv("HAI_STATE_PATH", DEFAULT_STATE_FILENAME)),
    }


# ---------- state ----------

def _load_state(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        _log(f"state load failed ({e}) — starting fresh")
    return {}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    try:
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        _log(f"state save failed: {e}")


# ---------- message shaping ----------

def _display_name(user: discord.abc.User) -> str:
    return getattr(user, "display_name", None) or getattr(user, "name", "") or ""


def _reaction_count(m: discord.Message) -> int:
    return sum(r.count for r in m.reactions) if m.reactions else 0


def _reply_count(m: discord.Message) -> int:
    thread = getattr(m, "thread", None)
    if thread is not None and getattr(thread, "message_count", None) is not None:
        return int(thread.message_count)
    return 0


async def _extract_best_answer(
    msg: discord.Message, enable_sibling_scan: bool
) -> dict[str, Any] | None:
    """
    Pick the "best answer" for a question:
      1. If the message has a thread, scan first 50 thread messages and
         pick the non-author / non-bot reply with highest reaction count.
      2. Else (and sibling-scan is enabled), scan the next 30 sibling
         messages; prefer a direct reply with reactions; else first
         non-author reply with >= 20 chars.

    Sibling-scan is expensive (30x API calls per question) so it's
    gated behind HAI_SIBLING_SCAN to keep first-backfill runs within
    Discord's rate-limit budget.
    """
    asker_id = msg.author.id
    thread = getattr(msg, "thread", None)

    try:
        if thread is not None:
            candidates: list[discord.Message] = []
            async for t in thread.history(limit=50, oldest_first=True):
                if t.author.bot or t.author.id == asker_id:
                    continue
                if not (t.content or "").strip():
                    continue
                candidates.append(t)
            # Fall back to asker follow-ups only if nobody else posted
            if not candidates:
                async for t in thread.history(limit=50, oldest_first=True):
                    if not t.author.bot and (t.content or "").strip():
                        candidates.append(t)
            if candidates:
                best = max(
                    candidates,
                    key=lambda m: (_reaction_count(m), len(m.content or "")),
                )
                return {
                    "content": (best.content or "")[:4000],
                    "author_name": _display_name(best.author),
                    "created_at": int(best.created_at.timestamp() * 1000),
                }

        # Sibling scan — opt-in only (30 API calls per question).
        if enable_sibling_scan:
            best: discord.Message | None = None
            best_score = -1
            async for m in msg.channel.history(
                limit=30, after=discord.Object(id=msg.id), oldest_first=True
            ):
                if m.author.bot or m.author.id == asker_id:
                    continue
                content = (m.content or "").strip()
                if not content:
                    continue
                score = _reaction_count(m)
                ref = getattr(m, "reference", None)
                if ref is not None and getattr(ref, "message_id", None) == msg.id:
                    score += 100
                if score > best_score and len(content) >= 20:
                    best = m
                    best_score = score
            if best is not None:
                return {
                    "content": (best.content or "")[:4000],
                    "author_name": _display_name(best.author),
                    "created_at": int(best.created_at.timestamp() * 1000),
                }
    except discord.Forbidden:
        _log(f"permissions denied reading message {msg.id}")
    except Exception as e:
        _log(f"best-answer extraction failed for {msg.id}: {e}")

    return None


# ---------- shipping ----------

async def _post_batch(
    session: aiohttp.ClientSession,
    url: str,
    secret: str,
    channel_id: int,
    guild_id: str,
    messages: list[dict[str, Any]],
) -> tuple[bool, str]:
    payload = {
        "secret": secret,
        "channel_id": str(channel_id),
        "guild_id": str(guild_id) if guild_id else "",
        "messages": messages,
    }
    try:
        async with session.post(
            url,
            json=payload,
            headers={"X-HAI-Secret": secret},
            timeout=aiohttp.ClientTimeout(total=60),
            allow_redirects=True,    # Apps Script /exec → googleusercontent.com
        ) as resp:
            text = await resp.text()
            if resp.status != 200:
                return False, f"HTTP {resp.status}: {text[:300]}"
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                return False, f"non-JSON response: {text[:300]}"
            if not data.get("ok"):
                return False, f"apps script error: {data.get('error', text[:300])}"
            return True, f"ok received={data.get('received')} upserted={data.get('upserted')}"
    except Exception as e:
        return False, f"exception: {e}"


# ---------- main run ----------

async def run_once(client: discord.Client) -> dict[str, Any]:
    cfg = _cfg()
    if not cfg["webhook_url"] or not cfg["webhook_secret"]:
        msg = "HAI_WEBHOOK_URL / HAI_WEBHOOK_SECRET not configured — skipping"
        _log(msg)
        return {"ok": False, "error": msg}

    channel = client.get_channel(cfg["channel_id"])
    if channel is None:
        try:
            channel = await client.fetch_channel(cfg["channel_id"])
        except Exception as e:
            msg = f"cannot resolve channel {cfg['channel_id']}: {e}"
            _log(msg)
            return {"ok": False, "error": msg}

    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        msg = (
            f"channel {cfg['channel_id']} is type "
            f"{type(channel).__name__} — expected TextChannel/Thread"
        )
        _log(msg)
        return {"ok": False, "error": msg}

    state = _load_state(cfg["state_path"])
    after_id: str | None = state.get("last_message_id")
    guild_id = str(channel.guild.id) if getattr(channel, "guild", None) else ""

    _log(
        f"scrape start: channel=#{channel.name} ({cfg['channel_id']}) "
        f"guild={guild_id} after={after_id} max={cfg['max_messages']} "
        f"sibling_scan={cfg['sibling_scan']}"
    )

    collected: list[dict[str, Any]] = []
    newest_id: str | None = after_id
    after_obj = discord.Object(id=int(after_id)) if after_id else None
    count = 0
    errored = 0
    progress_every = max(50, cfg["max_messages"] // 10)
    async for m in channel.history(
        limit=None, after=after_obj, oldest_first=True
    ):
        newest_id = str(m.id)
        count += 1
        if count > cfg["max_messages"]:
            break
        if count % progress_every == 0:
            _log(f"  walked {count} messages, collected {len(collected)} questions so far...")
        if m.author.bot:
            continue
        content = (m.content or "").strip()
        if not content:
            continue

        try:
            answer = await _extract_best_answer(m, cfg["sibling_scan"])
            thread_obj = getattr(m, "thread", None)

            collected.append({
                "id": str(m.id),
                "thread_id": str(thread_obj.id) if thread_obj is not None else None,
                "author_id": str(m.author.id),
                "author_name": _display_name(m.author),
                "content": content[:4000],
                "created_at": int(m.created_at.timestamp() * 1000),
                "reply_count": _reply_count(m),
                "reaction_count": _reaction_count(m),
                "answer": answer,
            })
        except Exception as ex:
            # Log + skip — one bad message should never abort the whole
            # scrape. Common culprits: forum-starter messages with odd
            # shapes, deleted referenced_message, transient API quirks.
            errored += 1
            _log(f"  failed to process message {getattr(m, 'id', '?')}: {ex}")
            continue

    if errored:
        _log(f"skipped {errored} problematic messages")

    _log(f"collected {len(collected)} new questions from {count} walked messages")

    if not collected:
        if newest_id and newest_id != after_id:
            state["last_message_id"] = newest_id
            _save_state(cfg["state_path"], state)
        return {"ok": True, "scanned": 0, "shipped": 0, "watermark": newest_id}

    CHUNK = 200
    shipped = 0
    async with aiohttp.ClientSession() as session:
        for i in range(0, len(collected), CHUNK):
            chunk = collected[i:i + CHUNK]
            ok, info = await _post_batch(
                session, cfg["webhook_url"], cfg["webhook_secret"],
                cfg["channel_id"], guild_id, chunk,
            )
            if not ok:
                _log(f"POST failed chunk {i}-{i + len(chunk) - 1}: {info}")
                # Rewind watermark so next run retries from this chunk
                state["last_message_id"] = chunk[0]["id"]
                _save_state(cfg["state_path"], state)
                return {"ok": False, "shipped": shipped, "error": info}
            shipped += len(chunk)
            _log(f"shipped chunk {i}-{i + len(chunk) - 1} ({info})")

    if newest_id:
        state["last_message_id"] = newest_id
        _save_state(cfg["state_path"], state)

    _log(f"scrape done: scanned={len(collected)} shipped={shipped} watermark={newest_id}")
    return {"ok": True, "scanned": len(collected), "shipped": shipped, "watermark": newest_id}


# ---------- daily task registration ----------

_registered_client: discord.Client | None = None
_daily_task = None


def _build_scrape_time(cfg: dict[str, Any]) -> dtime:
    """Construct the tz-aware time-of-day discord.ext.tasks.loop(time=...)
    fires on. Using a tz-aware time means discord.py converts to UTC
    internally each day and DST transitions are handled for free —
    "7am America/New_York" stays 7am local across the spring-forward /
    fall-back boundary. Mirrors the pattern used by weekly_reminder /
    midweek_reminder in bot.py."""
    try:
        tz = ZoneInfo(cfg["scrape_tz"])
    except Exception:
        _log(f"unknown timezone {cfg['scrape_tz']!r} — falling back to UTC")
        tz = timezone.utc
    return dtime(
        hour=int(cfg["scrape_hour_local"]),
        minute=0, second=0,
        tzinfo=tz,
    )


def register(client: discord.Client) -> None:
    """Mount the daily FAQ scrape task on the given client. Safe to call
    repeatedly (subsequent calls are no-ops)."""
    global _registered_client, _daily_task
    if _registered_client is client:
        return
    _registered_client = client

    cfg = _cfg()
    scrape_time = _build_scrape_time(cfg)
    pretty_time = f"{scrape_time.hour:02d}:00 {cfg['scrape_tz']}"

    @tasks.loop(time=scrape_time)
    async def _daily():
        try:
            await run_once(client)
        except Exception as e:
            import traceback
            _log(f"daily scrape errored: {e}\n{traceback.format_exc()}")

    @_daily.before_loop
    async def _before():
        await client.wait_until_ready()
        _log(f"daily scrape armed: fires every day at {pretty_time}")

    _daily.start()
    _daily_task = _daily
    _log(f"daily scrape task registered (time={pretty_time})")
