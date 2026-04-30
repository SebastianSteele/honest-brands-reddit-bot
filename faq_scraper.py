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

    HAI_WEBHOOK_URL       Apps Script Web App /exec URL
    HAI_WEBHOOK_SECRET    shared secret, same as Script Property in GAS
    HAI_CHANNEL_ID        (optional) default 1402210105960693810
    HAI_SCRAPE_HOUR_UTC   (optional) default 12 = 7am EDT / 8am EST
    HAI_MAX_MESSAGES      (optional) cap per run, default 2000
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import aiohttp
import discord
from discord.ext import tasks

log = logging.getLogger("hai.faq_scraper")

DEFAULT_CHANNEL_ID = "1402210105960693810"
DEFAULT_STATE_FILENAME = "faq_scraper_state.json"


# ---------- config ----------

def _cfg() -> dict[str, Any]:
    return {
        "channel_id": int(os.getenv("HAI_CHANNEL_ID", DEFAULT_CHANNEL_ID)),
        "webhook_url": os.getenv("HAI_WEBHOOK_URL", "").strip(),
        "webhook_secret": os.getenv("HAI_WEBHOOK_SECRET", "").strip(),
        "scrape_hour_utc": int(os.getenv("HAI_SCRAPE_HOUR_UTC", "12")),
        "max_messages": int(os.getenv("HAI_MAX_MESSAGES", "2000")),
        "state_path": Path(os.getenv("HAI_STATE_PATH", DEFAULT_STATE_FILENAME)),
    }


# ---------- state ----------

def _load_state(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning("state load failed (%s) — starting fresh", e)
    return {}


def _save_state(path: Path, state: dict[str, Any]) -> None:
    try:
        path.write_text(json.dumps(state, indent=2), encoding="utf-8")
    except Exception as e:
        log.error("state save failed: %s", e)


# ---------- message shaping ----------

def _display_name(user: discord.abc.User) -> str:
    return getattr(user, "display_name", None) or getattr(user, "name", "") or ""


def _reaction_count(m: discord.Message) -> int:
    return sum(r.count for r in m.reactions) if m.reactions else 0


def _reply_count(m: discord.Message) -> int:
    if m.thread is not None and m.thread.message_count is not None:
        return int(m.thread.message_count)
    return 0


async def _extract_best_answer(msg: discord.Message) -> dict[str, Any] | None:
    """
    Pick the "best answer" for a question:
      1. If the message has a thread, scan first 50 thread messages and
         pick the non-author / non-bot reply with highest reaction count.
      2. Else scan the next 30 sibling messages; prefer a direct reply
         to this message with reactions; else first non-author reply
         with >= 20 chars.
    """
    asker_id = msg.author.id

    try:
        if msg.thread is not None:
            candidates: list[discord.Message] = []
            async for t in msg.thread.history(limit=50, oldest_first=True):
                if t.author.bot or t.author.id == asker_id:
                    continue
                if not (t.content or "").strip():
                    continue
                candidates.append(t)
            # Fall back to asker follow-ups only if nobody else posted
            if not candidates:
                async for t in msg.thread.history(limit=50, oldest_first=True):
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

        # Sibling scan
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
        log.warning("permissions denied reading message %s", msg.id)
    except Exception as e:
        log.warning("best-answer extraction failed for %s: %s", msg.id, e)

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
        log.warning(msg)
        return {"ok": False, "error": msg}

    channel = client.get_channel(cfg["channel_id"])
    if channel is None:
        try:
            channel = await client.fetch_channel(cfg["channel_id"])
        except Exception as e:
            msg = f"cannot resolve channel {cfg['channel_id']}: {e}"
            log.error(msg)
            return {"ok": False, "error": msg}

    if not isinstance(channel, (discord.TextChannel, discord.Thread)):
        msg = (
            f"channel {cfg['channel_id']} is type "
            f"{type(channel).__name__} — expected TextChannel/Thread"
        )
        log.error(msg)
        return {"ok": False, "error": msg}

    state = _load_state(cfg["state_path"])
    after_id: str | None = state.get("last_message_id")
    guild_id = str(channel.guild.id) if getattr(channel, "guild", None) else ""

    log.info(
        "FAQ scrape: channel=#%s (%s) guild=%s after=%s max=%d",
        channel.name, cfg["channel_id"], guild_id, after_id, cfg["max_messages"],
    )

    collected: list[dict[str, Any]] = []
    newest_id: str | None = after_id
    after_obj = discord.Object(id=int(after_id)) if after_id else None
    count = 0
    async for m in channel.history(
        limit=None, after=after_obj, oldest_first=True
    ):
        newest_id = str(m.id)
        count += 1
        if count > cfg["max_messages"]:
            break
        if m.author.bot:
            continue
        content = (m.content or "").strip()
        if not content:
            continue

        answer = await _extract_best_answer(m)

        collected.append({
            "id": str(m.id),
            "thread_id": str(m.thread.id) if m.thread is not None else None,
            "author_id": str(m.author.id),
            "author_name": _display_name(m.author),
            "content": content[:4000],
            "created_at": int(m.created_at.timestamp() * 1000),
            "reply_count": _reply_count(m),
            "reaction_count": _reaction_count(m),
            "answer": answer,
        })

    log.info("collected %d new questions", len(collected))

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
                log.error("FAQ POST failed chunk %d-%d: %s",
                          i, i + len(chunk) - 1, info)
                # Rewind watermark so next run retries from this chunk
                state["last_message_id"] = chunk[0]["id"]
                _save_state(cfg["state_path"], state)
                return {"ok": False, "shipped": shipped, "error": info}
            shipped += len(chunk)
            log.info("shipped chunk %d-%d (%s)", i, i + len(chunk) - 1, info)

    if newest_id:
        state["last_message_id"] = newest_id
        _save_state(cfg["state_path"], state)

    return {"ok": True, "scanned": len(collected), "shipped": shipped, "watermark": newest_id}


# ---------- daily task registration ----------

_registered_client: discord.Client | None = None


def register(client: discord.Client) -> None:
    """Mount the daily FAQ scrape task on the given client. Safe to call
    repeatedly (subsequent calls are no-ops)."""
    global _registered_client
    if _registered_client is client:
        return
    _registered_client = client

    @tasks.loop(hours=24)
    async def _daily():
        try:
            await run_once(client)
        except Exception:
            log.exception("FAQ daily scrape errored")

    @_daily.before_loop
    async def _before():
        await client.wait_until_ready()
        cfg = _cfg()
        now = datetime.now(timezone.utc)
        target = now.replace(
            hour=cfg["scrape_hour_utc"], minute=0, second=0, microsecond=0,
        )
        if target <= now:
            target = target + timedelta(days=1)
        sleep_s = max(60, int((target - now).total_seconds()))
        log.info(
            "FAQ scraper: sleeping %ds until first run at %s UTC",
            sleep_s, target.isoformat(),
        )
        await asyncio.sleep(sleep_s)

    _daily.start()
    log.info("FAQ daily scrape task registered (runs every 24h)")
