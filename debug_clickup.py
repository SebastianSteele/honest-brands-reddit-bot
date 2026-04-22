#!/usr/bin/env python3
"""
Debug ClickUp check-in list + weekly hours field (no Discord, no bot startup).

Usage:
  python3 debug_clickup.py
"""
from __future__ import annotations

import asyncio
import os
import sys
from pathlib import Path

import aiohttp
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

CANONICAL = {
    "weekly number of hours",
    "hours spent this week",
    "weekly hours",
    "hours this week",
    "weekly hours (band)",
}


def _pick_weekly_hours_debug(fields: list, display_name: str, env_exact: str) -> dict | None:
    if env_exact:
        for f in fields:
            if (f.get("name") or "").strip() == env_exact:
                return f
    for f in fields:
        n = (f.get("name") or "").strip().lower()
        if n in CANONICAL:
            ty = f.get("type") or ""
            if ty in ("number", "drop_down", "short_text", "text"):
                return f
    want = display_name.strip().lower()
    for f in fields:
        if (f.get("name") or "").strip().lower() == want:
            return f
    candidates = []
    for f in fields:
        ty = f.get("type") or ""
        if ty not in ("number", "drop_down", "short_text", "text"):
            continue
        n = (f.get("name") or "").lower()
        if n == "week":
            continue
        if "hour" not in n:
            continue
        if any(k in n for k in ("week", "band", "spent", "number")):
            candidates.append(f)
    if len(candidates) == 1:
        return candidates[0]
    if len(candidates) > 1:
        return candidates[0]
    return None


async def main() -> int:
    token = (os.getenv("CLICKUP_TOKEN") or "").strip()
    list_id = (os.getenv("CLICKUP_LIST_ID") or "").strip()
    display = (os.getenv("CLICKUP_WEEKLY_HOURS_FIELD_DISPLAY_NAME") or "Weekly Number of Hours").strip()
    env_exact = (os.getenv("CLICKUP_WEEKLY_HOURS_FIELD_NAME") or "").strip()
    auto = os.getenv("CLICKUP_AUTO_CREATE_WEEKLY_HOURS_FIELD", "true").lower() not in (
        "0", "false", "no", "off",
    )

    if not token or not list_id:
        print("ERROR: Set CLICKUP_TOKEN and CLICKUP_LIST_ID in .env")
        return 1

    headers = {"Authorization": token, "Content-Type": "application/json"}
    base = "https://api.clickup.com/api/v2"

    async with aiohttp.ClientSession() as session:
        async with session.get(f"{base}/list/{list_id}", headers=headers, timeout=aiohttp.ClientTimeout(20)) as r:
            status = r.status
            try:
                li = await r.json()
            except Exception:
                li = {"_raw": (await r.text())[:500]}
            print(f"GET /list/{{id}}  HTTP {status}")
            if status != 200:
                print(li)
                return 1
            print(f"  name: {li.get('name')!r}")
            print(f"  id:   {li.get('id')}")

        async with session.get(
            f"{base}/list/{list_id}/field",
            headers=headers,
            timeout=aiohttp.ClientTimeout(20),
        ) as r:
            status = r.status
            data = await r.json()
            print(f"\nGET /list/{{id}}/field  HTTP {status}")
            if status != 200:
                print(data)
                return 1

        fields = data.get("fields") or []
        print(f"  fields: {len(fields)}")
        for f in sorted(fields, key=lambda x: (x.get("name") or "").lower()):
            nm = f.get("name") or ""
            fid = f.get("id") or ""
            ty = f.get("type") or ""
            print(f"    {nm!r}  type={ty}  id={fid}")

        picked = _pick_weekly_hours_debug(fields, display, env_exact)
        print(f"\nResolved weekly-hours field (same rules as bot):")
        if picked:
            print(f"  name: {picked.get('name')!r}")
            print(f"  type: {picked.get('type')}")
            print(f"  id:   {picked.get('id')}")
        else:
            print("  (none)")
            if auto:
                print(f"  CLICKUP_AUTO_CREATE_WEEKLY_HOURS_FIELD is on — bot would POST field {display!r}")
            else:
                print("  CLICKUP_AUTO_CREATE_WEEKLY_HOURS_FIELD is off — bot would not auto-create")

    print("\nOK — debug finished.")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
