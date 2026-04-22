#!/usr/bin/env python3
import os
import re
from pathlib import Path

import requests
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")
TOKEN = (os.getenv("CLICKUP_TOKEN") or "").strip()
LIST_ID = (os.getenv("CLICKUP_LIST_ID") or "").strip()
if not TOKEN or not LIST_ID:
    raise SystemExit("Set CLICKUP_TOKEN and CLICKUP_LIST_ID in .env")
HEADERS = {"Authorization": TOKEN}

tasks = []
page = 0
while True:
    resp = requests.get(
        f"https://api.clickup.com/api/v2/list/{LIST_ID}/task",
        params={"include_closed": "true", "subtasks": "true", "page": page},
        headers=HEADERS, timeout=15
    )
    batch = resp.json().get("tasks", [])
    if not batch:
        break
    tasks.extend(batch)
    page += 1

missing = 0
for t in tasks:
    desc = t.get("description", "") or ""
    if "**Full Name:**" not in desc:
        missing += 1
        name = t.get("name", "")
        dm = re.search(r"\*\*Discord Username:\*\*\s*(\S+)", desc)
        disc = dm.group(1) if dm else "NONE"
        print(f"  MISSING: {name} | discord={disc}")

print(f"\n{missing} of {len(tasks)} tasks still missing Full Name")
