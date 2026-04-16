#!/usr/bin/env python3
import requests, re

TOKEN = "pk_106691179_ZE5E83KN5B2J3Q0KZ2Z66RCOOKR9HD3Z"
LIST_ID = "901522659802"
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
