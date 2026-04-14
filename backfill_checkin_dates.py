"""
Backfill 'Last Weekly Check-in Date' on Member Database from actual
bot check-in submissions in the Check-in List.

For each member, finds their most recent check-in task (matched via
uid: tag or member name) and uses that task's creation date.

Run once:  python3 backfill_checkin_dates.py
Dry-run:   python3 backfill_checkin_dates.py --dry-run
"""
import os
import sys
from datetime import datetime
import requests
from dotenv import load_dotenv

load_dotenv()

CLICKUP_TOKEN = os.getenv("CLICKUP_TOKEN")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")  # Check-in List
CLICKUP_MEMBER_DB_LIST_ID = "901516122313"

# Member Database field IDs
CU_FIELD_DISCORD_USERNAME = "1aad9b55-223b-40f9-96e6-9388386b5ed2"
CU_FIELD_LAST_CHECKIN_DATE = "b504e08a-086f-402b-a76f-f5b158896b4c"

# Check-in List field IDs
CI_FIELD_MEMBER = "7a6a1a07-2e70-44ad-bb93-5e807ea7035c"


def main():
    if not CLICKUP_TOKEN:
        print("ERROR: CLICKUP_TOKEN not set. Check your .env file.")
        sys.exit(1)
    if not CLICKUP_LIST_ID:
        print("ERROR: CLICKUP_LIST_ID not set. Check your .env file.")
        sys.exit(1)

    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN — no changes will be made ===\n")

    headers = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}

    # --- Step 1: Fetch all check-in tasks and find latest per user ---
    print("Fetching check-in tasks...")
    checkins = []
    page = 0
    while True:
        resp = requests.get(
            f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task",
            params={"include_closed": "true", "page": page},
            headers=headers,
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"ERROR: Failed to fetch check-ins (page {page}): {resp.status_code}")
            sys.exit(1)
        batch = resp.json().get("tasks", [])
        if not batch:
            break
        checkins.extend(batch)
        page += 1

    print(f"  Found {len(checkins)} check-in tasks")

    # Build map: member display name (lowered) → latest check-in timestamp (ms)
    # Also try uid: tag → latest timestamp
    latest_by_name: dict[str, int] = {}
    latest_by_uid: dict[str, int] = {}

    for task in checkins:
        created_ms = int(task.get("date_created", 0))
        if not created_ms:
            continue

        # Extract member name from custom field
        member_name = None
        for cf in task.get("custom_fields", []):
            if cf.get("id") == CI_FIELD_MEMBER:
                member_name = (cf.get("value") or "").strip()
        if not member_name:
            # Fallback: parse from task name "Check-in — DisplayName — Date"
            name = task.get("name", "")
            parts = name.split("—")
            if len(parts) >= 2:
                member_name = parts[1].strip()

        if member_name:
            key = member_name.lower()
            if created_ms > latest_by_name.get(key, 0):
                latest_by_name[key] = created_ms

        # Also track by uid: tag
        for tag in task.get("tags", []):
            tag_name = tag.get("name", "")
            if tag_name.startswith("uid:"):
                uid = tag_name[4:]
                if created_ms > latest_by_uid.get(uid, 0):
                    latest_by_uid[uid] = created_ms

    print(f"  Unique members by name: {len(latest_by_name)}")
    print(f"  Unique members by uid:  {len(latest_by_uid)}\n")

    # --- Step 2: Fetch all members from Member Database ---
    print("Fetching members from Member Database...")
    members = []
    page = 0
    while True:
        resp = requests.get(
            f"https://api.clickup.com/api/v2/list/{CLICKUP_MEMBER_DB_LIST_ID}/task",
            params={"include_closed": "true", "subtasks": "true", "page": page},
            headers=headers,
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"ERROR: Failed to fetch members (page {page}): {resp.status_code}")
            sys.exit(1)
        batch = resp.json().get("tasks", [])
        if not batch:
            break
        members.extend(batch)
        page += 1

    print(f"  Found {len(members)} members\n")

    # --- Step 3: Match and backfill ---
    updated = 0
    already_set = 0
    no_checkin_found = 0
    errors = 0

    for member in members:
        name = member.get("name", "???")
        task_id = member["id"]

        # Check if Last Weekly Check-in Date is already set
        current_checkin_date = None
        for cf in member.get("custom_fields", []):
            if cf["id"] == CU_FIELD_LAST_CHECKIN_DATE:
                current_checkin_date = cf.get("value")

        if current_checkin_date:
            already_set += 1
            continue

        # Try to find their latest check-in by display name match
        name_lower = name.lower()
        latest_ms = latest_by_name.get(name_lower)

        # If no match by name, no uid-based match possible from member DB
        # (we don't have Discord user ID on the member DB task itself)
        if not latest_ms:
            no_checkin_found += 1
            continue

        if dry_run:
            dt = datetime.fromtimestamp(latest_ms / 1000)
            print(f"  WOULD UPDATE: {name} → {dt.strftime('%b %d, %Y')}")
            updated += 1
            continue

        r = requests.post(
            f"https://api.clickup.com/api/v2/task/{task_id}/field/{CU_FIELD_LAST_CHECKIN_DATE}",
            json={"value": latest_ms},
            headers=headers,
            timeout=10,
        )
        if r.status_code == 200:
            dt = datetime.fromtimestamp(latest_ms / 1000)
            print(f"  OK: {name} → {dt.strftime('%b %d, %Y')}")
            updated += 1
        else:
            print(f"  ERROR: {name} — {r.status_code} {r.text}")
            errors += 1

    label = "DRY RUN SUMMARY" if dry_run else "SUMMARY"
    print(f"\n=== {label} ===")
    print(f"  Updated:          {updated}")
    print(f"  Already set:      {already_set}")
    print(f"  No check-in found:{no_checkin_found}")
    print(f"  Errors:           {errors}")


if __name__ == "__main__":
    main()
