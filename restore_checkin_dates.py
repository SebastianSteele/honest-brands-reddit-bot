"""
Restore 'Last Weekly Check-in Date' for members whose Last Activity
is 'Weekly Check-in' by copying their Last Activity Date.

The bot sets both fields to the same timestamp during check-in, so
Last Activity Date IS the correct Last Weekly Check-in Date for
members who actually checked in.

Run once:  python3 restore_checkin_dates.py
Dry-run:   python3 restore_checkin_dates.py --dry-run
"""
import os
import sys
import time
from datetime import datetime
import requests
from dotenv import load_dotenv

load_dotenv()

CLICKUP_TOKEN = os.getenv("CLICKUP_TOKEN")
CLICKUP_MEMBER_DB_LIST_ID = "901516122313"

# Member Database field IDs
CU_FIELD_LAST_ACTIVITY = "245ff4b2-fbb0-446c-b398-5e2a75f57d21"
CU_FIELD_LAST_ACTIVITY_DATE = "7d31a36c-eccc-43e0-8311-861d82202850"
CU_FIELD_LAST_CHECKIN_DATE = "b504e08a-086f-402b-a76f-f5b158896b4c"


def main():
    if not CLICKUP_TOKEN:
        print("ERROR: CLICKUP_TOKEN not set.")
        sys.exit(1)

    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN — no changes will be made ===\n")

    headers = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}

    # Fetch all members
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

    restored = 0
    already_set = 0
    not_checkin = 0
    no_activity_date = 0
    errors = 0

    for member in members:
        name = member.get("name", "???")
        task_id = member["id"]

        last_activity = None
        last_activity_date = None
        last_checkin_date = None

        for cf in member.get("custom_fields", []):
            fid = cf.get("id")
            if fid == CU_FIELD_LAST_ACTIVITY:
                last_activity = (cf.get("value") or "").strip()
            elif fid == CU_FIELD_LAST_ACTIVITY_DATE:
                last_activity_date = cf.get("value")
            elif fid == CU_FIELD_LAST_CHECKIN_DATE:
                last_checkin_date = cf.get("value")

        # Only restore for members whose last activity was a check-in
        if last_activity != "Weekly Check-in":
            not_checkin += 1
            continue

        # Already has a check-in date set — skip
        if last_checkin_date:
            already_set += 1
            continue

        # No activity date to copy from
        if not last_activity_date:
            no_activity_date += 1
            print(f"  SKIP: {name} — Last Activity is 'Weekly Check-in' but no Last Activity Date")
            continue

        if dry_run:
            dt = datetime.fromtimestamp(int(last_activity_date) / 1000)
            print(f"  WOULD RESTORE: {name} → {dt.strftime('%b %d, %Y')}")
            restored += 1
            continue

        r = requests.post(
            f"https://api.clickup.com/api/v2/task/{task_id}/field/{CU_FIELD_LAST_CHECKIN_DATE}",
            json={"value": int(last_activity_date)},
            headers=headers,
            timeout=10,
        )
        time.sleep(0.5)
        if r.status_code == 200:
            dt = datetime.fromtimestamp(int(last_activity_date) / 1000)
            print(f"  RESTORED: {name} → {dt.strftime('%b %d, %Y')}")
            restored += 1
        else:
            print(f"  ERROR: {name} — {r.status_code} {r.text}")
            errors += 1

    label = "DRY RUN SUMMARY" if dry_run else "SUMMARY"
    print(f"\n=== {label} ===")
    print(f"  Restored:           {restored}")
    print(f"  Already set:        {already_set}")
    print(f"  Not a check-in:     {not_checkin}")
    print(f"  No activity date:   {no_activity_date}")
    print(f"  Errors:             {errors}")


if __name__ == "__main__":
    main()
