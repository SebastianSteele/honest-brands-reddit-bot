"""
Backfill check-in tasks with Discord username, Program, and Coach info.

For each check-in task:
1. Extracts the member name from the CI_FIELD_MEMBER custom field
2. Looks up the member in the Member Database
3. Adds program and coach tags
4. Updates the description with Discord username, program, and coach
5. Replaces old uid: tag with Discord username tag

Run:       python3 backfill_checkin_enrichment.py
Dry-run:   python3 backfill_checkin_enrichment.py --dry-run
"""
import os
import re
import sys
import time
import requests
from dotenv import load_dotenv

load_dotenv()

CLICKUP_TOKEN = os.getenv("CLICKUP_TOKEN")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")  # Check-in List
CLICKUP_MEMBER_DB_LIST_ID = "901516122313"

# Member Database field IDs
CU_FIELD_DISCORD_USERNAME = "1aad9b55-223b-40f9-96e6-9388386b5ed2"
CU_FIELD_PROGRAM_NAME = "d44e9584-d751-40fb-9b52-0cb7fb9d80aa"
CU_FIELD_COACH = "3c4c9ce5-07f5-4aa3-a0bf-1dbca6c9efe3"

# Check-in List field IDs
CI_FIELD_MEMBER = "7a6a1a07-2e70-44ad-bb93-5e807ea7035c"

PROGRAM_NAMES = {0: "Core", 1: "Accelerate", 2: "Scale", 3: "Velocity"}

HEADERS = {"Authorization": CLICKUP_TOKEN, "Content-Type": "application/json"}


def fetch_all_tasks(list_id):
    """Fetch all tasks from a ClickUp list (paginated)."""
    tasks = []
    page = 0
    while True:
        resp = requests.get(
            f"https://api.clickup.com/api/v2/list/{list_id}/task",
            params={"include_closed": "true", "subtasks": "true", "page": page},
            headers=HEADERS,
            timeout=15,
        )
        if resp.status_code != 200:
            print(f"  ERROR: Failed to fetch tasks (page {page}): {resp.status_code}")
            return tasks
        batch = resp.json().get("tasks", [])
        if not batch:
            break
        tasks.extend(batch)
        page += 1
    return tasks


def build_member_index(members):
    """Build lookup indexes: by lowered display name, by Discord username,
    and by first name (only stored if unique)."""
    by_name = {}
    by_discord = {}
    by_first_name = {}  # first_name_lower → member (or None if ambiguous)
    for m in members:
        name_lower = m.get("name", "").strip().lower()
        if name_lower:
            by_name[name_lower] = m
            first = name_lower.split()[0] if name_lower else ""
            if first:
                if first in by_first_name:
                    by_first_name[first] = None  # ambiguous
                else:
                    by_first_name[first] = m
        for cf in m.get("custom_fields", []):
            if cf.get("id") == CU_FIELD_DISCORD_USERNAME:
                discord_user = (cf.get("value") or "").strip().lower()
                if discord_user:
                    by_discord[discord_user] = m
    # Remove ambiguous first-name entries
    by_first_name = {k: v for k, v in by_first_name.items() if v is not None}
    return by_name, by_discord, by_first_name


def extract_member_info(member_task):
    """Extract program name, coach names, and Discord username from a member."""
    program = None
    coaches = []
    discord_username = None
    for cf in member_task.get("custom_fields", []):
        fid = cf.get("id")
        if fid == CU_FIELD_PROGRAM_NAME and cf.get("value") is not None:
            try:
                program = PROGRAM_NAMES.get(int(cf["value"]))
            except (ValueError, TypeError):
                pass
        elif fid == CU_FIELD_COACH and cf.get("value"):
            coaches = [u.get("username", "") for u in cf["value"] if u.get("username")]
        elif fid == CU_FIELD_DISCORD_USERNAME:
            discord_username = (cf.get("value") or "").strip()
    return program, coaches, discord_username


def get_checkin_member_name(task):
    """Get the member name from a check-in task's custom field."""
    for cf in task.get("custom_fields", []):
        if cf.get("id") == CI_FIELD_MEMBER:
            return (cf.get("value") or "").strip()
    return None


def update_task_description(task_id, old_desc, discord_username, program, coaches, dry_run):
    """Update the check-in task description with enrichment data."""
    # Replace **Discord ID:** line with **Discord Username:** line
    new_desc = re.sub(
        r'\*\*Discord ID:\*\*\s*\d+',
        f'**Discord Username:** {discord_username}',
        old_desc,
    )

    # If no Discord Username line exists at all, add it after Member line
    if "**Discord Username:**" not in new_desc:
        new_desc = re.sub(
            r'(\*\*Member:\*\*[^\n]*\n)',
            f'\\1**Discord Username:** {discord_username}\n',
            new_desc,
        )

    # Add program and coach before the --- separator if not already present
    extra = []
    if program and "**Program:**" not in new_desc:
        extra.append(f"**Program:** {program}")
    if coaches and "**Coach:**" not in new_desc:
        extra.append(f"**Coach:** {', '.join(coaches)}")

    if extra:
        insert_text = "\n".join(extra)
        if "\n\n---" in new_desc:
            new_desc = new_desc.replace("\n\n---", f"\n{insert_text}\n\n---", 1)
        else:
            # No --- separator, append at end
            new_desc += f"\n\n{insert_text}"

    if new_desc == old_desc:
        return False

    if not dry_run:
        resp = requests.put(
            f"https://api.clickup.com/api/v2/task/{task_id}",
            json={"description": new_desc},
            headers=HEADERS,
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"    WARN: Failed to update description: {resp.status_code}")
            return False
    return True


def add_tag(task_id, tag, dry_run):
    """Add a tag to a task."""
    if not dry_run:
        resp = requests.post(
            f"https://api.clickup.com/api/v2/task/{task_id}/tag/{tag}",
            headers=HEADERS,
            timeout=10,
        )
        if resp.status_code != 200:
            print(f"    WARN: Failed to add tag '{tag}': {resp.status_code}")
            return False
    return True


def remove_tag(task_id, tag, dry_run):
    """Remove a tag from a task."""
    if not dry_run:
        resp = requests.delete(
            f"https://api.clickup.com/api/v2/task/{task_id}/tag/{tag}",
            headers=HEADERS,
            timeout=10,
        )
        # 200 = removed, 400/404 = didn't exist (fine)
    return True


def main():
    if not CLICKUP_TOKEN or not CLICKUP_LIST_ID:
        print("ERROR: CLICKUP_TOKEN and CLICKUP_LIST_ID must be set in .env")
        sys.exit(1)

    dry_run = "--dry-run" in sys.argv
    if dry_run:
        print("=== DRY RUN — no changes will be made ===\n")

    # Step 1: Fetch all members from Member Database
    print("Fetching members from Member Database...")
    members = fetch_all_tasks(CLICKUP_MEMBER_DB_LIST_ID)
    print(f"  Found {len(members)} members")
    by_name, by_discord, by_first_name = build_member_index(members)
    print(f"  Indexed {len(by_name)} by name, {len(by_discord)} by Discord username, "
          f"{len(by_first_name)} unique first names\n")

    # Step 2: Fetch all check-in tasks
    print("Fetching check-in tasks...")
    checkins = fetch_all_tasks(CLICKUP_LIST_ID)
    print(f"  Found {len(checkins)} check-in tasks\n")

    # Step 3: Process each check-in task
    updated = 0
    skipped = 0
    no_match = 0
    errors = 0

    for i, task in enumerate(checkins):
        task_id = task["id"]
        task_name = task.get("name", "")
        member_name = get_checkin_member_name(task)
        tags = [t.get("name", "") for t in task.get("tags", [])]
        desc = task.get("description", "") or ""

        # Try to find the member in the DB using multiple strategies
        member = None
        search_name = member_name or ""

        # Also extract name from task title (Check-in — Name — Date)
        extracted_name = ""
        parts = task_name.split("—")
        if len(parts) >= 2:
            extracted_name = parts[1].strip()

        # Strategy 1: Exact match on member name custom field
        if search_name:
            member = by_name.get(search_name.lower())

        # Strategy 2: Exact match on name from task title
        if not member and extracted_name:
            member = by_name.get(extracted_name.lower())

        # Strategy 3: Display name might BE their Discord username
        if not member and search_name:
            member = by_discord.get(search_name.lower())
        if not member and extracted_name:
            member = by_discord.get(extracted_name.lower())

        # Strategy 4: Display name might be a prefix of Discord username
        #   e.g. "FeeMo" matches "feemo0437", "Charlie_C" matches "charlie_cawood..."
        if not member:
            for candidate_name in [search_name, extracted_name]:
                if not candidate_name:
                    continue
                prefix = candidate_name.lower().replace("_", "").replace(" ", "")
                matches = []
                for dname, m in by_discord.items():
                    cleaned = dname.replace("_", "").replace(" ", "")
                    if cleaned.startswith(prefix) and len(prefix) >= 3:
                        matches.append(m)
                if len(matches) == 1:
                    member = matches[0]
                    break

        # Strategy 5: First-name match (only if unambiguous in the DB)
        if not member and search_name:
            first = search_name.lower().split()[0]
            member = by_first_name.get(first)
        if not member and extracted_name:
            first = extracted_name.lower().split()[0]
            member = by_first_name.get(first)

        if not member:
            no_match += 1
            print(f"  [{i+1}/{len(checkins)}] SKIP (no match): {task_name}")
            continue

        program, coaches, discord_username = extract_member_info(member)

        # Check if already enriched
        already_has_program = any(t == (program or "").lower() for t in tags)
        already_has_coaches = all(c.lower() in tags for c in coaches) if coaches else True
        already_has_discord_tag = discord_username and discord_username.lower() in tags
        desc_ok = ("**Program:**" in desc or not program) and ("**Coach:**" in desc or not coaches)

        if already_has_program and already_has_coaches and already_has_discord_tag and desc_ok:
            skipped += 1
            continue

        print(f"  [{i+1}/{len(checkins)}] Enriching: {task_name}")
        print(f"    Program: {program}, Coach: {', '.join(coaches) if coaches else 'N/A'}, "
              f"Discord: {discord_username or 'N/A'}")

        # Add tags
        if program and not already_has_program:
            add_tag(task_id, program.lower(), dry_run)
        for coach in coaches:
            if coach.lower() not in tags:
                add_tag(task_id, coach.lower(), dry_run)
        if discord_username and not already_has_discord_tag:
            add_tag(task_id, discord_username.lower(), dry_run)

        # Remove old uid: tags
        for tag in tags:
            if tag.startswith("uid:"):
                remove_tag(task_id, tag, dry_run)

        # Update description
        if discord_username or program or coaches:
            update_task_description(
                task_id, desc,
                discord_username or "",
                program, coaches,
                dry_run,
            )

        updated += 1

        # Rate limiting: pause briefly every 10 tasks
        if not dry_run and updated % 10 == 0:
            time.sleep(2)

    print(f"\n{'='*50}")
    print(f"  SUMMARY")
    print(f"  Updated:  {updated}")
    print(f"  Skipped (already enriched):  {skipped}")
    print(f"  No match: {no_match}")
    print(f"  Total:    {len(checkins)}")
    if dry_run:
        print(f"\n  (DRY RUN — no changes were made)")
    print(f"{'='*50}")


if __name__ == "__main__":
    main()
