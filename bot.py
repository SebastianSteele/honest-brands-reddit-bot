import os
import json
import asyncio
import random
import discord
from discord import app_commands
from discord.ext import tasks
from dotenv import load_dotenv
import aiohttp
from datetime import datetime, timedelta, timezone

load_dotenv()

TEST_MODE = os.getenv("TEST_MODE", "false").lower() == "true"

DISCORD_TOKEN = os.getenv("DISCORD_TOKEN")
CLICKUP_TOKEN = os.getenv("CLICKUP_TOKEN")
CLICKUP_LIST_ID = os.getenv("CLICKUP_LIST_ID")
CLICKUP_MEMBER_DB_LIST_ID = "901516122313"

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

# Map bot stages to ClickUp Milestone dropdown options
STAGE_TO_MILESTONE = {
    "1. Finding a product": "1. Select a Product",
    "2. Building a store": "2. Build Site",
    "3. Creating ads": "3. Make Ads",
    "4. Getting sales": "4. First Sale",
    "5. Scaling": "Scaling",
}

# Roles that trigger the 1-week delayed check-in DM
CHECKIN_ROLES = {"accelerate", "core", "scale", "velocity"}

# File to persist pending new joiners awaiting their first check-in
PENDING_FILE = os.path.join(os.path.dirname(__file__), "pending_checkins.json")

# File to track weekly check-in submissions
CHECKIN_DATA_FILE = os.path.join(os.path.dirname(__file__), "checkin_data.json")

# File to track users who have DMs disabled (skip them instead of retrying)
DM_BLOCKED_FILE = os.path.join(os.path.dirname(__file__), "dm_blocked.json")

# DM pacing: send in batches to avoid spam detection
DM_DELAY_MIN = 8   # minimum seconds between DMs
DM_DELAY_MAX = 15  # maximum seconds between DMs
DM_BATCH_SIZE = 20  # pause after this many DMs
DM_BATCH_PAUSE = 60  # seconds to pause between batches

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


# --- Weekly check-in tracking ---
def _get_week_start():
    """Monday 00:00 EST of current week as ISO string."""
    _est = timezone(timedelta(hours=-5))
    now = datetime.now(_est)
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
    _est = timezone(timedelta(hours=-5))
    data["checkins"][str(user_id)] = datetime.now(_est).isoformat()
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

        # Update Last Activity text
        await _set_field(CU_FIELD_LAST_ACTIVITY, "Weekly Check-in", "Last Activity")

        # Update Weeks in Stage (number field)
        if weeks:
            try:
                await _set_field(CU_FIELD_WEEKS_IN_STAGE, int(weeks), "Weeks in Stage")
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


# --- Check-in Modal (the popup form) ---
class CheckInModal(discord.ui.Modal, title="Weekly Accountability Check-in"):
    def __init__(self, selected_stage: str):
        super().__init__()
        self.selected_stage = selected_stage

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
        headers = {
            "Authorization": CLICKUP_TOKEN,
            "Content-Type": "application/json",
        }
        task_data = {
            "name": f"Check-in — {interaction.user.display_name} — {today}",
            "description": (
                f"**Member:** {interaction.user.display_name}\n"
                f"**Discord ID:** {interaction.user.id}\n"
                f"**Date:** {today}\n\n"
                f"---\n\n"
                f"**Stage:** {self.selected_stage}\n\n"
                f"**Weeks in Stage:** {self.weeks.value}\n\n"
                f"**Blocker:** {self.blocker.value}\n\n"
                f"**What Would Help:** {self.help_needed.value}\n\n"
                f"**Next Steps:** {self.next_steps.value}"
            ),
            "priority": 3,
            "tags": ["check-in", interaction.user.display_name.lower()],
        }

        # Respond to Discord immediately (must be within 3 seconds)
        await interaction.response.defer(ephemeral=True, thinking=True)

        cu_status = None
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    f"https://api.clickup.com/api/v2/list/{CLICKUP_LIST_ID}/task",
                    json=task_data,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    cu_status = resp.status
                    if cu_status != 200:
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

                # Update member profile in background (non-blocking async task)
                asyncio.create_task(_update_member_after_checkin(
                    discord_username=interaction.user.name,
                    display_name=interaction.user.display_name,
                    stage=self.selected_stage,
                    weeks=self.weeks.value,
                    blocker=self.blocker.value,
                    help_needed=self.help_needed.value,
                    next_steps=self.next_steps.value,
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
                                       weeks, blocker, help_needed, next_steps):
    """Background task to update ClickUp member profile after check-in."""
    try:
        member_task = await find_member_by_discord(discord_username)
        if member_task:
            await update_member_profile(
                member_task["id"], stage,
                weeks=weeks, blocker=blocker,
                what_would_help=help_needed, next_steps=next_steps,
            )
            print(f"[CLICKUP] Member profile updated for {display_name}")
        else:
            print(f"[CLICKUP] No matching member for {display_name} (username: {discord_username})")
    except Exception as e:
        print(f"[CLICKUP] Error updating member {display_name}: {e}")


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
        await interaction.response.send_modal(CheckInModal(selected_stage=selected_stage))


class StageSelectView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=300)
        self.add_item(StageSelect())


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
            "**Which stage are you currently at?**\nSelect from the dropdown below:",
            view=view,
            ephemeral=True,
        )


# --- Slash command: /checkin ---
@tree.command(name="checkin", description="Submit your weekly accountability check-in")
async def checkin_command(interaction: discord.Interaction):
    view = StageSelectView()
    await interaction.response.send_message(
        "**Which stage are you currently at?**\nSelect from the dropdown below:",
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


# --- Detect new members getting accelerate/core role ---
@client.event
async def on_member_update(before: discord.Member, after: discord.Member):
    before_roles = {r.name.lower() for r in before.roles}
    after_roles = {r.name.lower() for r in after.roles}
    new_roles = after_roles - before_roles

    if new_roles & CHECKIN_ROLES:
        pending = load_pending()
        user_key = str(after.id)
        if user_key not in pending:
            pending[user_key] = {
                "guild_id": after.guild.id,
                "added_at": datetime.now().isoformat(),
            }
            save_pending(pending)
            print(f"[PENDING] {after.display_name} added — first check-in in 7 days")


# --- Background task: send first check-in DM after 1 week ---
@tasks.loop(hours=6)
async def check_pending_members():
    pending = load_pending()
    if not pending:
        return

    now = datetime.now()
    to_remove = []

    for user_id, info in pending.items():
        added_at = datetime.fromisoformat(info["added_at"])
        if now - added_at >= timedelta(days=7):
            guild = client.get_guild(info["guild_id"])
            if not guild:
                to_remove.append(user_id)
                continue
            member = guild.get_member(int(user_id))
            if not member:
                to_remove.append(user_id)
                continue
            try:
                view = CheckInButton()
                await member.send(
                    "**📋 Welcome to your first Check-in!**\n\n"
                    "You've been with us for a week now — time for your first accountability check-in.\n"
                    "Click the button below to submit your progress update.\n\n"
                    "*This helps us keep your support aligned each week.*",
                    view=view,
                )
                print(f"[DM] First check-in sent to {member.display_name}")
                await asyncio.sleep(random.uniform(DM_DELAY_MIN, DM_DELAY_MAX))
            except discord.Forbidden:
                mark_dm_blocked(int(user_id))
                print(f"[SKIP] Can't DM {member.display_name} (DMs disabled — marked blocked)")
            except discord.HTTPException as e:
                if e.status == 429:
                    retry_after = getattr(e, "retry_after", 60)
                    print(f"[RATE] 429 hit — backing off {retry_after}s")
                    await asyncio.sleep(retry_after)
                    # Don't remove from pending — retry next cycle
                    continue
                else:
                    print(f"[ERROR] DM to {member.display_name}: {e}")
            to_remove.append(user_id)

    for uid in to_remove:
        pending.pop(uid, None)
    save_pending(pending)


# --- Auto-DM tasks (for existing members with accelerate/core roles) ---
est = timezone(timedelta(hours=-5))
monday_time = datetime.now(est).replace(hour=9, minute=0, second=0).timetz()
wednesday_time = datetime.now(est).replace(hour=12, minute=0, second=0).timetz()


async def _send_checkin_dms(label: str, message: str):
    """Shared logic: DM members who haven't checked in this week.

    Anti-spam measures:
    - Random jitter between DMs (DM_DELAY_MIN to DM_DELAY_MAX seconds)
    - Batch pausing (DM_BATCH_PAUSE seconds every DM_BATCH_SIZE messages)
    - Skip users with DMs disabled (persistent tracking)
    - Exponential backoff on 429 rate limits
    - Cross-guild deduplication
    """
    pending = load_pending()
    sent = 0
    skipped = 0
    dm_blocked = 0
    seen_users = set()  # Dedupe across guilds

    for guild in client.guilds:
        for member in guild.members:
            if member.bot or member.id in seen_users:
                continue
            seen_users.add(member.id)
            # Only DM members with a check-in role
            member_roles = {r.name.lower() for r in member.roles}
            if not member_roles & CHECKIN_ROLES:
                continue
            if str(member.id) in pending:
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

    print(f"[{label.upper()}] Sent: {sent}, Skipped (checked in): {skipped}, DM-blocked: {dm_blocked}")


@tasks.loop(time=monday_time)
async def weekly_reminder():
    if datetime.now(est).weekday() != 0:
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
    if datetime.now(est).weekday() != 2:
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


# --- Bot ready ---
_synced = False


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


client.run(DISCORD_TOKEN)
