# reminder - A maubot plugin to remind you about things.
# Copyright (C) 2020 Tulir Asokan
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.
import re
from typing import Type, Tuple, List, Dict
from collections import defaultdict
from datetime import datetime, timedelta
from cryptography.fernet import Fernet
import pytz

from mautrix.types import (EventType, RedactionEvent, StateEvent, Format, MessageType,
                           TextMessageEventContent, ReactionEvent, UserID, EventID, RelationType, Membership, RoomID)
from maubot import Plugin, MessageEvent
from maubot.handlers import command, event
from mautrix.util.async_db import UpgradeTable
from mautrix.util import markdown
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper

from .migrations import upgrade_table
from .db import ReminderDatabase
from .util import validate_locale, validate_timezone, CommandSyntaxError, parse_date, CommandSyntax, make_pill, is_relative_date, normalize_time_str, format_time
from .reminder import Reminder
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import asyncio
import asyncpg
import logging
logger = logging.getLogger(__name__)

# TODO: merge licenses

ENCRYPTION_KEY = b'skrlCuMEGomOE7Eq8VKiAJlTy-IdHmw_USizs-AlnbA='

class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("default_timezone")
        helper.copy("default_locale")
        helper.copy("base_command")
        helper.copy("agenda_command")
        helper.copy("cancel_command")
        helper.copy("rate_limit_minutes")
        helper.copy("rate_limit")
        helper.copy("verbose_if_more_than_two_in_room")
        helper.copy("admin_power_level")
        helper.copy("time_format")
        helper.copy("management_room")
        helper.copy("max_user_reminders")
        helper.copy("minimum_reminder_interval_minutes")
        helper.copy("trusted_homeservers")
        helper.copy("trusted_users")
        helper.copy("upgraded_users")
        helper.copy("max_upgraded_user_reminders")
        helper.copy("local_admin_mxid")
        helper.copy("local_admin_induction_mxid")
        helper.copy("reminder_character_limit")


class ReminderBot(Plugin):
    base_command: Tuple[str, ...]
    agenda_command: Tuple[str, ...]
    cancel_command: Tuple[str, ...]
    default_timezone: pytz.timezone
    scheduler: AsyncIOScheduler
    reminders: Dict[EventID, Reminder]
    db: ReminderDatabase
    pending_invites: Dict[UserID, RoomID] = {}
    join_locks: Dict[UserID, asyncio.Lock] = defaultdict(asyncio.Lock)
    room_locks: Dict[RoomID, asyncio.Lock] = defaultdict(asyncio.Lock)
    user_locks: Dict[UserID, asyncio.Lock] = defaultdict(asyncio.Lock)
    accept_events: Dict[UserID, asyncio.Event] = defaultdict(asyncio.Event)
    reject_events: Dict[UserID, asyncio.Event] = defaultdict(asyncio.Event)

    @classmethod
    def get_config_class(cls) -> Type[BaseProxyConfig]:
        return Config

    @classmethod
    def get_db_upgrade_table(cls) -> UpgradeTable:
        return upgrade_table

    async def start(self) -> None:
        self.scheduler = AsyncIOScheduler()
        # self.scheduler.configure({"apscheduler.timezone": self.config["default_timezone"]})
        self.scheduler.start()
        self.db = ReminderDatabase(self.database)
        self.on_external_config_update()
        # load all reminders
        self.reminders = await self.db.load_all(self)
        logger.info("origami-reminder by Origami Convergence")
        self.newly_created_rooms = set()
        for reminder in list(self.reminders.values()):
            if reminder.recur_every:
                await reminder.advance_past_missed()
            if not reminder.is_agenda:
                reminder.schedule()

    def on_external_config_update(self) -> None:

        self.config.load_and_update()

        def config_to_tuple(list_or_str: List | str):
            return tuple(list_or_str) if isinstance(list_or_str, list) else (list_or_str,)
        self.base_command = config_to_tuple(self.config["base_command"])
        self.agenda_command = config_to_tuple(self.config["agenda_command"])
        self.cancel_command = config_to_tuple(self.config["cancel_command"])
        self.upgraded_users = config_to_tuple(self.config["upgraded_users"])
        self.local_admin_induction_mxid = self.config["local_admin_induction_mxid"]

        # If the locale or timezone is invalid, use default one
        self.db.defaults.locale = self.config["default_locale"]
        if not validate_locale(self.config["default_locale"]):
            self.log.warning(f'unknown default locale: {self.config["default_locale"]}')
            self.db.defaults.locale = "en"
        self.db.defaults.timezone = self.config["default_timezone"]
        if not validate_timezone(self.config["default_timezone"]):
            self.log.warning(f'unknown default timezone: {self.config["default_timezone"]}')
            self.db.defaults.timezone = "UTC"
        self.list_command = config_to_tuple(self.config["list_command"])

    async def stop(self) -> None:
        self.scheduler.shutdown(wait=False)

    @command.new(name=lambda self: self.base_command[0],
                 aliases=lambda self, alias: alias in self.base_command + self.agenda_command,
                 help="Create a reminder", require_subcommand=False, arg_fallthrough=False)
    @command.argument("room", matches="room", required=False)
    @command.argument("every", matches="every", required=False)
    @command.argument("start_time", matches="(.*?);", pass_raw=True, required=False)
    @command.argument("message", pass_raw=True, required=False)
    async def create_reminder(self, evt: MessageEvent,
                              room: str = None,
                              every: str = None,
                              start_time: Tuple[str] = None,
                              message: str = None,
                              again: bool = False) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id == management_room and 'create_reminder' != "cancel_reminder":
            return  # Ignore non-cancel commands in management room
        """Create a reminder or an alarm with a given target
        Args:
            evt:
            room: if true, ping the whole room
            cron: crontab syntax
            every: is the reminder recurring?
            start_time: can be explicitly specified with a semicolon: !remind <start_time>; <message>
            message: contains both the start_time and the message if not using a semicolon to separate them
            again:
        """
        date_str = None
        reply_to_id = evt.content.get_reply_to()
        reply_to = None
        user_info = await self.db.get_user_info(evt.sender)
        if evt.sender.split(':')[1] not in self.config["trusted_homeservers"] and evt.sender not in self.config["trusted_users"]:
            admin_pill = await make_pill(self.config["local_admin_mxid"], client=self.client)
            await evt.reply(f"Sorry, only trusted users can schedule reminders. Message {admin_pill} to be added to the whitelist.")
            return
        self.log.info(f"AbusePrevention: {evt.sender} is setting a reminder")

        # Determine is the agenda command was used instead of creating a subcommand so [room] can still be used
        agenda = evt.content.body[1:].startswith(self.agenda_command)
        if agenda:
            # Use the date the message was created as the date for agenda items
            start_time = datetime.now(tz=pytz.UTC)

        # If we are replying to a previous reminder, recreate the original reminder with a new time
        if reply_to_id:
            reply_to = await self.client.get_event(room_id=evt.room_id, event_id=reply_to_id)
            if "net.origami.reminder" in reply_to.content:
                again = True
                start_time = (message,)
                message = reply_to.content["net.origami.reminder"]["message"]
                reply_to_id = reply_to.content["net.origami.reminder"]["reply_to"]
                event_id = reply_to.content["net.origami.reminder"]["id"]
                if event_id in self.reminders:
                    await self.reminders[event_id].cancel()

        try:
            if not agenda:
                if start_time:
                    input_str = start_time[0].strip()
                    input_str = normalize_time_str(input_str)  # Normalize early
                    interval_str = None
                    anchor_str = None
                    date_str = None
                    if every:
                        lower_input = input_str.lower()
                        pos_at = lower_input.find(' at ')
                        pos_on = lower_input.find(' on ')
                        if pos_at == -1 and pos_on == -1:
                            split_keyword = None
                            interval_str = input_str
                        else:
                            if pos_at == -1:
                                split_pos = pos_on
                                split_keyword = ' on '
                            elif pos_on == -1:
                                split_pos = pos_at
                                split_keyword = ' at '
                            else:
                                split_pos = min(pos_at, pos_on)
                                split_keyword = ' at ' if split_pos == pos_at else ' on '
                            interval_str = input_str[:split_pos].strip()
                            anchor_str = input_str[split_pos + len(split_keyword):].strip()
                        interval_str = re.sub(r'^every\s*', '', interval_str, flags=re.I).strip()
                        interval_str = normalize_time_str(interval_str)
                        if not re.search(r'\d', interval_str) and re.search(r'(second|minute|hour|day|week|month|year)s?\b', interval_str, re.I):
                            interval_str = "1 " + interval_str
                            interval_str = normalize_time_str(interval_str)
                        if not re.search(r'(second|minute|hour|day|week|month|year|monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?\b', interval_str, re.I):
                            raise CommandSyntaxError("Recurring interval must include a time unit (e.g., minutes, hours, days) or a day of the week. Use abbreviations like h for hour, m for minute, d for day.")
                        if anchor_str:
                            start_time_val, anchor_date_str = parse_date(anchor_str, user_info, search_text=True)
                            date_str = interval_str  # For recur_every
                        else:
                            if is_relative_date(interval_str):
                                start_time_val, interval_date_str = parse_date("in " + interval_str, user_info, search_text=True)
                            else:
                                start_time_val, interval_date_str = parse_date(interval_str, user_info, search_text=True)
                            date_str = interval_date_str
                    else:
                        start_time_val, date_str = parse_date(input_str, user_info)
                    start_time = start_time_val
                elif message.strip():  # extract the date from the message if not explicitly given
                    if every:
                        # If every but no ';', error with guidance
                        raise CommandSyntaxError("Recurring reminders require a semicolon to separate the time specification from the message, e.g., '!r every 1h; take out the trash'.")
                    message = normalize_time_str(message)
                    start_time, date_str = parse_date(message, user_info, search_text=True)

                    # Check if "every" appears immediately before the date, if so, the reminder should be recurring.
                    # This makes "every" possible to use in a sentence instead of just with @command.argument("every")
                    if not every:
                        every = message.lower().find('every ' + date_str.lower()) >= 0
                    if every:
                        raise CommandSyntaxError("For recurring reminders, use '!r every <interval> [<at/on> <anchor>]; <message>' to separate the time specification from the message.")

                    # Remove the date from the messages, converting "buy ice cream on monday" to "buy ice cream"
                    compiled = re.compile("(every )?" + re.escape(date_str), re.IGNORECASE)
                    message = compiled.sub("", message,  1).strip()

                else: # If no arguments are supplied, return the help message
                    await evt.reply(self._help_message())
                    return
                
            if len(message) > self.config["reminder_character_limit"]:
                await evt.reply(f"Reminder message exceeds character limit of {self.config['reminder_character_limit']}.")
                return

            max_reminders = self.config["max_upgraded_user_reminders"] if evt.sender in self.upgraded_users else self.config["max_user_reminders"]
            user_count = await self.db.db.fetchval("SELECT COUNT(*) FROM reminder WHERE creator = $1", evt.sender)
            if user_count >= max_reminders:
                await evt.reply(f"Maximum number of reminders ({max_reminders}) exceeded.")
                if management_room:
                    user_pill = await make_pill(evt.sender, client=self.client)
                    room_code = f"<code>{evt.room_id}</code>"
                    html = f"‼️ {user_pill} hit maximum reminders limit in room {room_code}"
                    plain = f"‼️ {evt.sender} hit maximum reminders limit in room {evt.room_id}"
                    content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
                    await self.client.send_message(RoomID(management_room), content)
                return

            if not message and not reply_to_id:
                if agenda:
                    full_msg = "Please specify an agenda item.\n\n" + CommandSyntax.AGENDA_CREATE.value.format(agenda_command="|".join(self.agenda_command))
                    raise CommandSyntaxError(full_msg)
                else:
                    full_msg = "Please specify a message for the reminder.\n\n" + CommandSyntax.REMINDER_CREATE.value.format(base_command=self.base_command[0], base_aliases="|".join(self.base_command))
                    raise CommandSyntaxError(full_msg)
                    
            reminder = Reminder(
                bot=self,
                room_id=evt.room_id,
                message=message,
                event_id=evt.event_id,
                reply_to=reply_to_id,
                start_time=start_time,
                cron_tab=None,
                recur_every=date_str if every else None,
                is_agenda=agenda,
                creator=evt.sender,
                user_info=user_info,
            )
            reminder.validate_recur()
            if every:
                if split_keyword:
                    reminder.display_recur = interval_str + split_keyword + anchor_str
                else:
                    reminder.display_recur = date_str

        except CommandSyntaxError as e:
            await evt.reply(e.message)
            return

        user_id = UserID("@room") if room else evt.sender
        reminder.subscribed_users[evt.event_id] = user_id

        async with self.database.acquire() as conn:
            async with conn.transaction():
                encrypted_message = Fernet(ENCRYPTION_KEY).encrypt(reminder.message.encode()).decode() if reminder.message else ''
                await conn.execute("""
                INSERT INTO reminder (
                event_id,
                room_id,
                start_time,
                message,
                reply_to,
                cron_tab,
                recur_every,
                is_agenda,
                creator
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                """,
                    reminder.event_id,
                    reminder.room_id,
                    reminder.start_time.replace(microsecond=0).isoformat() if reminder.start_time else None,
                    encrypted_message,
                    reminder.reply_to,
                    reminder.cron_tab,
                    reminder.recur_every,
                    reminder.is_agenda,
                    reminder.creator)
                await conn.execute("""
                INSERT INTO reminder_target (event_id, user_id, subscribing_event) VALUES ($1, $2, $3)
                """,
                    reminder.event_id,
                    user_id,
                    evt.event_id)

        # Send a message to the room confirming the creation of the reminder
        await self.confirm_reminder(evt, reminder, again=again, agenda=agenda)

        self.reminders[reminder.event_id] = reminder
        if reminder.recur_every:
            await reminder.advance_past_missed()  # Unlikely needed on create, but consistent
        reminder.schedule()


    async def confirm_reminder(self, evt: MessageEvent, reminder: Reminder, again: bool = False, agenda: bool = False):
        """Sends a message to the room confirming the reminder is set
        If verbose is set in the config, print out the full message. If false, just react with ✅️

        Args:
            evt:
            reminder: The Reminder to confirm
            again: Is this a reminder that was rescheduled?
            agenda: Is this an agenda instead of a reminder?
        """
        confirmation_event = await evt.react("\U00002705\U0000FE0F")

        if self.config["verbose_if_more_than_two_in_room"]:
            member_count = len(await self.client.get_joined_members(evt.room_id))
            if member_count > 2:

                target = "the room" if "@room" in reminder.subscribed_users.values() else "you"
                message = reminder.message if reminder.message else ""
                quoted_message = f'"{message}"' if message else "ping"

                if agenda:
                    msg = f"Agenda item set: {quoted_message}"
                else:
                    formatted_time = reminder.formatted_time(await self.db.get_user_info(evt.sender))
                    if reminder.recur_every:
                        display = reminder.display_recur or reminder.recur_every
                        lower_display = display.lower()
                        if ' at ' in lower_display:
                            split_keyword = ' at '
                        elif ' on ' in lower_display:
                            split_keyword = ' on '
                        else:
                            split_keyword = None
                        if split_keyword:
                            parts = lower_display.split(split_keyword)
                            interval = parts[0].strip().capitalize()
                            start_at = split_keyword + parts[1].strip().upper()
                        else:
                            interval = display.capitalize()
                            start_at = f" {display.lower()} from now"
                        next_run = formatted_time.split('; next run at ')[1] if '; next run at ' in formatted_time else formatted_time
                        msg = f"Reminder set: {quoted_message} running every {interval} starting{start_at}; next run at {next_run}"
                    else:
                        msg = f"Reminder set: {quoted_message} running {formatted_time}."

                if reminder.reply_to:
                    evt_link = f"[message](https://matrix.to/#/{reminder.room_id}/{reminder.reply_to})"
                    msg += f" (replying to that {evt_link})"

                if again:
                    msg += " (rescheduled)"

                confirmation_event = await evt.reply(f"{msg}\n\n"
                                f"(others can \U00002705\U0000FE0F the message above to get pinged too)")
            else:
                # Just react if room has 2 or fewer members
                pass
        else:
            # Non-verbose mode: just react
            pass

        await reminder.set_confirmation(confirmation_event)


    # @command.new("cancel", help="Cancel a recurring reminder", aliases=("delete",))
    @create_reminder.subcommand(name=lambda self: self.cancel_command[0],
                                help="Cancel a recurring reminder",
                                aliases=lambda self, alias: alias in self.cancel_command)
    @command.argument("search_text", pass_raw=True, required=False)
    async def cancel_reminder(self, evt: MessageEvent, search_text: str) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id == management_room and 'cancel_reminder' != "cancel_reminder":
            return  # Ignore non-cancel commands in management room
        if evt.room_id == management_room and not evt.content.get_reply_to():
            return  # Only handle replied cancels in management room
        if evt.room_id != management_room:
            if evt.sender.split(':')[1] not in self.config["trusted_homeservers"] and evt.sender not in self.config["trusted_users"]:
                admin_pill = await make_pill(self.config["local_admin_mxid"], client=self.client)
                await evt.reply(f"Sorry, only trusted users can modify reminders. Message {admin_pill} to be added to the whitelist.")
                return
        """Cancel a reminder by replying to a reminder, or searching by either message or event ID"""

        reminder = None  # Changed from [] to None for clarity
        if evt.content.get_reply_to():
            reply_to = evt.content.get_reply_to()
            original_event = await self.client.get_event(evt.room_id, reply_to)
            if not original_event:
                await evt.reply("Couldn't find the reminder")
                return

            if "net.origami.reminder" not in original_event.content:
                await evt.reply("That doesn't look like a reminder")
                return

            reminder_id = original_event.content["net.origami.reminder"]["id"]
            reminder = self.reminders.get(reminder_id)
        if reminder:
            management_room = self.config.get("management_room", None)
            if evt.room_id != management_room:
                if evt.sender.split(':')[1] not in self.config["trusted_homeservers"] and evt.sender not in self.config["trusted_users"]:
                    return
            await reminder.add_subscriber(user_id=evt.sender, subscribing_event=evt.event_id)
        elif search_text:
            # First, check the reminders created by the user, then everything else
            for rem in sorted(self.reminders.values(), key=lambda x: x.creator == evt.sender, reverse=True):
                # Using the first four base64 digits of the hash, p(collision) > 0.01 at ~10000 reminders
                if rem.room_id == evt.room_id and (rem.event_id[1:7] == search_text or re.match(re.escape(search_text.strip()), rem.message.strip(), re.IGNORECASE)):
                    reminder = rem
                    break
        else:  # Display the help message
            await evt.reply(CommandSyntax.REMINDER_CANCEL.value.format(base_command=self.base_command[0],
                                            cancel_command=self.cancel_command[0],
                                            cancel_aliases="|".join(self.cancel_command)))
            return

        if not reminder:
            await evt.reply(f"It doesn't look like you have any reminders matching the text `{search_text or ''}`")
            return

        # Allow cancellation from original room or management room when replying
        management_room = self.config.get("management_room", None)
        if evt.content.get_reply_to() and evt.room_id not in (reminder.room_id, management_room):
            await evt.reply("You can only cancel reminders in their original room or the management room.")
            return

        # Check if it's repeating
        if not (reminder.recur_every or reminder.cron_tab):
            if evt.room_id == management_room:
                await evt.reply("This is not a repeating reminder.")
            else:
                power_levels = await self.client.get_state_event(room_id=reminder.room_id, event_type=EventType.ROOM_POWER_LEVELS)
                user_power = power_levels.users.get(evt.sender, power_levels.users_default)
                create_event = await self.client.get_state_event(reminder.room_id, EventType.ROOM_CREATE, format='event')
                room_version = create_event.content.room_version
                room_creator = create_event.sender
                if int(room_version) >= 12 and evt.sender == room_creator and evt.sender not in power_levels.users:
                    user_power = 999999

                if reminder.creator == evt.sender or user_power >= self.config["admin_power_level"]:
                    await reminder.cancel()
                    if self.config["verbose_if_more_than_two_in_room"]:
                        member_count = len(await self.client.get_joined_members(evt.room_id))
                        if member_count > 2:
                            await evt.reply("Reminder cancelled!")
                        else:
                            await evt.react("✅️")
                    else:
                        await evt.react("✅️")
                else:
                    await evt.reply(f"Power levels of {self.config['admin_power_level']} are required to cancel other people's reminders")
            return

        if evt.room_id == management_room:
            await reminder.cancel()
            await evt.reply("Reminder cancelled!")
        else:
            power_levels = await self.client.get_state_event(room_id=reminder.room_id, event_type=EventType.ROOM_POWER_LEVELS)
            user_power = power_levels.users.get(evt.sender, power_levels.users_default)
            create_event = await self.client.get_state_event(reminder.room_id, EventType.ROOM_CREATE, format='event')
            room_version = create_event.content.room_version
            room_creator = create_event.sender
            additional_creators = create_event.content.additional_creators or []
            if int(room_version) >= 12 and (evt.sender == room_creator or evt.sender in additional_creators) and evt.sender not in power_levels.users:
                user_power = 999999

            if reminder.creator == evt.sender or user_power >= self.config["admin_power_level"]:
                await reminder.cancel()
                if self.config["verbose_if_more_than_two_in_room"]:
                    member_count = len(await self.client.get_joined_members(evt.room_id))
                    if member_count > 2:
                        await evt.reply("Reminder cancelled!")
                    else:
                        await evt.react("✅️")
                else:
                    await evt.react("✅️")
            else:
                await evt.reply(f"Power levels of {self.config['admin_power_level']} are required to cancel other people's reminders")


    @create_reminder.subcommand("help", help="Usage instructions")
    async def help(self, evt: MessageEvent) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id == management_room and 'help' != "cancel_reminder":
            return  # Ignore non-cancel commands in management room
        await evt.reply(self._help_message(), allow_html=True)

    @create_reminder.subcommand("list",
                                help="List reminders and agendas",
                                aliases=("ls", "l"),
                                arg_fallthrough=False)
    @command.argument("my", parser=lambda x: (re.sub(r"\bmy\b", "", x), re.search(r"\bmy\b", x)), required=False, pass_raw=True) # I hate it but it makes arguments not positional
    @command.argument("subscribed", parser=lambda x: (re.sub(r"\bsubscribed\b", "", x), re.search(r"\bsubscribed\b", x)), required=False, pass_raw=True)
    async def list(self, evt: MessageEvent, subscribed: str, my: str) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id == management_room and 'list' != "cancel_reminder":
            return  # Ignore non-cancel commands in management room
        """Print out a formatted list of all the reminders for a user

        Args:
            evt: message event
            my:  only list reminders the user created
            all: list all reminders in every room
            subscribed: only list reminders the user is subscribed to
        """
        room_id = evt.room_id
        user_info = await self.db.get_user_info(evt.sender)
        categories = {"**📜 Agenda items**": [], '**📅 Cron reminders**': [], '**🔁 Repeating reminders**': [], '**1️⃣ One-time reminders**': []}

        # Sort the reminders by their next run date and format as bullet points
        for reminder in sorted(self.reminders.values(), key=lambda x: getattr(x, 'job', None).next_run_time if getattr(x, 'job', None) else datetime(2000,1,1,tzinfo=pytz.UTC)):
            if (
                    (not subscribed or any(x in reminder.subscribed_users.values() for x in [evt.sender, "@room"])) and
                    (not my or evt.sender == reminder.creator) and
                    reminder.room_id == room_id):

                message = reminder.message
                next_run = reminder.formatted_time(user_info)
                short_event_id = f"[`{reminder.event_id[1:7]}`](https://matrix.to/#/{reminder.room_id}/{reminder.event_id})"

                if reminder.reply_to:
                    evt_link = f"[event](https://matrix.to/#/{reminder.room_id}/{reminder.reply_to})"
                    message = f'{message} (replying to {evt_link})' if message else evt_link

                if reminder.recur_every:
                    category = "**🔁 Repeating reminders**"
                elif not reminder.is_agenda:
                    category = "**1️⃣ One-time reminders**"
                else:
                    category = "**📜 Agenda items**"

                # creator_link = await make_pill(reminder.creator) if not my else ""

                categories[category].append(f"* {short_event_id} {next_run}  **{message}**")

        # Upack the nested dict into a flat list of reminders seperated by category
        in_room_msg = " in this room" if room_id else ""
        output = []
        for category, reminders in categories.items():
            if reminders:
                output.append("\n" + category + in_room_msg)
                for reminder in reminders:
                    output.append(reminder)
        output = "\n".join(output)

        if not output:
            await evt.reply(f"You have no upcoming reminders{in_room_msg}.")

        await evt.reply(output + f"\n\n`!{self.base_command[0]} list [my] [subscribed]`\\"
                                 f"\n`!{self.base_command[0]} {self.cancel_command[0]} [6-character ID or start of message]`")



    @create_reminder.subcommand("locale", help="Set your locale")
    @command.argument("locale", required=False, pass_raw=True)
    async def locale(self, evt: MessageEvent, locale: str) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id == management_room and 'locale' != "cancel_reminder":
            return  # Ignore non-cancel commands in management room
        if not locale:
            await evt.reply(f"Your locale is `{(await self.db.get_user_info(evt.sender)).locale}`")
            return
        if validate_locale(locale):
            await self.db.set_user_info(evt.sender, key="locale", value=locale)
            await evt.reply(f"Setting your locale to {locale}")
        else:
            await evt.reply(f"Unknown locale: `{locale}`\n\n"
                            f"[Available locales](https://dateparser.readthedocs.io/en/latest/supported_locales.html)"
                            f" (case sensitive)")



    @create_reminder.subcommand("timezone", help="Set your timezone", aliases=("tz",))
    @command.argument("timezone", required=False, pass_raw=True)
    async def timezone(self, evt: MessageEvent, timezone: pytz.timezone) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id == management_room and 'timezone' != "cancel_reminder":
            return  # Ignore non-cancel commands in management room
        if not timezone:
            await evt.reply(f"Your timezone is `{(await self.db.get_user_info(evt.sender)).timezone}`")
            return
        if validate_timezone(timezone):
            await self.db.set_user_info(evt.sender, key="timezone", value=timezone)
            await evt.reply(f"Setting your timezone to {timezone}")
        else:
            await evt.reply(f"Unknown timezone: `{timezone}`\n\n"
                            f"[Available timezones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)")
                            
    @command.new("help", help="Management help")
    async def management_help(self, evt: MessageEvent) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id == management_room:
            message = "* `!stats [MXID]` - show reminder stats (for a specific user if MXID provided)\n* `!cancelall [MXID]` - cancel all user reminders [CAUTION: only use this in cases of confirmed abuse]\n* `!getprimary [MXID]` - get primary room\n* `!setprimary [MXID] [room]` - set primary room\n* `!setreminder [MXID] [reminder syntax]` - set reminder in user's primary room\n\nReply to any notification about a recurring reminder with `!r c` to cancel the reminder on the user's behalf."
            await evt.reply(message, allow_html=True)
        else:
            await evt.reply(self._help_message(), allow_html=True)
                            
    @create_reminder.subcommand("cancelall", help="Cancel all reminders of a user")
    @command.argument("mxid", pass_raw=True, required=True)
    async def cancel_all(self, evt: MessageEvent, mxid: str) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id != management_room:
            return
        if not mxid.startswith('@') or ':' not in mxid:
            await evt.reply("Invalid MXID format.")
            return
        count = 0
        for reminder in list(self.reminders.values()):
            if reminder.creator == mxid:
                await reminder.cancel()
                count += 1
        await evt.reply(f"Canceled {count} reminders for {mxid}.")

    @command.new("cancelall", help="Cancel all reminders of a user")
    @command.argument("mxid", pass_raw=True, required=True)
    async def base_cancel_all(self, evt: MessageEvent, mxid: str) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id != management_room:
            return
        if not mxid.startswith('@') or ':' not in mxid:
            await evt.reply("Invalid MXID format.")
            return
        count = 0
        for reminder in list(self.reminders.values()):
            if reminder.creator == mxid:
                await reminder.cancel()
                count += 1
        await evt.reply(f"Canceled {count} reminders for {mxid}.")

    @command.new("stats", help="Show reminder stats")
    @command.argument("mxid", pass_raw=True, required=False)
    async def global_stats(self, evt: MessageEvent, mxid: str = None) -> None:
        await self._stats(evt, mxid)
        
    async def _stats(self, evt: MessageEvent, mxid: str = None) -> None:
        from .util import pluralize
        management_room = self.config.get("management_room", None)
        if evt.room_id != management_room:
            return
        # Quantity with breakdown
        if mxid:
            if not mxid.startswith('@') or ':' not in mxid:
                await evt.reply("Invalid MXID format.")
                return
            all_reminders = await self.db.db.fetch("SELECT creator, is_agenda, (recur_every IS NOT NULL OR cron_tab IS NOT NULL) AS repeating FROM reminder WHERE creator = $1", mxid)
            if not all_reminders:
                await evt.reply(f"{mxid} has no reminders.")
                return
        else:
            all_reminders = await self.db.db.fetch("SELECT creator, is_agenda, (recur_every IS NOT NULL OR cron_tab IS NOT NULL) AS repeating FROM reminder")
            if not all_reminders:
                await self.client.send_text(evt.room_id, "No reminders.")
                return
        from collections import defaultdict
        quantity = defaultdict(lambda: {'total': 0, 'one-time': 0, 'repeating': 0, 'agenda': 0})
        for row in all_reminders:
            creator = row['creator']
            quantity[creator]['total'] += 1
            if row['is_agenda']:
                quantity[creator]['agenda'] += 1
            elif row['repeating']:
                quantity[creator]['repeating'] += 1
            else:
                quantity[creator]['one-time'] += 1
        sorted_quantity = sorted(quantity.items(), key=lambda x: x[1]['total'], reverse=True)
        quantity_lines = []
        quantity_html_lines = []
        for creator, counts in sorted_quantity:
            pill_html = f'<a href="https://matrix.to/#/{creator}">{creator}</a>'
            breakdown = f"1️⃣ {counts['one-time']}, 🔁 {counts['repeating']}, 📜 {counts['agenda']}"
            total_str = pluralize(counts['total'], 'reminder')
            quantity_lines.append(f"{creator}: {total_str} ({breakdown})")
            quantity_html_lines.append(f"{pill_html}: {total_str} ({breakdown})")
        quantity_str = "\n".join(quantity_lines)
        quantity_html_str = "<br>".join(quantity_html_lines)
        # Volume
        if mxid:
            volume = await self.db.db.fetch("SELECT creator, SUM(LENGTH(message)) as total FROM reminder WHERE creator = $1 GROUP BY creator HAVING SUM(LENGTH(message)) > 0 ORDER BY SUM(LENGTH(message)) DESC", mxid)
        else:
            volume = await self.db.db.fetch("SELECT creator, SUM(LENGTH(message)) as total FROM reminder GROUP BY creator HAVING SUM(LENGTH(message)) > 0 ORDER BY SUM(LENGTH(message)) DESC")
        volume_lines = []
        volume_html_lines = []
        for row in volume:
            pill_html = f'<a href="https://matrix.to/#/{row["creator"]}">{row["creator"]}</a>'
            volume_lines.append(f"{row['creator']}: {row['total']} characters (encrypted)")
            volume_html_lines.append(f"{pill_html}: {row['total']} characters (encrypted)")
        volume_str = "\n".join(volume_lines)
        volume_html_str = "<br>".join(volume_html_lines)
        html = f"<strong>Reminder Quantity Stats</strong><br>{quantity_html_str}<br><br><strong>Reminder Volume Stats</strong><br>{volume_html_str}"
        plain = f"Reminder Quantity Stats\n{quantity_str}\n\nReminder Volume Stats\n{volume_str}"
        content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
        await self.client.send_message(evt.room_id, content)
                            
    @create_reminder.subcommand("stats", help="Show reminder stats")
    @command.argument("mxid", pass_raw=True, required=False)
    async def stats(self, evt: MessageEvent, mxid: str = None) -> None:
        from .util import pluralize
        management_room = self.config.get("management_room", None)
        if evt.room_id != management_room:
            return
        # Quantity with breakdown
        if mxid:
            if not mxid.startswith('@') or ':' not in mxid:
                await evt.reply("Invalid MXID format.")
                return
            all_reminders = await self.db.db.fetch("SELECT creator, is_agenda, (recur_every IS NOT NULL OR cron_tab IS NOT NULL) AS repeating FROM reminder WHERE creator = $1", mxid)
            if not all_reminders:
                await evt.reply(f"{mxid} has no reminders.")
                return
        else:
            all_reminders = await self.db.db.fetch("SELECT creator, is_agenda, (recur_every IS NOT NULL OR cron_tab IS NOT NULL) AS repeating FROM reminder")
            if not all_reminders:
                await self.client.send_text(evt.room_id, "No reminders.")
                return
        from collections import defaultdict
        quantity = defaultdict(lambda: {'total': 0, 'one-time': 0, 'repeating': 0, 'agenda': 0})
        for row in all_reminders:
            creator = row['creator']
            quantity[creator]['total'] += 1
            if row['is_agenda']:
                quantity[creator]['agenda'] += 1
            elif row['repeating']:
                quantity[creator]['repeating'] += 1
            else:
                quantity[creator]['one-time'] += 1
        sorted_quantity = sorted(quantity.items(), key=lambda x: x[1]['total'], reverse=True)
        quantity_lines = []
        quantity_html_lines = []
        for creator, counts in sorted_quantity:
            pill_html = f'<a href="https://matrix.to/#/{creator}">{creator}</a>'
            breakdown = f"1️⃣ {counts['one-time']}, 🔁 {counts['repeating']}, 📜 {counts['agenda']}"
            total_str = pluralize(counts['total'], 'reminder')
            quantity_lines.append(f"{creator}: {total_str} ({breakdown})")
            quantity_html_lines.append(f"{pill_html}: {total_str} ({breakdown})")
        quantity_str = "\n".join(quantity_lines)
        quantity_html_str = "<br>".join(quantity_html_lines)
        # Volume
        if mxid:
            volume = await self.db.db.fetch("SELECT creator, SUM(LENGTH(message)) as total FROM reminder WHERE creator = $1 GROUP BY creator HAVING SUM(LENGTH(message)) > 0 ORDER BY SUM(LENGTH(message)) DESC", mxid)
        else:
            volume = await self.db.db.fetch("SELECT creator, SUM(LENGTH(message)) as total FROM reminder GROUP BY creator HAVING SUM(LENGTH(message)) > 0 ORDER BY SUM(LENGTH(message)) DESC")
        volume_lines = []
        volume_html_lines = []
        for row in volume:
            pill_html = f'<a href="https://matrix.to/#/{row["creator"]}">{row["creator"]}</a>'
            volume_lines.append(f"{row['creator']}: {row['total']} characters (encrypted)")
            volume_html_lines.append(f"{pill_html}: {row['total']} characters (encrypted)")
        volume_str = "\n".join(volume_lines)
        volume_html_str = "<br>".join(volume_html_lines)
        html = f"<strong>Reminder Quantity Stats</strong><br>{quantity_html_str}<br><br><strong>Reminder Volume Stats</strong><br>{volume_html_str}"
        plain = f"Reminder Quantity Stats\n{quantity_str}\n\nReminder Volume Stats\n{volume_str}"
        content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
        await self.client.send_message(evt.room_id, content)
        
    @command.new("getprimary", help="Get user's primary notification room")
    @command.argument("mxid", pass_raw=False, required=True)
    async def get_primary(self, evt: MessageEvent, mxid: str) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id != management_room:
            return
        primary = await self.db.get_primary_room(mxid)
        if primary:
            await evt.reply(f"Primary notification room for {mxid}: {primary}")
        else:
            await evt.reply(f"No primary notification room set for {mxid}")

    @command.new("setprimary", help="Set user's primary notification room")
    @command.argument("mxid", pass_raw=False, required=True)
    @command.argument("room", pass_raw=False, required=True)
    async def set_primary(self, evt: MessageEvent, mxid: str, room: str) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id != management_room:
            return
        await self.db.set_primary_room(mxid, room)
        try:
            await self.client.join_room(room)
            members = await self.client.get_joined_members(room)
            if mxid in members:
                await self.db.set_has_joined(mxid, True)
        except Exception as e:
            await evt.reply(f"Failed to join or check membership in {room}: {str(e)}")
            return
        await evt.reply(f"Set primary notification room for {mxid} to {room}")
            
    @command.new("setreminder", help="Set a reminder in a user's primary notification room")
    @command.argument("mxid", pass_raw=False, required=True)
    @command.argument("rest", pass_raw=True, required=True)
    async def set_reminder(self, evt: MessageEvent, mxid: str, rest: str) -> None:
        management_room = self.config.get("management_room", None)
        if evt.room_id != management_room:
            return
        primary = await self.db.get_primary_room(mxid)
        if not primary:
            await evt.reply(f"No primary notification room set for {mxid}.")
            return
        user_info = await self.db.get_user_info(mxid)
        try:
            # Parse flags and strip them from rest to mimic create_reminder syntax
            words = rest.split()
            room_flag = False
            every_flag = False
            agenda = False
            display_recur = None
            recur_every = None
            if words and words[0].lower() in [cmd.lower() for cmd in self.agenda_command]:
                agenda = True
                words = words[1:]
            elif words and words[0].lower() == 'room':
                room_flag = True
                words = words[1:]
                if words and words[0].lower() == 'every':
                    every_flag = True
                    words = words[1:]
            elif words and words[0].lower() == 'every':
                every_flag = True
                words = words[1:]
                if words and words[0].lower() == 'room':
                    room_flag = True
                    words = words[1:]
            rest_for_parse = ' '.join(words).strip()
            if agenda:
                start_time = datetime.now(tz=pytz.UTC)
                message = rest_for_parse
            else:
                if ';' in rest_for_parse:
                    parts = rest_for_parse.split(';', 1)
                    input_str = parts[0].strip()
                    message = parts[1].strip()
                elif rest_for_parse.strip():
                    start_time, date_str = parse_date(rest_for_parse, user_info, search_text=True)
                    every_flag = every_flag or rest_for_parse.lower().find('every ' + date_str.lower()) >= 0
                    compiled = re.compile("(every )?" + re.escape(date_str), re.IGNORECASE)
                    message = compiled.sub("", rest_for_parse, 1).strip()
                    input_str = date_str
                else:
                    raise CommandSyntaxError("No reminder details provided")
                if every_flag:
                    lower_input = input_str.lower()
                    pos_at = lower_input.find(' at ')
                    pos_on = lower_input.find(' on ')
                    if pos_at == -1 and pos_on == -1:
                        split_keyword = None
                        interval_str = input_str
                    else:
                        if pos_at == -1:
                            split_pos = pos_on
                            split_keyword = ' on '
                        elif pos_on == -1:
                            split_pos = pos_at
                            split_keyword = ' at '
                        else:
                            split_pos = min(pos_at, pos_on)
                            split_keyword = ' at ' if split_pos == pos_at else ' on '
                        interval_str = input_str[:split_pos].strip()
                        anchor_str = input_str[split_pos + len(split_keyword):].strip()
                    interval_str = re.sub(r'^every\s*', '', interval_str, flags=re.I).strip()
                    interval_str = normalize_time_str(interval_str)
                    if not re.search(r'\d', interval_str) and re.search(r'(second|minute|hour|day|week|month|year)s?\b', interval_str, re.I):
                        interval_str = "1 " + interval_str
                        interval_str = normalize_time_str(interval_str)
                    if not re.search(r'(second|minute|hour|day|week|month|year|monday|tuesday|wednesday|thursday|friday|saturday|sunday)s?\b', interval_str, re.I):
                        raise CommandSyntaxError("Recurring interval must include a time unit (e.g., minutes, hours, days) or a day of the week.")
                    if anchor_str:
                        start_time, _ = parse_date(anchor_str, user_info, search_text=True)
                        recur_every = interval_str
                        display_recur = interval_str + (split_keyword + anchor_str if split_keyword else "")
                    else:
                        if is_relative_date(interval_str):
                            start_time, _ = parse_date("in " + interval_str, user_info, search_text=True)
                        else:
                            start_time, _ = parse_date(interval_str, user_info, search_text=True)
                        recur_every = interval_str
                        display_recur = interval_str
                else:
                    start_time, _ = parse_date(input_str, user_info)
                    recur_every = None
                if recur_every and start_time < datetime.now(tz=pytz.UTC) + timedelta(minutes=self.config["minimum_reminder_interval_minutes"]):
                    raise CommandSyntaxError(f"Recurring interval too short. Minimum is {self.config['minimum_reminder_interval_minutes']} minutes.")
            if len(message) > self.config["reminder_character_limit"]:
                await evt.reply(f"Reminder message exceeds character limit of {self.config['reminder_character_limit']}.")
                return
            max_reminders = self.config["max_upgraded_user_reminders"] if mxid in self.upgraded_users else self.config["max_user_reminders"]
            user_count = await self.db.db.fetchval("SELECT COUNT(*) FROM reminder WHERE creator = $1", mxid)
            if user_count >= max_reminders:
                await evt.reply(f"Maximum number of reminders ({max_reminders}) exceeded for {mxid}.")
                return
            # Build msg but don't send yet
            target = "the room" if room_flag else "you"
            quoted_message = f'"{message}"' if message else "ping"
            if agenda:
                msg = f"Agenda item set: {quoted_message}"
            else:
                formatted_time = format_time(start_time, user_info, self.config['time_format'])
                if recur_every:
                    display = display_recur.capitalize() if display_recur else recur_every.capitalize()
                    msg = f"{quoted_message} running every {display}; next run at {formatted_time}"
                else:
                    msg = f"{quoted_message} running {formatted_time}."
            body = f"**Admin has set a reminder on your behalf:** {msg}\n\n*All of your reminders remain private and encrypted.*"
            html = markdown.render(body)
            content = TextMessageEventContent(msgtype=MessageType.TEXT, body=body, format=Format.HTML, formatted_body=html)
            # Create reminder first (with event_id=None)
            reminder = Reminder(
                bot=self,
                room_id=primary,
                message=message,
                event_id=None,
                reply_to=None,
                start_time=start_time,
                recur_every=recur_every,
                cron_tab=None,
                is_agenda=agenda,
                subscribed_users={},
                creator=mxid,
                user_info=user_info,
                confirmation_event=None,
            )
            reminder.validate_recur()
            if recur_every and display_recur:
                reminder.display_recur = display_recur
            # Now send
            event_id = await self.client.send_message(primary, content)
            reminder.event_id = event_id
            user_id = UserID("@room") if room_flag else mxid
            reminder.subscribed_users[event_id] = user_id
            encrypted_message = Fernet(ENCRYPTION_KEY).encrypt(reminder.message.encode()).decode() if reminder.message else ''
            async with self.database.acquire() as conn:
                async with conn.transaction():
                    await conn.execute("""
                    INSERT INTO reminder (
                    event_id,
                    room_id,
                    start_time,
                    message,
                    reply_to,
                    cron_tab,
                    recur_every,
                    is_agenda,
                    creator
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                    """,
                        reminder.event_id,
                        reminder.room_id,
                        reminder.start_time.replace(microsecond=0).isoformat() if reminder.start_time else None,
                        encrypted_message,
                        reminder.reply_to,
                        reminder.cron_tab,
                        reminder.recur_every,
                        reminder.is_agenda,
                        reminder.creator)
                    await conn.execute("""
                    INSERT INTO reminder_target (event_id, user_id, subscribing_event) VALUES ($1, $2, $3)
                    """,
                        reminder.event_id,
                        user_id,
                        event_id)
            self.reminders[reminder.event_id] = reminder
            if reminder.recur_every:
                await reminder.advance_past_missed()
            if not reminder.is_agenda:
                reminder.schedule()
            # Feedback in management mimicking confirm_reminder msg
            await evt.reply(f"Reminder set for {mxid}: {msg}")
        except CommandSyntaxError as e:
            await evt.reply(e.message)
        except Exception as e:
            await evt.reply(f"Failed to set reminder: {str(e)}")

    @command.passive(regex=r"(?:\U00002705\U0000FE0F[\U0001F3FB-\U0001F3FF]?)",
                     field=lambda evt: evt.content.relates_to.key,
                     event_type=EventType.REACTION, msgtypes=None)
    async def subscribe_react(self, evt: ReactionEvent, _: Tuple[str]) -> None:
        """
        Subscribe to a reminder by reacting with "✅️"️
        """
        reminder_id = evt.content.relates_to.event_id
        reminder = self.reminders.get(reminder_id)
        if reminder:
            await reminder.add_subscriber(user_id=evt.sender, subscribing_event=evt.event_id)


    @event.on(EventType.ROOM_REDACTION)
    async def redact(self, evt: RedactionEvent) -> None:
        """Unsubscribe from a reminder by redacting the message"""
        for key, reminder in self.reminders.items():
            if evt.redacts in reminder.subscribed_users:
                await reminder.remove_subscriber(subscribing_event=evt.redacts)

                # If the reminder has no users left, cancel it
                if not reminder.subscribed_users or reminder.event_id == evt.redacts:
                    await reminder.cancel(redact_confirmation=True)
                break


    @event.on(EventType.ROOM_TOMBSTONE)
    async def tombstone(self, evt: StateEvent) -> None:
        """If a room gets upgraded or replaced, move any reminders to the new room"""
        if evt.content.replacement_room:
            await self.db.update_room_id(old_id=evt.room_id, new_id=evt.content.replacement_room)
            
    @event.on(EventType.ROOM_CREATE)
    async def handle_room_create(self, evt: StateEvent) -> None:
        if evt.sender == self.client.mxid:
            self.newly_created_rooms.add(evt.room_id)
            
    @event.on(EventType.ROOM_MEMBER)
    async def handle_member(self, evt: StateEvent) -> None:
        management_room = self.config.get("management_room", None)
        if evt.content.membership == Membership.INVITE and evt.state_key == self.client.mxid:
            management_room = self.config.get("management_room", None)
            if management_room:
                inviter_pill = await make_pill(evt.sender, client=self.client)
                room_code = f"<code>{evt.room_id}</code>"
                html = f"Bot invited by {inviter_pill} to room {room_code}"
                plain = f"Bot invited by {evt.sender} to room {evt.room_id}"
                content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
                await self.client.send_message(RoomID(management_room), content)
        if evt.content.membership == Membership.INVITE and evt.room_id in self.newly_created_rooms:
            await self.db.set_primary_room(evt.state_key, evt.room_id)
            if evt.room_id in self.pending_invites and self.pending_invites.get(evt.state_key) == evt.room_id:
                # Handled in _handle_primary_leave task
                pass
            self.newly_created_rooms.discard(evt.room_id)  # Remove after setting, assuming single invite for DM
        if evt.content.membership == Membership.LEAVE and evt.sender == evt.state_key:
            primary_room = await self.db.get_primary_room(evt.state_key)
            prev_mem = evt.prev_content.get('membership') if evt.prev_content else None
            logger.info(f"LEAVE event for {evt.state_key} in {evt.room_id}: prev_mem={prev_mem}")
            # === Local Admin Induction: Auto-set primary room when admin leaves a 2-person room ===
            if evt.sender == self.local_admin_induction_mxid:
                async with self.room_locks[evt.room_id]:
                    for attempt in range(2):
                        try:
                            members = await self.client.get_joined_members(evt.room_id)
                            logger.info(f"Member fetch succeeded on attempt {attempt + 1} during induction")
                            break
                        except asyncpg.exceptions.UniqueViolationError as e:
                            if attempt == 0:
                                logger.warning(f"UniqueViolationError on attempt 1 during induction member check: {e}. Retrying after delay...")
                                await asyncio.sleep(0.5)
                            else:
                                logger.error(f"Persistent UniqueViolationError during induction member check: {e}. Skipping induction.")
                                members = None
                    if members and len(members) == 2:
                        other_user = next((user for user in members if user != self.client.mxid), None)
                        if other_user and other_user != self.local_admin_induction_mxid:
                            await self.db.set_primary_room(other_user, evt.room_id)
                            await self.db.set_has_joined(other_user, True)
                            await self.db.set_last_join_time(other_user, datetime.now(pytz.UTC))
                            logger.info(f"Auto-set primary room for {other_user} to {evt.room_id} via induction (admin left)")
            if primary_room and evt.room_id == primary_room:
                if prev_mem == 'invite' or (evt.room_id in self.pending_invites and self.pending_invites.get(evt.state_key) == evt.room_id):
                    # This is a reject of a pending invite
                    if evt.room_id in self.pending_invites and self.pending_invites[evt.state_key] == evt.room_id:
                        del self.pending_invites[evt.state_key]
                    user_pill = await make_pill(evt.state_key, client=self.client)
                    html = f"❌ {user_pill} rejected invite to primary room <code>{evt.room_id}</code>"
                    plain = f"❌ {evt.state_key} rejected invite to primary room {evt.room_id}"
                    content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
                    await self.client.send_message(RoomID(management_room), content)
                    self.reject_events[evt.state_key].set()
                    await self.db.set_has_joined(evt.state_key, False)
                    await self.db.set_last_join_time(evt.state_key, None)
                    logger.info(f"Reject detected for {evt.state_key} in {evt.room_id}")
                    asyncio.create_task(self._handle_primary_leave(evt.state_key, evt.room_id))
                else:
                    # This is a normal leave from the primary room (prev_mem 'join' or missing)
                    user_pill = await make_pill(evt.state_key, client=self.client)
                    html = f"‼️ {user_pill} left their primary notification room <code>{evt.room_id}</code>"
                    plain = f"‼️ {evt.state_key} left their primary notification room {evt.room_id}"
                    content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
                    await self.client.send_message(RoomID(management_room), content)
                    asyncio.create_task(self._handle_primary_leave(evt.state_key, evt.room_id))
                    await self.db.set_has_joined(evt.state_key, False)
                    await self.db.set_last_join_time(evt.state_key, None)
                    logger.info(f"Leave detected for {evt.state_key} in {evt.room_id}")
        elif evt.content.membership == Membership.JOIN:
            primary_room = await self.db.get_primary_room(evt.state_key)
            if primary_room and evt.room_id == primary_room:
                logger.info(f"JOIN detected for {evt.state_key} in {evt.room_id}: pending={evt.state_key in self.pending_invites}")
                if evt.content.membership != Membership.JOIN:
                    return
                skip = False
                if evt.unsigned.prev_content and 'membership' in evt.unsigned.prev_content:
                    prev_mem = Membership(evt.unsigned.prev_content['membership'])
                    if prev_mem == Membership.JOIN:
                        logger.info("Skipping profile update or redundant join")
                        skip = True
                now_ms = int(datetime.now(pytz.UTC).timestamp() * 1000)
                if abs(now_ms - evt.timestamp) > 60000:
                    logger.info(f"Skipping old membership event with ts {evt.timestamp}")
                    skip = True
                if skip:
                    return
                was_pending = evt.state_key in self.pending_invites and self.pending_invites[evt.state_key] == evt.room_id
                if was_pending:
                    del self.pending_invites[evt.state_key]
                    user_pill = await make_pill(evt.state_key, client=self.client)
                    html = f"✅ {user_pill} accepted invite to primary room <code>{evt.room_id}</code>"
                    plain = f"✅ {evt.state_key} accepted invite to primary room {evt.room_id}"
                    content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
                    await self.client.send_message(RoomID(management_room), content)
                self.accept_events[evt.state_key].set()
                async with self.join_locks[evt.state_key]:
                    has_joined = await self.db.get_has_joined(evt.state_key)
                    last_join = await self.db.get_last_join_time(evt.state_key)
                    now = datetime.now(pytz.UTC)
                    if not has_joined or (last_join is None or (now - last_join) > timedelta(seconds=30)):
                        await asyncio.sleep(2)
                        try:
                            members = await self.client.get_joined_members(evt.room_id)
                            if evt.state_key in members:
                                body = "You don't have to leave this room if you don't plan to set reminders right now 🙂 I'll be here if you need me!\n\n*This is an automated message. All of your reminders remain private and encrypted.*"
                                html = markdown.render(body)
                                content = TextMessageEventContent(msgtype=MessageType.TEXT, body=body, format=Format.HTML, formatted_body=html)
                                logger.info(f"Attempting to send post-join message to {evt.room_id} for {evt.state_key}")
                                await self.client.send_message(evt.room_id, content)
                                logger.info(f"Post-join message sent successfully to {evt.room_id} for {evt.state_key}")
                        except Exception as e:
                            logger.error(f"Failed to send post-join message: {e}")
                            if management_room:
                                user_pill = await make_pill(evt.state_key, client=self.client)
                                html_err = f"❓ Failed to send post-join message to {user_pill} in primary <code>{evt.room_id}</code>: {str(e)}"
                                plain_err = f"❓ Failed to send post-join message to {evt.state_key} in primary {evt.room_id}: {str(e)}"
                                err_content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain_err, format=Format.HTML, formatted_body=html_err)
                                await self.client.send_message(RoomID(management_room), err_content)
                    await self.db.set_last_join_time(evt.state_key, now)
                    await self.db.set_has_joined(evt.state_key, True)

    async def _handle_primary_leave(self, user_id: UserID, room_id: RoomID) -> None:
        async with self.user_locks[user_id]:
            while True:
                await asyncio.sleep(5)
                self.pending_invites[user_id] = room_id
                try:
                    members = await self.client.get_joined_members(room_id)
                    if user_id in members:
                        logger.info(f"Skipping invite for {user_id} to {room_id}: already joined")
                        if user_id in self.pending_invites:
                            del self.pending_invites[user_id]
                        break
                    await self.client.invite_user(room_id, user_id)
                except Exception as e:
                    logger.error(f"Failed to invite {user_id} to {room_id}: {e}")
                    if management_room := self.config.get("management_room"):
                        user_pill = await make_pill(user_id, client=self.client)
                        html_err = f"❓ Failed to invite {user_pill} to primary <code>{room_id}</code>: {str(e)}"
                        plain_err = f"❓ Failed to invite {user_id} to primary {room_id}: {str(e)}"
                        err_content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain_err, format=Format.HTML, formatted_body=html_err)
                        await self.client.send_message(RoomID(management_room), err_content)
                    break
                if management_room := self.config.get("management_room"):
                    user_pill = await make_pill(user_id, client=self.client)
                    html = f"📩 Invited {user_pill} to primary room <code>{room_id}</code>"
                    plain = f"📩 Invited {user_id} to primary room {room_id}"
                    content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
                    await self.client.send_message(RoomID(management_room), content)
                self.accept_events[user_id].clear()
                self.reject_events[user_id].clear()
                done, pending_tasks = await asyncio.wait(
                    [self.accept_events[user_id].wait(), self.reject_events[user_id].wait()],
                    timeout=60,
                    return_when=asyncio.FIRST_COMPLETED
                )
                logger.info(f"After wait for {user_id}: accept_set={self.accept_events[user_id].is_set()}, reject_set={self.reject_events[user_id].is_set()}")
                for task in pending_tasks:
                    task.cancel()
                if self.accept_events[user_id].is_set():
                    if user_id in self.pending_invites:
                        del self.pending_invites[user_id]
                    break
                elif self.reject_events[user_id].is_set():
                    if user_id in self.pending_invites:
                        del self.pending_invites[user_id]
                else:
                    if user_id in self.pending_invites:
                        del self.pending_invites[user_id]

    def _help_message(self) -> str:
        return f"""
**Origami [Reminder](https://github.com/sync85968211/origami-reminder) bot**\\
TLDR: `!{self.base_command[0]} every friday 3pm take out the trash` `!{self.base_command[0]} {self.cancel_command[0]} take out the trash`

**Creating optionally recurring reminders:**
{CommandSyntax.REMINDER_CREATE.value.format(base_command=self.base_command[0],
                                            base_aliases="|".join(self.base_command))}

**Creating agenda items**
{CommandSyntax.AGENDA_CREATE.value.format(agenda_command="|".join(self.agenda_command))}

**Listing active reminders**
{CommandSyntax.REMINDER_LIST.value.format(base_command=self.base_command[0])}

**Deleting reminders**

{CommandSyntax.REMINDER_CANCEL.value.format(base_command=self.base_command[0],
                                            cancel_command=self.cancel_command[0],
                                            cancel_aliases="|".join(self.cancel_command))}

**Rescheduling**

{CommandSyntax.REMINDER_RESCHEDULE.value.format(base_command=self.base_command[0])}

**Settings**

{CommandSyntax.REMINDER_SETTINGS.value.format(base_command=self.base_command[0],
                                              default_tz=self.db.defaults.timezone,
                                              default_locale=self.db.defaults.locale)}
"""
