# reminder - A maubot plugin to create_reminder you about things.
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
from datetime import datetime, timedelta
import pytz

from mautrix.types import (EventType, RedactionEvent, StateEvent, Format, MessageType,
                           TextMessageEventContent, ReactionEvent, UserID, EventID, RelationType)
from maubot import Plugin, MessageEvent
from maubot.handlers import command, event
from mautrix.util.async_db import UpgradeTable
from mautrix.util.config import BaseProxyConfig, ConfigUpdateHelper

from .migrations import upgrade_table
from .db import ReminderDatabase
from .util import validate_locale, validate_timezone, CommandSyntaxError, parse_date, CommandSyntax, make_pill
from .reminder import Reminder
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# TODO: merge licenses

class Config(BaseProxyConfig):
    def do_update(self, helper: ConfigUpdateHelper) -> None:
        helper.copy("default_timezone")
        helper.copy("default_locale")
        helper.copy("base_command")
        helper.copy("agenda_command")
        helper.copy("cancel_command")
        helper.copy("rate_limit_minutes")
        helper.copy("rate_limit")
        helper.copy("verbose")
        helper.copy("admin_power_level")
        helper.copy("time_format")
        helper.copy("management_room")


class ReminderBot(Plugin):
    base_command: Tuple[str, ...]
    agenda_command: Tuple[str, ...]
    cancel_command: Tuple[str, ...]
    default_timezone: pytz.timezone
    scheduler: AsyncIOScheduler
    reminders: Dict[EventID, Reminder]
    db: ReminderDatabase

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

    def on_external_config_update(self) -> None:

        self.config.load_and_update()

        def config_to_tuple(list_or_str: List | str):
            return tuple(list_or_str) if isinstance(list_or_str, list) else (list_or_str,)
        self.base_command = config_to_tuple(self.config["base_command"])
        self.agenda_command = config_to_tuple(self.config["agenda_command"])
        self.cancel_command = config_to_tuple(self.config["cancel_command"])

        # If the locale or timezone is invalid, use default one
        self.db.defaults.locale = self.config["default_locale"]
        if not validate_locale(self.config["default_locale"]):
            self.log.warning(f'unknown default locale: {self.config["default_locale"]}')
            self.db.defaults.locale = "en"
        self.db.defaults.timezone = self.config["default_timezone"]
        if not validate_timezone(self.config["default_timezone"]):
            self.log.warning(f'unknown default timezone: {self.config["default_timezone"]}')
            self.db.defaults.timezone = "UTC"


    async def stop(self) -> None:
        self.scheduler.shutdown(wait=False)



    @command.new(name=lambda self: self.base_command[0],
                 aliases=lambda self, alias: alias in self.base_command + self.agenda_command,
                 help="Create a reminder", require_subcommand=False, arg_fallthrough=False)
    @command.argument("room", matches="room", required=False)
    @command.argument("every", matches="every", required=False)
    @command.argument("start_time", matches="(.*?);", pass_raw=True, required=False)
    @command.argument("cron", matches="cron ?(?:\s*\S*){0,5}", pass_raw=True, required=False)
    @command.argument("message", pass_raw=True, required=False)
    async def create_reminder(self, evt: MessageEvent,
                              room: str = None,
                              every: str = None,
                              start_time: Tuple[str] = None,
                              cron: Tuple[str] = None,
                              message: str = None,
                              again: bool = False) -> None:
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

        # Determine is the agenda command was used instead of creating a subcommand so [room] can still be used
        agenda = evt.content.body[1:].startswith(self.agenda_command)
        if agenda:
            # Use the date the message was created as the date for agenda items
            start_time = datetime.now(tz=pytz.UTC)

        # If we are replying to a previous reminder, recreate the original reminder with a new time
        if reply_to_id:
            reply_to = await self.client.get_event(room_id=evt.room_id, event_id=reply_to_id)
            if "org.bytemarx.reminder" in reply_to.content:
                again = True
                start_time = (message,)
                message = reply_to.content["org.bytemarx.reminder"]["message"]
                reply_to_id = reply_to.content["org.bytemarx.reminder"]["reply_to"]
                event_id = reply_to.content["org.bytemarx.reminder"]["id"]
                if event_id in self.reminders:
                    await self.reminders[event_id].cancel()

        try:
            if not cron and not agenda:
                if start_time:
                    start_time, date_str = parse_date(start_time[0], user_info)
                elif message.strip(): # extract the date from the message if not explicitly given
                    start_time, date_str = parse_date(message, user_info, search_text=True)

                    # Check if "every" appears immediately before the date, if so, the reminder should be recurring.
                    # This makes "every" possible to use in a sentence instead of just with @command.argument("every")
                    if not every:
                        every = message.lower().find('every ' + date_str.lower()) >= 0

                    # Remove the date from the messages, converting "buy ice cream on monday" to "buy ice cream"
                    compiled = re.compile("(every )?" + re.escape(date_str), re.IGNORECASE)
                    message = compiled.sub("", message,  1).strip()

                else: # If no arguments are supplied, return the help message
                    await evt.reply(self._help_message())
                    return

            # If the reminder was created by replying to a message, use that message's text
            if reply_to_id and not message:
                message = reply_to.content["body"]

            reminder = Reminder(
                bot=self,
                room_id=evt.room_id,
                message=message,
                event_id=evt.event_id,
                reply_to=reply_to_id,
                start_time=start_time,
                cron_tab=cron,
                recur_every=date_str if every else None,
                is_agenda=agenda,
                creator=evt.sender,
                user_info=user_info,
            )

        except CommandSyntaxError as e:
            await evt.reply(e.message)
            return

        # Record the reminder and subscribe to it
        await self.db.store_reminder(reminder)

        # If the command was called with a "room_command", make the reminder ping the room
        user_id = UserID("@room") if room else evt.sender
        await reminder.add_subscriber(subscribing_event=evt.event_id, user_id=user_id)

        # Send a message to the room confirming the creation of the reminder
        await self.confirm_reminder(evt, reminder, again=again, agenda=agenda)

        self.reminders[reminder.event_id] = reminder


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

        if self.config["verbose"]:

            action = "add this to the agenda for" if agenda else "remind" if reminder.message else "ping"
            target = "the room" if "@room" in reminder.subscribed_users.values() else "you"
            message = f"to {reminder.message}" if reminder.message else ""

            if reminder.reply_to:
                evt_link = f"[message](https://matrix.to/#/{reminder.room_id}/{reminder.reply_to})"
                message += f" (replying to that {evt_link})" if reminder.message else f" about that {evt_link}"

            msg = f"I'll {action} {target} {message}"
            msg += " again" if again else ""

            if again:
                msg += " again"
            if not agenda:
                formatted_time = reminder.formatted_time(await self.db.get_user_info(evt.sender))
                msg += " " + formatted_time


            confirmation_event = await evt.reply(f"{msg}\n\n"
                            f"(others can \U00002705\U0000FE0F the message above to get pinged too)")

        await reminder.set_confirmation(confirmation_event)


    # @command.new("cancel", help="Cancel a recurring reminder", aliases=("delete",))
    @create_reminder.subcommand(name=lambda self: self.cancel_command[0],
                                help="Cancel a recurring reminder",
                                aliases=lambda self, alias: alias in self.cancel_command)
    @command.argument("search_text", pass_raw=True, required=False)
    async def cancel_reminder(self, evt: MessageEvent, search_text: str) -> None:
        """Cancel a reminder by replying to a reminder, or searching by either message or event ID"""

        reminder = []
        if evt.content.get_reply_to():
            reminder_message = await self.client.get_event(evt.room_id, evt.content.get_reply_to())
            if "org.bytemarx.reminder" not in reminder_message.content:
                await evt.reply("That doesn't look like a valid reminder event.")
                return
            reminder = self.reminders[reminder_message.content["org.bytemarx.reminder"]["id"]]
        elif search_text:
            # First, check the reminders created by the user, then everything else
            for rem in sorted(self.reminders.values(), key=lambda x: x.creator == evt.sender, reverse=True):
                # Using the first four base64 digits of the hash, p(collision) > 0.01 at ~10000 reminders
                if rem.event_id[1:5] == search_text or re.match(re.escape(search_text.strip()), rem.message.strip(), re.IGNORECASE):
                # if rem.event_id[1:5] == search_text or rem.message.upper().strip() == search_text.upper().strip():
                    reminder = rem
                    break
        else: # Display the help message
            await evt.reply(CommandSyntax.REMINDER_CANCEL.value.format(base_command=self.base_command[0],
                                            cancel_command=self.cancel_command[0],
                                            cancel_aliases="|".join(self.cancel_command)))
            return

        if not reminder:
            await evt.reply(f"It doesn't look like you have any reminders matching the text `{search_text}`")
            return

        power_levels = await self.client.get_state_event(room_id=reminder.room_id,event_type=EventType.ROOM_POWER_LEVELS)
        user_power = power_levels.users.get(evt.sender, power_levels.users_default)

        if reminder.creator == evt.sender or user_power >= self.config["admin_power_level"]:
            await reminder.cancel()
            await evt.reply("Reminder cancelled!") if self.config["verbose"] else await evt.react("✅️")
        else:
            await evt.reply(f"Power levels of {self.config['admin_power_level']} are required to cancel other people's reminders")


    @create_reminder.subcommand("help", help="Usage instructions")
    async def help(self, evt: MessageEvent) -> None:
        await evt.reply(self._help_message(), allow_html=True)


    @create_reminder.subcommand("list", help="List your reminders")
    @command.argument("my", parser=lambda x: (re.sub(r"\bmy\b", "", x), re.search(r"\bmy\b", x)), required=False, pass_raw=True) # I hate it but it makes arguments not positional
    @command.argument("subscribed", parser=lambda x: (re.sub(r"\bsubscribed\b", "", x), re.search(r"\bsubscribed\b", x)), required=False, pass_raw=True)
    @command.argument("all", parser=lambda x: (re.sub(r"\ball\b", "", x), re.search(r"\ball\b", x)), required=False, pass_raw=True)
    async def list(self, evt: MessageEvent, all: str, subscribed: str, my: str) -> None:
        """Print out a formatted list of all the reminders for a user

        Args:
            evt: message event
            my:  only list reminders the user created
            all: list all reminders in every room
            subscribed: only list reminders the user is subscribed to
        """
        room_id = None if all else evt.room_id
        user_info = await self.db.get_user_info(evt.sender)
        categories = {"**📜 Agenda items**": [], '**📅 Cron reminders**': [], '**🔁 Repeating reminders**': [], '**1️⃣ One-time reminders**': []}

        # Sort the reminders by their next run date and format as bullet points
        for reminder in sorted(self.reminders.values(), key=lambda x: x.job.next_run_time if x.job else datetime(2000,1,1,tzinfo=pytz.UTC)):
            if (
                    (not subscribed or any(x in reminder.subscribed_users.values() for x in [evt.sender, "@room"])) and
                    (not my or evt.sender == reminder.creator) and
                    (all or reminder.room_id == room_id)):

                message = reminder.message
                next_run = reminder.formatted_time(user_info)
                short_event_id = f"[`{reminder.event_id[1:5]}`](https://matrix.to/#/{reminder.room_id}/{reminder.event_id})"

                if reminder.reply_to:
                    evt_link = f"[event](https://matrix.to/#/{reminder.room_id}/{reminder.reply_to})"
                    message = f'{message} (replying to {evt_link})' if message else evt_link

                if reminder.cron_tab:
                    category = "**📅 Cron reminders**"
                elif reminder.recur_every:
                    category = "**🔁 Repeating reminders**"
                elif not reminder.is_agenda:
                    category = "**1️⃣ One-time reminders**"
                else:
                    category = "**📜 Agenda items**"

                room_link = f"https://matrix.to/#/{reminder.room_id}" if all else ""
                # creator_link = await make_pill(reminder.creator) if not my else ""

                categories[category].append(f"* {short_event_id + room_link} {next_run}  **{message}**")

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
            await evt.reply(f"You have no upcoming reminders{in_room_msg} :(")

        await evt.reply(output + f"\n\n`!{self.base_command[0]} list [all] [my] [subscribed]`\\"
                                 f"\n`!{self.base_command[0]} {self.cancel_command[0]} [4-letter ID or start of message]`")



    @create_reminder.subcommand("locale", help="Set your locale")
    @command.argument("locale", required=False, pass_raw=True)
    async def locale(self, evt: MessageEvent, locale: str) -> None:
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
        if not timezone:
            await evt.reply(f"Your timezone is `{(await self.db.get_user_info(evt.sender)).timezone}`")
            return
        if validate_timezone(timezone):
            await self.db.set_user_info(evt.sender, key="timezone", value=timezone)
            await evt.reply(f"Setting your timezone to {timezone}")
        else:
            await evt.reply(f"Unknown timezone: `{timezone}`\n\n"
                            f"[Available timezones](https://en.wikipedia.org/wiki/List_of_tz_database_time_zones)")


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

    def _help_message(self) -> str:
        return f"""
**Origami [Reminder](https://github.com/sync85968211/reminder) bot**\\
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
