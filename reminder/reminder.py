from __future__ import annotations
import logging
from datetime import datetime
from typing import Dict, Optional, TYPE_CHECKING

import pytz
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from mautrix.util import markdown

from mautrix.types import (Format, MessageType, TextMessageEventContent, UserID, EventID, RoomID)

from .util import CommandSyntaxError, CommandSyntax, make_pill, parse_date, UserInfo, format_time

import dateparser
import math
import re

try:
    from cron_descriptor import Options, CasingTypeEnum, DescriptionTypeEnum, ExpressionDescriptor
    USE_CRON_DESCRIPTOR = True
except ImportError:
    USE_CRON_DESCRIPTOR = False

if TYPE_CHECKING:
    from .bot import ReminderBot

logger = logging.getLogger(__name__)

class Reminder(object):
    def __init__(
        self,
        bot: ReminderBot,
        room_id: RoomID,
        message: str,
        event_id: Optional[EventID] = None,
        reply_to: Optional[EventID] = None,
        start_time: Optional[datetime] = None,
        recur_every: Optional[str] = None,
        cron_tab: Optional[str] = None,
        subscribed_users: Optional[Dict[EventID, UserID]] = None,
        creator: Optional[UserID] = None,
        user_info: Optional[UserInfo] = None,
        confirmation_event: EventID = None,
        is_agenda: bool = False,
    ):
        """An object containing information about a reminder, when it should go off,
        whether it is recurring, etc.

        Args:
            bot: instance of the main bot (this feels wrong but it works)
            room_id: The ID of the room the reminder should appear in
            message: The text to include in the reminder message
            event_id: The event ID of the message creating the reminder
            reply_to: The event ID of the message the reminder is replying to if applicable, so we can link it in the reply
            start_time: When the reminder should first go off
            recur_every: date string to parse to schedule the next recurring reminder
            cron_tab: cron text
            subscribed_users: dict of subscribed users with corresponding subscription events
            creator: ID of the creator
            user_info: contains timezone and locale for the person who created the reminder
            confirmation_event: EventID of the confirmation message. Store this so it can be redacted when the reminder is removed.
            is_agenda: Agenda items are reminders that never fire.
        """
        self.bot = bot
        self.room_id = room_id
        self.message = message
        self.event_id = event_id
        self.reply_to = reply_to
        self.start_time = start_time
        self.recur_every = recur_every
        self.cron_tab = cron_tab
        self.creator = creator
        self.subscribed_users = subscribed_users if subscribed_users else {}
        self.confirmation_event = confirmation_event
        self.is_agenda = is_agenda
        self.display_recur = recur_every
        self.user_info = user_info

        # Schedule the reminder

        # TODO add agenda
        trigger = None
        if self.recur_every:
            pass  # Validation moved to validate_recur

        if not is_agenda:
            # Determine how the reminder is triggered.
            # For both once-off and recurring reminders, user date trigger (runs once).
            # Recurring reminders are rescheduled when the job runs, so we don't need to worry about them here.

            if start_time:
                trigger = DateTrigger(run_date=start_time, timezone=user_info.timezone)

    def validate_recur(self):
        if self.recur_every:
            fixed_base = datetime(2000,1,1, tzinfo=pytz.UTC)
            settings = {'TIMEZONE': self.user_info.timezone,
                        'TO_TIMEZONE': 'UTC',
                        'PREFER_DATES_FROM': 'future',
                        'RETURN_AS_TIMEZONE_AWARE': True,
                        'RELATIVE_BASE': fixed_base}
            parsed_period = dateparser.parse("in " + self.recur_every, settings=settings)
            if not parsed_period:
                raise CommandSyntaxError(f"Invalid recurring interval '{self.recur_every}'.")
            period_seconds = (parsed_period - fixed_base).total_seconds()
            if period_seconds <= 0:
                raise CommandSyntaxError(f"Invalid non-positive recurring interval '{self.recur_every}' ({period_seconds}s).")
            if period_seconds < self.bot.config["minimum_reminder_interval_minutes"] * 60:
                raise CommandSyntaxError(f"Recurring interval '{self.recur_every}' too short ({math.ceil(period_seconds)}s). Minimum is {self.bot.config['minimum_reminder_interval_minutes']} minutes.")

    async def _fire(self, catchup: bool = False):
        """Called when a reminder fires"""
        logger.debug("Reminder in room %s fired", self.room_id)
        logger.info(f"AbusePrevention: Reminder for {self.creator} firing")

        user_info = await self.bot.db.get_user_info(self.creator)

        # If this is a recurring message, parse the date again and reschedule the reminder
        if self.recur_every and not catchup:
            start_time, _ = parse_date("in " + self.recur_every, user_info=user_info, relative_base=self.start_time, allow_past=True)
            now = datetime.now(tz=pytz.UTC)
            count = 0
            while start_time < now and count < 1000:
                start_time, _ = parse_date("in " + self.recur_every, user_info=user_info, relative_base=start_time, allow_past=True)
                count += 1
            if count >= 1000:
                logger.error(f"Infinite recurrence loop detected for reminder {self.event_id}")
            else:
                trigger = DateTrigger(run_date=start_time, timezone=user_info.timezone)
                self.job = self.bot.scheduler.add_job(self._fire, trigger=trigger, id=self.event_id)
                await self.bot.db.reschedule_reminder(start_time=start_time, event_id=self.event_id)
                self.start_time = start_time

        # Check if the user is rate limited
        reminder_count = user_info.check_rate_limit(max_calls=self.bot.config["rate_limit"],
                                                    time_window=self.bot.config["rate_limit_minutes"])

        management_room = self.bot.config.get("management_room", None)

        # Determine reminder type
        if self.is_agenda:
            reminder_type_icon = "📜"
            reminder_type_name = "agenda item"
        elif self.recur_every:
            reminder_type_icon = "🔁"
            reminder_type_name = "repeating reminder"
        else:
            reminder_type_icon = "1️⃣"
            reminder_type_name = "one-time reminder"
        reminder_type = f"{reminder_type_icon} {reminder_type_name}"

        # Build the message with the format "(users to ping) ⬆(link to the reminder): message text [next run]
        # Note: using "⬆" as a link seems to have trouble rendering in element for android, but "⬆" works and element on my PC still renders it as the emoji
        targets = list(self.subscribed_users.values())
        link = f"https://matrix.to/#/{self.room_id}/{self.event_id}"
        users = " ".join([(await make_pill(user_id=user_id, client=self.bot.client))
                          for user_id in targets])

        if self.message:
            body = f"{users}: [⬆]({link}) {self.message}"
        elif self.reply_to:
            reply_link = f"https://matrix.to/#/{self.room_id}/{self.reply_to}"
            body = f"{users}: {reply_link}"
        else:
            body = f"{users}"

        if self.recur_every:
            body += f"\n\nReminding again {self.formatted_time(user_info)}." \
                    f" Reply `!{self.bot.base_command[0]} {self.bot.cancel_command[0]}` or `!r c` to stop."

        # Create the message, and include all the data necessary to reschedule the reminder in net.origami.reminder
        content = TextMessageEventContent(
            msgtype=MessageType.TEXT, body=body, format=Format.HTML, formatted_body=markdown.render(body))
        content["net.origami.reminder"] = {"id": self.event_id,
                                          "message": self.message,
                                          "reply_to": self.reply_to}

        # Add subscribed users to MSC3952 mentions
        content["m.mentions"] = {"room": True} if "@room" in targets else {"user_ids": targets}

        if self.reply_to:
            content.set_reply(await self.bot.client.get_event(self.room_id, self.reply_to))

        try:
            await self.bot.client.send_message(self.room_id, content)
        except Exception as e:
            logger.error(f"Failed to send reminder {self.event_id} to room {self.room_id}: {e}")
            if management_room:
                user_pill = await make_pill(user_id=self.creator, client=self.bot.client)
                room_code = f"<code>{self.room_id}</code>"
                error_msg = str(e)
                html = f"❓ Reminder could not be sent by {user_pill} in room {room_code} for {reminder_type}: <code>{error_msg}</code>"
                plain = f"❓ Reminder could not be sent by {self.creator} in room {self.room_id} for {reminder_type}: `{error_msg}`"
                error_content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
                error_content["net.origami.reminder"] = {"id": self.event_id}
                try:
                    await self.bot.client.send_message(RoomID(management_room), error_content)
                except Exception as ee:
                    logger.error(f"Failed to send error notification to management room: {ee}")
            if not self.recur_every:
                await self.cancel(redact_confirmation=False)
            return  # Exit early on send failure

        if management_room:
            user_pill = await make_pill(user_id=self.creator, client=self.bot.client)
            room_code = f"<code>{self.room_id}</code>"
            if reminder_count == self.bot.config["rate_limit"]:
                html = f"Rate limit reached by {user_pill} in room {room_code} for {reminder_type}"
                plain = f"Rate limit reached by {self.creator} in room {self.room_id} for {reminder_type}"
                content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
                content["net.origami.reminder"] = {"id": self.event_id}
                await self.bot.client.send_message(RoomID(management_room), content)
            elif reminder_count > self.bot.config["rate_limit"]:
                html = f"Rate limit exceeded by {user_pill} in room {room_code} for {reminder_type}"
                plain = f"Rate limit exceeded by {self.creator} in room {self.room_id} for {reminder_type}"
                content = TextMessageEventContent(msgtype=MessageType.TEXT, body=plain, format=Format.HTML, formatted_body=html)
                content["net.origami.reminder"] = {"id": self.event_id}
                await self.bot.client.send_message(RoomID(management_room), content)

        # If this was a one-time reminder, cancel and remove from the reminders dict
        if not self.recur_every:
            # We set cancel_alarm to False here else the associated alarms wouldn't even fire
            await self.cancel(redact_confirmation=False)


    async def cancel(self, redact_confirmation: bool = False):
        """Cancels a reminder and all recurring instances

        Args:
            redact_confirmation: Whether to also redact the confirmation message
        """
        logger.debug(f"Removing reminder in room {self.room_id}")
        # Delete the reminder from the database
        await self.bot.db.delete_reminder(self.event_id)
        # Delete any ongoing jobs
        if hasattr(self, 'job') and self.job and self.bot.scheduler.get_job(self.job.id):
            self.job.remove()
        if redact_confirmation and self.confirmation_event:
            await self.bot.client.redact(self.room_id, self.confirmation_event)
        self.bot.reminders.pop(self.event_id)

    async def add_subscriber(self, user_id: UserID, subscribing_event: EventID):
        if user_id not in self.subscribed_users.values():
            self.subscribed_users[subscribing_event] = user_id
            await self.bot.db.add_subscriber(reminder_id=self.event_id,
                                             user_id=user_id,
                                             subscribing_event=subscribing_event)

    async def remove_subscriber(self, subscribing_event: EventID):
        if subscribing_event in self.subscribed_users:
            await self.bot.db.remove_subscriber(subscribing_event=subscribing_event)
            del self.subscribed_users[subscribing_event]


    def formatted_time(self, user_info: UserInfo):
        """
        Format the next run time. as
        Cron reminders are formatted using cron_descriptor,
        Args:
            user_info:
        Returns:

        """
        if self.is_agenda:
            return format_time(self.start_time, user_info=user_info, time_format=self.bot.config['time_format'])
        else:
            next_run_time = self.job.next_run_time if hasattr(self, 'job') and self.job else self.start_time
            if self.recur_every:
                # Force absolute time with seconds for next run
                tz = dateparser.utils.get_timezone_from_tz_string(user_info.timezone)
                next_run_time = next_run_time.astimezone(tz)
                hour = next_run_time.hour % 12
                if hour == 0:
                    hour = 12
                am_pm = 'AM' if next_run_time.hour < 12 else 'PM'
                time_str = f"{hour}:{next_run_time.minute:02d}:{next_run_time.second:02d} {am_pm}"
                date_format = "%Z on %A, %B %d %Y"
                date_str = next_run_time.strftime(date_format)
                date_str = re.sub(r'(^|\s)0(\d)', r'\1\2', date_str)  # Remove leading zeros only in date
                next_run = time_str + " " + date_str
                display = self.display_recur or self.recur_every
                return f"every {display}; next run at {next_run}"
            else: # once-off reminders
                return format_time(next_run_time, user_info=user_info, time_format=self.bot.config['time_format'])

    async def set_confirmation(self, confirmation_event: EventID):
        """ Set the confirmation message so that it can be redacted if the message is deleted"""
        self.confirmation_event = confirmation_event
        await self.bot.db.set_confirmation_event(event_id=self.event_id, confirmation_event=confirmation_event)
        
    def schedule(self):
        if self.is_agenda:
            return
        user_info = self.bot.db.cache.get(self.creator, UserInfo(locale=self.bot.db.defaults.locale, timezone=self.bot.db.defaults.timezone))  # Synchronous fallback; optimize if needed
        trigger = None
        if self.start_time:
            trigger = DateTrigger(run_date=self.start_time, timezone=user_info.timezone)
        self.job = self.bot.scheduler.add_job(self._fire, trigger=trigger, id=self.event_id)
        
    async def advance_past_missed(self):
        self.user_info = await self.bot.db.get_user_info(self.creator)
        now = datetime.now(tz=pytz.UTC)
        if not self.recur_every or self.start_time >= now:
            return
        previous = None
        current = self.start_time
        advanced = False
        count = 0
        while current < now and count < 1000:
            previous = current
            current, _ = parse_date("in " + self.recur_every, user_info=self.user_info, relative_base=current, allow_past=True)
            advanced = True
            count += 1
            if current <= previous:
                logger.error(f"Recurrence loop stalled for reminder {self.event_id}")
                break
        if count >= 1000:
            logger.error(f"Infinite recurrence loop detected for reminder {self.event_id}")
        if advanced:
            self.start_time = current
            await self.bot.db.reschedule_reminder(start_time=self.start_time, event_id=self.event_id)
            await self._fire(catchup=True)
