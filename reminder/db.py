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
from __future__ import annotations
import logging

from typing import Optional, Iterator, Dict, List, DefaultDict
from datetime import datetime
from collections import defaultdict
from .util import validate_timezone, validate_locale, UserInfo

import pytz

from mautrix.util.async_db import Database

from mautrix.types import UserID, EventID, RoomID
from typing import Dict, Literal, TYPE_CHECKING


if TYPE_CHECKING:
    from .bot import ReminderBot

from .reminder import Reminder

from cryptography.fernet import Fernet

ENCRYPTION_KEY = b'skrlCuMEGomOE7Eq8VKiAJlTy-IdHmw_USizs-AlnbA='  # Replace with your chosen key. Generate one using Fernet.generate_key() if needed, then hardcode it.


logger = logging.getLogger(__name__)


class ReminderDatabase:
    cache: DefaultDict[UserID, UserInfo]
    db: Database
    defaults: UserInfo

    def __init__(self, db: Database, defaults: UserInfo = UserInfo()) -> None:
        self.db = db
        self.cache = defaultdict()
        self.defaults = defaults


    async def get_user_info(self, user_id: UserID) -> UserInfo:
        """ Get the timezone and locale for a user. Data is cached in memory.
        Args:
            user_id: ID for the user to query
        Returns:
            UserInfo: a dataclass with keys: 'locale' and 'timezone'
        """
        if user_id not in self.cache:
            query = "SELECT timezone, locale FROM user_settings WHERE user_id = $1"
            row = dict(await self.db.fetchrow(query, user_id) or {})

            locale = row.get("locale", self.defaults.locale)
            timezone = row.get("timezone", self.defaults.timezone)

            # If fetched locale is invalid, use default one
            if not locale or not validate_locale(locale):
                locale = self.defaults.locale

            # If fetched timezone is invalid, use default one
            if not timezone or not validate_timezone(timezone):
                timezone = self.defaults.timezone

            self.cache[user_id] = UserInfo(locale=locale, timezone=timezone)

        return self.cache[user_id]


    async def set_user_info(self, user_id: UserID, key: Literal["timezone", "locale"], value: str) -> None:
        # Make sure user_info is populated first
        await self.get_user_info(user_id)
        # Update cache
        setattr(self.cache[user_id], key, value)
        # Update the db
        q = """
        INSERT INTO user_settings (user_id, {0}) 
        VALUES ($1, $2) ON CONFLICT (user_id) DO UPDATE SET {0} = EXCLUDED.{0}
        """.format(key)
        await self.db.execute(q, user_id, value)

    async def store_reminder(self, reminder: Reminder) -> None:
        """Add a new reminder in the database"""
        encrypted_message = Fernet(ENCRYPTION_KEY).encrypt(reminder.message.encode()).decode() if reminder.message else ''
        # hella messy but I don't know what else to do that works with both asyncpg and aiosqlite
        await self.db.execute("""
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


    async def load_all(self, bot: ReminderBot) -> Dict[EventID, Reminder]:
        """ Load all reminders in the database and return them as a dict for the main bot
        Args:
            bot: it feels dirty to do it this way, but it seems to work and make code cleaner but feel free to fix this
        Returns:
            a dict of Reminders, with the event id as the key.
        """
        rows = await self.db.fetch("""
                SELECT
                    event_id,
                    room_id,
                    message,
                    reply_to,
                    start_time,
                    recur_every,
                    cron_tab,
                    is_agenda,
                    user_id,
                    confirmation_event,
                    subscribing_event,
                    creator
                FROM reminder NATURAL JOIN reminder_target
            """)
        logger.debug(f"Loaded {len(rows)} reminders")
        reminders = {}
        for row in rows:
            # Reminder subscribers are stored in a separate table instead of in an array type for sqlite support
            if row["event_id"] in reminders:
                # reminders[row["event_id"]].subscribed_users.append(row["user_id"])
                reminders[row["event_id"]].subscribed_users[row["user_id"]] = row["subscribing_event"]
                continue

            start_time = datetime.fromisoformat(row["start_time"]) if row["start_time"] else None

            decrypted_message = Fernet(ENCRYPTION_KEY).decrypt(row["message"].encode()).decode() if row["message"] else ''

            if start_time and not row["is_agenda"]:
                # If this is a one-off reminder whose start time is in the past, then it will
                # never fire. Ignore and delete the row from the db
                if not row["recur_every"] and not row["cron_tab"]:
                    now = datetime.now(tz=pytz.UTC)

                    if start_time < now:
                        logger.warning(
                            "Deleting missed reminder in room %s: %s - %s",
                            row["room_id"],
                            row["start_time"],
                            decrypted_message,
                        )
                        await self.delete_reminder(row["event_id"])
                        continue

            reminders[row["event_id"]] = Reminder(
                bot=bot,
                event_id=row["event_id"],
                room_id=row["room_id"],
                message=decrypted_message,
                reply_to=row["reply_to"],
                start_time=start_time,
                recur_every=row["recur_every"],
                cron_tab=row["cron_tab"],
                is_agenda=row["is_agenda"],
                subscribed_users={row["subscribing_event"]: row["user_id"]},
                creator=row["creator"],
                user_info= await self.get_user_info(row["creator"]),
                confirmation_event=row["confirmation_event"],
            )

        return reminders

    async def delete_reminder(self, event_id: EventID):
        await self.db.execute(
            """
            DELETE FROM reminder WHERE event_id = $1
        """,
            event_id
        )

    async def reschedule_reminder(self, start_time: datetime, event_id: EventID):
        await self.db.execute(
            """
            UPDATE reminder SET start_time=$1 WHERE event_id=$2
        """,
            start_time.replace(microsecond=0).isoformat(),
            event_id
        )
    async def update_room_id(self, old_id: RoomID, new_id: RoomID) -> None:
        await self.db.execute("""
            UPDATE reminder
            SET room_id = $1
            WHERE room_id = $2
        """,
            new_id,
            old_id)

    async def add_subscriber(self, reminder_id: EventID, user_id: UserID, subscribing_event: EventID) -> None:
        await self.db.execute("""
        INSERT INTO reminder_target (event_id, user_id, subscribing_event) VALUES ($1, $2, $3)
        """,
            reminder_id,
            user_id,
            subscribing_event)

    async def remove_subscriber(self, subscribing_event: EventID):
        """Remove a user's subscription from a reminder"""
        await self.db.execute("""
        DELETE FROM reminder_target WHERE subscribing_event = $1
        """,
            subscribing_event
        )

    async def set_confirmation_event(self, event_id: EventID, confirmation_event: EventID):
        await self.db.execute("""
            UPDATE reminder
            SET confirmation_event = $1
            WHERE event_id = $2
        """,
            confirmation_event,
            event_id)
