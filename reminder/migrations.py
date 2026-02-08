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
from cryptography.fernet import Fernet

ENCRYPTION_KEY = b'skrlCuMEGomOE7Eq8VKiAJlTy-IdHmw_USizs-AlnbA='  # Must match the key in db.py

from mautrix.util.async_db import Connection, Scheme, UpgradeTable

upgrade_table = UpgradeTable()


@upgrade_table.register(description="Latest revision")
async def upgrade_v1(conn: Connection, scheme: Scheme) -> None:
    await conn.execute(
        f"""CREATE TABLE IF NOT EXISTS reminder (
            event_id    VARCHAR(255) NOT NULL,  /* event_id of the message that created the reminder */
            room_id     VARCHAR(255) NOT NULL,  /* room_id for this reminder */
            start_time  TEXT,                   /* time for one-off reminders, stored as an ISO 8601 string because sqlite doesn't have date types */
            message     TEXT,                   /* message for the reminder */
            reply_to    VARCHAR(255),           /* if the reminder created as a reply to another message, this is that message's event_id */
            cron_tab    TEXT,                   /* cron string if it's a cron reminder */
            creator     VARCHAR(255),           /* user_id of the person who created the reminder */
            recur_every TEXT,                   /* string to parse to schedule the next recurring reminder, e.g. 'tuesday at 2pm' */
            is_agenda   BOOL,                   /* agendas are alarms that don't trigger */
            confirmation_event TEXT,            /* event_id of the confirmation message, so that we can delete the confirmation if the reminder is deleted */
            PRIMARY KEY (event_id)
            
        )"""
    )

    await conn.execute(
        f"""CREATE TABLE IF NOT EXISTS reminder_target (
            event_id            VARCHAR(255) NOT NULL,  /* event_id in the reminder table */
            user_id             VARCHAR(255) NOT NULL,  /* user_id of the subscriber */
            subscribing_event   VARCHAR(255) NOT NULL,  /* event_id of the event creating the subscription, either a ✅️ or the reminder message itself */
            PRIMARY KEY (user_id, event_id),
            FOREIGN KEY (event_id) REFERENCES reminder (event_id) ON DELETE CASCADE
        )"""
    )

    await conn.execute(
        f"""CREATE TABLE IF NOT EXISTS user_settings (
            user_id     VARCHAR(255) NOT NULL,  /* user_id */
            timezone    TEXT,                   /* user's timezone, e.g. America/Los_Angeles, PST. see https://en.wikipedia.org/wiki/List_of_tz_database_time_zones */
            locale      TEXT,                   /* user's locale or langauge, e.g. en, en-AU, fr, fr-CA. See https://dateparser.readthedocs.io/en/latest/supported_locales.html */
            PRIMARY KEY (user_id)
        )"""
    )
    
@upgrade_table.register(description="Add user primary notification room table")
async def upgrade_v3(conn: Connection, scheme: Scheme) -> None:
    await conn.execute(
        f"""CREATE TABLE IF NOT EXISTS user_primary_room (
            user_id     VARCHAR(255) NOT NULL,  /* user_id */
            room_id     VARCHAR(255) NOT NULL,  /* primary notification room_id */
            PRIMARY KEY (user_id)
        )"""
    )

@upgrade_table.register(description="Add has_joined column")
async def upgrade_v4(conn: Connection, scheme: Scheme) -> None:
    await conn.execute(
        f"""ALTER TABLE user_primary_room ADD COLUMN has_joined BOOL DEFAULT FALSE
        """
    )
    
@upgrade_table.register(description="Add last_join_time column")
async def upgrade_v5(conn: Connection, scheme: Scheme) -> None:
    await conn.execute(
        f"""ALTER TABLE user_primary_room ADD COLUMN last_join_time TEXT
        """
    )
    
@upgrade_table.register(description="Encrypt existing reminder messages")
async def upgrade_v2(conn: Connection, scheme: Scheme) -> None:
    rows = await conn.fetch("SELECT event_id, message FROM reminder WHERE message IS NOT NULL AND message != ''")
    fernet = Fernet(ENCRYPTION_KEY)
    for row in rows:
        try:
            # Attempt decryption first to check if already encrypted; if it succeeds, skip.
            fernet.decrypt(row['message'].encode())
        except:
            encrypted = fernet.encrypt(row['message'].encode()).decode()
            await conn.execute("UPDATE reminder SET message = $1 WHERE event_id = $2", encrypted, row['event_id'])
