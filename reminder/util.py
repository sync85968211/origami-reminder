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

import re
from itertools import islice
from collections import deque

from typing import Optional, Dict, List, Tuple, TYPE_CHECKING
from datetime import datetime, timedelta
from attr import dataclass
from dateparser.search import search_dates
import dateparser
import logging
import pytz
import re
from enum import Enum
from dataclasses import dataclass, field

from maubot.client import MaubotMatrixClient
from mautrix.types import UserID, RoomID, EventID
from maubot.handlers.command import  ArgumentSyntaxError

if TYPE_CHECKING:
    from .reminder import Reminder

logger = logging.getLogger(__name__)

def normalize_time_str(time_str: str) -> str:
    """Normalize time interval strings by expanding abbreviations and handling pluralization."""
    # First, handle 'w' to 'wk' (with optional space)
    time_str = re.sub(r'(\b\d+)\s?w\b', r'\1 wk', time_str, flags=re.I)
    
    # Define a helper for pluralization
    def plural_unit(num: int, singular: str, plural: str = None) -> str:
        if plural is None:
            plural = singular + 's'
        return f"{num} {singular if num == 1 else plural}"
    
    # Seconds
    time_str = re.sub(r'(\d+)\s*(s|sec|secs?|seconds?)\b', lambda m: plural_unit(int(m.group(1)), 'second'), time_str, flags=re.I)
    # Minutes
    time_str = re.sub(r'(\d+)\s*(m|min|mins?|minutes?)\b', lambda m: plural_unit(int(m.group(1)), 'minute'), time_str, flags=re.I)
    # Hours
    time_str = re.sub(r'(\d+)\s*(h|hr|hrs?|hours?)\b', lambda m: plural_unit(int(m.group(1)), 'hour'), time_str, flags=re.I)
    # Days
    time_str = re.sub(r'(\d+)\s*(d|days?)\b', lambda m: plural_unit(int(m.group(1)), 'day'), time_str, flags=re.I)
    # Weeks
    time_str = re.sub(r'(\d+)\s*(wk|wks?|weeks?)\b', lambda m: plural_unit(int(m.group(1)), 'week'), time_str, flags=re.I)
    # Months
    time_str = re.sub(r'(\d+)\s*(mo|mon|mons?|months?)\b', lambda m: plural_unit(int(m.group(1)), 'month'), time_str, flags=re.I)
    # Years
    time_str = re.sub(r'(\d+)\s*(y|yr|yrs?|years?)\b', lambda m: plural_unit(int(m.group(1)), 'year'), time_str, flags=re.I)
    
    return time_str.strip()

def check_long_numbers(s: str, max_digits: int = 10) -> bool:
    numbers = re.findall(r'\d+', s)
    for num in numbers:
        if len(num) > max_digits:
            return True
    return False

class CommandSyntax(Enum):
    REMINDER_CREATE = """
`!{base_aliases} <date> <message>` Adds a reminder
* `!{base_command} 8 hours buy more pumpkins`
* `!{base_command} 2023-11-30 15:00 befriend rats`
* `!{base_command} abolish closed-access journals at 3pm tomorrow`
* `July 2`, `tuesday at 2pm`, `8pm`, `20 days`, `4d`, `2wk`, ...
* Dates don't need to be at the beginning of the string, but parsing works better if they are.

`!{base_command} [room] [every] ...`
* `[room]` pings the whole room
* `[every]` create recurring reminders:
  * `!{base_command} every 1h; take out the trash` (fires in 1 hour, then every hour thereafter)
  * `!{base_command} every 1 minute at 11:00; check the oven` (fires every minute starting at 11:00)
  * `!{base_command} every wednesday at 11:00; team meeting` (fires every Wednesday at 11:00)
  * `!{base_command} every 5h on wednesday at 6:00; test` (fires every 5 hours starting on Wednesday at 6:00)
  * `!{base_command} every 5h on 2026-11-15 at 5:00; test` (fires every 5 hours starting on 2026-11-15 at 5:00)
  * `!{base_command} every 5h on january 15 at 5:00; test` (fires every 5 hours starting on January 15 at 5:00)
* Supported units: seconds (s/sec), minutes (m/min), hours (h/hr), days (d/day), weeks (w/wk), months (mo/mon/month), years (y/yr). Plural forms and abbreviations are fine.

You can also reply to any message with `!{base_command} ...` to get reminded about that message.\\
To get pinged by someone else's reminder, react to their message with ✅️.
"""

    AGENDA_CREATE = """
`!{agenda_command} [room] <message>` creates an agenda item. Agenda items are like reminders but don't have a time, for things like to-do lists.
    """

    REMINDER_LIST = """
`!{base_command} list [my] [subscribed]` lists all reminders in a room 
* `my` lists only reminders you created
* `subscribed` lists only reminders you are subscribed to
    """

    REMINDER_CANCEL = """
Cancel reminders by removing the message creating it, unsubscribe by removing your checkmark.\\
Cancel recurring reminders by replying with `!{base_command} {cancel_aliases}` 
* `!{base_command} {cancel_aliases} <ID>` deletes a reminder matching the 4 letter ID shown by `list`
* `!{base_command} {cancel_aliases} <message>` deletes a reminder **beginning with** <message>
    * e.g. `!remind {cancel_command} buy more` would delete the reminder `buy more pumpkins`
"""

    REMINDER_RESCHEDULE = """
Reminders can be rescheduled by replying to the ping with `!{base_command} <new_date>`
"""

    REMINDER_SETTINGS = """
Dates are parsed using your [timezone](https://en.wikipedia.org/wiki/List_of_tz_database_time_zone) and [locale](https://dateparser.readthedocs.io/en/latest/supported_locales.html).
Defaults are `{default_tz}` and `{default_locale}`
* `!{base_command} tz|timezone [new-timezone]` view or set your timezone
* `!{base_command} locale [new-locale]` view or set your locale
"""

    PARSE_DATE_EXAMPLES = "Examples: `Tuesday at noon`, `2032-11-30 10:15 pm`, `July 2`, `6 hours`, `8pm`, `4d`, `2wk`"


@dataclass
class UserInfo:
    locale: str = None
    timezone: str = None
    last_reminders: deque = field(default_factory=deque)

    def check_rate_limit(self, max_calls=5, time_window=60) -> int:
        """ Implement a sliding window rate limit on the number of reminders per user
        Args:
            max_calls:
            time_window: moving window size in minutes
        Returns:
            The number of calls within the sliding window
        """
        now = datetime.now(pytz.UTC)
        # remove timestamps outside the sliding window
        while len(self.last_reminders) and self.last_reminders[0] + timedelta(minutes=time_window) < now:
            self.last_reminders.popleft()
        self.last_reminders.append(now)
        return len(self.last_reminders)

class CommandSyntaxError(ValueError):
    def __init__(self, message: str, command: CommandSyntax | None = None):
        """ Format error messages with examples """
        super().__init__(f"{message}")

        if command:
            message += "\n\n" + command.value
        self.message = message

def validate_timezone(tz: str) -> bool | str:
    try:
        return dateparser.utils.get_timezone_from_tz_string(tz).tzname(None)
    except pytz.UnknownTimeZoneError:
        return False

def validate_locale(locale: str):
    try:
        return dateparser.languages.loader.LocaleDataLoader().get_locale(locale)
    except ValueError:
        return False

def is_relative_date(s: str) -> bool:
    return bool(re.match(r'^\d+\s+\w+(s)?$', s.strip()))

def parse_date(str_with_time: str, user_info: UserInfo, search_text: bool=False, relative_base: Optional[datetime] = None, allow_past: bool = False) -> Tuple[datetime, str]:
    """
    Extract the date from a string.

    Args:
        str_with_time: A natural language string containing a date.
        user_info: contains locale and timezone to search within.
        search_text:
            if True, search for the date in str_with_time e.g. "Make tea in 4 hours".
            if False, expect no other text within str_with_time.

    Returns:
        date (datetime): The datetime of the parsed date.
        date_str (str): The string containing just the parsed date,
                e.g. "4 hours" for str_with_time="Make tea in 4 hours".
    """

    # Until dateparser makes it so locales can be used in the searcher, use this to get date order
    date_order = validate_locale(user_info.locale).info["date_order"]

    str_with_time = normalize_time_str(str_with_time)
    if check_long_numbers(str_with_time):
        raise CommandSyntaxError("Number in date string exceeds 10 digits.")

    settings = {'TIMEZONE': user_info.timezone,
                'TO_TIMEZONE': 'UTC',
                'DATE_ORDER': date_order,
                'PREFER_DATES_FROM': 'future',
                'RETURN_AS_TIMEZONE_AWARE': True}
    if relative_base:
        settings['RELATIVE_BASE'] = relative_base

    # dateparser.parse is more reliable than search_dates. If the date is at the beginning of the message,
    # try dateparser.parse on the first 8 words and use the date from the longest sequence that successfully parses.
    date = []
    date_str = []
    for i in reversed(list(islice(re.finditer(r"\S+", str_with_time), 8))):
        extracted_date = dateparser.parse(str_with_time[:i.end()], locales=[user_info.locale], settings=settings)
        if extracted_date:
            date = extracted_date
            date_str = str_with_time[:i.end()]
            break

    # If the above doesn't work or the date isn't at the beginning of the string, fallback to search_dates
    if not date:
        extracted_date = search_dates(str_with_time, languages=[user_info.locale.split('-')[0]], settings=settings)
        if extracted_date:
            date_str, date = extracted_date[0]

    if not date:
        raise CommandSyntaxError("Unable to extract date from string", CommandSyntax.PARSE_DATE_EXAMPLES)

    # Preserve original for substitution
    original_date_str = date_str

    # Remove leading "in " if present (common in relative parsed strings)
    stripped_in = False
    if isinstance(date_str, str) and date_str.lower().startswith("in "):
        date_str = date_str[3:].strip()
        stripped_in = True

    # For substitution, include "in " if it was stripped
    date_str_for_sub = "in " + date_str if stripped_in else original_date_str

    # Round datetime object to the nearest second for nicer display
    date = date.replace(microsecond=0)

    # Disallow times in the past
    if date < datetime.now(tz=pytz.UTC) and not allow_past:
        raise CommandSyntaxError(f"Sorry, `{format_time(date, user_info)}` is in the past and I don't have a time machine (yet...)")

    return date, date_str_for_sub

def pluralize(val: int, unit: str) -> str:
    if val == 1:
        return f"{val} {unit}"
    return f"{val} {unit}s"

def format_time(time: datetime, user_info: UserInfo, time_format: str = "%I:%M:%S%p %Z on %A, %B %d %Y") -> str:
    """
    Format time as something readable by humans.
    Args:
        time: datetime to format
        user_info: contains locale and timezone
        time_format:
    Returns:

    """
    now = datetime.now(tz=pytz.UTC).replace(microsecond=0)
    delta = abs(time - now)

    # If the date is coming up in less than a week, print the two most significant figures of the duration
    if abs(delta) <= timedelta(days=7):
            parts = []
            if delta.days > 0:
                parts.append(pluralize(delta.days, "day"))
            hours, seconds = divmod(delta.seconds, 60)
            hours, minutes = divmod(hours, 60)
            if hours > 0:
                parts.append(pluralize(hours, "hour"))
            if minutes > 0:
                parts.append(pluralize(minutes, "minute"))
            if seconds > 0:
                parts.append(pluralize(seconds, "second"))

            formatted_time = " and ".join(parts[0:2])
            if time > now:
                formatted_time = "in " + formatted_time
            else:
                formatted_time = formatted_time + " ago"
    else:
        formatted_time = time.astimezone(
        dateparser.utils.get_timezone_from_tz_string(user_info.timezone)).strftime(time_format)
        formatted_time = re.sub(r'(^|\s)0(\d)', r'\1\2', formatted_time)
    return formatted_time


async def make_pill(user_id: UserID, display_name: str = None, client: MaubotMatrixClient | None = None) -> str:
    """Convert a user ID (and optionally a display name) to a formatted user 'pill'

    Args:
        user_id: The MXID of the user.
        display_name: An optional display name. Clients like Element will figure out the
            correct display name no matter what, but other clients may not.
        client: mautrix client so get the display name.
    Returns:
        The formatted user pill.
    """
    # Use the user ID as the display_name if not provided
    if client and not display_name:
        if user_id == "@room":
            return '@room'
        else:
            display_name = await client.get_displayname(user_id)

    display_name = display_name or user_id

    # return f'<a href="https://matrix.to/#/{user_id}">{display_name}</a>'
    return f'[{display_name}](https://matrix.to/#/{user_id})'
