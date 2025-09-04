'''
    PM4Py – A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschränkt)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see this software project's root or
visit <https://www.gnu.org/licenses/>.

Website: https://processintelligence.solutions
Contact: info@processintelligence.solutions
'''
import math
from datetime import timedelta, datetime, time
from typing import List, Tuple

from pm4py.util import constants
from pm4py.util.dt_parsing.variants import strpfromiso


class BusinessHours:
    def __init__(self, datetime1, datetime2, **kwargs):
        # Remove timezone info for simplicity (assumes same timezone)
        self.datetime1 = datetime1.replace(tzinfo=None)
        self.datetime2 = datetime2.replace(tzinfo=None)
        # Use provided business hour slots or default
        self.business_hour_slots = (
            kwargs["business_hour_slots"]
            if "business_hour_slots" in kwargs
            else constants.DEFAULT_BUSINESS_HOUR_SLOTS
        )
        # Unify slots to avoid overlaps
        self.business_hour_slots_unified = []
        for begin, end in sorted(self.business_hour_slots):
            if self.business_hour_slots_unified and self.business_hour_slots_unified[-1][1] >= begin - 1:
                self.business_hour_slots_unified[-1][1] = max(self.business_hour_slots_unified[-1][1], end)
            else:
                self.business_hour_slots_unified.append([begin, end])
        # Work calendar (unused in this implementation)
        self.work_calendar = (
            kwargs["work_calendar"]
            if "work_calendar" in kwargs
            else constants.DEFAULT_BUSINESS_HOURS_WORKCALENDAR
        )

    def business_seconds_from_week_start(self, dt):
        """Calculate business seconds from the start of the week to the given datetime."""
        week_start = dt.date() - timedelta(days=dt.weekday())  # Monday 00:00
        seconds_since_week_start = (dt - datetime.combine(week_start, time.min)).total_seconds()
        sum_overlap = 0
        for start, end in self.business_hour_slots_unified:
            # Overlap is max(0, min(end, seconds) - start)
            sum_overlap += max(0, min(seconds_since_week_start, end) - start)
        return sum_overlap

    def get_seconds(self):
        """Calculate total business seconds between datetime1 and datetime2."""
        if self.datetime2 <= self.datetime1:
            return 0.0

        # Total business seconds in a full week
        total_business_seconds_per_week = sum(end - start for start, end in self.business_hour_slots_unified)

        # Week starts (Monday 00:00)
        week_start1 = self.datetime1.date() - timedelta(days=self.datetime1.weekday())
        week_start2 = self.datetime2.date() - timedelta(days=self.datetime2.weekday())

        # Number of weeks between week starts
        number_of_weeks = (week_start2 - week_start1).days // 7

        # Business seconds from week start to each datetime
        s1 = self.business_seconds_from_week_start(self.datetime1)
        s2 = self.business_seconds_from_week_start(self.datetime2)

        # Total = full weeks + partial week at end - partial week at start
        total = total_business_seconds_per_week * number_of_weeks + s2 - s1
        return total


def soj_time_business_hours_diff(
    st: datetime,
    et: datetime,
    business_hour_slots: List[Tuple[int]],
    work_calendar=constants.DEFAULT_BUSINESS_HOURS_WORKCALENDAR,
) -> float:
    """
    Calculates the difference between the provided timestamps based on business hours.

    Parameters
    ----------
    st : datetime
        Start timestamp
    et : datetime
        End timestamp
    business_hour_slots : List[Tuple[int]]
        Work schedule as list of tuples (start, end) in seconds since week start
    work_calendar
        Work calendar (unused in this implementation)

    Returns
    -------
    float
        Difference in business hours (seconds)
    """
    bh = BusinessHours(
        st,
        et,
        business_hour_slots=business_hour_slots,
        work_calendar=work_calendar,
    )
    return bh.get_seconds()