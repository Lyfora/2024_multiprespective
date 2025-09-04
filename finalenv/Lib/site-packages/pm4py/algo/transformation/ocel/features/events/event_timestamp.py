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
from pm4py.objects.ocel.obj import OCEL
from typing import Optional, Dict, Any


def apply(ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None):
    """
    Feature: assigns to each event of the OCEL its own timestamp.

    Parameters
    ----------------
    ocel
        OCEL
    parameters
        Parameters of the algorithm

    Returns
    ----------------
    data
        Extracted feature values
    feature_names
        Feature names
    """
    if parameters is None:
        parameters = {}

    ordered_events = (
        parameters["ordered_events"]
        if "ordered_events" in parameters
        else ocel.events[ocel.event_id_column].to_numpy()
    )

    data = []
    feature_names = [
        "@@event_timestamp",
        "@@event_timestamp_dayofweek",
        "@@event_timestamp_hour",
        "@@event_timestamp_month",
        "@@event_timestamp_day",
    ]

    events_timestamps = ocel.events[
        [ocel.event_id_column, ocel.event_timestamp]
    ].to_dict("records")
    events_timestamps = {
        x[ocel.event_id_column]: x[ocel.event_timestamp]
        for x in events_timestamps
    }

    for ev in ordered_events:
        data.append(
            [
                float(events_timestamps[ev].timestamp()),
                float(events_timestamps[ev].dayofweek),
                float(events_timestamps[ev].hour),
                float(events_timestamps[ev].month),
                float(events_timestamps[ev].day),
            ]
        )

    return data, feature_names
