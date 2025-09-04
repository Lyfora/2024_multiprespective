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
import os
import pandas as pd
from pm4py.objects.ocel.util import names_stripping
from enum import Enum
from pm4py.util import exec_utils, pandas_utils
from pm4py.objects.ocel.util import ocel_consistency
from pm4py.objects.ocel.util import filtering_utils


class Parameters(Enum):
    ENABLE_NAMES_STRIPPING = "enable_names_stripping"


def apply(ocel: OCEL, file_path: str, parameters: Optional[Dict[Any, Any]] = None):
    """
    Exports the given OCEL (OCEL 2.0) into a SQLite database.
    Automatically converts the event and object 'ocel:timestamp' fields
    to Python datetime objects to avoid SQLite binding errors.
    """
    if parameters is None:
        parameters = {}

    enable_names_stripping = exec_utils.get_param_value(Parameters.ENABLE_NAMES_STRIPPING, parameters, True)

    import sqlite3

    # If the target file already exists, remove it to avoid conflicts
    if os.path.exists(file_path):
        os.remove(file_path)

    # Enforce consistency checks and propagate filtering
    ocel = ocel_consistency.apply(ocel, parameters=parameters)
    ocel = filtering_utils.propagate_relations_filtering(ocel, parameters=parameters)

    event_id = ocel.event_id_column
    event_activity = ocel.event_activity
    event_timestamp = ocel.event_timestamp
    object_id = ocel.object_id_column
    object_type = ocel.object_type_column
    qualifier = ocel.qualifier
    changed_field = ocel.changed_field

    conn = sqlite3.connect(file_path)

    # Write the "event" table
    EVENTS = ocel.events[[event_id, event_activity]].rename(
        columns={event_id: "ocel_id", event_activity: "ocel_type"}
    )
    EVENTS = EVENTS.drop_duplicates()
    EVENTS.to_sql("event", conn, index=False)

    # Write the "object" table
    OBJECTS = ocel.objects[[object_id, object_type]].rename(
        columns={object_id: "ocel_id", object_type: "ocel_type"}
    )
    OBJECTS = OBJECTS.drop_duplicates()
    OBJECTS.to_sql("object", conn, index=False)

    # Prepare event and object type mappings
    event_types = sorted(pandas_utils.format_unique(EVENTS["ocel_type"].unique()))
    object_types = sorted(pandas_utils.format_unique(OBJECTS["ocel_type"].unique()))

    EVENT_CORR_TYPE = pandas_utils.instantiate_dataframe(
        {"ocel_type": event_types, "ocel_type_map": event_types}
    )
    OBJECT_CORR_TYPE = pandas_utils.instantiate_dataframe(
        {"ocel_type": object_types, "ocel_type_map": object_types}
    )

    # Optionally strip names
    if enable_names_stripping:
        EVENT_CORR_TYPE["ocel_type_map"] = EVENT_CORR_TYPE["ocel_type_map"].apply(
            lambda x: names_stripping.apply(x)
        )
        OBJECT_CORR_TYPE["ocel_type_map"] = OBJECT_CORR_TYPE["ocel_type_map"].apply(
            lambda x: names_stripping.apply(x)
        )

    EVENT_CORR_TYPE.to_sql("event_map_type", conn, index=False)
    OBJECT_CORR_TYPE.to_sql("object_map_type", conn, index=False)

    # Write the event-object relationships
    E2O = ocel.relations[[event_id, object_id, qualifier]].rename(
        columns={event_id: "ocel_event_id", object_id: "ocel_object_id", qualifier: "ocel_qualifier"}
    )
    E2O.to_sql("event_object", conn, index=False)

    # Write the object-object relationships
    O2O = ocel.o2o.rename(
        columns={
            object_id: "ocel_source_id",
            object_id + "_2": "ocel_target_id",
            qualifier: "ocel_qualifier",
        }
    )
    O2O.to_sql("object_object", conn, index=False)

    # Export event tables by activity
    e_types = sorted(pandas_utils.format_unique(ocel.events[event_activity].unique()))
    for act in e_types:
        df = ocel.events[ocel.events[event_activity] == act].dropna(how="all", axis="columns")
        # Rename timestamp column
        df = df.rename(columns={event_id: "ocel_id", event_timestamp: "ocel_time"})

        # Force ID to string
        df["ocel_id"] = df["ocel_id"].astype("string")

        # Convert 'ocel_time' to Python datetime (if it's a pandas Timestamp or string)
        df["ocel_time"] = pd.to_datetime(df["ocel_time"], errors="coerce")

        act_red = names_stripping.apply(act) if enable_names_stripping else act

        df = df.drop_duplicates()
        df.to_sql("event_" + act_red, conn, index=False)

    # Export object tables by type
    o_types = sorted(pandas_utils.format_unique(ocel.objects[object_type].unique()))
    for ot in o_types:
        df = ocel.objects[ocel.objects[object_type] == ot].dropna(how="all", axis="columns")
        df = df.rename(columns={object_id: "ocel_id"})
        del df[object_type]

        # Object changes table
        df2 = ocel.object_changes[ocel.object_changes[object_type] == ot].dropna(how="all", axis="columns")
        if len(df2) > 0:
            del df2[object_type]
            df2 = df2.rename(
                columns={
                    object_id: "ocel_id",
                    event_timestamp: "ocel_time",
                    changed_field: "ocel_changed_field",
                }
            )

            # Convert 'ocel_time' to Python datetime (if it's a pandas Timestamp or string)
            df2["ocel_time"] = pd.to_datetime(df2["ocel_time"], errors="coerce")

            df = pandas_utils.concat([df, df2], axis=0)

        df["ocel_id"] = df["ocel_id"].astype("string")

        ot_red = names_stripping.apply(ot) if enable_names_stripping else ot

        df.to_sql("object_" + ot_red, conn, index=False)

    conn.close()
