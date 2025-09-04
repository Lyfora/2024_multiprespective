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
from typing import Dict, Any, Optional
import pandas as pd
from pm4py.objects.ocel.util import ocel_consistency
from pm4py.objects.ocel.util import filtering_utils
from pm4py.objects.log.util import dataframe_utils
from pm4py.util import pandas_utils, constants as pm4_constants


def apply(file_path: str, parameters: Optional[Dict[Any, Any]] = None) -> OCEL:
    """
    Imports an OCEL from a SQLite database using Pandas

    Parameters
    --------------
    file_path
        Path to the SQLite database
    parameters
        Parameters of the import

    Returns
    --------------
    ocel
        Object-centric event log
    """
    if parameters is None:
        parameters = {}

    import sqlite3

    conn = sqlite3.connect(file_path)

    events = pd.read_sql("SELECT * FROM EVENTS", conn)
    objects = pd.read_sql("SELECT * FROM OBJECTS", conn)
    relations = pd.read_sql("SELECT * FROM RELATIONS", conn)

    events = dataframe_utils.convert_timestamp_columns_in_df(
        events,
        timest_format=pm4_constants.DEFAULT_TIMESTAMP_PARSE_FORMAT,
        timest_columns=["ocel:timestamp"],
    )

    relations = dataframe_utils.convert_timestamp_columns_in_df(
        relations,
        timest_format=pm4_constants.DEFAULT_TIMESTAMP_PARSE_FORMAT,
        timest_columns=["ocel:timestamp"],
    )

    ocel = OCEL(
        events=events,
        objects=objects,
        relations=relations,
        parameters=parameters,
    )
    ocel = ocel_consistency.apply(ocel, parameters=parameters)
    ocel = filtering_utils.propagate_relations_filtering(
        ocel, parameters=parameters
    )

    return ocel
