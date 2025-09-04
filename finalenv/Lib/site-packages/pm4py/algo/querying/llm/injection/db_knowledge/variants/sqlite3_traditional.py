'''
    PM4Py â€“ A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschrÃ¤nkt)

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
from sqlite3 import Connection as SQ3_Connection
from typing import Optional, Dict, Any
from pm4py.util import constants, xes_constants, exec_utils
from enum import Enum


class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_TIMESTAMP_KEY
    CASE_ID_KEY = constants.PARAMETER_CONSTANT_CASEID_KEY
    RESOURCE_KEY = constants.PARAMETER_CONSTANT_RESOURCE_KEY
    TABLE_NAME = "table_name"


def apply(
    db: SQ3_Connection, parameters: Optional[Dict[Any, Any]] = None
) -> str:
    """
    Provides a string containing the required database knowledge for SQLite3 querying
    (in order for the LLM to produce meaningful queries).

    Parameters
    ---------------
    db
        Database
    parameters
        Parameters of the method, including:
        - Parameters.ACTIVITY_KEY => the attribute of the log to be used as activity
        - Parameters.TIMESTAMP_KEY => the attribute of the log to be used as timestamp
        - Parameters.CASE_ID_KEY => the attribute of the log to be used as case identifier
        - Parameters.RESOURCE_KEY => the attribute of the log containing the resource
        - Parameters.TABLE_NAME => the name of the dataframe/table

    Returns
    ---------------
    db_knowledge
        String containing the required database knowledge.
    """
    if parameters is None:
        parameters = {}

    activity_key = exec_utils.get_param_value(
        Parameters.ACTIVITY_KEY, parameters, xes_constants.DEFAULT_NAME_KEY
    )
    timestamp_key = exec_utils.get_param_value(
        Parameters.TIMESTAMP_KEY,
        parameters,
        xes_constants.DEFAULT_TIMESTAMP_KEY,
    )
    case_id_key = exec_utils.get_param_value(
        Parameters.CASE_ID_KEY, parameters, constants.CASE_CONCEPT_NAME
    )
    resource_key = exec_utils.get_param_value(
        Parameters.RESOURCE_KEY, parameters, xes_constants.DEFAULT_RESOURCE_KEY
    )
    table_name = exec_utils.get_param_value(
        Parameters.TABLE_NAME, parameters, "dataframe"
    )

    descr = (
        """
The underlying database engine is SQLite.
The table is called """
        + table_name
        + """.
Each row of the table is corresponding to an event, along with its attributes. There is no separate table containing the process variant.
Please consider the following information: the case identifier is called '"""
        + case_id_key
        + """', the activity is stored inside the attribute '"""
        + activity_key
        + """', the timestamp is stored inside the attribute '"""
        + timestamp_key
        + """', the resource is stored inside the attribute '"""
        + resource_key
        + """'.
If you need to compute timestamp differences, please use the "julianday" function which gets the fractional number of days since noon in Greenwhich on November 24, 4714 B.C.
In SQLite, there is no STDDEV function but you need to compute it as: SQRT(AVG(col*col) - AVG(col)*AVG(col)).
The events are already sorted by timestamp, moreover you cannot use ORDER BY inside a concatenation operator.
    """
    )

    return descr
