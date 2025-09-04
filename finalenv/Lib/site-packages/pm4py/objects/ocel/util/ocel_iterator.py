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
from enum import Enum
from pm4py.util import exec_utils
from pm4py.objects.ocel import constants as ocel_constants
import pandas as pd


class Parameters(Enum):
    OCEL_TYPE_PREFIX = ocel_constants.PARAM_OBJECT_TYPE_PREFIX_EXTENDED


def apply(ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None):
    """
    Creates an iterator over the events of an object-centric event log

    Parameters
    ----------------
    ocel
        OCEL
    parameters
        Parameters of the method, including:
        - Parameters.OCEL_TYPE_PREFIX => the prefix of the object types in the OCEL (default: ocel:type)

    Returns
    ----------------
    yielded event
        The events of the OCEL, one by one.
    """
    if parameters is None:
        parameters = {}

    ot_prefix = exec_utils.get_param_value(
        Parameters.OCEL_TYPE_PREFIX,
        parameters,
        ocel_constants.DEFAULT_OBJECT_TYPE_PREFIX_EXTENDED,
    )

    ext_table = ocel.get_extended_table(ot_prefix)

    for k, ev in ext_table.iterrows():
        yield {
            x: y
            for x, y in dict(ev).items()
            if isinstance(y, list) or not pd.isna(y)
        }
