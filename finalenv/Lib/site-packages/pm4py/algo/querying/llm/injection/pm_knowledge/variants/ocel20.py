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
import pandas as pd
from typing import Optional, Dict, Any, Union
from pm4py.objects.ocel.obj import OCEL


def apply(ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None) -> str:
    """
    Provides a string containing the required process mining domain knowledge for object-centric process mining structures
    (in order for the LLM to produce meaningful queries).

    Parameters
    ---------------
    ocel
        OCEL (2.0) object
    parameters
        Optional parameters of the method

    Returns
    --------------
    pm_knowledge
        String containing the required process mining knowledge
    """
    if parameters is None:
        parameters = {}

    descr = """
If you need to compute the duration of a lifecycle of an object, compute the difference between the timestamp of the last and the first event of the lifecycle.
If you need to compute the variant for an object, aggregate the names of the activities.
    """

    return descr
