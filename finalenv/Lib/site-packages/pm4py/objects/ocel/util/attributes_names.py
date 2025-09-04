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
from typing import Optional, Dict, Any, List

from pm4py.objects.ocel import constants
from pm4py.objects.ocel.obj import OCEL


def get_attribute_names(
    ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None
) -> List[str]:
    """
    Gets the list of attributes at the event and the object level of an object-centric event log
    (e.g. ["cost", "amount", "name"])

    Parameters
    -------------------
    ocel
        Object-centric event log
    parameters
        Parameters of the algorithm

    Returns
    -------------------
    attributes_list
        List of attributes at the event and object level (e.g. ["cost", "amount", "name"])
    """
    if parameters is None:
        parameters = {}

    attributes = sorted(
        set(
            x
            for x in ocel.events.columns
            if not x.startswith(constants.OCEL_PREFIX)
        ).union(
            x
            for x in ocel.objects.columns
            if not x.startswith(constants.OCEL_PREFIX)
        )
    )

    return attributes
