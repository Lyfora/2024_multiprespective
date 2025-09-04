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
from typing import Dict, Any, Optional, List

from pm4py.objects.ocel.obj import OCEL
from pm4py.util import pandas_utils


def related_events_dct(
    ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None
) -> Dict[str, Dict[str, List[str]]]:
    if parameters is None:
        parameters = {}

    object_types = pandas_utils.format_unique(
        ocel.relations[ocel.object_type_column].unique()
    )
    dct = {}
    for ot in object_types:
        dct[ot] = (
            ocel.relations[ocel.relations[ocel.object_type_column] == ot]
            .groupby(ocel.object_id_column)[ocel.event_id_column]
            .apply(list)
            .to_dict()
        )
    return dct
