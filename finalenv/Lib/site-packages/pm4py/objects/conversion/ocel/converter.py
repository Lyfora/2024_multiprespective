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

from enum import Enum
from pm4py.util import exec_utils
from pm4py.objects.ocel.obj import OCEL
from typing import Optional, Dict, Any
from pm4py.objects.conversion.ocel.variants import (
    ocel_to_nx,
    ocel_features_to_nx,
)


class Variants(Enum):
    OCEL_TO_NX = ocel_to_nx
    OCEL_FEATURES_TO_NX = ocel_features_to_nx


def apply(
    ocel: OCEL,
    variant=Variants.OCEL_TO_NX,
    parameters: Optional[Dict[Any, Any]] = None,
):
    """
    Converts an OCEL to another object.

    Parameters
    -------------
    ocel
        Object-centric event log
    variant
        Variant of the algorithm to use, posible values:
        - Variants.OCEL_TO_NX: graph containing event and object IDS and two type of relations (REL=related objects, DF=directly-follows)
        - Variants.OCEL_FEATURES_TO_NX: graph containing different types of interconnection at the object level
    parameters
        Variant-specific parameters
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(ocel, parameters=parameters)
