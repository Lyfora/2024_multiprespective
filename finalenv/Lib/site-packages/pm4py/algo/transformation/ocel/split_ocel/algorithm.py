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
from pm4py.algo.transformation.ocel.split_ocel.variants import (
    connected_components,
    ancestors_descendants,
)
from pm4py.util import exec_utils


class Variants(Enum):
    CONNECTED_COMPONENTS = connected_components
    ANCESTORS_DESCENDANTS = ancestors_descendants


def apply(
    ocel: OCEL,
    variant=Variants.CONNECTED_COMPONENTS,
    parameters: Optional[Dict[Any, Any]] = None,
):
    """
    Splits an OCEL into sub-OCELs using a given criteria (variant).

    Parameters
    ----------------
    ocel
        OCEL
    variant
        Variant of the algorithm to be used, possible values:
        - Variants.CONNECTED_COMPONENTS

    Returns
    ----------------
    splitted_ocel
        List of OCELs found based on the connected components
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(ocel, parameters)
