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
from typing import Optional, Dict, Any

from pm4py.objects.ocel.exporter.jsonocel.variants import (
    classic,
    ocel20,
    ocel20_standard,
)
from pm4py.objects.ocel.obj import OCEL
from pm4py.util import exec_utils


class Variants(Enum):
    CLASSIC = classic
    OCEL20 = ocel20
    OCEL20_STANDARD = ocel20_standard


def apply(
    ocel: OCEL,
    target_path: str,
    variant=Variants.CLASSIC,
    parameters: Optional[Dict[Any, Any]] = None,
):
    """
    Exports an object-centric event log in a JSONOCEL file

    Parameters
    ------------------
    ocel
        Object-centric event log
    target_path
        Destination path
    variant
        Variant of the algorithm to use, possible values:
        - Variants.CLASSIC
    parameters
        Variant-specific parameters
    """
    return exec_utils.get_variant(variant).apply(ocel, target_path, parameters)
