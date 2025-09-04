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
from pm4py.algo.discovery.ocel.saw_nets.variants import classic
from pm4py.objects.ocel.obj import OCEL
from typing import Optional, Dict, Any


class Variants(Enum):
    CLASSIC = classic


def apply(
    ocel: OCEL,
    variant=Variants.CLASSIC,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Dict[str, Any]:
    """
    Discovers a SAW net representing the behavior of the provided object-centric event log.

    Parameters
    ----------------
    ocel
        Object-centric event log
    variant
        The variant of the algorithm to be used, possible values: Variants.CLASSIC
    parameters
        Variant-specific parameters

    Returns
    ---------------
    Dictionary with the following keys:
        - ot_saw_net => the SAW nets for the single object types
        - multi_saw_net => the overall SAW net
        - decorations_multi_saw_net => decorations for the visualization of the SAW net
    """
    return exec_utils.get_variant(variant).apply(ocel, parameters)
