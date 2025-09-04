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
from pm4py.algo.discovery.ocel.ocpn.variants import classic
from pm4py.objects.ocel.obj import OCEL
from pm4py.util import exec_utils
from typing import Optional, Dict, Any


class Variants(Enum):
    WO_ANNOTATION = classic
    CLASSIC = classic


def apply(
    ocel: OCEL,
    variant=Variants.CLASSIC,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Dict[str, Any]:
    """
    Discovers an object-centric Petri net from the provided object-centric event log.

    Reference paper: van der Aalst, Wil MP, and Alessandro Berti. "Discovering object-centric Petri nets." Fundamenta informaticae 175.1-4 (2020): 1-40.

    Parameters
    -----------------
    ocel
        Object-centric event log
    variant
        Variant of the algorithm to be used
    parameters
        Variant-specific parameters

    Returns
    ----------------
    ocpn
        Object-centric Petri net model, as a dictionary of properties.
    """
    return exec_utils.get_variant(variant).apply(ocel, parameters=parameters)
