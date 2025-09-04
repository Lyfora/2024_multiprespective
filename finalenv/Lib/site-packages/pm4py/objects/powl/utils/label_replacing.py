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
from pm4py.objects.powl.obj import POWL
from typing import Optional, Dict, Any
from copy import deepcopy


def apply(powl: POWL, string_dictio: Dict[str, str], rec_depth=0, parameters: Optional[Dict[Any, Any]] = None) -> POWL:
    """
    Replaces the labels in the given POWL object using the provided dictionary.

    Parameters
    ---------------
    powl
        POWL
    string_dictio
        Correspondence dictionary (old labels -> new labels)

    Returns
    ----------------
    revised_powl
        Revised POWL
    """
    if parameters is None:
        parameters = {}

    if rec_depth == 0:
        powl = deepcopy(powl)

    if powl.label is not None and powl.label in string_dictio:
        powl.label = string_dictio[powl.label]

    for child in powl.children:
        apply(child, string_dictio, rec_depth=rec_depth+1, parameters=parameters)

    return powl
