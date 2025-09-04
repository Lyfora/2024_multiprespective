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
from pm4py.objects.petri_net.obj import PetriNet, Marking
from typing import Optional, Dict, Any, Tuple
from copy import deepcopy


def apply(net: PetriNet, initial_marking: Marking, final_marking: Marking, string_dictio: Dict[str, str], parameters: Optional[Dict[Any, Any]] = None) -> Tuple[PetriNet, Marking, Marking]:
    """
    Replaces the labels in the provided accepting Petri net using the provided correspondence dictionary.

    Parameters
    ----------------
    net
        Petri net
    initial_marking
        Initial marking
    final_marking
        Final marking
    string_dictio
        Correspondence dictionary (old labels -> new labels)

    Returns
    ----------------
    net
        Petri net
    initial_marking
        Initial marking
    final_marking
        Final marking
    """
    if parameters is None:
        parameters = {}

    net, initial_marking, final_marking = deepcopy([net, initial_marking, final_marking])

    for trans in net.transitions:
        if trans.label is not None and trans.label in string_dictio:
            trans.label = string_dictio[trans.label]

    return net, initial_marking, final_marking
