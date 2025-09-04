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
from pm4py.objects.conversion.trie.variants import to_petri_net
from enum import Enum
from pm4py.util import exec_utils
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.trie.obj import Trie
from typing import Optional, Dict, Any, Tuple


class Variants(Enum):
    TO_PETRI_NET = to_petri_net


def apply(
    prefix_tree: Trie,
    variant=Variants.TO_PETRI_NET,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Tuple[PetriNet, Marking, Marking]:
    """
    Converts the prefix tree objects using the specified variant

    Parameters
    ----------------
    prefix_tree
        Prefix tree
    variant
        Variant of the conversion:
        - Variants.TO_PETRI_NET => converts the prefix tree object to a Petri net
    parameters
        Optional parameters of the method.

    Returns
    ----------------
    obj
        Converted object
    """
    return exec_utils.get_variant(variant).apply(prefix_tree, parameters)
