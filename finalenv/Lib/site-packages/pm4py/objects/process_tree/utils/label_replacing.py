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
from pm4py.objects.process_tree.obj import ProcessTree
from typing import Optional, Dict, Any
from copy import deepcopy


def apply(process_tree: ProcessTree, string_dictio: Dict[str, str], rec_depth=0, parameters: Optional[Dict[Any, Any]] = None) -> ProcessTree:
    """
    Replaces the labels in the given process tree using the provided dictionary.

    Parameters
    -----------------
    process_tree
        Process tree
    string_dictio
        Correspondence dictionary (old labels -> new labels)

    Returns
    -----------------
    revised_tree
        Revised process tree
    """
    if parameters is None:
        parameters = {}

    if rec_depth == 0:
        process_tree = deepcopy(process_tree)

    if process_tree.label is not None and process_tree.label in string_dictio:
        process_tree.label = string_dictio[process_tree.label]

    for child in process_tree.children:
        apply(child, string_dictio, rec_depth=rec_depth+1, parameters=parameters)

    return process_tree
