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
from pm4py.objects.bpmn.obj import BPMN
from typing import Optional, Dict, Any
from copy import deepcopy


def apply(bpmn_graph: BPMN, string_dictio: Dict[str, str], parameters: Optional[Dict[Any, Any]] = None) -> BPMN:
    if parameters is None:
        parameters = {}

    bpmn_graph = deepcopy(bpmn_graph)

    for node in bpmn_graph.get_nodes():
        if isinstance(node, BPMN.Task):
            name = node.get_name()
            if name in string_dictio:
                node.set_name(string_dictio[name])

    return bpmn_graph
