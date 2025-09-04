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

from pm4py.objects.bpmn.layout.variants import graphviz, graphviz_new
from pm4py.util import exec_utils


class Variants(Enum):
    GRAPHVIZ = graphviz
    GRAPHVIZ_NEW = graphviz_new


DEFAULT_VARIANT = Variants.GRAPHVIZ_NEW


def apply(bpmn_graph, variant=DEFAULT_VARIANT, parameters=None):
    """
    Layouts a BPMN graph (inserting the positions of the nodes and the layouting of the edges)

    Parameters
    -------------
    bpmn_graph
        BPMN graph
    variant
        Variant of the algorithm to use, possible values:
        - Variants.GRAPHVIZ
    parameters
        Parameters of the algorithm

    Returns
    -------------
    bpmn_graph
        BPMN graph with layout information
    """
    return exec_utils.get_variant(variant).apply(
        bpmn_graph, parameters=parameters
    )
