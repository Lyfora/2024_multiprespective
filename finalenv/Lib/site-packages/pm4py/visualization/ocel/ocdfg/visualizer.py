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
from graphviz import Digraph
from enum import Enum
from pm4py.util import exec_utils
from pm4py.visualization.ocel.ocdfg.variants import classic, elkjs
from typing import Optional, Dict, Any


class Variants(Enum):
    CLASSIC = classic
    ELKJS = elkjs


def apply(
    ocdfg: Dict[str, Any],
    variant=Variants.CLASSIC,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Digraph:
    """
    Visualizes an OC-DFG using one of the provided visualizations.

    Parameters
    ----------------
    ocdfg
        Object-centric directly-follows graph
    variant
        Available variants. Possible values:
        - Variants.CLASSIC
    parameters
        Variant-specific parameters

    Returns
    ----------------
    viz
        Graphviz DiGraph
    """
    return exec_utils.get_variant(variant).apply(ocdfg, parameters)


def save(
    gviz, output_file_path: str, variant=Variants.CLASSIC, parameters=None
):
    """
    Saves the diagram
    """
    return exec_utils.get_variant(variant).save(
        gviz, output_file_path, parameters
    )


def view(gviz, variant=Variants.CLASSIC, parameters=None):
    """
    Views the diagram
    """
    return exec_utils.get_variant(variant).view(gviz, parameters)


def matplotlib_view(gviz, variant=Variants.CLASSIC, parameters=None):
    """
    Views the diagram using Matplotlib
    """
    return exec_utils.get_variant(variant).matplotlib_view(gviz, parameters)
