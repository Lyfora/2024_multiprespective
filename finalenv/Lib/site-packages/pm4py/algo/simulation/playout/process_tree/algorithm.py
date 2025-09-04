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
from pm4py.algo.simulation.playout.process_tree.variants import basic_playout
from pm4py.algo.simulation.playout.process_tree.variants import (
    extensive,
    topbottom,
)
from enum import Enum
from pm4py.util import exec_utils
from pm4py.objects.process_tree.obj import ProcessTree
from typing import Optional, Dict, Any
from pm4py.objects.log.obj import EventLog


class Variants(Enum):
    BASIC_PLAYOUT = basic_playout
    EXTENSIVE = extensive
    TOPBOTTOM = topbottom


DEFAULT_VARIANT = Variants.TOPBOTTOM


def apply(
    tree: ProcessTree,
    variant=DEFAULT_VARIANT,
    parameters: Optional[Dict[Any, Any]] = None,
) -> EventLog:
    """
    Performs a playout of a process tree

    Parameters
    ---------------
    tree
        Process tree
    variant
        Variant of the algorithm:
        - Variants.BASIC_PLAYOUT: basic playout
        - Variants.EXTENSIVE: extensive playout (all the possible traces)
    parameters
        Parameters of the algorithm
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(tree, parameters=parameters)
