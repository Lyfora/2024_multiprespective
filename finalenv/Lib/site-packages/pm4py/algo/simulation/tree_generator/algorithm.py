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
from pm4py.algo.simulation.tree_generator.variants import (
    basic,
    ptandloggenerator,
)
from enum import Enum
from pm4py.util import exec_utils
from pm4py.objects.process_tree.obj import ProcessTree
from typing import Optional, Dict, Any


class Variants(Enum):
    BASIC = basic
    PTANDLOGGENERATOR = ptandloggenerator


BASIC = Variants.BASIC
PTANDLOGGENERATOR = Variants.PTANDLOGGENERATOR
DEFAULT_VARIANT = Variants.PTANDLOGGENERATOR

VERSIONS = {Variants.BASIC, Variants.PTANDLOGGENERATOR}


def apply(
    variant=DEFAULT_VARIANT, parameters: Optional[Dict[Any, Any]] = None
) -> ProcessTree:
    """
    Generate a process tree

    Parameters
    ------------
    variant
        Variant of the algorithm. Admitted values:
            - Variants.BASIC
            - Variants.PTANDLOGGENERATOR
    parameters
        Specific parameters of the algorithm

    Returns
    ------------
    tree
        Process tree
    """
    return exec_utils.get_variant(variant).apply(parameters=parameters)
