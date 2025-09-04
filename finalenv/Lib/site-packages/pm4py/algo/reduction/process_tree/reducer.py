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
from typing import Any

from pm4py.algo.reduction.process_tree.variants import tree_tr_based
from pm4py.util import exec_utils


class Variants(Enum):
    TREE_TR_BASED = tree_tr_based


def apply(*args, **kwargs) -> Any:
    """
    Apply a reduction algorithm to a PM4Py object

    Parameters
    ---------------
    args
        Arguments of the reduction algorithm
    kwargs
        Keyword arguments of the reduction algorithm (including the variant, that is an item of the Variants enum)

    Returns
    ---------------
    reduced_obj
        Reduced object
    """
    variant = kwargs["variant"] if "variant" in kwargs else None
    if variant is None:
        raise Exception(
            "please specify the variant of the reduction to be used."
        )
    return exec_utils.get_variant(variant).apply(*args, **kwargs)
