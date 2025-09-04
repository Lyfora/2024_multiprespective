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
from pm4py.algo.evaluation.earth_mover_distance.variants import pyemd
from enum import Enum
from pm4py.util import exec_utils
from typing import Optional, Dict, Any, List


class Variants(Enum):
    PYEMD = pyemd


DEFAULT_VARIANT = Variants.PYEMD


def apply(
    lang1: Dict[List[str], float],
    lang2: Dict[List[str], float],
    variant=Variants.PYEMD,
    parameters: Optional[Dict[Any, Any]] = None,
) -> float:
    """
    Gets the EMD language between the two languages

    Parameters
    -------------
    lang1
        First language
    lang2
        Second language
    variant
        Variants of the algorithm
    parameters
        Parameters
    variants
        Variants of the algorithm, including:
            - Variants.PYEMD: pyemd based distance

    Returns
    -------------
    dist
        EMD distance
    """
    return exec_utils.get_variant(variant).apply(
        lang1, lang2, parameters=parameters
    )
