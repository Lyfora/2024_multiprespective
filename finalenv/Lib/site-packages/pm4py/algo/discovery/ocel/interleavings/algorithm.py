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
from pm4py.algo.discovery.ocel.interleavings.variants import (
    timestamp_interleavings,
)
from enum import Enum
from pm4py.util import exec_utils
import pandas as pd
from typing import Optional, Dict, Any


class Variants(Enum):
    TIMESTAMP_INTERLEAVINGS = timestamp_interleavings


def apply(
    left_df: pd.DataFrame,
    right_df: pd.DataFrame,
    case_relations: pd.DataFrame,
    variant=Variants.TIMESTAMP_INTERLEAVINGS,
    parameters: Optional[Dict[Any, Any]] = None,
):
    """
    Discover the interleavings between two dataframes, given also a dataframe about the relations of the cases.

    Parameters
    -----------------
    left_df
        Left dataframe
    right_df
        Right dataframe
    case_relations
        Dictionary associating the cases of the first dataframe (column: case:concept:name_LEFT) to the
        cases of the second dataframe (column: case:concept:name_RIGHT)
    variant
        Variant of the algorithm to be used, possible values:
        - Variants.TIMESTAMP_INTERLEAVINGS
    parameters
        Variant-specific parameters

    Returns
    -----------------
    interleavings
        Interleavings dataframe
    """
    return exec_utils.get_variant(variant).apply(
        left_df, right_df, case_relations, parameters
    )
