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
from pm4py.statistics.process_cube.variants import classic
from enum import Enum
from pm4py.util import exec_utils
import pandas as pd
from typing import Optional, Dict, Any, Tuple


class Variants(Enum):
    CLASSIC = classic


def apply(feature_table: pd.DataFrame, x_col: str, y_col: str, agg_col: str, variant=Variants.CLASSIC, parameters: Optional[Dict[Any, Any]] = None) -> Tuple[pd.DataFrame, Dict[Any, Any]]:
    """
    Constructs a process cube by slicing data along two dimensions (x_col, y_col) and aggregating a third (agg_col).
    Additionally:

    1) If x_col (or y_col) is an actual column in df, we do numeric binning.
       Otherwise, we do 'prefix-based' binning (include any column starting with x_col,
       and assign a row to that bin if >= 1).
    2) We return both the pivoted DataFrame and a dict associating each cell
       (x_bin, y_bin) -> set of case IDs.

    Parameters
    ----------
    feature_table : pd.DataFrame
        A feature table that must contain 'case:concept:name' and agg_col, plus
        the columns for x_col, y_col (if in numeric mode) or the columns that start
        with x_col, y_col (if in prefix mode).
    x_col : str
        The X dimension. If x_col in df.columns, use numeric binning. Otherwise, treat
        it as a prefix for 'prefix-based' binning.
    y_col : str
        The Y dimension. If y_col in df.columns, use numeric binning. Otherwise, treat
        it as a prefix for 'prefix-based' binning.
    agg_col : str
        The column to aggregate (mean, sum, etc.).
    variant
        Variant of the algorithm to be used, possible values:
        - Variants.CLASSIC
    parameters: Dict[Any, Any]
        Variant-specific parameters, including potentially:
        * Parameters.MAX_DIVISIONS_X: If x_col is numeric, how many bins to divide it into.
        * Parameters.MAX_DIVISIONS_Y: If y_col is numeric, how many bins to divide it into.
        * Parameters.AGGREGATION_FUNCTION: The aggregation function, e.g., 'mean', 'sum', 'min', 'max'.

    Returns
    -------
    pivot_df : pd.DataFrame
        A pivoted DataFrame representing the process cube, with x bins as rows
        and y bins as columns, containing aggregated values of agg_col.
    cell_case_dict : dict
        A dictionary mapping (x_bin, y_bin) -> set of case IDs that fall in that cell.
    """
    return exec_utils.get_variant(variant).apply(feature_table, x_col, y_col, agg_col, parameters=parameters)
