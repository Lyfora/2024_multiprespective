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
import pandas as pd
import numpy as np
from enum import Enum
from typing import Optional, Dict, Any
from pm4py.util import exec_utils


class Parameters(Enum):
    MAX_DIVISIONS_X = "max_divisions_x"
    MAX_DIVISIONS_Y = "max_divisions_y"
    AGGREGATION_FUNCTION = "aggregation_function"
    X_BINS = "x_bins"           # Optional list of numeric bin edges for x_col
    Y_BINS = "y_bins"           # Optional list of numeric bin edges for y_col


def apply(
        feature_table: pd.DataFrame,
        x_col: str,
        y_col: str,
        agg_col: str,
        parameters: Optional[Dict[Any, Any]] = None
):
    """
    Constructs a process cube by slicing data along two dimensions
    (x_col, y_col) and aggregating a third (agg_col). Additionally:

    1) If x_col (or y_col) is an actual column in df, we do numeric binning.
       You can manually specify bin edges via parameters[Parameters.X_BINS]
       (a list of numeric edges) or parameters[Parameters.Y_BINS].
       Otherwise, we automatically divide into equal-width bins using
       parameters[Parameters.MAX_DIVISIONS_X] or MAX_DIVISIONS_Y.
    2) If x_col (or y_col) is not present, we do prefix-based binning.

    Parameters
    ----------
    feature_table : pd.DataFrame
        A feature table that must contain 'case:concept:name' and agg_col, plus
        the columns for x_col, y_col (if numeric) or the columns that start
        with x_col, y_col (if prefix-based).
    x_col : str
        The X dimension. If x_col in df.columns, numeric binning; else prefix-based.
    y_col : str
        The Y dimension. If y_col in df.columns, numeric binning; else prefix-based.
    agg_col : str
        The column to aggregate (mean, sum, etc.).
    parameters: Dict[Any, Any]
        Optional parameters of the method, including:
        * Parameters.X_BINS: List of numeric bin edges for x_col.
        * Parameters.Y_BINS: List of numeric bin edges for y_col.
        * Parameters.MAX_DIVISIONS_X: If x_col is numeric and X_BINS not provided,
          how many bins to divide it into.
        * Parameters.MAX_DIVISIONS_Y: If y_col is numeric and Y_BINS not provided,
          how many bins to divide it into.
        * Parameters.AGGREGATION_FUNCTION: The aggregation function,
          e.g., 'mean', 'sum', 'min', 'max'.

    Returns
    -------
    pivot_df : pd.DataFrame
        A pivoted DataFrame representing the process cube, with x bins as rows
        and y bins as columns, containing aggregated values of agg_col.
    cell_case_dict : dict
        A dictionary mapping (x_bin, y_bin) -> set of case IDs that fall in that cell.
    """
    if parameters is None:
        parameters = {}

    # Retrieve parameters, with None defaults for manual bins
    max_divisions_x = exec_utils.get_param_value(Parameters.MAX_DIVISIONS_X, parameters, 4)
    max_divisions_y = exec_utils.get_param_value(Parameters.MAX_DIVISIONS_Y, parameters, 4)
    agg_fn = exec_utils.get_param_value(Parameters.AGGREGATION_FUNCTION, parameters, "mean")
    x_bins_param = exec_utils.get_param_value(Parameters.X_BINS, parameters, None)
    y_bins_param = exec_utils.get_param_value(Parameters.Y_BINS, parameters, None)

    # Work with a view instead of copy when possible
    df = feature_table
    
    # Pre-compute column lists and masks for better performance
    numeric_x = x_col in df.columns
    numeric_y = y_col in df.columns
    
    if not numeric_x:
        x_prefix_cols = [c for c in df.columns if c.startswith(x_col)]
    if not numeric_y:
        y_prefix_cols = [c for c in df.columns if c.startswith(y_col)]

    # ------------------------------------------------------
    # Handle X dimension binning
    # ------------------------------------------------------
    if numeric_x:
        # Use manual bins if provided, else auto-generate equal-width bins
        if x_bins_param is not None:
            x_bins = sorted(list(set(x_bins_param)))  # Remove duplicates and sort
        else:
            x_min, x_max = df[x_col].min(), df[x_col].max()
            if x_min == x_max:
                # Handle case where all values are the same
                x_bins = [x_min - 0.5, x_max + 0.5]
            else:
                x_bins = np.linspace(x_min, x_max, max_divisions_x + 1)
                # Ensure bins are unique
                x_bins = np.unique(x_bins)
        
        # Create binned column directly without temporary column
        x_binned = pd.cut(df[x_col], bins=x_bins, include_lowest=True)
        x_valid_mask = pd.notna(x_binned)
    else:
        # Pre-filter and vectorize prefix-based column selection
        x_prefix_data = df[x_prefix_cols].fillna(0)
        x_valid_cols_mask = x_prefix_data >= 1
        x_valid_mask = x_valid_cols_mask.any(axis=1)

    # ------------------------------------------------------
    # Handle Y dimension binning
    # ------------------------------------------------------
    if numeric_y:
        if y_bins_param is not None:
            y_bins = sorted(list(set(y_bins_param)))  # Remove duplicates and sort
        else:
            y_min, y_max = df[y_col].min(), df[y_col].max()
            if y_min == y_max:
                # Handle case where all values are the same
                y_bins = [y_min - 0.5, y_max + 0.5]
            else:
                y_bins = np.linspace(y_min, y_max, max_divisions_y + 1)
                # Ensure bins are unique
                y_bins = np.unique(y_bins)
        
        y_binned = pd.cut(df[y_col], bins=y_bins, include_lowest=True)
        y_valid_mask = pd.notna(y_binned)
    else:
        y_prefix_data = df[y_prefix_cols].fillna(0)
        y_valid_cols_mask = y_prefix_data >= 1
        y_valid_mask = y_valid_cols_mask.any(axis=1)

    # Combined validity mask
    valid_mask = x_valid_mask & y_valid_mask
    if not valid_mask.any():
        return pd.DataFrame(), {}

    # Filter data to valid rows only
    valid_df = df[valid_mask]
    case_ids = valid_df["case:concept:name"].values
    agg_values = valid_df[agg_col].values

    # Build DataFrame directly using vectorized operations - much faster than building records list
    if numeric_x and numeric_y:
        # Both numeric - create DataFrame directly
        x_bins_valid = x_binned[valid_mask]
        y_bins_valid = y_binned[valid_mask]
        
        temp_df = pd.DataFrame({
            "case:concept:name": case_ids,
            "x_bin": x_bins_valid,
            "y_bin": y_bins_valid,
            agg_col: agg_values
        })
        
    elif numeric_x and not numeric_y:
        # X numeric, Y prefix-based - use more efficient vectorized approach
        x_bins_valid = x_binned[valid_mask]
        y_valid_cols_valid = y_valid_cols_mask[valid_mask]
        
        # Use numpy operations for much faster processing
        y_valid_array = y_valid_cols_valid.values
        row_counts = np.sum(y_valid_array, axis=1)
        total_rows = np.sum(row_counts)
        
        if total_rows == 0:
            return pd.DataFrame(), {}
        
        # Pre-allocate arrays for better performance
        case_ids_expanded = np.empty(total_rows, dtype=object)
        x_bins_expanded = np.empty(total_rows, dtype=object)
        y_cols_expanded = np.empty(total_rows, dtype=object)
        agg_values_expanded = np.empty(total_rows, dtype=float)
        
        # Fill arrays using vectorized operations
        idx = 0
        y_prefix_cols_array = np.array(y_prefix_cols)
        
        for i in range(len(case_ids)):
            if row_counts[i] > 0:
                valid_y_indices = np.where(y_valid_array[i])[0]
                n_valid = len(valid_y_indices)
                
                case_ids_expanded[idx:idx+n_valid] = case_ids[i]
                x_bins_expanded[idx:idx+n_valid] = x_bins_valid.iloc[i]
                y_cols_expanded[idx:idx+n_valid] = y_prefix_cols_array[valid_y_indices]
                agg_values_expanded[idx:idx+n_valid] = agg_values[i]
                idx += n_valid
            
        temp_df = pd.DataFrame({
            "case:concept:name": case_ids_expanded,
            "x_bin": x_bins_expanded,
            "y_bin": y_cols_expanded,
            agg_col: agg_values_expanded
        })
        
    elif not numeric_x and numeric_y:
        # X prefix-based, Y numeric - use more efficient vectorized approach
        x_valid_cols_valid = x_valid_cols_mask[valid_mask]
        y_bins_valid = y_binned[valid_mask]
        
        # Use numpy operations for much faster processing
        x_valid_array = x_valid_cols_valid.values
        row_counts = np.sum(x_valid_array, axis=1)
        total_rows = np.sum(row_counts)
        
        if total_rows == 0:
            return pd.DataFrame(), {}
        
        # Pre-allocate arrays for better performance
        case_ids_expanded = np.empty(total_rows, dtype=object)
        x_cols_expanded = np.empty(total_rows, dtype=object)
        y_bins_expanded = np.empty(total_rows, dtype=object)
        agg_values_expanded = np.empty(total_rows, dtype=float)
        
        # Fill arrays using vectorized operations
        idx = 0
        x_prefix_cols_array = np.array(x_prefix_cols)
        
        for i in range(len(case_ids)):
            if row_counts[i] > 0:
                valid_x_indices = np.where(x_valid_array[i])[0]
                n_valid = len(valid_x_indices)
                
                case_ids_expanded[idx:idx+n_valid] = case_ids[i]
                x_cols_expanded[idx:idx+n_valid] = x_prefix_cols_array[valid_x_indices]
                y_bins_expanded[idx:idx+n_valid] = y_bins_valid.iloc[i]
                agg_values_expanded[idx:idx+n_valid] = agg_values[i]
                idx += n_valid
            
        temp_df = pd.DataFrame({
            "case:concept:name": case_ids_expanded,
            "x_bin": x_cols_expanded,
            "y_bin": y_bins_expanded,
            agg_col: agg_values_expanded
        })
        
    else:
        # Both prefix-based - most complex case, use highly optimized vectorized approach
        x_valid_cols_valid = x_valid_cols_mask[valid_mask]
        y_valid_cols_valid = y_valid_cols_mask[valid_mask]
        
        # Use numpy operations for much faster processing
        x_valid_array = x_valid_cols_valid.values
        y_valid_array = y_valid_cols_valid.values
        
        # Calculate total number of combinations for pre-allocation
        x_row_counts = np.sum(x_valid_array, axis=1)
        y_row_counts = np.sum(y_valid_array, axis=1)
        row_combinations = x_row_counts * y_row_counts
        total_rows = np.sum(row_combinations)
        
        if total_rows == 0:
            return pd.DataFrame(), {}
        
        # Pre-allocate arrays for maximum performance
        case_ids_expanded = np.empty(total_rows, dtype=object)
        x_cols_expanded = np.empty(total_rows, dtype=object)
        y_cols_expanded = np.empty(total_rows, dtype=object)
        agg_values_expanded = np.empty(total_rows, dtype=float)
        
        # Use vectorized operations with pre-converted arrays
        x_prefix_cols_array = np.array(x_prefix_cols)
        y_prefix_cols_array = np.array(y_prefix_cols)
        
        idx = 0
        for i in range(len(case_ids)):
            if row_combinations[i] > 0:
                valid_x_indices = np.where(x_valid_array[i])[0]
                valid_y_indices = np.where(y_valid_array[i])[0]
                
                # Create cartesian product using numpy operations
                x_mesh, y_mesh = np.meshgrid(valid_x_indices, valid_y_indices, indexing='ij')
                x_flat = x_mesh.flatten()
                y_flat = y_mesh.flatten()
                n_combinations = len(x_flat)
                
                # Fill arrays efficiently
                case_ids_expanded[idx:idx+n_combinations] = case_ids[i]
                x_cols_expanded[idx:idx+n_combinations] = x_prefix_cols_array[x_flat]
                y_cols_expanded[idx:idx+n_combinations] = y_prefix_cols_array[y_flat]
                agg_values_expanded[idx:idx+n_combinations] = agg_values[i]
                idx += n_combinations
            
        temp_df = pd.DataFrame({
            "case:concept:name": case_ids_expanded,
            "x_bin": x_cols_expanded,
            "y_bin": y_cols_expanded,
            agg_col: agg_values_expanded
        })

    if temp_df.empty:
        return pd.DataFrame(), {}

    # Optimized aggregation using more efficient groupby operations
    grouped = temp_df.groupby(["x_bin", "y_bin"], sort=False)
    
    # Compute aggregations separately for better performance
    agg_values_result = grouped[agg_col].agg(agg_fn).reset_index()
    case_sets_result = grouped["case:concept:name"].apply(lambda x: set(x)).reset_index()
    case_sets_result.rename(columns={"case:concept:name": "case_set"}, inplace=True)
    
    # Merge results efficiently
    agg_result = pd.merge(agg_values_result, case_sets_result, on=["x_bin", "y_bin"])

    # Use pivot_table directly for better performance and handling of missing values
    pivot_df = temp_df.pivot_table(
        index="x_bin", 
        columns="y_bin", 
        values=agg_col, 
        aggfunc=agg_fn,
        dropna=False
    )
    
    # Drop completely empty rows/columns more efficiently
    pivot_df = pivot_df.dropna(how="all", axis=0).dropna(how="all", axis=1)

    # Build cell-case mapping using vectorized operations
    valid_x_mask = agg_result["x_bin"].isin(pivot_df.index)
    valid_y_mask = agg_result["y_bin"].isin(pivot_df.columns)
    valid_mask = valid_x_mask & valid_y_mask
    
    valid_agg_result = agg_result[valid_mask]
    cell_case_dict = dict(zip(
        zip(valid_agg_result["x_bin"], valid_agg_result["y_bin"]),
        valid_agg_result["case_set"]
    ))

    return pivot_df, cell_case_dict
