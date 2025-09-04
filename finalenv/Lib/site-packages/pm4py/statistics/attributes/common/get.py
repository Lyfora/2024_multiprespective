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
import json
import logging
import importlib.util

from pm4py.util.points_subset import pick_chosen_points_list
from pm4py.util import exec_utils, pandas_utils, constants
from enum import Enum


class Parameters(Enum):
    GRAPH_POINTS = "graph_points"
    POINT_TO_SAMPLE = "points_to_sample"


def get_sorted_attributes_list(attributes):
    """
    Gets sorted attributes list

    Parameters
    ----------
    attributes
        Dictionary of attributes associated with their count

    Returns
    ----------
    listact
        Sorted end attributes list
    """
    listattr = []
    for a in attributes:
        listattr.append([a, attributes[a]])
    listattr = sorted(listattr, key=lambda x: x[1], reverse=True)
    return listattr


def get_attributes_threshold(
    alist, decreasing_factor, min_activity_count=1, max_activity_count=25
):
    """
    Get attributes cutting threshold

    Parameters
    ----------
    alist
        Sorted attributes list
    decreasing_factor
        Decreasing factor of the algorithm
    min_activity_count
        Minimum number of activities to include
    max_activity_count
        Maximum number of activities to include

    Returns
    ---------
    threshold
        Activities cutting threshold
    """
    index = max(0, min(min_activity_count - 1, len(alist) - 1))
    threshold = alist[index][1]
    index = index + 1
    for i in range(index, len(alist)):
        value = alist[i][1]
        if value > threshold * decreasing_factor:
            threshold = value
        if i >= max_activity_count:
            break
    return threshold


def get_kde_numeric_attribute(values, parameters=None):
    """
    Gets the KDE estimation for the distribution of a numeric attribute values

    Parameters
    -------------
    values
        Values of the numeric attribute value
    parameters
        Possible parameters of the algorithm, including:
            graph_points -> number of points to include in the graph

    Returns
    --------------
    x
        X-axis values to represent (including the exact min and max)
    y
        Y-axis values to represent
    """
    if importlib.util.find_spec("scipy") and importlib.util.find_spec("numpy"):
        from scipy.stats import gaussian_kde
        import numpy as np

        if parameters is None:
            parameters = {}

        graph_points = exec_utils.get_param_value(
            Parameters.GRAPH_POINTS, parameters, 200
        )
        values = np.sort(values)
        
        # Check if we have enough unique values for KDE
        unique_values = np.unique(values)
        if len(unique_values) < 2:
            # Handle edge case: not enough unique values for KDE
            if len(unique_values) == 0:
                # No values at all
                return [], []
            else:
                # Single unique value - create a simple representation
                single_val = float(unique_values[0])
                # Create a small range around the single value for visualization
                eps = max(abs(single_val) * 0.01, 1e-6) if single_val != 0 else 1.0
                xs = np.linspace(single_val - eps, single_val + eps, graph_points)
                # Create a spike at the single value
                ys = np.zeros(graph_points)
                mid_idx = graph_points // 2
                ys[mid_idx] = 1.0
                return xs.tolist(), ys.tolist()
        
        density = gaussian_kde(values)

        # ensure we have at least two points for each spacing
        half = max(int(graph_points // 2), 2)

        min_val, max_val = values[0], values[-1]
        eps = 1e-6

        # linear space including both endpoints
        xs1 = np.linspace(min_val, max_val, half, endpoint=True)
        # geometric space including both endpoints (avoid zero)
        xs2 = np.geomspace(max(min_val, eps), max_val, half, endpoint=True)

        # combine, add exact endpoints, dedupe & sort
        xs = np.unique(
            np.concatenate([xs1, xs2, [min_val, max_val]])
        )

        return xs.tolist(), density(xs).tolist()
    else:
        msg = "scipy is not available. graphs cannot be built!"
        logging.error(msg)
        raise Exception(msg)


def get_kde_numeric_attribute_json(values, parameters=None):
    """
    Gets the KDE estimation for the distribution of a numeric attribute values
    (expressed as JSON)

    Parameters
    --------------
    values
        Values of the numeric attribute value
    parameters
        Possible parameters of the algorithm, including:
            graph_points: number of points to include in the graph

    Returns
    --------------
    json
        JSON representing the graph points
    """
    x, y = get_kde_numeric_attribute(values, parameters=parameters)

    ret = []
    for i in range(len(x)):
        ret.append((x[i], y[i]))

    return json.dumps(ret)


def get_kde_date_attribute(values, parameters=None):
    """
    Gets the KDE estimation for the distribution of a date attribute values

    Parameters
    -------------
    values
        Values of the date attribute value
    parameters
        Possible parameters of the algorithm, including:
            graph_points -> number of points to include in the graph


    Returns
    --------------
    x
        X-axis values to represent
    y
        Y-axis values to represent
    """
    if importlib.util.find_spec("scipy") and importlib.util.find_spec("numpy"):
        from scipy.stats import gaussian_kde
        import numpy as np
        import pandas as pd

        if parameters is None:
            parameters = {}

        graph_points = exec_utils.get_param_value(
            Parameters.GRAPH_POINTS, parameters, 200
        )
        points_to_sample = exec_utils.get_param_value(
            Parameters.POINT_TO_SAMPLE, parameters, 400
        )

        red_values = pick_chosen_points_list(points_to_sample, values, include_extremes=True)
        int_values = sorted(
            [x.replace(tzinfo=None).timestamp() for x in red_values]
        )
        
        # Check if we have enough unique values for KDE
        unique_int_values = np.unique(int_values)
        if len(unique_int_values) < 2:
            # Handle edge case: not enough unique values for KDE
            if len(unique_int_values) == 0:
                # No values at all
                return [[], []]
            else:
                # Single unique value - create a simple representation
                single_val = float(unique_int_values[0])
                # Create a small time range around the single value (1 hour range)
                time_eps = 3600  # 1 hour in seconds
                xs = np.linspace(single_val - time_eps, single_val + time_eps, graph_points)
                xs_transf = pd.to_datetime(xs * 10**9, unit="ns")
                # Create a spike at the single value
                ys = np.zeros(graph_points)
                mid_idx = graph_points // 2
                ys[mid_idx] = 1.0
                return [xs_transf, ys.tolist()]
        
        density = gaussian_kde(int_values)
        xs = np.linspace(min(int_values), max(int_values), graph_points)
        xs_transf = pd.to_datetime(xs * 10**9, unit="ns")

        return [xs_transf, density(xs)]
    else:
        msg = "scipy is not available. graphs cannot be built!"
        logging.error(msg)
        raise Exception(msg)


def get_kde_date_attribute_json(values, parameters=None):
    """
    Gets the KDE estimation for the distribution of a date attribute values
    (expressed as JSON)

    Parameters
    --------------
    values
        Values of the date attribute value
    parameters
        Possible parameters of the algorithm, including:
            graph_points: number of points to include in the graph

    Returns
    --------------
    json
        JSON representing the graph points
    """
    x, y = get_kde_date_attribute(values, parameters=parameters)

    ret = []
    for i in range(len(x)):
        ret.append((x[i].replace(tzinfo=None).timestamp(), y[i]))

    return json.dumps(ret)
