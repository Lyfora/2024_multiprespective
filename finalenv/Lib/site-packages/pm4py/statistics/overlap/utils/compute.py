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
from typing import Optional, Dict, Any, Tuple, List, Union

from intervaltree import Interval, IntervalTree

from pm4py.util import exec_utils


class Parameters(Enum):
    EPSILON = "epsilon"


def apply(
    points: List[Tuple[float, float]],
    parameters: Optional[Dict[Union[str, Parameters], Any]] = None,
) -> List[int]:
    """
    Computes the overlap statistic given a list of points, expressed as (min_timestamp, max_timestamp)

    Parameters
    -----------------
    points
        List of points with the aforementioned features
    parameters
        Parameters of the method, including:
        - Parameters.EPSILON

    Returns
    -----------------
    overlap
        List associating to each point the number of intersecting points
    """
    if parameters is None:
        parameters = {}

    epsilon = exec_utils.get_param_value(
        Parameters.EPSILON, parameters, 10 ** (-5)
    )
    points = [(x[0] - epsilon, x[1] + epsilon) for x in points]
    sorted_points = sorted(points)
    tree = IntervalTree()

    for p in sorted_points:
        tree.add(Interval(p[0], p[1]))

    overlap = []
    for p in points:
        overlap.append(len(tree[p[0]: p[1]]))

    return overlap
