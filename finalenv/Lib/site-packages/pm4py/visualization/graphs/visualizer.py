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
from pm4py.visualization.graphs.variants import (
    cases,
    attributes,
    dates,
    barplot,
)
from pm4py.visualization.graphs.util.common import (
    save,
    view,
    matplotlib_view,
    serialize,
)
from enum import Enum
from pm4py.util import exec_utils
from typing import Optional, Dict, Any, List


class Variants(Enum):
    CASES = cases
    ATTRIBUTES = attributes
    DATES = dates
    BARPLOT = barplot


DEFAULT_VARIANT = Variants.CASES


def apply(
    x: List[float],
    y: List[float],
    parameters: Optional[Dict[Any, Any]] = None,
    variant=DEFAULT_VARIANT,
) -> str:
    """
    Method to plot (non-logarithmic way) the graph with axis values contained in x and y

    Parameters
    ------------
    x
        Values for x-axis
    y
        Values for y-axis
    parameters
        Parameters of the algorithm, including:
            Parameters.FORMAT -> Format of the target image
            Parameters.TITLE -> Title of the image
    variant
        Variant of the algorithm to apply, including:
            - Variants.CASES
            - Variants.ATTRIBUTES
            - Variants.DATES
            - Variants.BARPLOT

    Returns
    ------------
    temp_file_name
        Representation temporary file name
    """
    return exec_utils.get_variant(variant).apply_plot(
        x, y, parameters=parameters
    )


def apply_plot(
    x: List[float],
    y: List[float],
    parameters: Optional[Dict[Any, Any]] = None,
    variant=DEFAULT_VARIANT,
) -> str:
    """
    Method to plot (non-logarithmic way) the graph with axis values contained in x and y

    Parameters
    ------------
    x
        Values for x-axis
    y
        Values for y-axis
    parameters
        Parameters of the algorithm, including:
            Parameters.FORMAT -> Format of the target image
            Parameters.TITLE -> Title of the image
    variant
        Variant of the algorithm to apply, including:
            - Variants.CASES
            - Variants.ATTRIBUTES
            - Variants.DATES
            - Variants.BARPLOT

    Returns
    ------------
    temp_file_name
        Representation temporary file name
    """
    return exec_utils.get_variant(variant).apply_plot(
        x, y, parameters=parameters
    )


def apply_semilogx(
    x: List[float],
    y: List[float],
    parameters: Optional[Dict[Any, Any]] = None,
    variant=DEFAULT_VARIANT,
) -> str:
    """
    Method to plot (semi-logarithmic way) the graph with axis values contained in x and y

    Parameters
    ------------
    x
        Values for x-axis
    y
        Values for y-axis
    parameters
        Parameters of the algorithm, including:
            Parameters.FORMAT -> Format of the target image
            Parameters.TITLE -> Title of the image
    variant
        Variant of the algorithm to apply, including:
            - Variants.CASES
            - Variants.ATTRIBUTES
            - Variants.DATES
            - Variants.BARPLOT

    Returns
    ------------
    temp_file_name
        Representation temporary file name
    """
    return exec_utils.get_variant(variant).apply_semilogx(
        x, y, parameters=parameters
    )
