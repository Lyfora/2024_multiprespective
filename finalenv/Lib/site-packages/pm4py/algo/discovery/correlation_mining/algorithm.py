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
from pm4py.algo.discovery.correlation_mining.variants import (
    classic_split,
    classic,
    trace_based,
)
from pm4py.util import exec_utils
from enum import Enum
from typing import Optional, Dict, Any, Union, Tuple
from pm4py.objects.log.obj import EventLog, EventStream
import pandas as pd


class Variants(Enum):
    CLASSIC_SPLIT = classic_split
    CLASSIC = classic
    TRACE_BASED = trace_based


DEFAULT_VARIANT = Variants.CLASSIC


def apply(
    log: Union[EventLog, EventStream, pd.DataFrame],
    variant=DEFAULT_VARIANT,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Tuple[Dict[Tuple[str, str], int], Dict[Tuple[str, str], float]]:
    """
    Applies the Correlation Miner to the event stream (a log is converted to a stream)

    The approach is described in:
    Pourmirza, Shaya, Remco Dijkman, and Paul Grefen. "Correlation miner: mining business process models and event
    correlations without case identifiers." International Journal of Cooperative Information Systems 26.02 (2017):
    1742002.

    Parameters
    -------------
    log
        Log object
    variant
        Variant of the algorithm to use
    parameters
        Parameters of the algorithm

    Returns
    --------------
    dfg
        Directly-follows graph
    performance_dfg
        Performance DFG (containing the estimated performance for the arcs)
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(log, parameters=parameters)
