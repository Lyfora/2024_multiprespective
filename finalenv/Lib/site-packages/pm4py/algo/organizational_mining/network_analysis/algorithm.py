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
from pm4py.algo.organizational_mining.network_analysis.variants import (
    dataframe,
)
from enum import Enum
from pm4py.util import exec_utils
from typing import Dict, Optional, Any, Tuple, Union
import pandas as pd
from pm4py.objects.log.obj import EventLog, EventStream
from pm4py.objects.conversion.log import converter as log_converter


class Variants(Enum):
    DATAFRAME = dataframe


def apply(
    log: Union[pd.DataFrame, EventLog, EventStream],
    variant=Variants.DATAFRAME,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """
    Performs the network analysis on the provided event log

    Parameters
    ----------------
    log
        Event log
    parameters
        Version-specific parameters

    Returns
    ----------------
    network_analysis
        Edges of the network analysis (first key: edge; second key: type; value: number of occurrences)
    """
    return exec_utils.get_variant(variant).apply(
        log_converter.apply(
            log,
            variant=log_converter.Variants.TO_DATA_FRAME,
            parameters=parameters,
        ),
        parameters=parameters,
    )
