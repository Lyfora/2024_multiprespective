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
from enum import Enum
from pm4py.algo.concept_drift.variants import bose
from pm4py.util import exec_utils
import pandas as pd
from pm4py.objects.log.obj import EventLog
from typing import Union, Dict, Any, Optional, Tuple, List


class Variants(Enum):
    BOSE = bose


def apply(log: Union[EventLog, pd.DataFrame], variant=Variants.BOSE, parameters: Optional[Dict[Any, Any]] = None) -> \
Tuple[List[pd.DataFrame], List[int], List[float]]:
    """
    Parameters
    --------------
    log
        Event log or Pandas dataframe
    variant
        Variant of the algorithm (available: Variants.BOSE)
    parameters
        Variant-specific parameters

    Returns
    ---------------
    returned_sublogs : List[EventLog]
        A list of sub-logs, where each sub-log is an EventLog object representing the cumulative segment of the original event log from the start up to each detected change point (and the final sub-log up to the end). Note: Due to a potential implementation issue, these sub-logs are not segments between change points but rather cumulative logs up to each change point.
    change_timestamps : List[float]
        A list of timestamps where concept drifts are detected. Each timestamp corresponds to the start time of the first trace in the sub-log where a change point occurs, based on case start timestamps.
    p_values : List[float]
        A list of p-values associated with each detected change point, indicating the statistical significance of the drift (lower values suggest stronger evidence of a change).
    """
    return exec_utils.get_variant(variant).apply(log, parameters)
