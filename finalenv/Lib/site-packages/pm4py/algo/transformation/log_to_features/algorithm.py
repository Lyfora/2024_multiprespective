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
from typing import Any, Optional, Dict, Union, List, Tuple

import pandas as pd

from pm4py.objects.log.obj import EventLog, EventStream
from pm4py.util import exec_utils
from pm4py.algo.transformation.log_to_features.variants import (
    event_based,
    trace_based,
    temporal,
)


class Variants(Enum):
    EVENT_BASED = event_based
    TRACE_BASED = trace_based
    TEMPORAL = temporal


def apply(
    log: Union[EventLog, pd.DataFrame, EventStream],
    variant: Any = Variants.TRACE_BASED,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Tuple[Any, List[str]]:
    """
    Extracts the features from a log object

    Parameters
    ---------------
    log
        Event log
    variant
        Variant of the feature extraction to use:
        - Variants.EVENT_BASED => (default) extracts, for each trace, a list of numerical vectors containing for each
            event the corresponding features
        - Variants.TRACE_BASED => extracts for each trace a single numerical vector containing the features
            of the trace
        - Variants.TEMPORAL => extracts temporal features from the traditional event log

    Returns
    ---------------
    data
        Data to provide for decision tree learning
    feature_names
        Names of the features, in order
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(log, parameters=parameters)
