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

from pm4py.util import exec_utils
from enum import Enum
from pm4py.algo.discovery.declare.variants import classic
from pm4py.objects.log.obj import EventLog
import pandas as pd
from typing import Union, Dict, Optional, Any


class Variants(Enum):
    CLASSIC = classic


def apply(
    log: Union[EventLog, pd.DataFrame],
    variant=Variants.CLASSIC,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Dict[str, Dict[Any, Dict[str, int]]]:
    """
    Discovers a DECLARE model from the provided event log

    Parameters
    ---------------
    log
        Log object (EventLog, Pandas dataframe)
    variant
        Variant of the algorithm to be used, including:
        - Variants.CLASSIC
    parameters
        Variant-specific parameters

    Returns
    ---------------
    declare_model
        DECLARE model (as Python dictionary), where each template is associated with its own rules
    """
    return exec_utils.get_variant(variant).apply(log, parameters)
