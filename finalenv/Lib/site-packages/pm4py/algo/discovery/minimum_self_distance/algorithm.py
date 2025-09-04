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
from typing import Union, Optional, Dict, Any

import pandas as pd

from pm4py.algo.discovery.minimum_self_distance.variants import log, pandas
from pm4py.objects.log.obj import EventLog, EventStream
from pm4py.util import exec_utils, pandas_utils


class Variants(Enum):
    LOG = log
    PANDAS = pandas


def apply(
    log_obj: Union[EventLog, pd.DataFrame, EventStream],
    variant: Union[str, None] = None,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Dict[str, int]:
    if parameters is None:
        parameters = {}

    if variant is None:
        if pandas_utils.check_is_pandas_dataframe(log_obj):
            variant = Variants.PANDAS
        else:
            variant = Variants.LOG

    return exec_utils.get_variant(variant).apply(
        log_obj, parameters=parameters
    )
