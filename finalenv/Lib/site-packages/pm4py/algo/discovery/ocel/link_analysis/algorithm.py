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
from pm4py.algo.discovery.ocel.link_analysis.variants import classic
from enum import Enum
from pm4py.util import exec_utils
import pandas as pd
from typing import Optional, Dict, Any
from pm4py.objects.log.obj import EventLog, EventStream
from typing import Union
from pm4py.objects.conversion.log import converter


class Variants(Enum):
    CLASSIC = classic


def apply(
    log: Union[EventLog, EventStream, pd.DataFrame],
    variant=Variants.CLASSIC,
    parameters: Optional[Dict[Any, Any]] = None,
) -> pd.DataFrame:
    """
    Applies a link analysis algorithm on the provided log object.

    Parameters
    -----------------
    log
        Event log
    variant
        Variant of the algorithm to consider
    parameters
        Variant-specific parameters

    Returns
    -----------------
    link_analysis_dataframe
        Link analysis dataframe
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(
        converter.apply(
            log,
            variant=converter.Variants.TO_DATA_FRAME,
            parameters=parameters,
        ),
        parameters=parameters,
    )
