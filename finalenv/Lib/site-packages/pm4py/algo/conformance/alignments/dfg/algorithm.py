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
from pm4py.algo.conformance.alignments.dfg.variants import classic
from enum import Enum
from pm4py.util import exec_utils
from pm4py.objects.log.obj import EventLog, Trace
from typing import Optional, Dict, Any, Union, Tuple
from pm4py.util import typing
import pandas as pd


class Variants(Enum):
    CLASSIC = classic


def apply(
    obj: Union[EventLog, pd.DataFrame, Trace],
    dfg: Dict[Tuple[str, str], int],
    sa: Dict[str, int],
    ea: Dict[str, int],
    variant=Variants.CLASSIC,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Union[typing.AlignmentResult, typing.ListAlignments]:
    """
    Applies the alignment algorithm provided a log/trace object, and a *connected* DFG

    Parameters
    --------------
    obj
        Event log / Trace
    dfg
        *Connected* directly-Follows Graph
    sa
        Start activities
    ea
        End activities
    variant
        Variant of the DFG alignments to be used. Possible values:
        - Variants.CLASSIC
    parameters
        Variant-specific parameters.

    Returns
    --------------
    ali
        Result of the alignment
    """
    return exec_utils.get_variant(variant).apply(
        obj, dfg, sa, ea, parameters=parameters
    )
