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
from pm4py.algo.simulation.playout.dfg.variants import classic, performance
from enum import Enum
from pm4py.util import exec_utils
from typing import Optional, Dict, Any, Union, Tuple
from pm4py.objects.log.obj import EventLog


class Variants(Enum):
    CLASSIC = classic
    PERFORMANCE = performance


def apply(
    dfg: Dict[Tuple[str, str], int],
    start_activities: Dict[str, int],
    end_activities: Dict[str, int],
    variant=Variants.CLASSIC,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Union[EventLog, Dict[Tuple[str, str], int]]:
    """
    Applies the playout algorithm on a DFG, extracting the most likely traces according to the DFG

    Parameters
    ---------------
    dfg
        *Complete* DFG
    start_activities
        Start activities
    end_activities
        End activities
    variant
        Variant of the playout to be used, possible values:
        - Variants.CLASSIC
        - Variants.PERFORMANCE
    parameters
        Parameters of the algorithm

    Returns
    ---------------
    simulated_log
        Simulated log
    """
    return exec_utils.get_variant(variant).apply(
        dfg, start_activities, end_activities, parameters=parameters
    )
