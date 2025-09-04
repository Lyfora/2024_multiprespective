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

from pm4py.algo.discovery.inductive.dtypes.im_ds import IMDataStructureUVCL
from pm4py.util import constants
from typing import Optional, Dict, Any, Union, Tuple


class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_TIMESTAMP_KEY
    START_TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_START_TIMESTAMP_KEY
    KEEP_FIRST_FOLLOWING = "keep_first_following"


def apply(
    interval_log: IMDataStructureUVCL,
    parameters: Optional[Dict[Union[str, Parameters], Any]] = None,
) -> Dict[Tuple[str, str], int]:
    if parameters is None:
        parameters = {}

    ret_dict = {}
    for trace, freq in interval_log.data_structure.items():
        i = 0
        while i < len(trace):
            act1 = trace[i]
            j = i + 1
            while j < len(trace):
                act2 = trace[j]
                tup = (act1, act2)
                if tup in ret_dict.keys():
                    ret_dict[tup] = ret_dict[tup] + freq
                else:
                    ret_dict[tup] = freq
                j = j + 1
            i = i + 1

    return ret_dict
