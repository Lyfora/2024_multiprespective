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
from pm4py import util
from pm4py.objects.log.obj import EventLog
from pm4py.objects.trie.obj import Trie
from pm4py.statistics.variants.log import get as get_variants_log
from pm4py.statistics.variants.pandas import get as get_variants_pandas
from pm4py.util import pandas_utils, exec_utils
from typing import Optional, Dict, Any, Union
import pandas as pd


class Parameters(Enum):
    ACTIVITY_KEY = util.constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    MAX_PATH_LENGTH = (
        "max_path_length"  # New parameter for maximum path length
    )


def apply(
    log: Union[EventLog, pd.DataFrame],
    parameters: Optional[Dict[Union[str, Parameters], Any]] = None,
) -> Trie:
    parameters = parameters if parameters is not None else dict()

    # Extract the maximum path length if provided
    max_path_length = exec_utils.get_param_value(
        Parameters.MAX_PATH_LENGTH, parameters, None
    )

    if pandas_utils.check_is_pandas_dataframe(log):
        variants = get_variants_pandas.get_variants_set(
            log, parameters=parameters
        )
    else:
        variants = get_variants_log.get_variants(log, parameters=parameters)

    variants = list(variants)

    root = Trie()

    for variant in variants:
        # If maximum path length is defined, truncate the variant
        if max_path_length is not None and len(variant) > max_path_length:
            variant = variant[:max_path_length]

        trie = root
        for i, activity in enumerate(variant):
            match = False
            for c in trie.children:
                if c.label == activity:
                    trie = c
                    match = True
                    break
            if not match:
                node = Trie(label=activity, parent=trie, depth=trie.depth + 1)
                trie.children.append(node)
                trie = node
            # If at the end of this (possibly truncated) variant, mark as final
            if i == len(variant) - 1:
                trie.final = True

    return root
