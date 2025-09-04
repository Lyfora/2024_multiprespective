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
from pm4py.util import exec_utils, constants, xes_constants
from typing import Union, Dict, Collection, List
from enum import Enum


class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    PARAMETER_VARIANT_DELIMITER = "variant_delimiter"


def variant_to_trace(variant, parameters=None):
    if parameters is None:
        parameters = {}

    activity_key = exec_utils.get_param_value(
        Parameters.ACTIVITY_KEY, parameters, xes_constants.DEFAULT_NAME_KEY
    )
    variant_delimiter = exec_utils.get_param_value(
        Parameters.PARAMETER_VARIANT_DELIMITER,
        parameters,
        constants.DEFAULT_VARIANT_SEP,
    )

    from pm4py.objects.log.obj import Trace, Event

    trace = Trace()
    if type(variant) is tuple or type(variant) is list:
        for act in variant:
            event = Event({activity_key: act})
            trace.append(event)
    elif type(variant) is str:
        var_act = variant.split(variant_delimiter)
        for act in var_act:
            event = Event({activity_key: act})
            trace.append(event)

    return trace


def get_activities_from_variant(variant, parameters=None):
    if parameters is None:
        parameters = {}

    return tuple(variant)


def get_variant_from_trace(trace, parameters=None):
    if parameters is None:
        parameters = {}

    activity_key = exec_utils.get_param_value(
        Parameters.ACTIVITY_KEY, parameters, xes_constants.DEFAULT_NAME_KEY
    )

    return tuple([x[activity_key] for x in trace])


def __aggregate_variant(
    variant: Collection[str], max_repetitions: int = 1
) -> Collection[str]:
    """
    Internal method
    """
    aggregated_variant = []
    act = None
    count = 0
    i = 0
    while i < len(variant):
        count = count + 1
        if variant[i] != act:
            # reset the counter when a new activity is encountered
            act = variant[i]
            count = 1
        if count <= max_repetitions:
            aggregated_variant.append(act)
        i = i + 1
    return tuple(aggregated_variant)


def aggregate_consecutive_activities_in_variants(
    variants: Dict[Collection[str], Union[int, List]], max_repetitions: int = 1
) -> Dict[Collection[str], Union[int, List]]:
    """
    Aggregate the consecutive activities in the variant.

    For example, {('A', 'B', 'C'): 3, ('A', 'B', 'B', 'B', 'C'): 2, ('A', 'B', 'B', 'B', 'B', 'B', 'C'): 1}
    Would be reduced to:
    - {('A', 'B', 'C'): 6} if max_repetitions=1
    - {('A', 'B', 'C'): 3, ('A', 'B', 'B', 'C'): 3} if max_repetitions=2
    - {('A', 'B', 'C'): 3, ('A', 'B', 'B', 'B', 'C'): 3} if max_repetitions=3
    - {('A', 'B', 'C'): 3, ('A', 'B', 'B', 'B', 'C'): 2, ('A', 'B', 'B', 'B', 'B', 'C'): 1} if max_repetitions=4

    Parameters
    ----------------
    variants
        Dictionary of variants (where each variant is associated to its count (pd.DataFrame) or the list of traces (EventLog))
    max_repetitions
        Maximum number of consecutive repetitions for an activity

    Returns
    ----------------
    aggregated_variants
        Dictionary of variants aggregated based on the limit of consecutive repetitions for the same activity
    """
    aggregated_variants = {}

    for variant, value in variants.items():
        aggregated_variant = __aggregate_variant(variant, max_repetitions)
        if aggregated_variant not in aggregated_variants:
            if type(value) is int:
                aggregated_variants[aggregated_variant] = 0
            else:
                aggregated_variants[aggregated_variant] = []

        aggregated_variants[aggregated_variant] += value

    return aggregated_variants
