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
from pm4py.streaming.algo.discovery.dfg.variants import frequency
from enum import Enum
from pm4py.util import exec_utils


class Variants(Enum):
    FREQUENCY = frequency


DEFAULT_VARIANT = Variants.FREQUENCY


def apply(variant=DEFAULT_VARIANT, parameters=None):
    """
    Discovers a DFG from an event stream

    Parameters
    --------------
    variant
        Variant of the algorithm (default: Variants.FREQUENCY)

    Returns
    --------------
    stream_dfg_obj
        Streaming DFG discovery object
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(parameters=parameters)
