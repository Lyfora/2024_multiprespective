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

from pm4py.streaming.util.dictio.versions import classic, thread_safe, redis
from pm4py.util import exec_utils


class Variants(Enum):
    CLASSIC = classic
    THREAD_SAFE = thread_safe
    REDIS = redis


DEFAULT_VARIANT = Variants.THREAD_SAFE


def apply(variant=DEFAULT_VARIANT, parameters=None):
    """
    Generates a Python dictionary object
    (different implementations are possible)

    Parameters
    ----------------
    variant
        Variant to use
    parameters
        Parameters to use in the generation

    Returns
    -----------------
    dictio
        Dictionary
    """
    return exec_utils.get_variant(variant).apply(parameters=parameters)
