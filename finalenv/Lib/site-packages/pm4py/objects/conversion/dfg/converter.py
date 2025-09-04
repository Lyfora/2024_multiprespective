'''
    PM4Py â€“ A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschrÃ¤nkt)

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
from pm4py.objects.conversion.dfg.variants import (
    to_petri_net_activity_defines_place,
    to_petri_net_invisibles_no_duplicates,
)
from enum import Enum
from pm4py.util import exec_utils


class Variants(Enum):
    VERSION_TO_PETRI_NET_ACTIVITY_DEFINES_PLACE = (
        to_petri_net_activity_defines_place
    )
    VERSION_TO_PETRI_NET_INVISIBLES_NO_DUPLICATES = (
        to_petri_net_invisibles_no_duplicates
    )


DEFAULT_VARIANT = Variants.VERSION_TO_PETRI_NET_ACTIVITY_DEFINES_PLACE


def apply(dfg, parameters=None, variant=DEFAULT_VARIANT):
    return exec_utils.get_variant(variant).apply(dfg, parameters=parameters)
