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
from pm4py.objects.conversion.heuristics_net.variants import to_petri_net
from enum import Enum
from pm4py.util import exec_utils


class Variants(Enum):
    TO_PETRI_NET = to_petri_net


def apply(heu_net, parameters=None, variant=Variants.TO_PETRI_NET):
    """
    Converts an Heuristics Net to a different type of object

    Parameters
    --------------
    heu_net
        Heuristics net
    parameters
        Possible parameters of the algorithm
    variant
        Variant of the algorithm:
            - Variants.TO_PETRI_NET
    """
    return exec_utils.get_variant(variant).apply(
        heu_net, parameters=parameters
    )
