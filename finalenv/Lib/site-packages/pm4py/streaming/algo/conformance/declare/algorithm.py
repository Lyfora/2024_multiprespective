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
from enum import Enum
from pm4py.util import exec_utils
from pm4py.streaming.algo.conformance.declare.variants import automata


class Variants(Enum):
    AUTOMATA = automata


def apply(declare_model, variant=Variants.AUTOMATA, parameters=None):
    """
    Streaming Conformance Checking Algorithm for DECLARE models.
    Attempts to implement state-based checks for all Declare constraint types.
    When a violation occurs, prints out which constraints are violated.

    Streaming algorithm interface implemented.

    Implementation of:
    Maggi, Fabrizio Maria, et al. "Monitoring business constraints with linear temporal logic: An approach based on colored automata." Business Process Management: 9th International Conference, BPM 2011, Clermont-Ferrand, France, August 30-September 2, 2011. Proceedings 9. Springer Berlin Heidelberg, 2011.
    -------

    """
    return exec_utils.get_variant(variant).apply(declare_model, parameters)
