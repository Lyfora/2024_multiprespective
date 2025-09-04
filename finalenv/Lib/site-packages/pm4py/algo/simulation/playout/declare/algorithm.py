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
from pm4py.objects.log.obj import EventLog
from enum import Enum
from pm4py.util import exec_utils
from pm4py.algo.simulation.playout.declare.variants import classic


class Variants(Enum):
    CLASSIC = classic


def apply(declare_model, variant=Variants.CLASSIC, parameters=None) -> EventLog:
    """
    Produce a playout EventLog of a given DECLARE model.
    Optional parameters:
      - n_traces (int): number of traces to generate. Default = 1000
      - min_length (int): minimal length of each trace. Default = 3
      - max_length (int): maximal length of each trace. Default = 15
    """
    return exec_utils.get_variant(variant).apply(declare_model, parameters)
