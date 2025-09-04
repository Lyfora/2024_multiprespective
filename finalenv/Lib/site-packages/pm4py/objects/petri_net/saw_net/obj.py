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

from typing import Dict, List, Tuple
from pm4py.objects.petri_net.stochastic.obj import StochasticPetriNet


class StochasticArcWeightNet(StochasticPetriNet):
    """
    Petri nets with stochastic arc weights. Arcs are assumed to desribe distributions of token consumption/production.
    We utilize the weight attribute that is defined in the Petri net based class to store the distribution.
    """

    class Arc(StochasticPetriNet.Arc):

        def __init__(self, source, target, weight={1: 1.0}, properties=None):
            super().__init__(source, target, weight, properties)

        def __get_weight_distribution(self) -> Dict[int, float]:
            return self.weight

        weight_distribution = property(__get_weight_distribution)

    Binding = List[Tuple[Arc, int]]
