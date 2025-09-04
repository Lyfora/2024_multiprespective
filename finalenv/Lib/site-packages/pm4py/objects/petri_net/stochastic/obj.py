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

from typing import Any, Collection, Dict

from pm4py.objects.petri_net.obj import PetriNet


class StochasticPetriNet(PetriNet):

    class Transition(PetriNet.Transition):
        def __init__(
            self,
            name: str,
            label: str = None,
            in_arcs: Collection[PetriNet.Arc] = None,
            out_arcs: Collection[PetriNet.Arc] = None,
            weight: float = 1.0,
            properties: Dict[str, Any] = None,
        ):
            super().__init__(name, label, in_arcs, out_arcs, properties)
            self.__weight = weight

        def __set_weight(self, weight: float):
            self.__weight = weight

        def __get_weight(self) -> float:
            return self.__weight

        weight = property(__get_weight, __set_weight)
