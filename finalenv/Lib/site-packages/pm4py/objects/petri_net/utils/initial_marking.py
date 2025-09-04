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
from pm4py.objects.petri_net.obj import Marking


def discover_initial_marking(petri):
    """
    Discovers initial marking from a Petri net

    Parameters
    ------------
    petri
        Petri net

    Returns
    ------------
    initial_marking
        Initial marking of the Petri net
    """
    initial_marking = Marking()

    for place in petri.places:
        if len(place.in_arcs) == 0:
            initial_marking[place] = 1

    return initial_marking
