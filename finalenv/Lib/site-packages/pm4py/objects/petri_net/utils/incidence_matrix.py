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
class IncidenceMatrix(object):

    def __init__(self, net):
        self.__A, self.__place_indices, self.__transition_indices = (
            self.__construct_matrix(net)
        )

    def encode_marking(self, marking):
        x = [0 for i in range(len(self.places))]
        for p in marking:
            x[self.places[p]] = marking[p]
        return x

    def __get_a_matrix(self):
        return self.__A

    def __get_transition_indices(self):
        return self.__transition_indices

    def __get_place_indices(self):
        return self.__place_indices

    def __construct_matrix(self, net):
        self.matrix_built = True
        p_index, t_index = {}, {}
        places = sorted(
            [x for x in net.places], key=lambda x: (str(x.name), id(x))
        )
        transitions = sorted(
            [x for x in net.transitions], key=lambda x: (str(x.name), id(x))
        )

        for p in places:
            p_index[p] = len(p_index)
        for t in transitions:
            t_index[t] = len(t_index)
        a_matrix = [
            [0 for i in range(len(t_index))] for j in range(len(p_index))
        ]
        for p in net.places:
            for a in p.in_arcs:
                a_matrix[p_index[p]][t_index[a.source]] += 1
            for a in p.out_arcs:
                a_matrix[p_index[p]][t_index[a.target]] -= 1
        return a_matrix, p_index, t_index

    a_matrix = property(__get_a_matrix)
    places = property(__get_place_indices)
    transitions = property(__get_transition_indices)


def construct(net):
    return IncidenceMatrix(net)
