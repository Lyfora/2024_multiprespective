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
import sys

import numpy as np

GS_SCORE = 1  # score has sensitivity of 1


def score(output_universes):
    return [x for x in np.flip(output_universes)]


def exp_mech(output_universes, epsilon):
    scores = score(output_universes)
    raw_prob = [np.exp((epsilon * x) / (2 * GS_SCORE)) for x in scores]
    i = 0
    for prob in raw_prob:
        if prob == float('inf'):
            raw_prob[i] = sys.float_info.max
        i += 1
    prob = np.exp(raw_prob - np.max(raw_prob))
    prob = prob / prob.sum()
    return np.random.choice(output_universes, p=prob)
