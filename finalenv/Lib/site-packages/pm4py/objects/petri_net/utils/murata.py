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

from pm4py.util.lp import solver
from pm4py.util import constants
from pm4py.objects.petri_net.utils.petri_utils import remove_place
from copy import copy
from typing import Tuple
import warnings
import importlib.util
from pm4py.objects.petri_net.obj import PetriNet, Marking


def apply_reduction(
    net: PetriNet, im: Marking, fm: Marking
) -> Tuple[PetriNet, Marking, Marking]:
    """
    Apply the Murata reduction to an accepting Petri net, removing the structurally redundant places.

    The implementation follows the Berthelot algorithm as in:
    https://svn.win.tue.nl/repos/prom/Packages/Murata/Trunk/src/org/processmining/algorithms/BerthelotAlgorithm.java

    Parameters
    ---------------
    net
        Petri net
    im
        Initial marking
    fm
        Final marking

    Returns
    --------------
    net
        Petri net
    im
        Initial marking
    fm
        Final marking
    """
    places = sorted(list(net.places), key=lambda x: x.name)
    redundant = set()
    for place in places:
        # Skip places in the initial or final markings
        if place in im or place in fm:
            continue

        Aeq = []
        Aub = []
        beq = []
        bub = []

        # first constraint
        constraint = [0] * (len(net.places) + 1)
        for p2 in im:
            if p2 not in redundant:
                if p2 == place:
                    constraint[places.index(p2)] = im[p2]
                else:
                    constraint[places.index(p2)] = -im[p2]
        constraint[-1] = -1
        Aeq.append(constraint)
        beq.append(0)

        # second constraints
        for trans in net.transitions:
            constraint = [0] * (len(net.places) + 1)

            for arc in trans.in_arcs:
                p2 = arc.source
                if p2 not in redundant:
                    if p2 == place:
                        constraint[places.index(p2)] = arc.weight
                    else:
                        constraint[places.index(p2)] = -arc.weight
            constraint[-1] = -1
            Aub.append(constraint)
            bub.append(0)

        # third constraints
        for trans in net.transitions:
            constraint = [0] * (len(net.places) + 1)

            for arc in trans.out_arcs:
                p2 = arc.target
                if p2 not in redundant:
                    if p2 == place:
                        constraint[places.index(p2)] = -arc.weight
                    else:
                        constraint[places.index(p2)] = arc.weight
            Aub.append(constraint)
            bub.append(0)

        # fourth constraint
        for p2 in net.places:
            if p2 not in redundant:
                constraint = [0] * (len(net.places) + 1)

                constraint[places.index(p2)] = -1

                Aub.append(constraint)
                if p2 == place:
                    bub.append(-1)
                else:
                    bub.append(0)

        # fifth constraint
        constraint = [0] * (len(net.places) + 1)
        constraint[-1] = -1
        Aub.append(constraint)
        bub.append(0)

        c = [1] * (len(net.places) + 1)
        integrality = [1] * (len(net.places) + 1)

        proposed_solver = solver.SCIPY
        if importlib.util.find_spec("pulp"):
            proposed_solver = solver.PULP
        else:
            if constants.SHOW_INTERNAL_WARNINGS:
                warnings.warn(
                    "solution from scipy may be unstable. Please install PuLP (pip install pulp) for fully reliable results."
                )

        xx = solver.apply(
            c,
            Aub,
            bub,
            Aeq,
            beq,
            variant=proposed_solver,
            parameters={"integrality": integrality},
        )

        if (hasattr(xx, "success") and xx.success) or (
            hasattr(xx, "sol_status") and xx.sol_status > -1
        ):
            redundant.add(place)

    for place in redundant:
        # Remove the redundant place from the net
        net = remove_place(net, place)

    return net, im, fm
