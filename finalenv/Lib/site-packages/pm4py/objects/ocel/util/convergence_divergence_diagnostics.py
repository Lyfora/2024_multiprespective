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
from pm4py.objects.ocel.util import (
    events_per_type_per_activity,
    objects_per_type_per_activity,
)
from typing import Optional, Dict, Any
from pm4py.objects.ocel.obj import OCEL


def apply(ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None):
    """
    Reports the activities and the object types for which the convergence / divergence problems occur.

    Parameters
    ----------------
    ocel
        Object-centric event log
    parameters
        Parameters of the algorithm

    Returns
    ----------------
    ret
        Dictionary with two keys ("convergence" and "divergence"). Each key is associated to a set
        of (activity, object_type) for which the specific problem occurs. An activity/object type
        which does not appear neither in the "convergence" and "divergence" section does not suffer
        of convergence and divergence problems.
    """
    if parameters is None:
        parameters = {}

    ev_per_type_per_act = events_per_type_per_activity.apply(
        ocel, parameters=parameters
    )
    obj_per_type_per_act = objects_per_type_per_activity.apply(
        ocel, parameters=parameters
    )

    ret = {"divergence": set(), "convergence": set()}

    # analyze the divergence problems
    for act in ev_per_type_per_act:
        for ot in ev_per_type_per_act[act]:
            if ev_per_type_per_act[act][ot]["median"] > 1:
                ret["divergence"].add((act, ot))

    # analyze the convergence problems
    for act in obj_per_type_per_act:
        for ot in obj_per_type_per_act[act]:
            if obj_per_type_per_act[act][ot]["median"] > 1:
                ret["convergence"].add((act, ot))

    return ret
