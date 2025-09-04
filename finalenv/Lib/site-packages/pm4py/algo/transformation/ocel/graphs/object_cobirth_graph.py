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
from pm4py.objects.ocel.obj import OCEL
from typing import Optional, Dict, Any, Set, Tuple


def apply(
    ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None
) -> Set[Tuple[str, str]]:
    """
    Calculates the object cobirth graph.
    This is calculated as follows:
     - Given the set of objects related to an event, they belong to two different categories:
        - The "seen" objects (they have appeared in some earlier event)
        - The "unseen" objects (they appear for the first time in the current event).
     - Every "unseen" object is connected to every "unseen" object

    Parameters
    -----------------
    ocel
        Object-centric event log
    parameters
        Parameters of the algorithm

    Returns
    ------------------
    object_cobirth_graph
        Object cobirth graph (undirected)
    """
    if parameters is None:
        parameters = {}

    graph = set()

    ordered_events = ocel.events[ocel.event_id_column].to_numpy()
    ev_rel_obj = (
        ocel.relations.groupby(ocel.event_id_column)[ocel.object_id_column]
        .agg(list)
        .to_dict()
    )
    set_objects = set()

    for ev in ordered_events:
        rel_obj = ev_rel_obj[ev]
        rel_obj_seen = {x for x in rel_obj if x in set_objects}
        rel_obj_unseen = {x for x in rel_obj if x not in rel_obj_seen}

        for o1 in rel_obj_unseen:
            for o2 in rel_obj_unseen:
                if o1 < o2:
                    graph.add((o1, o2))

        for obj in rel_obj_unseen:
            set_objects.add(obj)

    return graph
