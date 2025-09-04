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
import uuid
from pm4py.objects.petri_net.utils import petri_utils
from typing import Optional, Dict, Any, Tuple
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.trie.obj import Trie


def __add_prefix_tree_node_to_petri(
    node: Trie, net: PetriNet, prev: PetriNet.Place, sink: PetriNet.Place
):
    """
    Internal method to add a prefix tree node to a Petri net.
    """
    trans = PetriNet.Transition(str(uuid.uuid4()), label=node.label)
    net.transitions.add(trans)
    petri_utils.add_arc_from_to(prev, trans, net)
    if node.children:
        succ_place = PetriNet.Place(str(uuid.uuid4()))
        net.places.add(succ_place)
        petri_utils.add_arc_from_to(trans, succ_place, net)
        for child in node.children:
            __add_prefix_tree_node_to_petri(child, net, succ_place, sink)
        if node.final:
            new_trans = PetriNet.Transition(str(uuid.uuid4()))
            net.transitions.add(new_trans)
            petri_utils.add_arc_from_to(succ_place, new_trans, net)
            petri_utils.add_arc_from_to(new_trans, sink, net)
    else:
        petri_utils.add_arc_from_to(trans, sink, net)


def apply(
    prefix_tree: Trie, parameters: Optional[Dict[Any, Any]] = None
) -> Tuple[PetriNet, Marking, Marking]:
    """
    Transforms a prefix tree to an accepting Petri net.

    Parameters
    -----------------
    prefix_tree
        Prefix_tree
    parameters
        Optional parameters of the method.

    Returns
    -----------------
    net
        Petri net
    im
        Initial marking
    fm
        Final marking
    """
    if parameters is None:
        parameters = {}

    # create a empty Petri net
    net = PetriNet("petri")
    im = Marking()
    fm = Marking()
    source = PetriNet.Place(str(uuid.uuid4()))
    net.places.add(source)
    sink = PetriNet.Place(str(uuid.uuid4()))
    net.places.add(sink)
    im[source] = 1
    fm[sink] = 1

    __add_prefix_tree_node_to_petri(
        list(prefix_tree.children)[0], net, source, sink
    )

    return net, im, fm
