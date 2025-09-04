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
import networkx as nx

from pm4py.objects.process_tree.obj import ProcessTree, Operator as PTOperator
from pm4py.objects.powl.obj import POWL, StrictPartialOrder, OperatorPOWL, Transition, SilentTransition
from typing import Optional, Dict, Any
from pm4py.util import nx_utils
from collections import deque
import warnings


def assign_levels_to_toposort(graph):
    """
    Internal method
    """
    # First, find all nodes with in-degree == 0 to identify start nodes
    start_nodes = [node for node, degree in graph.in_degree() if degree == 0]

    # Initialize a queue for BFS
    queue = deque(start_nodes)
    # Initialize level dictionary to hold the level of each node
    levels = {node: 0 for node in start_nodes}

    while queue:
        current_node = queue.popleft()
        current_level = levels[current_node]

        # For each successor of the current node
        for successor in graph.successors(current_node):
            # If the successor has not been assigned a level yet
            if successor not in levels:
                # Assign the level as current node's level + 1
                levels[successor] = current_level + 1
                # Add the successor to the queue to process its successors later
                queue.append(successor)

    return levels


def apply_recursive(powl: POWL, rec_depth=0) -> ProcessTree:
    """
    Internal method
    """
    tree = ProcessTree()

    if isinstance(powl, Transition):
        tree.label = powl.label
    elif isinstance(powl, OperatorPOWL):
        tree.operator = powl.operator

        for c in powl.children:
            tree.children.append(apply_recursive(c, rec_depth + 1))
    else:
        # detects the connected components of the POWL partial order
        # which correspond to parts of the process executed in parallel
        DG = nx_utils.DiGraph()

        for c in powl.children:
            DG.add_node(c)

        for n1 in powl.children:
            for n2 in powl.children:
                if n1 != n2:
                    if powl.order.is_edge(n1, n2):
                        DG.add_edge(n1, n2)

        DG = nx.transitive_reduction(DG)
        G = nx.Graph(DG)
        conn_comp = list(nx_utils.connected_components(G))

        children = []

        for scc in conn_comp:
            # for every connected component, create the subgraph and get the topological sort
            subgraph = DG.subgraph(scc)

            # Safety check: the subgraph must be a DAG (no cycles)
            if not nx.is_directed_acyclic_graph(subgraph):
                raise Exception("The provided POWL model is invalid!")

            topo_sort = list(nx.topological_sort(subgraph))

            if len(topo_sort) == 1:
                # take the only node of the topological sort and continue the translation
                subtree = apply_recursive(topo_sort[0], rec_depth + 1)
            else:
                # assign a level using BFS starting from the nodes without any ancestor
                levels0 = assign_levels_to_toposort(subgraph)
                max_level = max(v for v in levels0.values())
                levels = []
                for i in range(max_level+1):
                    levels.append([])
                for k, v in levels0.items():
                    levels[v].append(k)

                # check if from every node of the level below you can go to every node of the level after
                for i in range(len(levels)-1):
                    for n in levels[i]:
                        for m in levels[i+1]:
                            if not G.has_edge(n, m):
                                warnings.warn("This POWL model cannot be converted precisely.")

                # if so, the POWL model can be converted to a process tree.
                # do that systematically
                internal_children1 = []
                for i in range(len(levels)):
                    internal_children2 = []
                    for c in levels[i]:
                        internal_children2.append(apply_recursive(c, rec_depth + 1))

                    if len(internal_children2) == 1:
                        internal_children1.append(internal_children2[0])
                    else:
                        subtree2 = ProcessTree(operator=PTOperator.PARALLEL, children=internal_children2)
                        for c2 in subtree2.children:
                            c2.parent = subtree2
                        internal_children1.append(subtree2)

                # create a sequence including potential many parallel operators (one for every BFS level of the
                # topological sort)
                subtree = ProcessTree(operator=PTOperator.SEQUENCE, children=internal_children1)
                for c1 in subtree.children:
                    c1.parent = subtree

            children.append(subtree)

        if len(children) == 1:
            # if there is only one connected component, return the process tree corresponding to that
            tree = children[0]
        else:
            # create an AND operator
            tree.operator = PTOperator.PARALLEL

            for child in children:
                tree.children.append(child)

    for child in tree.children:
        child.parent = tree

    return tree


def apply(powl: POWL, parameters: Optional[Dict[Any, Any]] = None) -> ProcessTree:
    """
    Converts a POWL model to a process tree, in the situations in which this is possible

    Parameters
    ---------------
    powl
        POWL model

    Returns
    ---------------
    process_tree
        Process tree
    """
    if parameters is None:
        parameters = {}

    return apply_recursive(powl)
