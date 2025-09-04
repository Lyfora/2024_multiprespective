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
import numpy as np
import math
import copy
from typing import Optional, Dict, Any, List, Tuple, Set
from graphviz import Digraph
from enum import Enum
from pm4py.util import exec_utils, constants, vis_utils
from pm4py.util.lp import solver as lp_solver
from pm4py.objects.petri_net.obj import PetriNet  # For type hinting if needed
from copy import deepcopy


# --- Constants matching TypeScript OCPNLayout ---
PLACE_TYPE = "place"
TRANSITION_TYPE = "transition"
DUMMY_TYPE = "dummy"


# --- Helper Functions ---

def ot_to_color(ot: str) -> str:
    """Generates a deterministic hex color based on the object type string."""
    import hashlib
    hash_obj = hashlib.md5(ot.encode('utf-8'))
    hash_hex = hash_obj.hexdigest()
    color = "#" + hash_hex[:6]
    return color


def generate_dummy_id() -> str:
    """Generates a unique ID for a dummy node."""
    return "dummy_" + str(uuid.uuid4())


def generate_arc_id(source_id: str, target_id: str) -> str:
    """Generates a unique ID for an arc."""
    return f"arc_{source_id}_{target_id}_{uuid.uuid4()}"


def get_neighbors(vertex_id: str, layout_data: Dict, direction: str) -> List[str]:
    """Gets upper ('up') or lower ('down') neighbors of a vertex."""
    neighbors = set()
    arcs = layout_data.get("arcs", {})
    if direction == 'up':
        for arc_id, arc_data in arcs.items():
            if arc_data['target'] == vertex_id:
                neighbors.add(arc_data['source'])
    elif direction == 'down':
        for arc_id, arc_data in arcs.items():
            if arc_data['source'] == vertex_id:
                neighbors.add(arc_data['target'])
    return list(neighbors)


def get_arcs_between(u: str, v: str, layout_data: Dict) -> List[Dict]:
    """Gets arcs connecting vertex u and vertex v directly."""
    connecting_arcs = []
    for arc_id, arc_data in layout_data.get("arcs", {}).items():
        if (arc_data['source'] == u and arc_data['target'] == v) or \
                (arc_data['source'] == v and arc_data['target'] == u):
            connecting_arcs.append(arc_data)
    return connecting_arcs


def get_arcs_between_layers(layer_idx1: int, layer_idx2: int, layout_data: Dict) -> List[Dict]:
    """Gets all arcs connecting nodes between two specific layers."""
    arcs_between = []
    vertices = layout_data.get("vertices", {})
    layering = layout_data.get("layering", [])

    if layer_idx1 < 0 or layer_idx1 >= len(layering) or \
            layer_idx2 < 0 or layer_idx2 >= len(layering):
        return []

    layer1_nodes = set(layering[layer_idx1])
    layer2_nodes = set(layering[layer_idx2])

    for arc_id, arc_data in layout_data.get("arcs", {}).items():
        src = arc_data['source']
        tgt = arc_data['target']
        if (src in layer1_nodes and tgt in layer2_nodes) or \
                (src in layer2_nodes and tgt in layer1_nodes):
            if src in layer1_nodes and tgt in layer2_nodes:
                arcs_between.append(arc_data)
            elif src in layer2_nodes and tgt in layer1_nodes:
                arcs_between.append(arc_data)

    return arcs_between


def is_incident_to_inner_segment(ocpn_layout: Dict, vertex_id: str) -> bool:
    """Checks if a vertex is a dummy node connected to another dummy node above it."""
    vertices = ocpn_layout.get("vertices", {})
    v = vertices.get(vertex_id)
    if v and v.get("type") == DUMMY_TYPE:
        upper_neighbor_id = v.get("upper_neighbor")
        if upper_neighbor_id:
            upper_neighbor = vertices.get(upper_neighbor_id)
            if upper_neighbor and upper_neighbor.get("type") == DUMMY_TYPE:
                return True
    return False


# --- Core Sugiyama Steps ---

def _build_initial_layout(ocpn: Dict[str, Any], parameters: Dict) -> Dict:
    object_types = sorted(ocpn.get("object_types", []))
    layout_data = {
        "vertices": {},
        "arcs": {},
        "original_arcs": {},
        "object_types": object_types,
        "config": parameters
    }
    vertex_map = {}

    for ot in object_types:
        net_data = ocpn.get("petri_nets", {}).get(ot)
        if not net_data:
            print(f"Warning: No Petri net found for object type '{ot}', skipping.")
            continue
        net, im, fm = net_data
        # Create places
        for place in net.places:
            place_id = str(place)
            if (PLACE_TYPE, place_id) not in vertex_map:
                layout_id = f"place_{ot}_{place.name}_{uuid.uuid4()}"
                vertex_map[(PLACE_TYPE, place_id)] = layout_id
                is_source = place in im
                is_sink = place in fm
                layout_data["vertices"][layout_id] = {
                    "id": layout_id,
                    "original_id": place_id,
                    "name": place.name or f"p_{ot}",
                    "label": place.name or f"p_{ot}",
                    "objectType": ot,
                    "type": PLACE_TYPE,
                    "layer": -1,
                    "pos": -1,
                    "x": None,
                    "y": None,
                    "is_source": is_source,
                    "is_sink": is_sink,
                    "root": layout_id,
                    "align": layout_id,
                    "sink": layout_id,
                    "shift": float('inf'),
                    "upper_neighbor": None,
                    "lower_neighbor": None,
                }

        # Create transitions
        for trans in net.transitions:
            trans_id = str(trans)
            layout_id = None
            if trans.label:
                if (TRANSITION_TYPE, trans.label) in vertex_map:
                    layout_id = vertex_map[(TRANSITION_TYPE, trans.label)]
                    layout_data["vertices"][layout_id].setdefault("adjacentObjectTypes", set()).add(ot)
                else:
                    layout_id = f"trans_{trans.label}_{uuid.uuid4()}"
                    vertex_map[(TRANSITION_TYPE, trans.label)] = layout_id
                    layout_data["vertices"][layout_id] = {
                        "id": layout_id,
                        "original_id": trans_id,
                        "name": trans.label,
                        "label": trans.label,
                        "objectType": None,
                        "adjacentObjectTypes": {ot},
                        "type": TRANSITION_TYPE,
                        "silent": False,
                        "layer": -1,
                        "pos": -1,
                        "x": None,
                        "y": None,
                        "root": layout_id,
                        "align": layout_id,
                        "sink": layout_id,
                        "shift": float('inf'),
                        "upper_neighbor": None,
                        "lower_neighbor": None,
                    }
            else:
                layout_id = f"silent_{ot}_{trans.name}_{uuid.uuid4()}"
                vertex_map[(TRANSITION_TYPE, trans_id)] = layout_id
                layout_data["vertices"][layout_id] = {
                    "id": layout_id,
                    "original_id": trans_id,
                    "name": trans.name or "silent",
                    "label": None,
                    "objectType": ot,
                    "adjacentObjectTypes": {ot},
                    "type": TRANSITION_TYPE,
                    "silent": True,
                    "layer": -1,
                    "pos": -1,
                    "x": None,
                    "y": None,
                    "root": layout_id,
                    "align": layout_id,
                    "sink": layout_id,
                    "shift": float('inf'),
                    "upper_neighbor": None,
                    "lower_neighbor": None,
                }

        # Create arcs
        for arc in net.arcs:
            source_node = arc.source
            target_node = arc.target
            source_id_orig = str(source_node)
            target_id_orig = str(target_node)

            source_type = PLACE_TYPE if isinstance(source_node, PetriNet.Place) else TRANSITION_TYPE
            target_type = PLACE_TYPE if isinstance(target_node, PetriNet.Place) else TRANSITION_TYPE

            if source_type == TRANSITION_TYPE and source_node.label:
                source_layout_id = vertex_map.get((source_type, source_node.label))
            else:
                source_layout_id = vertex_map.get((source_type, source_id_orig))

            if target_type == TRANSITION_TYPE and target_node.label:
                target_layout_id = vertex_map.get((target_type, target_node.label))
            else:
                target_layout_id = vertex_map.get((target_type, target_id_orig))

            if source_layout_id and target_layout_id:
                arc_exists = False
                existing_arc_id = None
                for arc_id_check, arc_data_check in layout_data["arcs"].items():
                    if (arc_data_check['source'] == source_layout_id and arc_data_check['target'] == target_layout_id) or \
                       (arc_data_check['source'] == target_layout_id and arc_data_check['target'] == source_layout_id):
                        arc_exists = True
                        existing_arc_id = arc_id_check
                        break

                if not arc_exists:
                    arc_id = generate_arc_id(source_layout_id, target_layout_id)
                    is_variable = False
                    if ot in ocpn.get("double_arcs_on_activity", {}):
                        act_label = None
                        if source_type == TRANSITION_TYPE and source_node.label:
                            act_label = source_node.label
                        elif target_type == TRANSITION_TYPE and target_node.label:
                            act_label = target_node.label
                        if act_label and ocpn["double_arcs_on_activity"][ot].get(act_label, False):
                            is_variable = True

                    arc_data = {
                        "id": arc_id,
                        "source": source_layout_id,   # keep ORIGINAL semantics
                        "target": target_layout_id,   # keep ORIGINAL semantics
                        "objectType": ot,
                        "original": True,
                        "reversed": False,            # will be set True if reversed for DAG
                        "dummy_nodes": [],
                        "minLayer": -1,
                        "maxLayer": -1,
                        "type1": False,
                        "weight": arc.weight if hasattr(arc, 'weight') else 1,
                        "variable": is_variable,
                    }
                    layout_data["arcs"][arc_id] = arc_data
                    layout_data["original_arcs"][arc_id] = copy.deepcopy(arc_data)
                else:
                    if source_type == TRANSITION_TYPE:
                        layout_data["vertices"][source_layout_id].setdefault("adjacentObjectTypes", set()).add(ot)
                    if target_type == TRANSITION_TYPE:
                        layout_data["vertices"][target_layout_id].setdefault("adjacentObjectTypes", set()).add(ot)
                    pass
            else:
                print(f"Warning: Could not find layout nodes for arc {source_id_orig} -> {target_id_orig} in OT {ot}")

    all_activity_nodes = {v['name'] for v in layout_data['vertices'].values() if
                          v['type'] == TRANSITION_TYPE and not v['silent']}
    for act in ocpn.get("activities", set()):
        if act not in all_activity_nodes:
            layout_id = f"trans_{act}_unconnected_{uuid.uuid4()}"
            layout_data["vertices"][layout_id] = {
                "id": layout_id, "original_id": None, "name": act, "label": act,
                "objectType": None, "adjacentObjectTypes": set(), "type": TRANSITION_TYPE,
                "silent": False, "layer": -1, "pos": -1, "x": None, "y": None,
                "root": layout_id, "align": layout_id, "sink": layout_id, "shift": float('inf'),
                "upper_neighbor": None, "lower_neighbor": None,
            }
            print(f"Warning: Activity '{act}' not found in Petri net transitions, adding as unconnected node.")

    return layout_data


# --- Graph representation for algorithms ---
class OCPNGraph:
    """Helper class to represent the graph for cycle breaking and layer assignment."""

    def __init__(self, layout_data: Dict):
        self.nodes = list(layout_data["vertices"].keys())
        self.arcs_data = layout_data["arcs"]
        self.adj = {n: [] for n in self.nodes}
        self.rev_adj = {n: [] for n in self.nodes}
        self.arc_list = []

        for arc_id, arc in self.arcs_data.items():
            src, tgt = arc['source'], arc['target']
            if src == tgt:
                continue
            if src in self.nodes and tgt in self.nodes:
                self.adj[src].append(tgt)
                self.rev_adj[tgt].append(src)
                self.arc_list.append({'source': src, 'target': tgt, 'id': arc_id})

    def get_out_degree(self, node: str) -> int:
        return len(self.adj.get(node, []))

    def get_in_degree(self, node: str) -> int:
        return len(self.rev_adj.get(node, []))

    def remove_node(self, node: str):
        if node not in self.nodes:
            return
        successors = self.adj.pop(node, [])
        predecessors = self.rev_adj.pop(node, [])
        for succ in successors:
            if succ in self.rev_adj:
                self.rev_adj[succ].remove(node)
        for pred in predecessors:
            if pred in self.adj:
                self.adj[pred].remove(node)
        self.nodes.remove(node)
        self.arc_list = [a for a in self.arc_list if a['source'] != node and a['target'] != node]

    def remove_nodes(self, nodes_to_remove: List[str]):
        for node in nodes_to_remove:
            self.remove_node(node)

    def get_sink(self) -> Optional[str]:
        for node in self.nodes:
            if self.get_out_degree(node) == 0:
                return node
        return None

    def get_source(self) -> Optional[str]:
        for node in self.nodes:
            if self.get_in_degree(node) == 0:
                return node
        return None

    def get_max_out_degree_node(self) -> Optional[str]:
        max_degree = -1
        max_node = None
        for node in self.nodes:
            degree = self.get_out_degree(node) - self.get_in_degree(node)
            if degree > max_degree:
                max_degree = degree
                max_node = node
        return max_node


def _modified_greedy_fas(graph: OCPNGraph, sources: List[str], sinks: List[str]) -> List[str]:
    """Computes Feedback Arc Set using greedy algorithm considering sources/sinks."""
    s1 = list(sources)
    s2 = list(sinks)

    s1 = [n for n in s1 if n in graph.nodes]
    s2 = [n for n in s2 if n in graph.nodes]

    if s1:
        s1.sort(key=lambda n: graph.get_out_degree(n) - graph.get_in_degree(n), reverse=True)
        graph.remove_nodes(s1)

    if s2:
        s2.sort(key=lambda n: graph.get_out_degree(n) - graph.get_in_degree(n), reverse=True)
        graph.remove_nodes(s2)

    temp_s1 = []
    temp_s2 = []
    while graph.nodes:
        sink = graph.get_sink()
        while sink:
            temp_s2.insert(0, sink)
            graph.remove_node(sink)
            sink = graph.get_sink()

        source = graph.get_source()
        while source:
            temp_s1.append(source)
            graph.remove_node(source)
            source = graph.get_source()

        if graph.nodes:
            node = graph.get_max_out_degree_node()
            if node:
                temp_s1.append(node)
                graph.remove_node(node)
            else:
                print("Warning: No node found in FAS loop, graph might be empty or disconnected unexpectedly.")
                break

    return s1 + temp_s1 + temp_s2 + s2


def _reverse_cycles(layout_data: Dict, config: Dict) -> int:
    """Marks arcs to reverse (for DAG) without swapping endpoints."""
    print("Step 1: Cycle Breaking...")
    graph = OCPNGraph(layout_data)

    potential_sources = [vid for vid, vdata in layout_data["vertices"].items() if vdata.get("is_source")]
    potential_sinks = [vid for vid, vdata in layout_data["vertices"].items() if vdata.get("is_sink")]
    sources = config.get("sources", potential_sources)
    sinks = config.get("sinks", potential_sinks)

    sources = [s for s in sources if s in layout_data["vertices"]]
    sinks = [s for s in sinks if s in layout_data["vertices"]]

    fas_order = _modified_greedy_fas(graph, sources, sinks)
    fas_indices = {node_id: index for index, node_id in enumerate(fas_order)}

    reversed_count = 0
    for arc_id, arc in layout_data["arcs"].items():
        src = arc['source']
        tgt = arc['target']
        src_index = fas_indices.get(src)
        tgt_index = fas_indices.get(tgt)

        if src_index is not None and tgt_index is not None:
            if src_index > tgt_index:
                # Do NOT swap. Just mark as reversed for layout purposes.
                arc['reversed'] = True
                reversed_count += 1
            else:
                arc['reversed'] = False
        else:
            arc['reversed'] = False

    print(f"  Marked {reversed_count} arcs as reversed for DAG.")
    return reversed_count


def _assign_layers(layout_data: Dict) -> bool:
    """Assigns layers to vertices using LP to minimize edge length."""
    print("Step 2: Layer Assignment (using LP)...")
    vertices = layout_data["vertices"]
    arcs = layout_data["arcs"]
    node_list = list(vertices.keys())
    node_to_index = {node_id: i for i, node_id in enumerate(node_list)}
    num_vars = len(node_list)

    # Objective: Minimize sum(weight * (layer[target] - layer[source]))
    c = np.zeros(num_vars)
    active_arcs = []

    for arc_id, arc in arcs.items():
        src_orig, tgt_orig = arc['source'], arc['target']
        weight = arc.get('weight', 1)

        # Use the 'reversed' flag to determine DAG direction
        if arc['reversed']:
            src, tgt = tgt_orig, src_orig
        else:
            src, tgt = src_orig, tgt_orig

        if src in node_to_index and tgt in node_to_index:
            src_idx = node_to_index[src]
            tgt_idx = node_to_index[tgt]
            c[tgt_idx] += weight
            c[src_idx] -= weight
            active_arcs.append({'source': src, 'target': tgt, 'id': arc_id})

    num_constraints = len(active_arcs)
    Aub = np.zeros((num_constraints, num_vars))
    bub = np.full(num_constraints, -1.0)

    for i, arc in enumerate(active_arcs):
        src_idx = node_to_index[arc['source']]
        tgt_idx = node_to_index[arc['target']]
        Aub[i, src_idx] = 1.0
        Aub[i, tgt_idx] = -1.0

    num_positivity_constraints = num_vars
    Aub_pos = np.zeros((num_positivity_constraints, num_vars))
    bub_pos = np.zeros(num_positivity_constraints)
    for i in range(num_vars):
        Aub_pos[i, i] = -1.0

    Aub_combined = np.vstack((Aub, Aub_pos))
    bub_combined = np.concatenate((bub, bub_pos))

    Aeq = None
    beq = None

    try:
        print(f"  Setting up LP with {num_vars} variables and {Aub_combined.shape[0]} constraints.")
        sol = lp_solver.apply(c, Aub_combined, bub_combined, Aeq, beq,
                              parameters={"verbose": False}, variant=lp_solver.SCIPY)

        if sol is None:
            print("  LP Solver did not return a solution.")
            return False

        points = lp_solver.get_points_from_sol(sol, variant=lp_solver.SCIPY)

        if points is None or len(points) != num_vars:
            print("  LP Solver failed to extract points or returned incorrect number.")
            return False

        max_layer = 0
        layering_dict = {}
        for i, node_id in enumerate(node_list):
            layer = int(round(points[i]))
            vertices[node_id]['layer'] = layer
            if layer not in layering_dict:
                layering_dict[layer] = []
            layering_dict[layer].append(node_id)
            max_layer = max(max_layer, layer)

        layout_data['layering'] = []
        for i in range(max_layer + 1):
            layout_data['layering'].append(layering_dict.get(i, []))

        print(f"  Assigned nodes to {max_layer + 1} layers.")
        return True

    except Exception as e:
        print(f"  Error during LP solving or layer assignment: {e}")
        import traceback
        traceback.print_exc()
        return False


def _insert_dummy_vertices(layout_data: Dict):
    """Inserts dummy nodes for arcs spanning multiple layers."""
    print("Step 3: Dummy Vertex Insertion...")
    vertices = layout_data["vertices"]
    arcs = layout_data["arcs"]
    layering = layout_data["layering"]
    arcs_to_add = {}
    arcs_to_remove = []
    dummy_count = 0

    original_arc_ids = list(arcs.keys())

    for arc_id in original_arc_ids:
        arc = arcs[arc_id]
        src_id_orig, tgt_id_orig = arc['source'], arc['target']

        # Use DAG direction when computing span
        if arc['reversed']:
            src_id, tgt_id = tgt_id_orig, src_id_orig
        else:
            src_id, tgt_id = src_id_orig, tgt_id_orig

        if src_id not in vertices or tgt_id not in vertices:
            print(f"Warning: Skipping dummy insertion for arc {arc_id} - source or target not found.")
            continue

        src_node = vertices[src_id]
        tgt_node = vertices[tgt_id]

        src_layer = src_node.get('layer', -1)
        tgt_layer = tgt_node.get('layer', -1)

        if src_layer == -1 or tgt_layer == -1:
            print(f"Warning: Skipping dummy insertion for arc {arc_id} ({src_id} -> {tgt_id}) - layers not assigned.")
            continue

        arc['minLayer'] = min(src_layer, tgt_layer)
        arc['maxLayer'] = max(src_layer, tgt_layer)

        slack = tgt_layer - src_layer

        if slack > 1:
            arcs_to_remove.append(arc_id)
            dummies_in_path = []

            ot = arc.get("objectType")
            if vertices[src_id_orig]["type"] == PLACE_TYPE:
                ot = vertices[src_id_orig]["objectType"]
            elif vertices[tgt_id_orig]["type"] == PLACE_TYPE:
                ot = vertices[tgt_id_orig]["objectType"]

            src_pos = -1
            tgt_pos = -1
            if src_layer < len(layering) and src_id in layering[src_layer]:
                src_pos = layering[src_layer].index(src_id)
            if tgt_layer < len(layering) and tgt_id in layering[tgt_layer]:
                tgt_pos = layering[tgt_layer].index(tgt_id)

            median_pos = len(layering[src_layer + 1]) // 2
            if src_pos != -1 and tgt_pos != -1:
                median_pos = (src_pos + tgt_pos) // 2

            last_node_id = src_id
            for i in range(1, slack):
                dummy_layer_idx = src_layer + i
                dummy_id = generate_dummy_id()
                dummy_count += 1
                dummies_in_path.append(dummy_id)

                dummy_node = {
                    "id": dummy_id,
                    "original_id": None,
                    "name": f"dummy_{dummy_count}",
                    "label": None,
                    "objectType": ot,
                    "type": DUMMY_TYPE,
                    "layer": dummy_layer_idx,
                    "pos": -1,
                    "x": None,
                    "y": None,
                    "dummy_of_arc": arc_id,
                    "root": dummy_id,
                    "align": dummy_id,
                    "sink": dummy_id,
                    "shift": float('inf'),
                    "upper_neighbor": last_node_id,
                    "lower_neighbor": None,
                }
                vertices[dummy_id] = dummy_node

                if i > 1:
                    vertices[dummies_in_path[-2]]["lower_neighbor"] = dummy_id

                if dummy_layer_idx < len(layering):
                    insert_pos = min(median_pos, len(layering[dummy_layer_idx]))
                    layering[dummy_layer_idx].insert(insert_pos, dummy_id)
                else:
                    print(f"Warning: Dummy layer index {dummy_layer_idx} out of bounds.")
                    layering.append([dummy_id])

                segment_arc_id = generate_arc_id(last_node_id, dummy_id)
                arcs_to_add[segment_arc_id] = {
                    "id": segment_arc_id,
                    "source": last_node_id,
                    "target": dummy_id,
                    "objectType": ot,
                    "original": False,
                    "reversed": False,
                    "dummy_nodes": [],
                    "minLayer": dummy_layer_idx - 1,
                    "maxLayer": dummy_layer_idx,
                    "type1": False,
                    "weight": arc.get('weight', 1),
                    "variable": arc.get('variable', False),
                    "original_arc_id": arc_id
                }

                last_node_id = dummy_id

            segment_arc_id = generate_arc_id(last_node_id, tgt_id)
            arcs_to_add[segment_arc_id] = {
                "id": segment_arc_id,
                "source": last_node_id,
                "target": tgt_id,
                "objectType": ot,
                "original": False,
                "reversed": False,
                "dummy_nodes": [],
                "minLayer": tgt_layer - 1,
                "maxLayer": tgt_layer,
                "type1": False,
                "weight": arc.get('weight', 1),
                "variable": arc.get('variable', False),
                "original_arc_id": arc_id
            }
            vertices[last_node_id]["lower_neighbor"] = tgt_id

            if arc_id in layout_data["original_arcs"]:
                layout_data["original_arcs"][arc_id]["dummy_nodes"] = dummies_in_path

            arcs[arc_id]['dummy_nodes'] = dummies_in_path

    for arc_id in arcs_to_remove:
        del arcs[arc_id]
    arcs.update(arcs_to_add)

    print(f"  Inserted {dummy_count} dummy vertices.")


# --- Vertex Ordering (Barycenter Heuristic) ---

def _compute_barycenter(ocpn_layout: Dict, vertex_id: str, layer_idx: int, fixed_layer_idx: int, config: Dict) -> float:
    """Computes the barycenter value for a vertex based on neighbors in the fixed layer."""
    vertices = ocpn_layout["vertices"]
    layering = ocpn_layout["layering"]

    if fixed_layer_idx < 0 or fixed_layer_idx >= len(layering):
        return 0.0

    fixed_layer = layering[fixed_layer_idx]
    fixed_layer_positions = {node_id: pos for pos, node_id in enumerate(fixed_layer)}

    neighbors_in_fixed = []
    arcs = ocpn_layout.get("arcs", {})
    for arc_id, arc in arcs.items():
        src_orig, tgt_orig = arc['source'], arc['target']
        other_node = None
        if src_orig == vertex_id and tgt_orig in fixed_layer_positions:
            other_node = tgt_orig
        elif tgt_orig == vertex_id and src_orig in fixed_layer_positions:
            other_node = src_orig

        if other_node:
            other_node_layer = vertices.get(other_node, {}).get('layer', -1)
            if other_node_layer == fixed_layer_idx:
                neighbors_in_fixed.append(other_node)

    if not neighbors_in_fixed:
        return -1.0

    barycenter_sum = 0.0
    for neighbor in neighbors_in_fixed:
        barycenter_sum += fixed_layer_positions[neighbor]

    avg_barycenter = barycenter_sum / len(neighbors_in_fixed)

    if vertices[vertex_id]["type"] == PLACE_TYPE and config.get("object_attraction", 0) > 0:
        object_attraction = config.get("object_attraction", 0.5)
        obj_attr_min = config.get("object_attraction_range_min", 1)
        obj_attr_max = config.get("object_attraction_range_max", 1)
        current_ot = vertices[vertex_id]["objectType"]

        object_neighbor_positions = []
        direction = 1 if fixed_layer_idx > layer_idx else -1

        for i in range(obj_attr_min, obj_attr_max + 1):
            target_layer_idx = layer_idx + direction * (2 * i)
            if 0 <= target_layer_idx < len(layering):
                target_layer = layering[target_layer_idx]
                for neighbor_pos, node_id in enumerate(target_layer):
                    if vertices[node_id]["type"] == PLACE_TYPE and vertices[node_id]["objectType"] == current_ot:
                        object_neighbor_positions.append(neighbor_pos)

        if object_neighbor_positions:
            avg_object_pos = sum(object_neighbor_positions) / len(object_neighbor_positions)
            return (1.0 - object_attraction) * avg_barycenter + object_attraction * avg_object_pos
        else:
            return avg_barycenter
    elif vertices[vertex_id]["type"] == DUMMY_TYPE:
        if neighbors_in_fixed:
            return fixed_layer_positions[neighbors_in_fixed[0]]
        else:
            return -1.0
    else:
        return avg_barycenter


def _barycenter_ordering_pass(ocpn_layout: Dict, current_layering: List[List[str]], fixed_layer_idx: int,
                              sweep_down: bool, config: Dict) -> List[List[str]]:
    new_layering = copy.deepcopy(current_layering)
    layer_to_order_idx = fixed_layer_idx + 1 if sweep_down else fixed_layer_idx - 1

    if layer_to_order_idx < 0 or layer_to_order_idx >= len(new_layering):
        return new_layering

    layer_to_order = new_layering[layer_to_order_idx]
    if not layer_to_order:
        return new_layering

    barycenters = {}
    for node_id in layer_to_order:
        bc_val = _compute_barycenter(ocpn_layout, node_id, layer_to_order_idx, fixed_layer_idx, config)
        barycenters[node_id] = bc_val

    adjusted_barycenters = {}
    last_valid_bc = 0.0
    for i, node_id in enumerate(layer_to_order):
        bc = barycenters[node_id]
        if bc == -1.0:
            adjusted_bc = last_valid_bc + i * 1e-6
            adjusted_barycenters[node_id] = adjusted_bc
        else:
            adjusted_barycenters[node_id] = bc
            last_valid_bc = bc

    original_indices = {node_id: i for i, node_id in enumerate(layer_to_order)}

    def sort_key(node_id):
        return (adjusted_barycenters[node_id], original_indices[node_id])

    ordered_layer = sorted(layer_to_order, key=sort_key)

    new_layering[layer_to_order_idx] = ordered_layer
    return new_layering


def _count_crossings(ocpn_layout: Dict, layering: List[List[str]]) -> int:
    crossings = 0
    vertices = ocpn_layout["vertices"]
    arcs_data = ocpn_layout.get("arcs", {})

    node_positions = {}
    for r, layer in enumerate(layering):
        for c, node_id in enumerate(layer):
            node_positions[node_id] = (r, c)

    for r in range(len(layering) - 1):
        layer1 = layering[r]
        layer2 = layering[r + 1]

        arcs_between = []
        for arc_id, arc in arcs_data.items():
            src, tgt = arc['source'], arc['target']
            pos_src = node_positions.get(src)
            pos_tgt = node_positions.get(tgt)
            if pos_src and pos_tgt:
                if (pos_src[0] == r and pos_tgt[0] == r + 1):
                    arcs_between.append(
                        {'source': src, 'target': tgt, 'source_pos': pos_src[1], 'target_pos': pos_tgt[1]})
                elif (pos_src[0] == r + 1 and pos_tgt[0] == r):
                    arcs_between.append(
                        {'source': tgt, 'target': src, 'source_pos': pos_tgt[1], 'target_pos': pos_src[1]})

        for i in range(len(arcs_between)):
            for j in range(i + 1, len(arcs_between)):
                arc1 = arcs_between[i]
                arc2 = arcs_between[j]
                if (arc1['source_pos'] < arc2['source_pos'] and arc1['target_pos'] > arc2['target_pos']) or \
                   (arc1['source_pos'] > arc2['source_pos'] and arc1['target_pos'] < arc2['target_pos']):
                    crossings += 1

    return crossings


def _order_vertices(layout_data: Dict, config: Dict):
    """Orders vertices within layers using the barycenter method."""
    print("Step 4: Vertex Ordering (Barycenter Heuristic)...")

    max_iterations = config.get("max_barycenter_iterations", 24)

    current_layering = layout_data["layering"]
    best_layering = copy.deepcopy(current_layering)
    best_score = _count_crossings(layout_data, best_layering)
    print(f"  Initial crossing score: {best_score}")

    no_improvement_counter = 0
    iter_count = 0

    while no_improvement_counter < max_iterations:
        iter_count += 1
        layering_before_sweep = copy.deepcopy(current_layering)

        for i in range(len(current_layering) - 1):
            current_layering = _barycenter_ordering_pass(layout_data, current_layering, fixed_layer_idx=i,
                                                         sweep_down=True, config=config)

        for i in range(len(current_layering) - 1, 0, -1):
            current_layering = _barycenter_ordering_pass(layout_data, current_layering, fixed_layer_idx=i,
                                                         sweep_down=False, config=config)

        current_score = _count_crossings(layout_data, current_layering)

        if current_score < best_score:
            best_score = current_score
            best_layering = copy.deepcopy(current_layering)
            no_improvement_counter = 0
            print(f"  Iteration {iter_count}: Improved score to {best_score}")
        else:
            no_improvement_counter += 1
            print(f"  Iteration {iter_count}: Score {current_score} (no improvement)")

        if current_layering == layering_before_sweep:
            print(f"  Converged after {iter_count} iterations.")
            break

    print(f"  Final crossing score: {best_score} after {iter_count} iterations.")
    layout_data["layering"] = best_layering

    for r, layer in enumerate(best_layering):
        for c, node_id in enumerate(layer):
            layout_data["vertices"][node_id]['pos'] = c


# --- Vertex Positioning (Brandes & Köpf Heuristic Variant) ---

def _mark_type1_conflicts(layout_data: Dict):
    vertices = layout_data["vertices"]
    arcs = layout_data["arcs"]
    layering = layout_data["layering"]

    for arc_id, arc in arcs.items():
        arc['type1'] = False

    for i in range(len(layering) - 2):
        layer_i = layering[i]
        layer_i1 = layering[i + 1]

        pos_i1 = {node_id: pos for pos, node_id in enumerate(layer_i1)}
        pos_i = {node_id: pos for pos, node_id in enumerate(layer_i)}

        inner_segments = []
        for k, v_i1 in enumerate(layer_i1):
            node_i1_data = vertices[v_i1]
            if node_i1_data['type'] == DUMMY_TYPE:
                upper_neighbor_id = node_i1_data.get("upper_neighbor")
                if upper_neighbor_id and upper_neighbor_id in pos_i:
                    upper_neighbor_data = vertices[upper_neighbor_id]
                    if upper_neighbor_data['type'] == DUMMY_TYPE:
                        inner_segments.append((pos_i[upper_neighbor_id], k))

        if not inner_segments:
            continue

        inner_segments.sort()

        k0 = 0
        current_inner_idx = 0
        for l1, v_i1 in enumerate(layer_i1):
            k1 = len(layer_i) - 1
            is_lower_end_of_inner = False

            while current_inner_idx < len(inner_segments) and inner_segments[current_inner_idx][1] <= l1:
                k1 = inner_segments[current_inner_idx][0]
                is_lower_end_of_inner = (inner_segments[current_inner_idx][1] == l1)
                current_inner_idx += 1

            if not is_lower_end_of_inner:
                upper_neighbors_i = []
                for arc_id, arc_data in arcs.items():
                    if arc_data['target'] == v_i1 and arc_data['source'] in pos_i:
                        upper_neighbors_i.append(arc_data['source'])

                for u_i in upper_neighbors_i:
                    k = pos_i[u_i]
                    if k < k0 or k > k1:
                        connecting_arcs = get_arcs_between(u_i, v_i1, layout_data)
                        for conn_arc in connecting_arcs:
                            is_seg_inner = (vertices[u_i]['type'] == DUMMY_TYPE and vertices[v_i1]['type'] == DUMMY_TYPE)
                            if not is_seg_inner:
                                conn_arc['type1'] = True

            if is_lower_end_of_inner:
                k0 = k1


def _vertical_alignment(ocpn_layout: Dict, current_layering: List[List[str]], pos: Dict[str, int], sweep_down: bool) -> \
Tuple[Dict[str, str], Dict[str, str]]:
    root = {vid: vid for vid in ocpn_layout["vertices"]}
    align = {vid: vid for vid in ocpn_layout["vertices"]}
    vertices = ocpn_layout["vertices"]

    layer_indices = range(len(current_layering)) if sweep_down else range(len(current_layering) - 1, -1, -1)

    for i in layer_indices:
        fixed_layer_idx = i - 1 if sweep_down else i + 1
        if fixed_layer_idx < 0 or fixed_layer_idx >= len(current_layering):
            continue

        layer_k = current_layering[i]
        r = -1

        for vk in layer_k:
            neighbors = []
            neighbor_dir = 'up' if sweep_down else 'down'

            arcs_data = ocpn_layout.get("arcs", {})
            fixed_layer_nodes = set(current_layering[fixed_layer_idx])

            for arc_id, arc in arcs_data.items():
                src, tgt = arc['source'], arc['target']
                if sweep_down:
                    if tgt == vk and src in fixed_layer_nodes:
                        neighbors.append(src)
                else:
                    if src == vk and tgt in fixed_layer_nodes:
                        neighbors.append(tgt)

            if not neighbors:
                continue

            neighbors.sort(key=lambda n: pos[n])

            m_low = math.floor((len(neighbors) - 1) / 2)
            m_high = math.ceil((len(neighbors) - 1) / 2)
            median_indices = list(set([m_low, m_high]))

            for m_idx in median_indices:
                median_neighbor = neighbors[m_idx]

                if align[vk] == vk:
                    connecting_arcs = get_arcs_between(median_neighbor, vk, ocpn_layout)
                    is_marked = any(a.get('type1', False) for a in connecting_arcs)

                    if not is_marked and pos[median_neighbor] > r:
                        align[median_neighbor] = vk
                        root[vk] = root[median_neighbor]
                        align[vk] = root[vk]
                        r = pos[median_neighbor]

    return root, align


def _place_block(ocpn_layout: Dict, layering: List[List[str]], v: str, x: Dict[str, Optional[float]],
                 pos: Dict[str, int], roots: Dict[str, str], sink: Dict[str, str], shift: Dict[str, float],
                 aligns: Dict[str, str], config: Dict):
    if x.get(v) is None:
        x[v] = 0.0
        w = v
        while True:
            w_pos = pos[w]
            w_layer_idx = -1
            for idx, l in enumerate(layering):
                if w in l:
                    w_layer_idx = idx
                    break

            if w_pos > 0 and w_layer_idx != -1:
                predecessor = layering[w_layer_idx][w_pos - 1]
                u = roots[predecessor]
                _place_block(ocpn_layout, layering, u, x, pos, roots, sink, shift, aligns, config)

                if sink[v] == v:
                    sink[v] = sink[u]

                place_dim = config.get("place_radius", 10) * 2
                trans_h = config.get("transition_height", 20)
                trans_w = config.get("transition_width", 50)
                silent_w = config.get("silent_transition_width", 10)
                max_trans_dim = max(trans_h, trans_w, silent_w)

                delta = config.get("vertex_sep", 20) + max(place_dim, max_trans_dim)

                if sink[v] != sink[u]:
                    current_shift = (x.get(v, 0.0) or 0.0) - (x.get(u, 0.0) or 0.0) - delta
                    shift[sink[u]] = min(shift.get(sink[u], float('inf')), current_shift)
                else:
                    new_x = (x.get(u, 0.0) or 0.0) + delta
                    x[v] = max(x.get(v, 0.0) or 0.0, new_x)

            w = aligns[w]
            if w == v:
                break


def _horizontal_compaction(ocpn_layout: Dict, layering: List[List[str]], roots: Dict[str, str], aligns: Dict[str, str],
                           pos: Dict[str, int], config: Dict) -> Tuple[Dict[str, Optional[float]], float]:
    x: Dict[str, Optional[float]] = {vid: None for vid in ocpn_layout["vertices"]}
    sink: Dict[str, str] = {vid: vid for vid in ocpn_layout["vertices"]}
    shift: Dict[str, float] = {vid: float('inf') for vid in ocpn_layout["vertices"]}

    for layer in layering:
        for v in layer:
            if roots[v] == v:
                _place_block(ocpn_layout, layering, v, x, pos, roots, sink, shift, aligns, config)

    abs_x: Dict[str, Optional[float]] = {}
    max_coord = 0.0
    min_coord = float('inf')

    for layer in layering:
        for v in layer:
            root_v = roots[v]
            if x.get(root_v) is not None:
                current_x = x[root_v]
                sink_root_v = sink[root_v]
                if shift.get(sink_root_v, float('inf')) != float('inf'):
                    current_x += shift[sink_root_v]
                abs_x[v] = current_x
                max_coord = max(max_coord, current_x)
                min_coord = min(min_coord, current_x)
            else:
                abs_x[v] = None

    border = config.get("border_padding", 20)
    shift_val = border - min_coord if min_coord != float('inf') else border
    final_x = {}
    final_max = 0.0
    for v, val in abs_x.items():
        if val is not None:
            final_x[v] = val + shift_val
            final_max = max(final_max, final_x[v])
        else:
            final_x[v] = None

    final_max += border

    return final_x, final_max


def _position_vertices(layout_data: Dict, config: Dict):
    """Assigns final X/Y coordinates using 4-pass heuristic."""
    print("Step 5: Vertex Positioning...")

    _mark_type1_conflicts(layout_data)

    layouts = []

    original_layering = copy.deepcopy(layout_data["layering"])

    for vert_dir in [0, 1]:  # 0: Down, 1: Up
        for horz_dir in [0, 1]:  # 0: Left->Right, 1: Right->Left

            print(f"  Running positioning pass (vert_dir={vert_dir}, horz_dir={horz_dir})...")
            current_layering = copy.deepcopy(original_layering)

            if vert_dir == 1:
                current_layering.reverse()
            if horz_dir == 1:
                for layer in current_layering:
                    layer.reverse()

            pos = {}
            for r, layer in enumerate(current_layering):
                for c, node_id in enumerate(layer):
                    pos[node_id] = c

            roots, aligns = _vertical_alignment(layout_data, current_layering, pos, sweep_down=(vert_dir == 0))

            coords, max_coord = _horizontal_compaction(layout_data, current_layering, roots, aligns, pos, config)

            if horz_dir == 1:
                final_coords = {}
                for v, c in coords.items():
                    if c is not None:
                        final_coords[v] = max_coord - c
                    else:
                        final_coords[v] = None
                layouts.append(final_coords)
            else:
                layouts.append(coords)

    final_coords_x = {}
    final_coords_y = {}
    vertices = layout_data["vertices"]
    layering = layout_data["layering"]
    border_padding = config.get("border_padding", 20)
    layer_sep = config.get("layer_sep", 50)
    direction = config.get("rankdir", "TB")

    for v_id in vertices.keys():
        candidate_coords = []
        for layout_pass in layouts:
            coord = layout_pass.get(v_id)
            if coord is not None:
                candidate_coords.append(coord)

        if candidate_coords:
            candidate_coords.sort()
            if len(candidate_coords) > 0:
                median_idx1 = math.floor((len(candidate_coords) - 1) / 2)
                median_idx2 = math.ceil((len(candidate_coords) - 1) / 2)
                median_coord = (candidate_coords[median_idx1] + candidate_coords[median_idx2]) / 2
                final_coords_x[v_id] = median_coord
        else:
            final_coords_x[v_id] = border_padding

    layer_sizes = []
    max_layer_dim = 0
    for i, layer in enumerate(layering):
        max_dim = 0
        for node_id in layer:
            node = vertices[node_id]
            node_h, node_w = 0, 0
            if node['type'] == PLACE_TYPE:
                r = config.get("place_radius", 10)
                node_h, node_w = r * 2, r * 2
            elif node['type'] == TRANSITION_TYPE:
                node_h = config.get("transition_height", 20)
                if node['silent']:
                    node_w = config.get("silent_transition_width", 10)
                else:
                    node_w = config.get("transition_width", 50)
            elif node['type'] == DUMMY_TYPE:
                node_h, node_w = 1, 1

            if direction == "TB":
                max_dim = max(max_dim, node_h)
            else:
                max_dim = max(max_dim, node_w)
        layer_sizes.append({'layer': i, 'size': max_dim})
        max_layer_dim = max(max_layer_dim, max_dim)

    current_pos = border_padding
    layer_centers = {}
    for i in range(len(layering)):
        layer_size = layer_sizes[i]['size'] if i < len(layer_sizes) else max_layer_dim
        layer_center = current_pos + layer_size / 2.0
        layer_centers[i] = layer_center
        current_pos += layer_size + layer_sep

    max_x, max_y = 0.0, 0.0
    for v_id, node in vertices.items():
        x_coord = final_coords_x.get(v_id, border_padding)
        y_coord = layer_centers.get(node['layer'], border_padding)

        if direction == "TB":
            node['x'] = x_coord
            node['y'] = y_coord
            max_x = max(max_x, x_coord)
            max_y = max(max_y, y_coord)
        else:
            node['x'] = y_coord
            node['y'] = x_coord
            max_x = max(max_x, y_coord)
            max_y = max(max_y, x_coord)

    layout_data["max_x"] = max_x + border_padding
    layout_data["max_y"] = max_y + border_padding

    print(f"  Assigned final coordinates (max_x={max_x:.2f}, max_y={max_y:.2f}).")


# --- Graphviz Drawing ---

def _create_graphviz_digraph(ocpn: Dict, layout_data: Dict, parameters: Dict) -> Digraph:
    """Generates the Graphviz Digraph object from the layout with separate handling for direct edges and waypoint edges."""
    print("Step 6: Generating Graphviz output...")

    image_format = exec_utils.get_param_value(Parameters.FORMAT, parameters, "png")
    bgcolor = exec_utils.get_param_value(Parameters.BGCOLOR, parameters, constants.DEFAULT_BGCOLOR)
    rankdir = exec_utils.get_param_value(Parameters.RANKDIR, parameters, "TB")
    engine = exec_utils.get_param_value(Parameters.ENGINE, parameters, "dot")
    enable_graph_title = exec_utils.get_param_value(Parameters.ENABLE_GRAPH_TITLE, parameters, constants.DEFAULT_ENABLE_GRAPH_TITLES)
    graph_title = exec_utils.get_param_value(Parameters.GRAPH_TITLE, parameters, "Object-Centric Petri Net (Sugiyama Layout)")

    viz = Digraph(
        "ocpn_sugiyama",
        engine=engine,
        graph_attr={
            "bgcolor": bgcolor,
            "rankdir": rankdir,
            "splines": "line",
        },
        node_attr={'shape': 'box'},
    )

    if enable_graph_title:
        viz.attr(label=f'<<FONT POINT-SIZE="20">{graph_title}</FONT>>', labelloc="t")

    vertices = layout_data["vertices"]

    def get_contrasting_color(hex_color: str) -> str:
        hex_color = hex_color.lstrip('#')
        rgb = tuple(int(hex_color[i:i+2], 16) for i in (0, 2, 4))
        luminance = 0.299 * rgb[0] + 0.587 * rgb[1] + 0.114 * rgb[2]
        return "white" if luminance < 128 else "black"

    # Add Nodes
    for node_id, node in vertices.items():
        if node.get('x') is None or node.get('y') is None:
            print(f"Warning: Node {node_id} has no coordinates, skipping.")
            continue

        pos_x = node['x']
        pos_y = node['y']
        pos_str = f"{pos_x:.2f},{pos_y:.2f}!"

        attrs = {
            "pos": pos_str,
            "label": node['label'] if node['label'] else "",
            "id": node_id
        }

        if node['type'] == PLACE_TYPE:
            ot_color = ot_to_color(node['objectType'])
            font_color = get_contrasting_color(ot_color)
            radius_gv = parameters.get("place_radius", 10) / 72.0 * 1.5
            if node.get("is_source"):
                ot_label = f"{node['objectType']}"
                attrs.update({
                    "shape": "ellipse",
                    "fillcolor": ot_color,
                    "style": "filled",
                    "fontcolor": font_color,
                    "width": f"{radius_gv * 2.5:.2f}",
                    "height": f"{radius_gv * 1.5:.2f}",
                    "fixedsize": "true",
                    "label": f"<<FONT POINT-SIZE='8'>{ot_label}</FONT>>",
                    "labelloc": "c",
                })
                if node.get("is_source") and node.get("is_sink"):
                    attrs["peripheries"] = "2"
            else:
                attrs.update({
                    "shape": "circle",
                    "fillcolor": ot_color,
                    "style": "filled",
                    "fontcolor": font_color,
                    "width": f"{radius_gv * 2:.2f}",
                    "height": f"{radius_gv * 2:.2f}",
                    "fixedsize": "true",
                    "label": "",
                })
                if node.get("is_sink"):
                    attrs["peripheries"] = "2"

        elif node['type'] == TRANSITION_TYPE:
            height_gv = parameters.get("transition_height", 20) / 72.0
            if node.get('silent', False):
                width_gv = parameters.get("silent_transition_width", 10) / 72.0
                attrs.update({
                    "shape": "box",
                    "fillcolor": parameters.get("transition_fill_color", "black"),
                    "style": "filled",
                    "width": f"{width_gv:.2f}",
                    "height": f"{height_gv:.2f}",
                    "fixedsize": "true",
                    "label": "",
                })
            else:
                base_width_gv = parameters.get("transition_width", 40) / 72.0
                width_gv = base_width_gv * 1.5
                font_size = parameters.get("font_size", 10) - 2
                label = node['label']
                max_label_width = 10
                if label and len(label) > max_label_width:
                    wrapped_label = "<BR/>".join([label[i:i+max_label_width] for i in range(0, len(label), max_label_width)])
                    attrs["label"] = f"<{wrapped_label}>"
                else:
                    attrs["label"] = label or ""
                attrs.update({
                    "shape": "box",
                    "fillcolor": parameters.get("transition_fill_color", "white"),
                    "style": "filled",
                    "color": parameters.get("transition_color", "black"),
                    "width": f"{width_gv:.2f}",
                    "height": f"{height_gv:.2f}",
                    "fixedsize": "true",
                    "fontname": parameters.get("font_name", "Arial"),
                    "fontsize": str(font_size),
                    "fontcolor": parameters.get("transition_textcolor", "black"),
                })

        elif node['type'] == DUMMY_TYPE:
            attrs.update({
                "shape": "point",
                "width": "0.01",
                "height": "0.01",
                "label": "",
                "fixedsize": "true",
                "style": "invis",
            })

        viz.node(node_id, **attrs)

    # Add Edges
    arcs = layout_data["arcs"]
    original_arcs = layout_data["original_arcs"]

    direct_edges = []
    waypoint_edges = {}

    for arc_id, arc in arcs.items():
        src = arc['source']
        tgt = arc['target']

        if src not in vertices or tgt not in vertices or \
                vertices[src].get('x') is None or vertices[tgt].get('x') is None:
            continue

        src_type = vertices[src]['type']
        tgt_type = vertices[tgt]['type']

        original_arc_id = arc.get('original_arc_id', arc_id)
        original_arc = original_arcs.get(original_arc_id, arc)

        if src_type != DUMMY_TYPE and tgt_type != DUMMY_TYPE and not original_arc.get('dummy_nodes'):
            direct_edges.append(arc)
        else:
            if original_arc_id not in waypoint_edges:
                ultimate_src = original_arc.get('source')
                ultimate_tgt = original_arc.get('target')
                dummy_nodes = original_arc.get('dummy_nodes', [])
                waypoint_edges[original_arc_id] = {
                    'original_arc': original_arc,
                    'segments': [],
                    'ultimate_source': ultimate_src,
                    'ultimate_target': ultimate_tgt,
                    'dummy_nodes': dummy_nodes
                }
            waypoint_edges[original_arc_id]['segments'].append(arc)

    # Draw direct edges: ALWAYS in original semantic direction (forward)
    for arc in direct_edges:
        src = arc['source']
        tgt = arc['target']
        ot = arc.get("objectType", layout_data["object_types"][0] if layout_data["object_types"] else "unknown")
        ot_color = ot_to_color(ot)

        edge_attrs = {
            "color": ot_color,
            "penwidth": str(parameters.get("arc_size", 1.0) * arc.get("weight", 1.0)),
            "arrowsize": str(parameters.get("arrowhead_size", 0.7)),
            "id": arc['id'],
            "splines": "line",
            "dir": "forward",   # <<--- force forward to avoid inverted arrows
        }

        if arc.get('variable'):
            edge_attrs["penwidth"] = str(float(edge_attrs["penwidth"]) * 1.5)
            edge_attrs["style"] = "dashed"

        viz.edge(src, tgt, **edge_attrs)

    # Draw segmented (dummy) edges with arrow at the original target side
    for orig_arc_id, wp_data in waypoint_edges.items():
        orig_arc = wp_data['original_arc']
        segments = wp_data['segments']
        ultimate_src = wp_data['ultimate_source']
        ultimate_tgt = wp_data['ultimate_target']

        ot = orig_arc.get("objectType", layout_data["object_types"][0] if layout_data["object_types"] else "unknown")
        ot_color = ot_to_color(ot)

        base_edge_attrs = {
            "color": ot_color,
            "penwidth": str(parameters.get("arc_size", 1.0) * orig_arc.get("weight", 1.0)),
            "arrowsize": str(parameters.get("arrowhead_size", 0.7)),
            "splines": "polyline",
        }

        if orig_arc.get('variable'):
            base_edge_attrs["penwidth"] = str(float(base_edge_attrs["penwidth"]) * 1.5)
            base_edge_attrs["style"] = "dashed"

        for segment in segments:
            src = segment['source']
            tgt = segment['target']
            segment_attrs = base_edge_attrs.copy()
            segment_attrs['id'] = segment['id']

            # Put the arrow at the original target side:
            if tgt == ultimate_tgt:
                segment_attrs['arrowhead'] = 'normal'
                segment_attrs['dir'] = 'forward'
            elif src == ultimate_tgt:
                segment_attrs['arrowhead'] = 'normal'
                segment_attrs['dir'] = 'back'
            else:
                segment_attrs['arrowhead'] = 'none'

            viz.edge(src, tgt, **segment_attrs)

    viz.format = image_format.replace("html", "plain-ext")
    return viz


# --- Main Apply Function ---

class Parameters(Enum):
    """ Parameters for the Sugiyama OCPN visualization """
    FORMAT = "format"
    BGCOLOR = "bgcolor"
    RANKDIR = "rankdir"  # TB or LR
    ENGINE = "engine"  # dot, neato, fdp
    ENABLE_GRAPH_TITLE = "enable_graph_title"
    GRAPH_TITLE = "graph_title"

    # Sugiyama specific parameters (matching TS config where possible)
    SOURCES = "sources"  # List of node IDs to force as sources
    SINKS = "sinks"  # List of node IDs to force as sinks
    VERTEX_SEP = "vertex_sep"  # Minimum horizontal distance between nodes
    LAYER_SEP = "layer_sep"  # Minimum vertical distance between layers
    EDGE_SEP = "edge_sep"  # (Not directly used here, vertex_sep dominates)
    BORDER_PADDING = "border_padding"  # Padding around the graph
    OBJECT_CENTRALITY = "object_centrality"  # Dict {ot: centrality_value}
    OBJECT_ATTRACTION = "object_attraction"  # Weight (0-1) for place attraction in barycenter
    OBJECT_ATTRACTION_RANGE_MIN = "object_attraction_range_min"
    OBJECT_ATTRACTION_RANGE_MAX = "object_attraction_range_max"
    MAX_BARYCENTER_ITERATIONS = "max_barycenter_iterations"

    # Visual element sizes (in points, roughly 1/72 inch)
    PLACE_RADIUS = "place_radius"
    TRANSITION_WIDTH = "transition_width"
    TRANSITION_HEIGHT = "transition_height"
    SILENT_TRANSITION_WIDTH = "silent_transition_width"
    ARC_SIZE = "arc_size"
    ARROWHEAD_SIZE = "arrowhead_size"
    INDICATE_ARC_WEIGHT = "indicate_arc_weight"
    INDICATE_VARIABLE_ARCS = "indicate_variable_arcs"
    VARIABLE_ARC_INDICATOR_COLOR = "variable_arc_indicator_color"
    VARIABLE_ARC_INDICATOR_SIZE = "variable_arc_indicator_size"

    # Colors
    TRANSITION_COLOR = "transition_color"
    TRANSITION_FILL_COLOR = "transition_fill_color"
    TRANSITION_TEXT_COLOR = "transition_text_color"
    DEFAULT_PLACE_COLOR = "default_place_color"
    ARC_DEFAULT_COLOR = "arc_default_color"
    TYPE_COLOR_MAPPING = "type_color_mapping"

    # Font
    FONT_NAME = "font_name"
    FONT_SIZE = "font_size"


def apply(ocpn: Dict[str, Any], parameters: Optional[Dict[Any, Any]] = None) -> Digraph:
    """
    Obtains a visualization of the provided object-centric Petri net using
    a Sugiyama-based layout algorithm.

    Args:
        ocpn (Dict[str, Any]): Object-centric Petri net structure.
        parameters (Optional[Dict[Any, Any]], optional): Algorithm parameters.

    Returns:
        Digraph: A Graphviz digraph object representing the layout.
    """
    if parameters is None:
        parameters = {}

    ocpn = deepcopy(ocpn)

    for ot, (net, im, fm) in ocpn.get("petri_nets", {}).items():
        for place in net.places:
            place.name = ot + "@@" + place.name

    # --- Set default configuration parameters ---
    config = {
        # Basic viz
        Parameters.FORMAT.value: constants.DEFAULT_FORMAT_GVIZ_VIEW,
        Parameters.BGCOLOR.value: constants.DEFAULT_BGCOLOR,
        Parameters.RANKDIR.value: constants.DEFAULT_RANKDIR_GVIZ,
        Parameters.ENGINE.value: "dot",
        Parameters.ENABLE_GRAPH_TITLE.value: constants.DEFAULT_ENABLE_GRAPH_TITLES,
        Parameters.GRAPH_TITLE.value: "Object-Centric Petri Net (Sugiyama Layout)",
        # Sugiyama layout params
        Parameters.SOURCES.value: [],
        Parameters.SINKS.value: [],
        Parameters.VERTEX_SEP.value: 30,
        Parameters.LAYER_SEP.value: 50,
        Parameters.BORDER_PADDING.value: 20,
        Parameters.OBJECT_CENTRALITY.value: {},
        Parameters.OBJECT_ATTRACTION.value: 0.2,
        Parameters.OBJECT_ATTRACTION_RANGE_MIN.value: 1,
        Parameters.OBJECT_ATTRACTION_RANGE_MAX.value: 2,
        Parameters.MAX_BARYCENTER_ITERATIONS.value: 24,
        # Visual sizes (points)
        Parameters.PLACE_RADIUS.value: 10.0,
        Parameters.TRANSITION_WIDTH.value: 40.0,
        Parameters.TRANSITION_HEIGHT.value: 20.0,
        Parameters.SILENT_TRANSITION_WIDTH.value: 8.0,
        Parameters.ARC_SIZE.value: 1.0,
        Parameters.ARROWHEAD_SIZE.value: 0.7,
        Parameters.INDICATE_ARC_WEIGHT.value: False,
        Parameters.INDICATE_VARIABLE_ARCS.value: True,
        Parameters.VARIABLE_ARC_INDICATOR_COLOR.value: "#FF0000",
        Parameters.VARIABLE_ARC_INDICATOR_SIZE.value: 1.5,
        # Colors
        Parameters.TRANSITION_COLOR.value: "black",
        Parameters.TRANSITION_FILL_COLOR.value: "white",
        Parameters.TRANSITION_TEXT_COLOR.value: "black",
        Parameters.DEFAULT_PLACE_COLOR.value: "#D3D3D3",
        Parameters.ARC_DEFAULT_COLOR.value: "black",
        Parameters.TYPE_COLOR_MAPPING.value: {},
        # Font
        Parameters.FONT_NAME.value: "Arial",
        Parameters.FONT_SIZE.value: 10,
    }
    config.update({param.value: val for param, val in parameters.items() if isinstance(param, Parameters)})
    config.update({k: v for k, v in parameters.items() if isinstance(k, str) and k in [p.value for p in Parameters]})

    layout_data = _build_initial_layout(ocpn, config)
    if not layout_data["vertices"]:
        print("Error: No vertices found in OCPN structure.")
        return Digraph()

    _reverse_cycles(layout_data, config)

    if not _assign_layers(layout_data):
        print("Error: Layer assignment failed. Cannot proceed.")
        return Digraph()

    _insert_dummy_vertices(layout_data)

    _order_vertices(layout_data, config)

    _position_vertices(layout_data, config)

    gviz_graph = _create_graphviz_digraph(ocpn, layout_data, config)

    return gviz_graph
