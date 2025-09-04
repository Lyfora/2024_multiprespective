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
import math
from collections import Counter
from typing import Tuple, List, Dict

import numpy as np
from pm4py.objects.process_tree import obj as pt

###############################################################################
# 1 ⸺ Feature extraction
###############################################################################

# Order is fixed so vector positions stay comparable
FEATURES: List[str] = [
    # global size / complexity
    "n_nodes",  # all nodes (operators + activities)
    "n_edges",  # = n_nodes - 1  in a tree
    "depth",  # max root-to-leaf distance
    "avg_branching",  # mean #children of operator nodes
    # connector counts by type (splits in trees)
    "parallel_splits",
    "xor_splits",
    "or_splits",
    "loop_splits",
    # joins and structuredness are implied by the tree
    "joins",  # = #splits   (1-to-1 in block-structured models)
    "structuredness"  # always 1 for a process tree
]


def _traverse(node: pt.ProcessTree,
              depth: int,
              c: Counter,
              depths: List[int]) -> None:
    """DFS that collects counts and path depths."""
    c["n_nodes"] += 1
    depths.append(depth)
    if node.operator is not None:  # operator (AND / XOR / …)
        name = node.operator.name.lower()
        c[f"{name}_splits"] += 1
        c["n_splits"] += 1
        c["sum_branching"] += len(node.children)
        for child in node.children:
            _traverse(child, depth + 1, c, depths)


def features_from_process_tree(tree: pt.ProcessTree) -> np.ndarray:
    """
    Parameters
    ----------
    tree : pm4py.objects.process_tree.obj.ProcessTree

    Returns
    -------
    numpy.ndarray   (len = 10)
        Components in the order defined by FEATURES.
    """
    c = Counter()
    depths: List[int] = []
    _traverse(tree, 1, c, depths)

    n_nodes = c["n_nodes"]
    n_edges = n_nodes - 1  # property of trees
    depth = max(depths)
    n_ops = c["n_splits"]
    avg_br = (c["sum_branching"] / n_ops) if n_ops else 0.0

    vec = np.array([
        n_nodes,
        n_edges,
        depth,
        avg_br,
        c["parallel_splits"],
        c["xor_splits"],
        c["or_splits"],
        c["loop_splits"],
        n_ops,  # joins  (one join per split in a tree)
        1.0  # structuredness
    ], dtype=float)

    return vec


###############################################################################
# 2 ⸺ Distance / similarity measure (Dijkman structural metric)
###############################################################################

def _normalise(v1: np.ndarray, v2: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    """
    Scales each dimension to [0,1] across the two vectors
    (exactly what Dijkman et al. do to make features commensurable).
    """
    max_vals = np.maximum(v1, v2)
    max_vals[max_vals == 0] = 1.0  # avoid division-by-zero
    return v1 / max_vals, v2 / max_vals


def structural_distance(tree1: pt.ProcessTree,
                        tree2: pt.ProcessTree,
                        weights: Dict[str, float] = None) -> float:
    """
    Euclidean distance of the normalised feature vectors.
    `weights` lets you emphasise certain dimensions (defaults = 1).
    """
    v1 = features_from_process_tree(tree1)
    v2 = features_from_process_tree(tree2)

    v1n, v2n = _normalise(v1, v2)

    if weights:
        w = np.array([weights.get(f, 1.0) for f in FEATURES])
        v1n *= w
        v2n *= w

    return float(np.linalg.norm(v1n - v2n))


###############################################################################
# 3 ⸺ Convenience similarity (1 – normalised distance)
###############################################################################

def structural_similarity(tree1: pt.ProcessTree,
                          tree2: pt.ProcessTree,
                          weights: Dict[str, float] = None) -> float:
    """
    Computes the structural similarity between two process trees, following an approach similar to:
    Dijkman, Remco, et al. "Similarity of business process models: Metrics and evaluation." Information Systems 36.2 (2011): 498-516.

    Parameters
    ---------------
    tree1
        First process tree
    tree2
        Second process tree
    weights
        Weights of each feature

    Returns
    ---------------
    sim_metric
        Similarity metric
    """
    dist = structural_distance(tree1, tree2, weights)
    # maximum possible distance after normalisation is √len(FEATURES)
    return 1.0 - dist / math.sqrt(len(FEATURES))
