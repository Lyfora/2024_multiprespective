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
import random
from typing import List

import numpy as np
import networkx as nx
from pm4py.objects.petri_net.obj import PetriNet


###############################################################################
# 1 ⸺ Convert the Petri net to a plain (bidirectional) graph
###############################################################################

def _petri_to_nx(net) -> nx.Graph:
    """
    Returns an undirected NetworkX graph with node attribute ``type``
    ∈ {'place', 'transition'} and edge attribute ``flow`` (weight 1).
    """
    G = nx.Graph()
    # add nodes
    for p in net.places:
        G.add_node(f"P_{p.name}", type="place")
    for t in net.transitions:
        G.add_node(f"T_{t.name}", type="transition")
    # add arcs
    for arc in net.arcs:
        u, v = arc.source, arc.target
        uid = f"{'P' if u in net.places else 'T'}_{u.name}"
        vid = f"{'P' if v in net.places else 'T'}_{v.name}"
        G.add_edge(uid, vid, flow=1)
    return G


###############################################################################
# 2 ⸺ Node2Vec walk generation (simplified, unbiased)
###############################################################################

def _random_walk(G: nx.Graph, start: str, length: int) -> List[str]:
    walk = [start]
    for _ in range(length - 1):
        cur = walk[-1]
        neigh = list(G.neighbors(cur))
        if not neigh:
            break
        walk.append(random.choice(neigh))
    return walk


def _generate_walks(G: nx.Graph,
                    num_walks: int = 10,
                    walk_length: int = 40,
                    seed: int = 42) -> List[List[str]]:
    random.seed(seed)
    nodes = list(G.nodes())
    walks = []
    for _ in range(num_walks):
        random.shuffle(nodes)
        for n in nodes:
            walks.append(_random_walk(G, n, walk_length))
    return walks


###############################################################################
# 3 ⸺ Train Skip-gram (gensim Word2Vec)
###############################################################################

def _train_word2vec(walks: List[List[str]],
                    dimensions: int = 64,
                    window: int = 5,
                    workers: int = 1,
                    epochs: int = 10):
    from gensim.models import Word2Vec

    return Word2Vec(
        sentences=walks,
        vector_size=dimensions,
        window=window,
        min_count=0,
        sg=1,            # skip-gram
        workers=workers,
        epochs=epochs,
    )


###############################################################################
# 4 ⸺ Read-out: aggregate node embeddings into **one vector per net**
###############################################################################

def _readout(model,
             G: nx.Graph,
             mode: str = "mean",
             transition_only: bool = False) -> np.ndarray:
    """
    Parameters
    ----------
    mode : {'mean', 'sum', 'max'}
        How to pool the node embeddings.
    transition_only : bool
        If True, use only transition nodes (ignores places).
    """
    nodes = [
        n for n, data in G.nodes(data=True)
        if (not transition_only) or data["type"] == "transition"
    ]
    vecs = np.array([model.wv[n] for n in nodes])
    if mode == "sum":
        return vecs.sum(axis=0)
    if mode == "max":
        return vecs.max(axis=0)
    return vecs.mean(axis=0)      # default


###############################################################################
# 5 ⸺ Public helper
###############################################################################

def petri_net_embedding(net,
                        dimensions: int = 64,
                        num_walks: int = 10,
                        walk_length: int = 40,
                        window: int = 5,
                        workers: int = 1,
                        epochs: int = 10,
                        readout: str = "mean",
                        transition_only: bool = False,
                        seed: int = 42) -> np.ndarray:
    """
    Returns
    -------
    np.ndarray  (shape = [dimensions,])
    """
    G = _petri_to_nx(net)
    walks = _generate_walks(G, num_walks, walk_length, seed)
    w2v = _train_word2vec(walks, dimensions, window, workers, epochs)
    return _readout(w2v, G, readout, transition_only)


def cosine_similarity(a, b, eps=1e-10):
    a = np.asarray(a, dtype=float)
    b = np.asarray(b, dtype=float)

    num   = np.dot(a, b)                 # dot product
    denom = np.linalg.norm(a) * np.linalg.norm(b)

    if denom < eps:                      # avoid division-by-zero
        return 0.0                       # or raise an error / np.nan
    return num / denom


def apply(net1: PetriNet, net2: PetriNet) -> float:
    """
    Computes the embeddings-based similarity between two Petri nets,
    based on the approach described in:

    Colonna, Juan G., et al. "Process mining embeddings: Learning vector representations for Petri nets." Intelligent Systems with Applications 23 (2024): 200423.


    Parameters
    ----------------
    net1
        Petri net
    net2
        Second Petri net

    Returns
    -----------------
    similarity_metric
        Cosine similarity between the two embeddings
    """
    emb1 = petri_net_embedding(net1)
    emb2 = petri_net_embedding(net2)

    return cosine_similarity(emb1, emb2)
