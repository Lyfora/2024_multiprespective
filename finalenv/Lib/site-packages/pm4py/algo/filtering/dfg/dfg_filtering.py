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
import math
from copy import deepcopy

from pm4py.objects.dfg.utils import dfg_utils
from pm4py.util import constants, nx_utils
from collections import deque

DEFAULT_NOISE_THRESH_DF = 0.16


def generate_nx_graph_from_dfg(
    dfg, start_activities, end_activities, activities_count
):
    """
    Generate a NetworkX graph for reachability-checking purposes out of the DFG

    Parameters
    --------------
    dfg
        DFG
    start_activities
        Start activities
    end_activities
        End activities
    activities_count
        Activities of the DFG along with their count

    Returns
    --------------
    G
        NetworkX digraph
    start_node
        Identifier of the start node (connected to all the start activities)
    end_node
        Identifier of the end node (connected to all the end activities)
    """
    start_node = "4d872045-8664-4e21-bd55-5da5edb096fe"  # made static to avoid undeterminism
    # made static to avoid undeterminism
    end_node = "b8136db7-b162-4763-bd68-4d5ccbcdff87"
    G = nx_utils.DiGraph()
    G.add_node(start_node)
    G.add_node(end_node)
    for act in activities_count:
        G.add_node(act)
    for edge in dfg:
        G.add_edge(edge[0], edge[1])
    for act in start_activities:
        G.add_edge(start_node, act)
    for act in end_activities:
        G.add_edge(act, end_node)
    return G, start_node, end_node


def build_adjacency_structures(dfg, start_activities, end_activities):
    """
    Build forward (adj) and reverse (rev_adj) adjacency lists for the DFG,
    plus two synthetic nodes for the "start" and "end".
    - start_node points to each node in start_activities.
    - each node in end_activities points to end_node.

    Returns:
        adj, rev_adj, start_node, end_node
    """
    # Synthetic labels for start and end
    start_node = "_S_START_"
    end_node = "_S_END_"

    # Gather all real nodes in the DFG
    nodes = set()
    for (a, b) in dfg.keys():
        nodes.add(a)
        nodes.add(b)
    nodes.update(start_activities.keys())
    nodes.update(end_activities.keys())

    # Initialize adjacency and reverse adjacency
    adj = {}
    rev_adj = {}
    for n in nodes:
        adj[n] = set()
        rev_adj[n] = set()

    # Fill adjacencies from the DFG
    for (a, b), _ in dfg.items():
        adj[a].add(b)
        rev_adj[b].add(a)

    # Synthetic start_node
    adj[start_node] = set(start_activities.keys())
    rev_adj[start_node] = set()
    for sa in start_activities:
        rev_adj[sa].add(start_node)

    # Synthetic end_node
    adj[end_node] = set()
    rev_adj[end_node] = set(end_activities.keys())
    for ea in end_activities:
        adj[ea].add(end_node)

    return adj, rev_adj, start_node, end_node


def bfs_reachable(start, adj):
    """
    Returns the set of nodes reachable from 'start' in the directed graph
    defined by adjacency list 'adj'.
    """
    visited = set()
    queue = deque([start])
    visited.add(start)
    while queue:
        u = queue.popleft()
        for v in adj[u]:
            if v not in visited:
                visited.add(v)
                queue.append(v)
    return visited


def remove_unreachable_nodes(
    dfg, start_activities, end_activities, activities_count,
    adj, rev_adj, start_node, end_node
):
    """
    Removes from the DFG (and related dictionaries) any activity/node that is not
    reachable from start_node or cannot reach end_node, based on the current
    adjacency structure 'adj' and 'rev_adj'.
    """
    reachable_from_start = bfs_reachable(start_node, adj)
    reachable_to_end = bfs_reachable(end_node, rev_adj)
    # We only keep nodes that are both reachable from start and can reach end
    truly_reachable = reachable_from_start.intersection(reachable_to_end)

    # Exclude the synthetic nodes from normal activities
    if start_node in truly_reachable:
        truly_reachable.remove(start_node)
    if end_node in truly_reachable:
        truly_reachable.remove(end_node)

    # Remove any activity not in truly_reachable from the dictionaries
    all_activities = set(activities_count.keys())
    to_remove = all_activities - truly_reachable

    for act in to_remove:
        # remove from dfg
        dfg = {edge: cnt for edge, cnt in dfg.items()
               if edge[0] != act and edge[1] != act}
        if act in activities_count:
            del activities_count[act]
        if act in start_activities:
            del start_activities[act]
        if act in end_activities:
            del end_activities[act]

    # Also ensure DFG has only edges among remaining activities
    remaining = set(activities_count.keys())
    dfg = {
        edge: cnt for edge, cnt in dfg.items()
        if edge[0] in remaining and edge[1] in remaining
    }

    return dfg, start_activities, end_activities, activities_count


def filter_dfg_on_activities_percentage(
    dfg0, start_activities0, end_activities0, activities_count0, percentage
):
    """
    Filters a DFG (complete, and so connected) on the specified percentage of
    activities (but ensuring that every node is still reachable from the start
    and can reach the end).

    Parameters
    ----------------
    dfg0
        (Complete, and so connected) DFG
    start_activities0
        Start activities
    end_activities0
        End activities
    activities_count0
        Activities of the DFG along with their count
    percentage
        Percentage of activities

    Returns
    ----------------
    dfg
        (Filtered) DFG
    start_activities
        (Filtered) start activities
    end_activities
        (Filtered) end activities
    activities_count
        (Filtered) activities of the DFG along with their count
    """
    # Copy dictionaries (to avoid mutating the originals)
    dfg = deepcopy(dfg0)
    start_activities = deepcopy(start_activities0)
    end_activities = deepcopy(end_activities0)
    activities_count = deepcopy(activities_count0)

    if len(activities_count) > 1 and len(dfg) > 1:
        # Sort activities by frequency (descending)
        sorted_activities = sorted(
            activities_count.items(),
            key=lambda x: (x[1], x[0]),
            reverse=True
        )
        # The set of activities we must keep according to the percentage
        cut_idx = math.ceil((len(activities_count) - 1) * percentage) + 1
        min_set_activities_to_keep = {act for act, _ in sorted_activities[:cut_idx]}
        # The rest are discard candidates (lowest freq first, so reverse the slice)
        activities_to_discard_candidates = [act for act, _ in sorted_activities[cut_idx:]]
        activities_to_discard_candidates.reverse()

        # Build adjacency once
        adj, rev_adj, start_node, end_node = build_adjacency_structures(
            dfg, start_activities, end_activities
        )

        # Try removing each candidate activity
        for act in activities_to_discard_candidates:
            # Save adjacency for reversion
            saved_succ = adj.get(act, set()).copy()
            saved_pred = rev_adj.get(act, set()).copy()

            # Remove node in-place
            if act in adj:
                for p in saved_pred:
                    adj[p].discard(act)
                for s in saved_succ:
                    rev_adj[s].discard(act)
                del adj[act]
                del rev_adj[act]

            # Check if min_set_activities_to_keep is still fully connected
            reachable_from_start = bfs_reachable(start_node, adj)
            reachable_to_end = bfs_reachable(end_node, rev_adj)

            if (min_set_activities_to_keep.issubset(reachable_from_start)
                    and min_set_activities_to_keep.issubset(reachable_to_end)):
                # If okay, remove from DFG dicts
                # Remove edges that have 'act'
                dfg = {
                    (a, b): cnt for (a, b), cnt in dfg.items()
                    if a != act and b != act
                }
                if act in activities_count:
                    del activities_count[act]
                if act in start_activities:
                    del start_activities[act]
                if act in end_activities:
                    del end_activities[act]
            else:
                # Revert removal
                adj[act] = saved_succ
                rev_adj[act] = saved_pred
                for p in saved_pred:
                    adj[p].add(act)
                for s in saved_succ:
                    rev_adj[s].add(act)

        # Final pass: remove any leftover unreachable activities
        dfg, start_activities, end_activities, activities_count = remove_unreachable_nodes(
            dfg, start_activities, end_activities, activities_count,
            adj, rev_adj, start_node, end_node
        )

    return dfg, start_activities, end_activities, activities_count


def __filter_specified_paths_adjacency(
    dfg,
    start_activities,
    end_activities,
    activities_count,
    adj,
    rev_adj,
    start_node,
    end_node,
    discardable_edges,
    activities_not_to_discard
):
    """
    Removes edges from 'discardable_edges' if it does not break connectivity
    for all activities_not_to_discard from start_node to end_node.
    This version uses adjacency lists (adj, rev_adj) for in-place removal.
    """

    for (u, v) in discardable_edges:
        # If edge exists in adjacency, try removing it
        if u in adj and v in adj[u]:
            adj[u].remove(v)
            rev_adj[v].remove(u)

            # Check if all activities_not_to_discard remain connected
            reachable_from_start = bfs_reachable(start_node, adj)
            reachable_to_end = bfs_reachable(end_node, rev_adj)

            if (activities_not_to_discard.issubset(reachable_from_start)
                    and activities_not_to_discard.issubset(reachable_to_end)):
                # Removal is successful: update DFG
                if (u, v) in dfg:
                    del dfg[(u, v)]
                elif u == start_node and v in start_activities:
                    del start_activities[v]
                elif v == end_node and u in end_activities:
                    del end_activities[u]
            else:
                # Revert removal
                adj[u].add(v)
                rev_adj[v].add(u)

    # Finally, remove any unreachable activities
    dfg, start_activities, end_activities, activities_count = remove_unreachable_nodes(
        dfg, start_activities, end_activities, activities_count,
        adj, rev_adj, start_node, end_node
    )

    return dfg, start_activities, end_activities, activities_count


def filter_dfg_on_paths_percentage(
    dfg0,
    start_activities0,
    end_activities0,
    activities_count0,
    percentage,
    keep_all_activities=False,
):
    """
    Filters a DFG (complete, and so connected) on the specified percentage of paths
    (but ensuring that every node is still reachable from the start and can reach the end).

    Parameters
    ----------------
    dfg0
        (Complete, and so connected) DFG
    start_activities0
        Start activities
    end_activities0
        End activities
    activities_count0
        Activities of the DFG along with their count
    percentage
        Percentage of paths
    keep_all_activities
        If True, keep all activities (only remove edges) and preserve connectivity;
        otherwise, only guarantee that the activities in the high-percentage edges
        remain connected.

    Returns
    ----------------
    dfg
        (Filtered) DFG
    start_activities
        (Filtered) start activities
    end_activities
        (Filtered) end activities
    activities_count
        (Filtered) activities of the DFG along with their count
    """
    dfg = deepcopy(dfg0)
    start_activities = deepcopy(start_activities0)
    end_activities = deepcopy(end_activities0)
    activities_count = deepcopy(activities_count0)

    if len(activities_count) > 1 and len(dfg) > 1:
        # Build adjacency
        adj, rev_adj, start_node, end_node = build_adjacency_structures(
            dfg, start_activities, end_activities
        )

        # Gather all edges (including start->act and act->end) with their frequencies
        all_edges_with_freq = list(dfg.items()) + \
            [((start_node, x), freq) for x, freq in start_activities.items()] + \
            [((x, end_node), freq) for x, freq in end_activities.items()]

        # Sort edges descending by frequency
        all_edges_with_freq.sort(key=lambda x: (x[1], x[0]), reverse=True)

        # Determine how many edges to keep as "non-discardable" according to percentage
        cut_idx = math.ceil((len(all_edges_with_freq) - 1) * percentage) + 1
        non_discardable_edges = [edge for (edge, freq) in all_edges_with_freq[:cut_idx]]
        discardable_edges = [edge for (edge, freq) in all_edges_with_freq[cut_idx:]]
        # Reverse so we remove the lowest-frequency edges first
        discardable_edges.reverse()

        # Based on keep_all_activities, decide the set of must-keep activities
        if keep_all_activities:
            activities_not_to_discard = set(activities_count.keys()) \
                .union(set(start_activities.keys())) \
                .union(set(end_activities.keys()))
        else:
            # Only keep the activities that appear in the non-discardable edges
            nd_acts_src = [u for (u, v) in non_discardable_edges if u != start_node]
            nd_acts_tgt = [v for (u, v) in non_discardable_edges if v != end_node]
            activities_not_to_discard = set(nd_acts_src).union(set(nd_acts_tgt))

        # Filter using our adjacency-based path filtering
        dfg, start_activities, end_activities, activities_count = \
            __filter_specified_paths_adjacency(
                dfg,
                start_activities,
                end_activities,
                activities_count,
                adj,
                rev_adj,
                start_node,
                end_node,
                discardable_edges,
                activities_not_to_discard,
            )

    return dfg, start_activities, end_activities, activities_count


def filter_dfg_keep_connected(
    dfg0,
    start_activities0,
    end_activities0,
    activities_count0,
    threshold,
    keep_all_activities=False,
):
    """
    Filters a DFG (complete, and so connected) on the specified dependency threshold
    (similar to Heuristics Miner dependency), but ensuring every node is still
    reachable from the start and can reach the end.

    Parameters
    ----------------
    dfg0
        (Complete, and so connected) DFG
    start_activities0
        Start activities
    end_activities0
        End activities
    activities_count0
        Activities of the DFG along with their count
    threshold
        Dependency threshold as in the Heuristics Miner
    keep_all_activities
        If True, keep all activities (only remove edges that fall below threshold);
        otherwise, remove activities not connected by high-dependency edges.

    Returns
    ----------------
    dfg
        (Filtered) DFG
    start_activities
        (Filtered) start activities
    end_activities
        (Filtered) end activities
    activities_count
        (Filtered) activities of the DFG along with their count
    """
    dfg = deepcopy(dfg0)
    start_activities = deepcopy(start_activities0)
    end_activities = deepcopy(end_activities0)
    activities_count = deepcopy(activities_count0)

    if len(activities_count) > 1 and len(dfg) > 1:
        # Build adjacency
        adj, rev_adj, start_node, end_node = build_adjacency_structures(
            dfg, start_activities, end_activities
        )

        # Compute Heuristics Miner "dependency" measure for each edge
        dependency = {}
        for (a, b), val_ab in dfg.items():
            inv = (b, a)
            val_ba = dfg.get(inv, 0)
            # dep = (freq_ab - freq_ba) / (freq_ab + freq_ba + 1)
            # if inverse not there, or freq_ba=0 => simpler formula
            dep = (val_ab - val_ba) / (val_ab + val_ba + 1) if inv in dfg else val_ab / (val_ab + 1)
            dependency[(a, b)] = dep

        # Also treat start->act and act->end edges as dependency = 1.0
        # so they won't be discarded if threshold <= 1
        all_edges_with_dep = list(dependency.items()) + \
            [((start_node, s), 1.0) for s in start_activities.keys()] + \
            [((e, end_node), 1.0) for e in end_activities.keys()]

        # Sort descending by dependency
        all_edges_with_dep.sort(key=lambda x: (x[1], x[0]), reverse=True)

        # Partition edges into "non-discardable" vs. "discardable"
        non_discardable_edges = [edge for (edge, dep) in all_edges_with_dep if dep >= threshold]
        discardable_edges = [edge for (edge, dep) in all_edges_with_dep if dep < threshold]
        # Reverse discardable so we remove the smallest first
        discardable_edges.reverse()

        # Decide which activities must remain connected
        if keep_all_activities:
            activities_not_to_discard = set(activities_count.keys()) \
                .union(set(start_activities.keys())) \
                .union(set(end_activities.keys()))
        else:
            nd_acts_src = [u for (u, v) in non_discardable_edges if u != start_node]
            nd_acts_tgt = [v for (u, v) in non_discardable_edges if v != end_node]
            activities_not_to_discard = set(nd_acts_src).union(set(nd_acts_tgt))

        # Filter using our adjacency-based path removal
        dfg, start_activities, end_activities, activities_count = \
            __filter_specified_paths_adjacency(
                dfg,
                start_activities,
                end_activities,
                activities_count,
                adj,
                rev_adj,
                start_node,
                end_node,
                discardable_edges,
                activities_not_to_discard,
            )

    return dfg, start_activities, end_activities, activities_count


def filter_dfg_to_activity(
    dfg0,
    start_activities0,
    end_activities0,
    activities_count0,
    target_activity,
    parameters=None,
):
    """
    Filters the DFG, making "target_activity" the only possible end activity of the graph

    Parameters
    ---------------
    dfg0
        Directly-follows graph
    start_activities0
        Start activities
    end_activities0
        End activities
    activities_count0
        Activities count
    target_activity
        Target activity (only possible end activity after the filtering)
    parameters
        Parameters

    Returns
    ---------------
    dfg
        Filtered DFG
    start_activities
        Filtered start activities
    end_activities
        Filtered end activities
    activities_count
        Filtered activities count
    """
    if parameters is None:
        parameters = {}

    # since the dictionaries/sets are modified, a deepcopy is the best option
    # to ensure data integrity
    dfg = deepcopy(dfg0)
    start_activities = deepcopy(start_activities0)
    activities_count = deepcopy(activities_count0)

    dfg = {x: y for x, y in dfg.items() if x[0] != target_activity}
    end_activities = {target_activity: activities_count[target_activity]}

    changed = True
    while changed:
        changed = False
        predecessors = dfg_utils.get_predecessors(dfg, activities_count)
        successors = dfg_utils.get_successors(dfg, activities_count)

        successors_from_sa = set()
        for act in start_activities:
            successors_from_sa = successors_from_sa.union(successors[act])
            successors_from_sa.add(act)

        reachable_nodes = successors_from_sa.intersection(
            predecessors[target_activity]
        ).union({target_activity})
        if reachable_nodes != set(activities_count.keys()):
            changed = True
            activities_count = {
                x: y
                for x, y in activities_count.items()
                if x in reachable_nodes
            }
            start_activities = {
                x: y
                for x, y in start_activities.items()
                if x in reachable_nodes
            }
            dfg = {
                x: y
                for x, y in dfg.items()
                if x[0] in reachable_nodes and x[1] in reachable_nodes
            }

    return dfg, start_activities, end_activities, activities_count


def filter_dfg_from_activity(
    dfg0,
    start_activities0,
    end_activities0,
    activities_count0,
    source_activity,
    parameters=None,
):
    """
    Filters the DFG, making "source_activity" the only possible source activity of the graph

    Parameters
    ---------------
    dfg0
        Directly-follows graph
    start_activities0
        Start activities
    end_activities0
        End activities
    activities_count0
        Activities count
    source_activity
        Source activity (only possible start activity after the filtering)
    parameters
        Parameters

    Returns
    ---------------
    dfg
        Filtered DFG
    start_activities
        Filtered start activities
    end_activities
        Filtered end activities
    activities_count
        Filtered activities count
    """
    if parameters is None:
        parameters = {}

    # since the dictionaries/sets are modified, a deepcopy is the best option
    # to ensure data integrity
    dfg = deepcopy(dfg0)
    end_activities = deepcopy(end_activities0)
    activities_count = deepcopy(activities_count0)

    dfg = {x: y for x, y in dfg.items() if x[1] != source_activity}
    start_activities = {source_activity: activities_count[source_activity]}

    changed = True
    while changed:
        changed = False
        predecessors = dfg_utils.get_predecessors(dfg, activities_count)
        successors = dfg_utils.get_successors(dfg, activities_count)

        predecessors_from_ea = set()
        for ea in end_activities:
            predecessors_from_ea = predecessors_from_ea.union(predecessors[ea])
            predecessors_from_ea.add(ea)

        reachable_nodes = predecessors_from_ea.intersection(
            successors[source_activity]
        ).union({source_activity})
        if reachable_nodes != set(activities_count.keys()):
            changed = True
            activities_count = {
                x: y
                for x, y in activities_count.items()
                if x in reachable_nodes
            }
            end_activities = {
                x: y for x, y in end_activities.items() if x in reachable_nodes
            }
            dfg = {
                x: y
                for x, y in dfg.items()
                if x[0] in reachable_nodes and x[1] in reachable_nodes
            }

    return dfg, start_activities, end_activities, activities_count


def filter_dfg_contain_activity(
    dfg0,
    start_activities0,
    end_activities0,
    activities_count0,
    activity,
    parameters=None,
):
    """
    Filters the DFG keeping only nodes that can reach / are reachable from activity

    Parameters
    ---------------
    dfg0
        Directly-follows graph
    start_activities0
        Start activities
    end_activities0
        End activities
    activities_count0
        Activities count
    activity
        Activity that should be reachable / should reach all the nodes of the filtered graph
    parameters
        Parameters

    Returns
    ---------------
    dfg
        Filtered DFG
    start_activities
        Filtered start activities
    end_activities
        Filtered end activities
    activities_count
        Filtered activities count
    """
    if parameters is None:
        parameters = {}

    # since the dictionaries/sets are modified, a deepcopy is the best option
    # to ensure data integrity
    dfg = deepcopy(dfg0)
    start_activities = deepcopy(start_activities0)
    end_activities = deepcopy(end_activities0)
    activities_count = deepcopy(activities_count0)

    changed = True
    while changed:
        changed = False
        predecessors = dfg_utils.get_predecessors(dfg, activities_count)
        successors = dfg_utils.get_successors(dfg, activities_count)

        predecessors_act = predecessors[activity].union({activity})
        successors_act = successors[activity].union({activity})

        start_activities1 = {
            x: y for x, y in start_activities.items() if x in predecessors_act
        }
        end_activities1 = {
            x: y for x, y in end_activities.items() if x in successors_act
        }

        if (
            start_activities != start_activities1
            or end_activities != end_activities1
        ):
            changed = True

        start_activities = start_activities1
        end_activities = end_activities1

        reachable_nodes = predecessors_act.union(successors_act)
        if reachable_nodes != set(activities_count.keys()):
            changed = True
            activities_count = {
                x: y
                for x, y in activities_count.items()
                if x in reachable_nodes
            }
            dfg = {
                x: y
                for x, y in dfg.items()
                if x[0] in reachable_nodes and x[1] in reachable_nodes
            }

    return dfg, start_activities, end_activities, activities_count


def clean_dfg_based_on_noise_thresh(
    dfg, activities, noise_threshold, parameters=None
):
    """
    Clean Directly-Follows graph based on noise threshold

    Parameters
    ----------
    dfg
        Directly-Follows graph
    activities
        Activities in the DFG graph
    noise_threshold
        Noise threshold

    Returns
    ----------
    newDfg
        Cleaned dfg based on noise threshold
    """
    if parameters is None:
        parameters = {}

    most_common_paths = (
        parameters[constants.PARAM_MOST_COMMON_PATHS]
        if constants.PARAM_MOST_COMMON_PATHS in parameters
        else None
    )
    if most_common_paths is None:
        most_common_paths = []

    new_dfg = None
    activ_max_count = {}
    for act in activities:
        activ_max_count[act] = dfg_utils.get_max_activity_count(dfg, act)

    for el in dfg:
        if type(el[0]) is str:
            if new_dfg is None:
                new_dfg = {}
            act1 = el[0]
            act2 = el[1]
            val = dfg[el]
        else:
            if new_dfg is None:
                new_dfg = []
            act1 = el[0][0]
            act2 = el[0][1]
            val = el[1]

        if el not in most_common_paths and val < min(
            activ_max_count[act1] * noise_threshold,
            activ_max_count[act2] * noise_threshold,
        ):
            pass
        else:
            if type(el[0]) is str:
                new_dfg[el] = dfg[el]
                pass
            else:
                new_dfg.append(el)
                pass

    if new_dfg is None:
        return dfg

    return new_dfg
