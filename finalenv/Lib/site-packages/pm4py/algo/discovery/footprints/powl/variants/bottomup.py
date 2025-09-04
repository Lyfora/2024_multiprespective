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
from pm4py.objects.powl.obj import (
    POWL,
    Transition,
    SilentTransition,
    OperatorPOWL,
    StrictPartialOrder,
    FrequentTransition,
)
from pm4py.objects.process_tree.obj import Operator
from enum import Enum
from typing import Optional, Dict, Any, Set, Tuple

###############################################################################
# Enums and constants
###############################################################################

class Outputs(Enum):
    DFG = "dfg"
    SEQUENCE = "sequence"
    PARALLEL = "parallel"
    START_ACTIVITIES = "start_activities"
    END_ACTIVITIES = "end_activities"
    ACTIVITIES = "activities"
    SKIPPABLE = "skippable"
    ACTIVITIES_ALWAYS_HAPPENING = "activities_always_happening"
    MIN_TRACE_LENGTH = "min_trace_length"
    TRACE = "trace"

START_ACTIVITIES = Outputs.START_ACTIVITIES.value
END_ACTIVITIES = Outputs.END_ACTIVITIES.value
ACTIVITIES = Outputs.ACTIVITIES.value
SKIPPABLE = Outputs.SKIPPABLE.value
SEQUENCE = Outputs.SEQUENCE.value
PARALLEL = Outputs.PARALLEL.value
ACTIVITIES_ALWAYS_HAPPENING = Outputs.ACTIVITIES_ALWAYS_HAPPENING.value


###############################################################################
# Utility functions to fix parallel vs sequence footprints
###############################################################################

def fix_fp(sequence: Set[Tuple[str, str]], parallel: Set[Tuple[str, str]]):
    """
    Fix footprints:
    - Remove parallel relations from the sequence relations
    - If A->B and B->A both appear in sequence, that is a conflict. Turn them into parallel.
    """
    # Remove from sequence any pair that also appears in parallel
    sequence = sequence.difference(parallel)

    # If a pair (x,y) is in sequence and (y,x) is in sequence, they are actually parallel
    conflicting = {(x[0], x[1]) for x in sequence if (x[1], x[0]) in sequence}
    for el in conflicting:
        parallel.add(el)
        sequence.remove(el)

    return sequence, parallel


###############################################################################
# Combine footprints
###############################################################################

def merge_footprints(list_of_footprints):
    """
    Utility function to merge a list of footprints dictionaries in a 'parallel/AND' sense.

    Returns a dictionary that merges all sets and booleans according to the appropriate logic.
    - 'activities' => union
    - 'skippable' => AND
    - 'start_activities' => union (will be refined later in partial order)
    - 'end_activities' => union (refined later)
    - 'activities_always_happening' => union for non-skippable children
    - 'sequence', 'parallel' => union, then fix_fp at the end
    """
    if not list_of_footprints:
        return {
            START_ACTIVITIES: set(),
            END_ACTIVITIES: set(),
            ACTIVITIES: set(),
            SKIPPABLE: True,
            SEQUENCE: set(),
            PARALLEL: set(),
            ACTIVITIES_ALWAYS_HAPPENING: set(),
        }

    merged = {
        START_ACTIVITIES: set(list_of_footprints[0][START_ACTIVITIES]),
        END_ACTIVITIES: set(list_of_footprints[0][END_ACTIVITIES]),
        ACTIVITIES: set(list_of_footprints[0][ACTIVITIES]),
        SKIPPABLE: list_of_footprints[0][SKIPPABLE],
        SEQUENCE: set(list_of_footprints[0][SEQUENCE]),
        PARALLEL: set(list_of_footprints[0][PARALLEL]),
        ACTIVITIES_ALWAYS_HAPPENING: set(list_of_footprints[0][ACTIVITIES_ALWAYS_HAPPENING]),
    }

    for fp in list_of_footprints[1:]:
        merged[ACTIVITIES] = merged[ACTIVITIES].union(fp[ACTIVITIES])
        merged[SKIPPABLE] = merged[SKIPPABLE] and fp[SKIPPABLE]
        merged[SEQUENCE] = merged[SEQUENCE].union(fp[SEQUENCE])
        merged[PARALLEL] = merged[PARALLEL].union(fp[PARALLEL])
        if not fp[SKIPPABLE]:
            merged[ACTIVITIES_ALWAYS_HAPPENING] = merged[ACTIVITIES_ALWAYS_HAPPENING].union(
                fp[ACTIVITIES_ALWAYS_HAPPENING]
            )

    return merged


###############################################################################
# Combining minimum trace length from child footprints
###############################################################################

def combine_min_trace_length_par(children_fps):
    """
    For partial order or parallel/AND: we must execute each non-skippable submodel at least once.
    So the min trace length = sum of the min lengths of non-skippable children.
    """
    total = 0
    for fp in children_fps:
        if not fp[SKIPPABLE]:
            total += fp.get(Outputs.MIN_TRACE_LENGTH.value, 0)
    return total


def combine_min_trace_length_xor(children_fps):
    """
    For XOR/choice: min trace length = min among children.
    """
    if not children_fps:
        return 0
    return min(fp.get(Outputs.MIN_TRACE_LENGTH.value, 0) for fp in children_fps)


def combine_min_trace_length_loop(do_fp, redo_fp):
    """
    For LOOP (do, redo): must do 'do' at least once, so min length = min_length(do).
    """
    return do_fp.get(Outputs.MIN_TRACE_LENGTH.value, 0)


###############################################################################
# Handling transitions
###############################################################################

def get_footprints_of_transition(node: Transition) -> Dict[str, Any]:
    """
    Footprints for a simple Transition (labeled or silent).
    """
    if node.label is None:
        # Silent transition => effectively "skip"
        return {
            START_ACTIVITIES: set(),
            END_ACTIVITIES: set(),
            ACTIVITIES: set(),
            SKIPPABLE: True,
            SEQUENCE: set(),
            PARALLEL: set(),
            ACTIVITIES_ALWAYS_HAPPENING: set(),
            Outputs.MIN_TRACE_LENGTH.value: 0,
        }
    else:
        # Labeled transition => a leaf with one activity
        return {
            START_ACTIVITIES: {node.label},
            END_ACTIVITIES: {node.label},
            ACTIVITIES: {node.label},
            SKIPPABLE: False,
            SEQUENCE: set(),
            PARALLEL: set(),
            ACTIVITIES_ALWAYS_HAPPENING: {node.label},
            Outputs.MIN_TRACE_LENGTH.value: 1,
        }


###############################################################################
# Handling XOR
###############################################################################

def get_footprints_of_xor(node: OperatorPOWL, footprints_cache: Dict[POWL, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Footprints for an XOR/choice node:
        - start_activities = union(children.start_activities)
        - end_activities = union(children.end_activities)
        - activities = union
        - skippable = OR of children.skippable
        - activities_always_happening = intersection of all non-skippable children's AAH
        - sequence = union of children
        - parallel = union of children
        - min_trace_length = min of children
    """
    child_fps = [get_footprints_powl(ch, footprints_cache) for ch in node.children]

    start_activities = set()
    end_activities = set()
    activities = set()
    skippable = False
    sequence = set()
    parallel = set()

    # Intersection of always-happening among non-skippable children
    aah = None

    for fp in child_fps:
        start_activities |= fp[START_ACTIVITIES]
        end_activities |= fp[END_ACTIVITIES]
        activities |= fp[ACTIVITIES]
        skippable = skippable or fp[SKIPPABLE]
        sequence |= fp[SEQUENCE]
        parallel |= fp[PARALLEL]
        if not fp[SKIPPABLE]:
            if aah is None:
                aah = set(fp[ACTIVITIES_ALWAYS_HAPPENING])
            else:
                aah &= fp[ACTIVITIES_ALWAYS_HAPPENING]

    if aah is None:
        aah = set()

    sequence, parallel = fix_fp(sequence, parallel)

    min_trace_length = combine_min_trace_length_xor(child_fps)

    return {
        START_ACTIVITIES: start_activities,
        END_ACTIVITIES: end_activities,
        ACTIVITIES: activities,
        SKIPPABLE: skippable,
        SEQUENCE: sequence,
        PARALLEL: parallel,
        ACTIVITIES_ALWAYS_HAPPENING: aah,
        Outputs.MIN_TRACE_LENGTH.value: min_trace_length,
    }


###############################################################################
# Handling LOOP
###############################################################################

def get_footprints_of_loop(node: OperatorPOWL, footprints_cache: Dict[POWL, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Footprints for a LOOP node: children=[do, redo].
    Similar logic as process-tree loop footprints.
    """
    do_child = node.children[0]
    redo_child = node.children[1]

    do_fp = get_footprints_powl(do_child, footprints_cache)
    redo_fp = get_footprints_powl(redo_child, footprints_cache)

    start_activities = set(do_fp[START_ACTIVITIES])
    end_activities = set(do_fp[END_ACTIVITIES])
    activities = do_fp[ACTIVITIES] | redo_fp[ACTIVITIES]
    sequence = set(do_fp[SEQUENCE]) | set(redo_fp[SEQUENCE])
    parallel = set(do_fp[PARALLEL]) | set(redo_fp[PARALLEL])

    # skippable if 'do' is skippable
    skippable = do_fp[SKIPPABLE]

    # always happening if 'do' is not skippable
    aah = set()
    if not do_fp[SKIPPABLE]:
        aah = set(do_fp[ACTIVITIES_ALWAYS_HAPPENING])

    # if do is skippable => can also start/end in redo
    if do_fp[SKIPPABLE]:
        start_activities |= redo_fp[START_ACTIVITIES]
        end_activities |= redo_fp[END_ACTIVITIES]

    # Add edges do.end -> redo.start, redo.end -> do.start, etc.
    for a1 in do_fp[END_ACTIVITIES]:
        for a2 in redo_fp[START_ACTIVITIES]:
            sequence.add((a1, a2))
    for a1 in redo_fp[END_ACTIVITIES]:
        for a2 in do_fp[START_ACTIVITIES]:
            sequence.add((a1, a2))

    # If do is skippable => redo can loop with itself
    if do_fp[SKIPPABLE]:
        for a1 in redo_fp[END_ACTIVITIES]:
            for a2 in redo_fp[START_ACTIVITIES]:
                sequence.add((a1, a2))
    # If redo is skippable => do can loop with itself
    if redo_fp[SKIPPABLE]:
        for a1 in do_fp[END_ACTIVITIES]:
            for a2 in do_fp[START_ACTIVITIES]:
                sequence.add((a1, a2))

    sequence, parallel = fix_fp(sequence, parallel)

    min_trace_length = combine_min_trace_length_loop(do_fp, redo_fp)

    return {
        START_ACTIVITIES: start_activities,
        END_ACTIVITIES: end_activities,
        ACTIVITIES: activities,
        SKIPPABLE: skippable,
        SEQUENCE: sequence,
        PARALLEL: parallel,
        ACTIVITIES_ALWAYS_HAPPENING: aah,
        Outputs.MIN_TRACE_LENGTH.value: min_trace_length,
    }


###############################################################################
# Handling StrictPartialOrder + transitive reduction
###############################################################################

def transitive_reduction(node: StrictPartialOrder):
    """
    Applies transitive reduction on the partial order contained in `node`.
    If node.partial_order has edges A->B and B->C and A->C, we remove A->C.
    In general, if there's an alternative path from A to C, we remove the direct edge A->C.
    """
    from collections import defaultdict, deque

    children = node.children
    adjacency = {c: [] for c in children}
    edges = []

    # Build adjacency + list of edges
    for c in children:
        for d in children:
            if node.partial_order.is_edge(c, d):
                adjacency[c].append(d)
                edges.append((c, d))

    edges_to_remove = []
    for (source, target) in edges:
        # Check if there's another path from source to target ignoring the direct edge
        visited = set()
        queue = deque([source])
        found_target = False
        while queue:
            current = queue.popleft()
            if current == target and current != source:
                # We reached target from source ignoring direct edge => it's transitive
                found_target = True
                break
            # Traverse neighbors
            for nxt in adjacency[current]:
                # skip the direct edge source->target
                if current == source and nxt == target:
                    continue
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append(nxt)

        if found_target:
            # The direct edge source->target is implied by a longer path
            edges_to_remove.append((source, target))

    # Remove them from the partial_order
    for (source, target) in edges_to_remove:
        node.partial_order.remove_edge(source, target)


def get_footprints_of_partial_order(
    node: StrictPartialOrder, footprints_cache: Dict[POWL, Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Footprints for a StrictPartialOrder node:
      1) Build original adjacency (before transitive reduction).
      2) Compute transitive closure for concurrency + skip logic + start/end detection.
      3) Apply transitive reduction to keep the partial order minimal.
      4) Compute footprints for each child, merge them (AND/parallel).
      5) Refine start_activities: child c is 'start' if no non-skippable p != c has c in closure[p].
      6) Refine end_activities: child c is 'end' if no non-skippable q != c is in closure[c].
      7) Build sequence links:
         - direct edges from partial_order
         - skip edges if all intermediates are skippable
      8) Concurrency detection with the closure.
      9) fix_fp(...) and set min_trace_length.
    """
    from collections import defaultdict, deque

    # --------------------------------------------------------------------------
    # 1) Original adjacency
    # --------------------------------------------------------------------------
    children = node.children
    original_adjacency = {c: [] for c in children}
    for c in children:
        for d in children:
            if node.partial_order.is_edge(c, d):
                original_adjacency[c].append(d)

    # --------------------------------------------------------------------------
    # 2) Compute transitive closure from original adjacency
    # --------------------------------------------------------------------------
    # closure[c] = set of nodes reachable from c (including c itself).
    closure = {c: set() for c in children}
    for c in children:
        visited = set([c])
        queue = deque([c])
        while queue:
            cur = queue.popleft()
            for nxt in original_adjacency[cur]:
                if nxt not in visited:
                    visited.add(nxt)
                    queue.append(nxt)
        closure[c] = visited

    # --------------------------------------------------------------------------
    # 3) Transitive reduction => partial order is minimal
    # --------------------------------------------------------------------------
    transitive_reduction(node)

    # --------------------------------------------------------------------------
    # 4) Footprints of each child, merge them
    # --------------------------------------------------------------------------
    child_fps = [get_footprints_powl(c, footprints_cache) for c in children]
    merged_fp = merge_footprints(child_fps)
    sequence = set(merged_fp[SEQUENCE])
    parallel = set(merged_fp[PARALLEL])
    start_activities = set()
    end_activities = set()

    child_index = {c: i for i, c in enumerate(children)}

    # --------------------------------------------------------------------------
    # 5) Refine START
    #    c is a start node if there is NO non-skippable p != c with c in closure[p].
    #    i.e. c can't be preceded by a mandatory node that must happen first.
    # --------------------------------------------------------------------------
    for i, c in enumerate(children):
        fp_c = child_fps[i]
        is_start = True
        for p in children:
            if p != c:
                fp_p = child_fps[child_index[p]]
                if not fp_p[SKIPPABLE]:
                    # p is mandatory
                    if c in closure[p]:
                        # means p->...->c
                        is_start = False
                        break
        if is_start:
            start_activities |= fp_c[START_ACTIVITIES]

    # --------------------------------------------------------------------------
    # 6) Refine END
    #    c is an end node if there is NO non-skippable q != c with q in closure[c].
    #    i.e. c can't be followed by a mandatory node in any path.
    # --------------------------------------------------------------------------
    for i, c in enumerate(children):
        fp_c = child_fps[i]
        is_end = True
        for q in children:
            if q != c:
                fp_q = child_fps[child_index[q]]
                if not fp_q[SKIPPABLE]:
                    # q is mandatory
                    if q in closure[c]:
                        # means c->...->q
                        is_end = False
                        break
        if is_end:
            end_activities |= fp_c[END_ACTIVITIES]

    # --------------------------------------------------------------------------
    # 7) Build sequence edges
    #    (a) direct edges from partial_order after reduction
    #    (b) skip edges if all intermediates are skippable
    # --------------------------------------------------------------------------
    reduced_adjacency = defaultdict(list)
    for c in children:
        for d in children:
            if node.partial_order.is_edge(c, d):
                reduced_adjacency[c].append(d)
                # direct link in footprints
                fp_c = child_fps[child_index[c]]
                fp_d = child_fps[child_index[d]]
                for a1 in fp_c[END_ACTIVITIES]:
                    for a2 in fp_d[START_ACTIVITIES]:
                        sequence.add((a1, a2))

    def all_paths_through_only_skippable(c, d):
        """
        Return True if c can reach d (c!=d) via closure, AND
        for every path c->...->d, all intermediate nodes are skippable.
        Equivalently:
           There's no non-skippable n != c,d with c->...->n->...->d.
        If there is any mandatory n in that path, we cannot skip it.
        """
        if d not in closure[c] or c == d:
            return False

        for n in children:
            if n != c and n != d:
                fp_n = child_fps[child_index[n]]
                if not fp_n[SKIPPABLE]:
                    # n is mandatory
                    # if c->n->...->d => can't skip n
                    if (n in closure[c]) and (d in closure[n]):
                        return False
        return True

    # skip edges
    for c in children:
        fp_c = child_fps[child_index[c]]
        for d in children:
            if c != d:
                if all_paths_through_only_skippable(c, d):
                    fp_d = child_fps[child_index[d]]
                    for a1 in fp_c[END_ACTIVITIES]:
                        for a2 in fp_d[START_ACTIVITIES]:
                            sequence.add((a1, a2))

    # --------------------------------------------------------------------------
    # 8) Concurrency detection
    #    c,d concurrent if c->...->d not in closure and d->...->c not in closure
    # --------------------------------------------------------------------------
    for i in range(len(children)):
        for j in range(i + 1, len(children)):
            c = children[i]
            d = children[j]
            if (d not in closure[c]) and (c not in closure[d]):
                # concurrency
                fp_c = child_fps[i]
                fp_d = child_fps[j]
                for a1 in fp_c[ACTIVITIES]:
                    for a2 in fp_d[ACTIVITIES]:
                        parallel.add((a1, a2))
                        parallel.add((a2, a1))

    # --------------------------------------------------------------------------
    # 9) fix_fp(...) and finalize
    # --------------------------------------------------------------------------
    sequence, parallel = fix_fp(sequence, parallel)

    merged_fp[START_ACTIVITIES] = start_activities
    merged_fp[END_ACTIVITIES] = end_activities
    merged_fp[SEQUENCE] = sequence
    merged_fp[PARALLEL] = parallel
    # min_trace_length = sum of non-skippable children
    merged_fp[Outputs.MIN_TRACE_LENGTH.value] = combine_min_trace_length_par(child_fps)

    return merged_fp


###############################################################################
# Recursive footprints function with memoization
###############################################################################

def get_footprints_powl(node: POWL, footprints_cache: Dict[POWL, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Computes the footprints of a POWL node, caching results in footprints_cache to avoid recomputation.
    """
    if node in footprints_cache:
        return footprints_cache[node]

    if isinstance(node, Transition):
        fp = get_footprints_of_transition(node)

    elif isinstance(node, OperatorPOWL):
        if node.operator == Operator.XOR:
            fp = get_footprints_of_xor(node, footprints_cache)
        elif node.operator == Operator.LOOP:
            fp = get_footprints_of_loop(node, footprints_cache)
        else:
            raise NotImplementedError(f"Unsupported Operator: {node.operator}")

    elif isinstance(node, StrictPartialOrder):
        fp = get_footprints_of_partial_order(node, footprints_cache)

    else:
        raise NotImplementedError(f"Unknown POWL node type: {type(node)}")

    footprints_cache[node] = fp
    return fp


###############################################################################
# Main entry point
###############################################################################

def apply(model: POWL, parameters: Optional[Dict[Any, Any]] = None) -> Dict[str, Any]:
    """
    Main entry point to compute footprints of a POWL model.
    Applies transitive reduction on any partial order, then computes standard footprints.

    The returned dictionary includes:
        "start_activities",
        "end_activities",
        "activities",
        "skippable",
        "sequence",
        "parallel",
        "activities_always_happening",
        "min_trace_length"
    """
    footprints_cache = {}
    footprints = get_footprints_powl(model, footprints_cache)
    return footprints
