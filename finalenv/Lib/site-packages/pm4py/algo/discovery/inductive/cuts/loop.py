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
from abc import ABC
from collections import Counter
from typing import List, Optional, Collection, Any, Tuple, Generic, Dict

from pm4py.util import nx_utils

from pm4py.algo.discovery.inductive.cuts.abc import Cut, T
from pm4py.algo.discovery.inductive.dtypes.im_dfg import InductiveDFG
from pm4py.algo.discovery.inductive.dtypes.im_ds import (
    IMDataStructureUVCL,
    IMDataStructureDFG,
)
from pm4py.objects.dfg import util as dfu
from pm4py.objects.dfg.obj import DFG
from pm4py.objects.process_tree.obj import Operator, ProcessTree
from pm4py.util.compression.dtypes import UVCL


class LoopCut(Cut[T], ABC, Generic[T]):

    @classmethod
    def operator(
        cls, parameters: Optional[Dict[str, Any]] = None
    ) -> ProcessTree:
        return ProcessTree(operator=Operator.LOOP)

    @classmethod
    def holds(
            cls, obj: T, parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[List[Collection[Any]]]:
        """
        Finds a loop cut in the DFG and returns exactly two groups:
        - groups[0]: the 'do' group (start ∪ end activities plus merged components as per checks)
        - groups[1]: a single 'redo' group obtained by merging all remaining non-empty groups

        If no non-empty redo part remains, returns None.
        """
        dfg = obj.dfg
        start_activities = set(dfg.start_activities.keys())
        end_activities = set(dfg.end_activities.keys())
        if len(dfg.graph) == 0:
            return None

        # Initial groups: do-part is start ∪ end; other parts are connected components after removing boundaries
        groups = [start_activities.union(end_activities)]
        for c in cls._compute_connected_components(dfg, start_activities, end_activities):
            groups.append(set(c.nodes))

        # Apply the original reachability/completeness checks
        groups = cls._exclude_sets_non_reachable_from_start(dfg, start_activities, end_activities, groups)
        groups = cls._exclude_sets_no_reachable_from_end(dfg, start_activities, end_activities, groups)
        groups = cls._check_start_completeness(dfg, start_activities, end_activities, groups)
        groups = cls._check_end_completeness(dfg, start_activities, end_activities, groups)

        # Keep only non-empty groups
        groups = [set(g) for g in groups if len(g) > 0]

        # Require at least a do group and something to redo
        if len(groups) <= 1:
            return None

        # Merge all remaining non-empty groups (from the second to the last) into a single redo group
        redo_merged = set()
        for g in groups[1:]:
            redo_merged.update(g)

        return [set(groups[0]), redo_merged]

    @classmethod
    def _check_start_completeness(
        cls,
        dfg: DFG,
        start_activities: Collection[Any],
        end_activities: Collection[Any],
        groups: List[Collection[Any]],
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Collection[Any]]:
        i = 1
        while i < len(groups):
            merge = False
            for a in groups[i]:
                if merge:
                    break
                for x, b in dfg.graph:
                    if x == a and b in start_activities:
                        for s in start_activities:
                            if not (a, s) in dfg.graph:
                                merge = True
            if merge:
                groups[0] = set(groups[0]).union(groups[i])
                del groups[i]
                continue
            i = i + 1
        return groups

    @classmethod
    def _check_end_completeness(
        cls,
        dfg: DFG,
        start_activities: Collection[Any],
        end_activities: Collection[Any],
        groups: List[Collection[Any]],
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Collection[Any]]:
        i = 1
        while i < len(groups):
            merge = False
            for a in groups[i]:
                if merge:
                    break
                for b, x in dfg.graph:
                    if x == a and b in end_activities:
                        for e in end_activities:
                            if not (e, a) in dfg.graph:
                                merge = True
            if merge:
                groups[0] = set(groups[0]).union(groups[i])
                del groups[i]
                continue
            i = i + 1
        return groups

    @classmethod
    def _exclude_sets_non_reachable_from_start(
        cls,
        dfg: DFG,
        start_activities: Collection[Any],
        end_activities: Collection[Any],
        groups: List[Collection[Any]],
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Collection[Any]]:
        for a in set(start_activities).difference(set(end_activities)):
            for x, b in dfg.graph:
                if x == a:
                    group_a, group_b = None, None
                    for group in groups:
                        group_a = group if a in group else group_a
                        group_b = group if b in group else group_b
                    groups = [
                        group
                        for group in groups
                        if group != group_a and group != group_b
                    ]
                    # we are always merging on the do-part
                    groups.insert(0, group_a.union(group_b))
        return groups

    @classmethod
    def _exclude_sets_no_reachable_from_end(
        cls,
        dfg: DFG,
        start_activities: Collection[Any],
        end_activities: Collection[Any],
        groups: List[Collection[Any]],
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Collection[Any]]:
        for b in set(end_activities).difference(start_activities):
            for a, x in dfg.graph:
                if x == b:
                    group_a, group_b = None, None
                    for group in groups:
                        group_a = group if a in group else group_a
                        group_b = group if b in group else group_b
                    groups = [
                        group
                        for group in groups
                        if group != group_a and group != group_b
                    ]
                    groups.insert(0, group_a.union(group_b))
        return groups

    @classmethod
    def _compute_connected_components(
        cls,
        dfg: DFG,
        start_activities: Collection[Any],
        end_activities: Collection[Any],
        parameters: Optional[Dict[str, Any]] = None,
    ):
        nxd = dfu.as_nx_graph(dfg)
        [
            nxd.remove_edge(a, b)
            for (a, b) in dfg.graph
            if a in start_activities
            or a in end_activities
            or b in start_activities
            or b in end_activities
        ]
        [nxd.remove_node(a) for a in start_activities if nxd.has_node(a)]
        [nxd.remove_node(a) for a in end_activities if nxd.has_node(a)]
        nxu = nxd.to_undirected()
        return [
            nxd.subgraph(c).copy() for c in nx_utils.connected_components(nxu)
        ]


class LoopCutUVCL(LoopCut[IMDataStructureUVCL]):

    @classmethod
    def project(
        cls,
        obj: IMDataStructureUVCL,
        groups: List[Collection[Any]],
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[IMDataStructureUVCL]:
        """
        Split the (compressed) log into one 'do' log and N 'redo' logs.
        Traces are segmented into do/redo slices; redo slices are routed to the
        redo log whose activity set overlaps most with the slice.
        """
        do = set(groups[0])
        redo_groups = [set(g) for g in groups[1:]]

        # For quick membership checks
        redo_activities = set().union(*redo_groups) if redo_groups else set()

        do_log = Counter()
        redo_logs = [Counter() for _ in redo_groups]

        for t, card in obj.data_structure.items():
            do_trace: Tuple[Any, ...] = tuple()
            redo_trace: Tuple[Any, ...] = tuple()

            for e in t:
                if e in do:
                    do_trace += (e,)
                    # flush redo if we were inside a redo slice
                    if redo_trace:
                        redo_logs = cls._append_trace_to_redo_log(
                            redo_trace, redo_logs, redo_groups, card
                        )
                        redo_trace = tuple()
                elif e in redo_activities:
                    redo_trace += (e,)
                    # flush do if we were inside a do slice
                    if do_trace:
                        do_log.update({do_trace: card})
                        do_trace = tuple()
                else:
                    # Safety: if an event is in neither do nor any redo group,
                    # flush any current slice and ignore the event
                    if do_trace:
                        do_log.update({do_trace: card})
                        do_trace = tuple()
                    if redo_trace:
                        redo_logs = cls._append_trace_to_redo_log(
                            redo_trace, redo_logs, redo_groups, card
                        )
                        redo_trace = tuple()

            # Flush tail slices
            if redo_trace:
                redo_logs = cls._append_trace_to_redo_log(
                    redo_trace, redo_logs, redo_groups, card
                )
            do_log.update({do_trace: card})  # keep empty do slices, consistent with original

        logs = [do_log] + redo_logs
        return [IMDataStructureUVCL(l) for l in logs]

    @classmethod
    def _append_trace_to_redo_log(
        cls,
        redo_trace: Tuple[Any, ...],
        redo_logs: List[UVCL],
        redo_groups: List[Collection[Any]],
        cardinality: int,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[UVCL]:
        """
        Append a redo slice to the best matching redo log (largest activity overlap).
        """
        if not redo_logs:
            return redo_logs

        activities = set(redo_trace)
        overlaps = [
            (i, len(activities.intersection(set(redo_groups[i]))))
            for i in range(len(redo_groups))
        ]
        overlaps.sort(key=lambda x: (x[1], x[0]), reverse=True)
        target = overlaps[0][0]
        redo_logs[target].update({redo_trace: cardinality})
        return redo_logs


class LoopCutDFG(LoopCut[IMDataStructureDFG]):

    @classmethod
    def project(
        cls,
        obj: IMDataStructureUVCL,
        groups: List[Collection[Any]],
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[IMDataStructureDFG]:
        dfg = obj.dfg
        dfgs = []
        skippable = [False, False]
        for gind, g in enumerate(groups):
            dfn = DFG()
            for a, b in dfg.graph:
                if a in g and b in g:
                    dfn.graph[(a, b)] = dfg.graph[(a, b)]
                if b in dfg.start_activities and a in dfg.end_activities:
                    skippable[1] = True
            if gind == 0:
                for a in dfg.start_activities:
                    if a in g:
                        dfn.start_activities[a] = dfg.start_activities[a]
                    else:
                        skippable[0] = True
                for a in dfg.end_activities:
                    if a in g:
                        dfn.end_activities[a] = dfg.end_activities[a]
                    else:
                        skippable[0] = True
            elif gind == 1:
                for a in g:
                    dfn.start_activities[a] = 1
                    dfn.end_activities[a] = 1
            dfgs.append(dfn)
        return [
            IMDataStructureDFG(InductiveDFG(dfg=dfgs[i], skip=skippable[i]))
            for i in range(len(dfgs))
        ]
