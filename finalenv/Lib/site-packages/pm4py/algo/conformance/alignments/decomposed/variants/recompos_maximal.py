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
import importlib.util
import sys
import time
from copy import copy

from pm4py.algo.conformance.alignments.petri_net.variants import (
    state_equation_a_star,
)
from pm4py.objects.log import obj as log_implementation
from pm4py.objects.log.obj import Trace
from pm4py.objects.log.util.xes import DEFAULT_NAME_KEY
from pm4py.objects.petri_net.utils import (
    align_utils as utils,
    decomposition as decomp_utils,
)
from pm4py.statistics.variants.log import get as variants_module
from pm4py.util import exec_utils
from pm4py.util import variants_util

from enum import Enum
from pm4py.util import constants, nx_utils

from typing import Optional, Dict, Any, Union
from pm4py.objects.log.obj import EventLog
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.util import typing
from pm4py.objects.conversion.log import converter as log_converter


class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    BEST_WORST_COST = "best_worst_cost"
    PARAM_TRACE_COST_FUNCTION = "trace_cost_function"
    ICACHE = "icache"
    MCACHE = "mcache"
    PARAM_THRESHOLD_BORDER_AGREEMENT = "thresh_border_agreement"
    PARAMETER_VARIANT_DELIMITER = "variant_delimiter"
    PARAM_MODEL_COST_FUNCTION = "model_cost_function"
    PARAM_SYNC_COST_FUNCTION = "sync_cost_function"
    PARAM_TRACE_NET_COSTS = "trace_net_costs"
    PARAM_MAX_ALIGN_TIME = "max_align_time"
    PARAM_MAX_ALIGN_TIME_TRACE = "max_align_time_trace"
    SHOW_PROGRESS_BAR = "show_progress_bar"


def get_best_worst_cost(
        petri_net, initial_marking, final_marking, parameters=None
):
    if parameters is None:
        parameters = {}

    trace = log_implementation.Trace()

    best_worst, cf = align(
        trace, petri_net, initial_marking, final_marking, parameters=parameters
    )

    best_worst_cost = (
        sum(cf[x] for x in best_worst["alignment"])
        // utils.STD_MODEL_LOG_MOVE_COST
        if best_worst
        else 0
    )

    return best_worst_cost


def apply_from_variants_list_petri_string(
        var_list, petri_net_string, parameters=None
):
    if parameters is None:
        parameters = {}

    from pm4py.objects.petri_net.importer.variants import (
        pnml as petri_importer,
    )

    petri_net, initial_marking, final_marking = (
        petri_importer.import_petri_from_string(petri_net_string)
    )

    res = apply_from_variants_list(
        var_list,
        petri_net,
        initial_marking,
        final_marking,
        parameters=parameters,
    )
    return res


def apply_from_variants_list(
        var_list, petri_net, initial_marking, final_marking, parameters=None
):
    """
    Apply the alignments from the specification of a list of variants in the log

    Parameters
    -------------
    var_list
        List of variants (for each item, the first entry is the variant itself, the second entry may be the number of cases)
    petri_net
        Petri net
    initial_marking
        Initial marking
    final_marking
        Final marking
    parameters
        Parameters of the algorithm (same as 'apply' method, plus 'variant_delimiter' that is , by default)

    Returns
    --------------
    dictio_alignments
        Dictionary that assigns to each variant its alignment
    """
    if parameters is None:
        parameters = {}

    # Create traces directly without repeatedly appending to log
    traces = [variants_util.variant_to_trace(varitem[0], parameters=parameters) for varitem in var_list]
    log = log_implementation.EventLog(traces)

    alignment = apply(log, petri_net, initial_marking, final_marking, parameters=parameters)

    # Create dictionary of alignments more efficiently
    dictio_alignments = {var_list[i][0]: alignment[i] for i in range(len(var_list))}

    return dictio_alignments


def apply(
        log: EventLog,
        net: PetriNet,
        im: Marking,
        fm: Marking,
        parameters: Optional[Dict[Union[str, Parameters], Any]] = None,
) -> typing.ListAlignments:
    """
    Apply the recomposition alignment approach
    to a log and a Petri net performing decomposition

    Parameters
    --------------
    log
        Event log
    net
        Petri net
    im
        Initial marking
    fm
        Final marking
    parameters
        Parameters of the algorithm

    Returns
    --------------
    aligned_traces
        For each trace, return its alignment
    """
    if parameters is None:
        parameters = {}

    log = log_converter.apply(
        log, variant=log_converter.Variants.TO_EVENT_LOG, parameters=parameters
    )

    best_worst_cost = get_best_worst_cost(net, im, fm, parameters=parameters)
    parameters[Parameters.BEST_WORST_COST] = best_worst_cost

    list_nets = decomp_utils.decompose(net, im, fm)

    return apply_log(log, list_nets, parameters=parameters)


def apply_log(log, list_nets, parameters=None):
    """
    Apply the recomposition alignment approach
    to a log and a decomposed Petri net
    """
    if parameters is None:
        parameters = {}

    show_progress_bar = exec_utils.get_param_value(
        Parameters.SHOW_PROGRESS_BAR, parameters, constants.SHOW_PROGRESS_BAR
    )

    # Use efficient caches
    icache = exec_utils.get_param_value(Parameters.ICACHE, parameters, dict())
    mcache = exec_utils.get_param_value(Parameters.MCACHE, parameters, dict())

    parameters[Parameters.ICACHE] = icache
    parameters[Parameters.MCACHE] = mcache

    # Get variants more efficiently
    variants_idxs = variants_module.get_variants_from_log_trace_idx(
        log, parameters=parameters
    )

    progress = None
    if importlib.util.find_spec("tqdm") and show_progress_bar:
        from tqdm.auto import tqdm
        progress = tqdm(
            total=len(variants_idxs),
            desc="aligning log with decomposition/recomposition, completed variants :: ",
        )

    # Process variants one by one
    variants_to_process = []

    for index_variant, variant in enumerate(variants_idxs):
        variants_to_process.append((variant, log[variants_idxs[variant][0]], index_variant))

    all_alignments = [None] * len(variants_to_process)  # Pre-allocate result list

    # Serial processing
    max_align_time = exec_utils.get_param_value(
        Parameters.PARAM_MAX_ALIGN_TIME, parameters, sys.maxsize
    )
    start_time = time.time()

    for variant_info in variants_to_process:
        this_time = time.time()
        if this_time - start_time <= max_align_time:
            alignment = apply_trace(variant_info[1], list_nets, parameters=parameters)
        else:
            alignment = None

        all_alignments[variant_info[2]] = alignment

        if progress is not None:
            progress.update()

    # Map alignments back to original traces
    al_idx = {}
    for index_variant, variant in enumerate(variants_idxs):
        for trace_idx in variants_idxs[variant]:
            al_idx[trace_idx] = all_alignments[index_variant]

    alignments = [al_idx[i] for i in range(len(log))]

    # Close progress bar
    if progress is not None:
        progress.close()
        del progress

    return alignments


def get_acache(cons_nets):
    """
    Calculates the A-Cache of the given decomposition

    Parameters
    --------------
    cons_nets
        List of considered nets

    Returns
    --------------
    acache
        A-Cache
    """
    # Optimized version that pre-allocates dictionary and avoids repeated lookups
    ret = {}
    for index, el in enumerate(cons_nets):
        for lab in el[0].lvis_labels:
            if lab not in ret:
                ret[lab] = []
            ret[lab].append(index)

    return ret


def get_alres(al):
    """
    Gets a description of the alignment for the border agreement

    Parameters
    --------------
    al
        Alignment

    Returns
    --------------
    alres
        Description of the alignment
    """
    if al is not None:
        # Use a more efficient approach
        ret = {}
        for move in al["alignment"]:
            model_move = move[1][0]
            if model_move is not None and model_move != ">>":
                if model_move not in ret:
                    ret[model_move] = []

                log_move = move[1][1]
                ret[model_move].append(0 if log_move is not None and log_move != ">>" else 1)

        return ret
    return None


def order_nodes_second_round(to_visit, G0):
    """
    Orders the second round of nodes to visit to reconstruct the alignment

    Optimized version with improved algorithm
    """
    edges_cache = {}
    # Pre-calculate all edges for faster lookups
    for i in range(len(to_visit)):
        for j in range(i + 1, len(to_visit)):
            node_i, node_j = to_visit[i], to_visit[j]
            if node_i != node_j:
                # Cache edge checks to avoid repeated graph lookups
                key_ij = (node_j, node_i)
                key_ji = (node_i, node_j)

                if key_ij not in edges_cache:
                    edges_cache[key_ij] = any(e[0] == node_j and e[1] == node_i for e in G0.edges)

                if key_ji not in edges_cache:
                    edges_cache[key_ji] = any(e[0] == node_i and e[1] == node_j for e in G0.edges)

    # Now use the cache for a more efficient sorting algorithm
    swapped = True
    while swapped:
        swapped = False
        for i in range(len(to_visit) - 1):
            node_i, node_j = to_visit[i], to_visit[i + 1]
            if node_i != node_j:
                key_ij = (node_j, node_i)
                key_ji = (node_i, node_j)

                if edges_cache.get(key_ij, False) and not edges_cache.get(key_ji, False):
                    to_visit[i], to_visit[i + 1] = to_visit[i + 1], to_visit[i]
                    swapped = True

    return to_visit


def recompose_alignment(cons_nets, cons_nets_result):
    """
    Alignment recomposition

    Optimized version with more efficient graph operations
    """
    # Create graph of valid nodes
    G0 = nx_utils.DiGraph()
    valid_nodes = [i for i in range(len(cons_nets_result)) if cons_nets_result[i] is not None]

    # Add nodes in one batch
    G0.add_nodes_from(valid_nodes)

    # Efficiently add edges
    edges_to_add = []
    for i in valid_nodes:
        for j in valid_nodes:
            if i != j and cons_nets_result[i]["alignment"][-1][1] == cons_nets_result[j]["alignment"][0][1]:
                edges_to_add.append((i, j))

    G0.add_edges_from(edges_to_add)

    # Find starting nodes (nodes with initial markings)
    to_visit = [i for i in range(len(cons_nets)) if len(list(cons_nets[i][1])) > 0]
    visited = set()
    overall_ali = []
    count = 0

    # Process the first round of nodes
    while to_visit:
        curr = to_visit.pop(0)
        output_edges = [e for e in G0.edges if e[0] == curr]
        to_visit.extend(e[1] for e in output_edges)

        sind = 1 if count > 0 else 0

        if cons_nets_result[curr] is not None:
            overall_ali.extend(cons_nets_result[curr]["alignment"][sind:])

        visited.add(curr)
        count += 1

    # Process remaining nodes
    all_available = [i for i in range(len(cons_nets_result)) if cons_nets_result[i] is not None]
    to_visit = [x for x in all_available if x not in visited]
    to_visit = order_nodes_second_round(to_visit, G0)

    added = set()
    while to_visit:
        curr = to_visit.pop(0)
        if curr not in visited:
            output_edges = [e for e in G0.edges if e[0] == curr]
            to_visit.extend(e[1] for e in output_edges)

            sind = 1 if count > 0 else 0

            if cons_nets_result[curr] is not None:
                for y in cons_nets_result[curr]["alignment"][sind:]:
                    if y not in added:
                        overall_ali.append(y)
                        added.add(y)

            visited.add(curr)
        count += 1

    return overall_ali


def apply_trace(trace, list_nets, parameters=None):
    """
    Align a trace against a decomposition

    Optimized version with improved algorithms
    """
    if parameters is None:
        parameters = {}

    max_align_time_trace = exec_utils.get_param_value(
        Parameters.PARAM_MAX_ALIGN_TIME_TRACE, parameters, sys.maxsize
    )
    threshold_border_agreement = exec_utils.get_param_value(
        Parameters.PARAM_THRESHOLD_BORDER_AGREEMENT, parameters, 100000000
    )
    activity_key = exec_utils.get_param_value(
        Parameters.ACTIVITY_KEY, parameters, DEFAULT_NAME_KEY
    )
    icache = exec_utils.get_param_value(Parameters.ICACHE, parameters, dict())
    mcache = exec_utils.get_param_value(Parameters.MCACHE, parameters, dict())

    # Make a shallow copy to avoid modifying the original
    cons_nets = list(list_nets)
    acache = get_acache(cons_nets)

    # Pre-allocate arrays
    cons_nets_result = []
    cons_nets_alres = []
    cons_nets_costs = []
    max_val_alres = 0
    start_time = time.time()

    # Extract activities from trace once to avoid repeated operations
    trace_activities = {x[activity_key] for x in trace}

    i = 0
    while i < len(cons_nets):
        this_time = time.time()
        if this_time - start_time > max_align_time_trace:
            # Time limit exceeded
            return None

        net, im, fm = cons_nets[i]

        # Find intersection of trace activities and net labels more efficiently
        relevant_activities = trace_activities.intersection(net.lvis_labels)

        # Create projection more efficiently
        proj = Trace([x for x in trace if x[activity_key] in relevant_activities])

        if proj:
            # Use tuple for immutable key
            acti = tuple(x[activity_key] for x in proj)
            tup = (cons_nets[i], acti)

            if tup not in icache:
                al, cf = align(proj, net, im, fm, parameters=parameters)
                alres = get_alres(al)
                icache[tup] = (al, cf, alres)

            al, cf, alres = icache[tup]
            cons_nets_result.append(al)
            cons_nets_alres.append(alres)
            cons_nets_costs.append(cf)

            if this_time - start_time > max_align_time_trace:
                return None

            # Compute max_val_alres more efficiently
            if alres:
                current_max = max(max(y) for y in alres.values() if y)
                max_val_alres = max(max_val_alres, current_max)

            border_disagreements = 0
            if max_val_alres > 0:
                comp_to_merge = set()

                # Only process relevant activities
                for act in relevant_activities:
                    for ind in acache.get(act, []):
                        if ind >= i:
                            break

                        if (not cons_nets_alres[ind] or not cons_nets_alres[i] or
                                act not in cons_nets_alres[ind] or act not in cons_nets_alres[i] or
                                cons_nets_alres[ind][act] != cons_nets_alres[i][act]):
                            # Add all components to merge
                            comp_to_merge.update(acache.get(act, []))

                if comp_to_merge:
                    comp_to_merge = sorted(list(comp_to_merge), reverse=True)
                    border_disagreements += len(comp_to_merge)

                    # Check threshold early
                    if border_disagreements > threshold_border_agreement:
                        return None

                    # Use frozenset for immutable dictionary key
                    comp_to_merge_ids = frozenset(cons_nets[j][0].t_tuple for j in comp_to_merge)

                    if comp_to_merge_ids not in mcache:
                        mcache[comp_to_merge_ids] = decomp_utils.merge_sublist_nets(
                            [cons_nets[zz] for zz in comp_to_merge]
                        )

                    new_comp = mcache[comp_to_merge_ids]
                    cons_nets.append(new_comp)

                    # Remove components more efficiently
                    for z in sorted(comp_to_merge, reverse=True):
                        if z < i:
                            i -= 1
                        if z <= i:
                            cons_nets_result.pop(z)
                            cons_nets_alres.pop(z)
                            cons_nets_costs.pop(z)
                        cons_nets.pop(z)

                    # Recalculate the activity cache
                    acache = get_acache(cons_nets)
                    continue
        else:
            cons_nets_result.append(None)
            cons_nets_alres.append(None)
            cons_nets_costs.append(None)

        i += 1

    if time.time() - start_time > max_align_time_trace:
        return None

    # Recompose the alignment
    alignment = recompose_alignment(cons_nets, cons_nets_result)

    # Build cost dictionary more efficiently
    overall_cost_dict = {}
    for cf in filter(None, cons_nets_costs):
        overall_cost_dict.update(cf)

    # Calculate total cost
    cost = sum(overall_cost_dict[el] for el in alignment)

    # Extract alignment
    alignment = [x[1] for x in alignment]

    if time.time() - start_time > max_align_time_trace:
        return None

    # Build result with fitness if needed
    res = {"cost": cost, "alignment": alignment}

    best_worst_cost = exec_utils.get_param_value(
        Parameters.BEST_WORST_COST, parameters, None
    )

    if best_worst_cost is not None and trace:
        cost1 = cost // utils.STD_MODEL_LOG_MOVE_COST
        fitness = 1.0 - cost1 / (best_worst_cost + len(trace))
        res["fitness"] = fitness
        res["bwc"] = (best_worst_cost + len(trace)) * utils.STD_MODEL_LOG_MOVE_COST

    return res


def align(trace, petri_net, initial_marking, final_marking, parameters=None):
    """
    Perform alignment using state_equation_a_star
    """
    if parameters is None:
        parameters = {}

    new_parameters = copy(parameters)
    new_parameters[
        state_equation_a_star.Parameters.RETURN_SYNC_COST_FUNCTION
    ] = True
    new_parameters[
        state_equation_a_star.Parameters.PARAM_ALIGNMENT_RESULT_IS_SYNC_PROD_AWARE
    ] = True

    aligned_trace, cost_function = state_equation_a_star.apply(
        trace,
        petri_net,
        initial_marking,
        final_marking,
        parameters=new_parameters,
    )

    # Create cost function dictionary more efficiently
    cf = {((x.name[0], x.name[1]), (x.label[0], x.label[1])): cost_function[x]
          for x in cost_function}

    return aligned_trace, cf