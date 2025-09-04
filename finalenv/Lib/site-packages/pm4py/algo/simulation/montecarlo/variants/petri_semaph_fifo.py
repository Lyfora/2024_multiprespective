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
from pm4py.statistics.traces.generic.log import case_arrival
from pm4py.algo.simulation.montecarlo.utils import replay
from pm4py.objects.petri_net.semantics import enabled_transitions, weak_execute
from pm4py.objects.log.obj import Trace, Event
from pm4py.util import xes_constants, exec_utils, constants
from pm4py.objects.stochastic_petri import utils as stochastic_utils
from pm4py.util.dt_parsing.variants import strpfromiso
from pm4py.objects.petri_net.obj import PetriNet, Marking

from intervaltree import Interval, IntervalTree
from statistics import median
import datetime
import heapq
from enum import Enum


# -- Retained original Parameters class --
class Parameters(Enum):
    ACTIVITY_KEY = constants.PARAMETER_CONSTANT_ACTIVITY_KEY
    TIMESTAMP_KEY = constants.PARAMETER_CONSTANT_TIMESTAMP_KEY
    TOKEN_REPLAY_VARIANT = "token_replay_variant"
    PARAM_NUM_SIMULATIONS = "num_simulations"
    PARAM_FORCE_DISTRIBUTION = "force_distribution"
    PARAM_ENABLE_DIAGNOSTICS = "enable_diagnostics"
    PARAM_DIAGN_INTERVAL = "diagn_interval"
    PARAM_CASE_ARRIVAL_RATIO = "case_arrival_ratio"
    PARAM_PROVIDED_SMAP = "provided_stochastic_map"
    PARAM_MAP_RESOURCES_PER_PLACE = "map_resources_per_place"
    PARAM_DEFAULT_NUM_RESOURCES_PER_PLACE = "default_num_resources_per_place"
    PARAM_SMALL_SCALE_FACTOR = "small_scale_factor"
    PARAM_MAX_THREAD_EXECUTION_TIME = "max_thread_exec_time"


# -- Simulation Case Generator (replacing threads) --
def simulate_case(case_id, net, im, fm, smap, start_time,
                  places_interval_trees, transitions_interval_trees,
                  cases_ex_time, list_cases, small_scale_factor):
    """
    Generator that simulates a single case.
    Instead of sleeping, it yields its current virtual (simulation) time.
    """
    current_time = start_time
    # Use a copy of the initial marking if necessary
    current_marking = im.copy()
    acquired_places = set()

    source = list(im)[0]
    sink = list(fm)[0]
    # Use a simple resource counter instead of threading.Semaphore.
    if hasattr(source, "sem_value"):
        if source.sem_value > 0:
            source.sem_value -= 1
    else:
        source.sem_value = 0
    source.assigned_time.append(current_time)
    first_event = None
    last_event = None

    while not (fm <= current_marking) and (len(enabled_transitions(net, current_marking)) > 0):
        et = list(enabled_transitions(net, current_marking))
        ct = stochastic_utils.pick_transition(et, smap)

        simulated_execution_plus_waiting_time = smap[ct].get_value() if ct in smap else 0.0
        while simulated_execution_plus_waiting_time < 0:
            simulated_execution_plus_waiting_time = smap[ct].get_value() if ct in smap else 0.0

        waiting_time = 0
        # For each output arc, attempt to "acquire" the target resource.
        for arc in ct.out_arcs:
            place = arc.target
            sem_value = getattr(place, "sem_value", 0)
            if sem_value > 0:
                place.sem_value -= 1
                acquired_places.add(place)
            else:
                if place.assigned_time:
                    waiting_interval = place.assigned_time.pop(0) - current_time
                    waiting_time = max(waiting_time, waiting_interval)

        if waiting_time > 0:
            transitions_interval_trees[ct].add(Interval(current_time, current_time + waiting_time))
        execution_time = max(simulated_execution_plus_waiting_time - waiting_time, 0)
        current_time += waiting_time + execution_time

        for arc in ct.out_arcs:
            place = arc.target
            place.assigned_time.append(current_time)
            place.assigned_time.sort()

        current_marking = weak_execute(ct, current_marking)

        if ct.label is not None:
            event = Event({
                xes_constants.DEFAULT_NAME_KEY: ct.label,
                xes_constants.DEFAULT_TIMESTAMP_KEY: strpfromiso.fix_naivety(
                    datetime.datetime.fromtimestamp(current_time)
                )
            })
            last_event = event
            if first_event is None:
                first_event = event
            list_cases[case_id].append(event)

        # For each input arc, record occupancy and "release" the resource.
        for arc in ct.in_arcs:
            place = arc.source
            if place.assigned_time:
                p_ex_time = place.assigned_time.pop(0)
                if current_time - p_ex_time > 0:
                    places_interval_trees[place].add(Interval(p_ex_time, current_time))
            place.assigned_time.append(current_time)
            place.assigned_time.sort()
            place.sem_value += 1

        # Yield the current simulation time to allow scheduler interleaving.
        yield current_time

    if first_event is not None and last_event is not None:
        exec_duration = (
                last_event[xes_constants.DEFAULT_TIMESTAMP_KEY].timestamp() -
                first_event[xes_constants.DEFAULT_TIMESTAMP_KEY].timestamp()
        )
        cases_ex_time.append(exec_duration)
    else:
        cases_ex_time.append(0)

    # Release any remaining acquired resources.
    for place in set(current_marking).union(acquired_places):
        place.sem_value += 1


# -- Scheduler to interleave simulation generators --
def run_simulation_generators(sim_gens):
    """
    Runs the simulation generators concurrently (in virtual time) using a priority queue.
    """
    queue = []
    for case_id, gen in sim_gens.items():
        try:
            next_time = next(gen)
            heapq.heappush(queue, (next_time, case_id, gen))
        except StopIteration:
            continue

    while queue:
        current_time, case_id, gen = heapq.heappop(queue)
        try:
            next_time = gen.send(current_time)
            heapq.heappush(queue, (next_time, case_id, gen))
        except StopIteration:
            continue


# -- Main apply function --
def apply(log, net, im, fm, parameters=None):
    """
    Performs a Monte Carlo simulation of an accepting Petri net using a generator-based scheduler.

    Parameters:
        log         : Event log.
        net         : Petri net.
        im          : Initial marking.
        fm          : Final marking.
        parameters  : Dictionary of simulation parameters.

    Returns:
        A tuple (simulated_log, simulation_result) where simulation_result is a dictionary:

            simulation_result = {
                "output_places_interval_trees": places_interval_trees,
                "output_transitions_interval_trees": transitions_interval_trees_named,
                "cases_ex_time": cases_ex_time,
                "median_cases_ex_time": median(cases_ex_time),
                "case_arrival_ratio": case_arrival_ratio,
                "total_cases_time": max_timestamp - min_timestamp,
            }
    """
    if parameters is None:
        parameters = {}

    timestamp_key = exec_utils.get_param_value(
        Parameters.TIMESTAMP_KEY, parameters, xes_constants.DEFAULT_TIMESTAMP_KEY
    )
    no_simulations = exec_utils.get_param_value(
        Parameters.PARAM_NUM_SIMULATIONS, parameters, 100
    )
    force_distribution = exec_utils.get_param_value(
        Parameters.PARAM_FORCE_DISTRIBUTION, parameters, None
    )
    case_arrival_ratio = exec_utils.get_param_value(
        Parameters.PARAM_CASE_ARRIVAL_RATIO, parameters, None
    )
    smap = exec_utils.get_param_value(
        Parameters.PARAM_PROVIDED_SMAP, parameters, None
    )
    resources_per_places = exec_utils.get_param_value(
        Parameters.PARAM_MAP_RESOURCES_PER_PLACE, parameters, None
    )
    default_num_resources_per_places = exec_utils.get_param_value(
        Parameters.PARAM_DEFAULT_NUM_RESOURCES_PER_PLACE, parameters, 1
    )
    small_scale_factor = exec_utils.get_param_value(
        Parameters.PARAM_SMALL_SCALE_FACTOR, parameters, 864000
    )
    # The PARAM_MAX_THREAD_EXECUTION_TIME parameter is not used in this single-threaded version.

    if case_arrival_ratio is None:
        case_arrival_ratio = case_arrival.get_case_arrival_avg(log, parameters=parameters)

    places_interval_trees = {}
    transitions_interval_trees = {}
    cases_ex_time = []
    list_cases = {}

    # Initialize places: assign resource counters and an empty assigned_time list.
    for place in net.places:
        if resources_per_places is not None and place in resources_per_places:
            place.sem_value = resources_per_places[place]
        else:
            place.sem_value = default_num_resources_per_places
        place.assigned_time = []
        places_interval_trees[place] = IntervalTree()
    for trans in net.transitions:
        transitions_interval_trees[trans] = IntervalTree()

    if smap is None:
        if force_distribution is not None:
            smap = replay.get_map_from_log_and_net(
                log, net, im, fm,
                force_distribution=force_distribution,
                parameters=parameters
            )
        else:
            smap = replay.get_map_from_log_and_net(
                log, net, im, fm, parameters=parameters
            )

    start_time = 1000000  # Avoid using 0 for timestamp issues.
    sim_generators = {}
    for i in range(no_simulations):
        list_cases[i] = Trace()
        sim_generators[i] = simulate_case(
            i, net, im, fm, smap, start_time,
            places_interval_trees, transitions_interval_trees,
            cases_ex_time, list_cases, small_scale_factor
        )
        start_time += case_arrival_ratio

    run_simulation_generators(sim_generators)

    valid_ids = [i for i in range(no_simulations) if i in list_cases]
    valid_traces = [list_cases[i] for i in valid_ids]
    min_timestamp = valid_traces[0][0][timestamp_key].timestamp()
    max_timestamp = max(
        event[timestamp_key].timestamp() for trace in valid_traces for event in trace
    )

    transitions_interval_trees_named = {
        t.name: tree for t, tree in transitions_interval_trees.items()
    }

    simulated_log = log.__class__(valid_traces)
    simulation_result = {
        "output_places_interval_trees": places_interval_trees,
        "output_transitions_interval_trees": transitions_interval_trees_named,
        "cases_ex_time": cases_ex_time,
        "median_cases_ex_time": median(cases_ex_time),
        "case_arrival_ratio": case_arrival_ratio,
        "total_cases_time": max_timestamp - min_timestamp,
    }

    return simulated_log, simulation_result
