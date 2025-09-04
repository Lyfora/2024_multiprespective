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
__doc__ = """
"""

from typing import List, Optional, Tuple, Dict, Union, Generator, Set, Any

from pm4py.objects.log.obj import Trace, EventLog, EventStream
from pm4py.utils import __event_log_deprecation_warning
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.process_tree.obj import ProcessTree
from pm4py.utils import get_properties, pandas_utils, constants
from pm4py.util.pandas_utils import check_is_pandas_dataframe, check_pandas_dataframe_columns
from pm4py.util import labels_similarity as ls_util

import pandas as pd
import deprecation


@deprecation.deprecated(
    deprecated_in="2.3.0",
    removed_in="3.0.0",
    details="this method will be removed in a future release.",
)
def construct_synchronous_product_net(
    trace: Trace,
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
) -> Tuple[PetriNet, Marking, Marking]:
    """
    Constructs the synchronous product net between a trace and a Petri net process model.

    :param trace: A trace from an event log.
    :param petri_net: The Petri net process model.
    :param initial_marking: The initial marking of the Petri net.
    :param final_marking: The final marking of the Petri net.
    :return: A tuple containing the synchronous Petri net, the initial marking, and the final marking.
    :rtype: Tuple[PetriNet, Marking, Marking]

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('model.pnml')
        log = pm4py.read_xes('log.xes')
        sync_net, sync_im, sync_fm = pm4py.construct_synchronous_product_net(log[0], net, im, fm)
    """
    from pm4py.objects.petri_net.utils.petri_utils import construct_trace_net
    from pm4py.objects.petri_net.utils.synchronous_product import construct
    from pm4py.objects.petri_net.utils.align_utils import SKIP

    trace_net, trace_im, trace_fm = construct_trace_net(trace)
    sync_net, sync_im, sync_fm = construct(
        trace_net,
        trace_im,
        trace_fm,
        petri_net,
        initial_marking,
        final_marking,
        SKIP,
    )
    return sync_net, sync_im, sync_fm


def compute_emd(
    language1: Dict[List[str], float], language2: Dict[List[str], float]
) -> float:
    """
    Computes the Earth Mover Distance (EMD) between two stochastic languages. For example, one language may be extracted from a log, and the other from a process model.

    :param language1: The first stochastic language.
    :param language2: The second stochastic language.
    :return: The computed Earth Mover Distance.
    :rtype: float

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes('tests/input_data/running-example.xes')
        language_log = pm4py.get_stochastic_language(log)
        print(language_log)
        net, im, fm = pm4py.read_pnml('tests/input_data/running-example.pnml')
        language_model = pm4py.get_stochastic_language(net, im, fm)
        print(language_model)
        emd_distance = pm4py.compute_emd(language_log, language_model)
        print(emd_distance)
    """
    from pm4py.algo.evaluation.earth_mover_distance import (
        algorithm as earth_mover_distance,
    )

    return earth_mover_distance.apply(language1, language2)


def solve_marking_equation(
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
    cost_function: Dict[PetriNet.Transition, float] = None,
) -> float:
    """
    Solves the marking equation of a Petri net using an Integer Linear Programming (ILP) approach. An optional transition-based cost function can be provided to minimize the solution.

    :param petri_net: The Petri net.
    :param initial_marking: The initial marking of the Petri net.
    :param final_marking: The final marking of the Petri net.
    :param cost_function: (Optional) A dictionary mapping transitions to their associated costs. If not provided, a default cost of 1 is assigned to each transition.
    :return: The heuristic value obtained by solving the marking equation.
    :rtype: float

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('model.pnml')
        heuristic = pm4py.solve_marking_equation(net, im, fm)
    """
    from pm4py.algo.analysis.marking_equation import (
        algorithm as marking_equation,
    )

    if cost_function is None:
        cost_function = {t: 1 for t in petri_net.transitions}

    me = marking_equation.build(
        petri_net,
        initial_marking,
        final_marking,
        parameters={"costs": cost_function},
    )
    return marking_equation.get_h_value(me)


@deprecation.deprecated(
    deprecated_in="2.3.0",
    removed_in="3.0.0",
    details="this method will be removed in a future release.",
)
def solve_extended_marking_equation(
    trace: Trace,
    sync_net: PetriNet,
    sync_im: Marking,
    sync_fm: Marking,
    split_points: Optional[List[int]] = None,
) -> float:
    """
    Computes a heuristic value (an underestimation of the cost of an alignment) between a trace
    and a synchronous product net using the extended marking equation with the standard cost function.
    For example, synchronization moves have a cost of 0, invisible moves have a cost of 1,
    and other moves on the model or log have a cost of 10,000. This method provides optimal provisioning of the split points.

    :param trace: The trace to evaluate.
    :param sync_net: The synchronous product net.
    :param sync_im: The initial marking of the synchronous net.
    :param sync_fm: The final marking of the synchronous net.
    :param split_points: (Optional) The indices of the events in the trace to be used as split points. If not specified, the split points are identified automatically.
    :return: The heuristic value representing the cost underestimation.
    :rtype: float

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('model.pnml')
        log = pm4py.read_xes('log.xes')
        ext_mark_eq_heu = pm4py.solve_extended_marking_equation(log[0], net, im, fm)
    """
    from pm4py.algo.analysis.extended_marking_equation import (
        algorithm as extended_marking_equation,
    )

    parameters = {}
    if split_points is not None:
        parameters[
            extended_marking_equation.Variants.CLASSIC.value.Parameters.SPLIT_IDX
        ] = split_points
    me = extended_marking_equation.build(
        trace, sync_net, sync_im, sync_fm, parameters=parameters
    )
    return extended_marking_equation.get_h_value(me)


def check_soundness(
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
    print_diagnostics: bool = False,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Checks if a given Petri net is a sound Workflow net (WF-net).

    A Petri net is a WF-net if and only if:
        - It has a unique source place.
        - It has a unique end place.
        - Every element in the WF-net is on a path from the source to the sink place.

    A WF-net is sound if and only if:
        - It contains no live-locks.
        - It contains no deadlocks.
        - It is always possible to reach the final marking from any reachable marking.

    For a formal definition of a sound WF-net, refer to: http://www.padsweb.rwth-aachen.de/wvdaalst/publications/p628.pdf

    The returned tuple consists of:
        - A boolean indicating whether the Petri net is a sound WF-net.
        - A dictionary containing diagnostics collected while running WOFLAN, associating diagnostic names with their corresponding details.

    :param petri_net: The Petri net to check.
    :param initial_marking: The initial marking of the Petri net.
    :param final_marking: The final marking of the Petri net.
    :param print_diagnostics: If True, additional diagnostics will be printed during the execution of WOFLAN.
    :return: A tuple containing a boolean indicating soundness and a dictionary of diagnostics.
    :rtype: Tuple[bool, Dict[str, Any]]

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('model.pnml')
        is_sound = pm4py.check_soundness(net, im, fm)
    """
    from pm4py.algo.analysis.woflan import algorithm as woflan

    return woflan.apply(
        petri_net,
        initial_marking,
        final_marking,
        parameters={
            "return_asap_when_not_sound": True,
            "return_diagnostics": True,
            "print_diagnostics": print_diagnostics,
        },
    )


def cluster_log(
    log: Union[EventLog, EventStream, pd.DataFrame],
    sklearn_clusterer=None,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> Generator[EventLog, None, None]:
    """
    Applies clustering to the provided event log by extracting profiles for the log's traces and clustering them using a Scikit-Learn clusterer (default is K-Means with two clusters).

    :param log: The event log to cluster.
    :param sklearn_clusterer: (Optional) The Scikit-Learn clusterer to use. Default is KMeans with `n_clusters=2`, `random_state=0`, and `n_init="auto"`.
    :param activity_key: The key used to identify activities in the log.
    :param timestamp_key: The key used to identify timestamps in the log.
    :param case_id_key: The key used to identify case IDs in the log.
    :return: A generator that yields clustered event logs as pandas DataFrames.
    :rtype: Generator[pd.DataFrame, None, None]

    .. code-block:: python3

        import pm4py

        for clust_log in pm4py.cluster_log(df):
            print(clust_log)
    """
    __event_log_deprecation_warning(log)

    properties = get_properties(
        log,
        activity_key=activity_key,
        case_id_key=case_id_key,
        timestamp_key=timestamp_key,
    )
    if sklearn_clusterer is not None:
        properties["sklearn_clusterer"] = sklearn_clusterer

    from pm4py.algo.clustering.profiles import algorithm as clusterer

    return clusterer.apply(log, parameters=properties)


def insert_artificial_start_end(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    artificial_start=constants.DEFAULT_ARTIFICIAL_START_ACTIVITY,
    artificial_end=constants.DEFAULT_ARTIFICIAL_END_ACTIVITY,
) -> Union[EventLog, pd.DataFrame]:
    """
    Inserts artificial start and end activities into an event log or a Pandas DataFrame.

    :param log: The event log or Pandas DataFrame to modify.
    :param activity_key: The attribute key used for activities.
    :param timestamp_key: The attribute key used for timestamps.
    :param case_id_key: The attribute key used to identify cases.
    :param artificial_start: The symbol to use for the artificial start activity.
    :param artificial_end: The symbol to use for the artificial end activity.
    :return: The event log or Pandas DataFrame with artificial start and end activities inserted.
    :rtype: Union[EventLog, pd.DataFrame]

    .. code-block:: python3

        import pm4py

        dataframe = pm4py.insert_artificial_start_end(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
    """
    __event_log_deprecation_warning(log)

    properties = get_properties(
        log,
        activity_key=activity_key,
        case_id_key=case_id_key,
        timestamp_key=timestamp_key,
    )
    properties[constants.PARAM_ARTIFICIAL_START_ACTIVITY] = artificial_start
    properties[constants.PARAM_ARTIFICIAL_END_ACTIVITY] = artificial_end

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            case_id_key=case_id_key,
            timestamp_key=timestamp_key,
        )
        from pm4py.objects.log.util import dataframe_utils

        return dataframe_utils.insert_artificial_start_end(
            log, parameters=properties
        )
    else:
        from pm4py.objects.log.util import artificial

        return artificial.insert_artificial_start_end(
            log, parameters=properties
        )


def insert_case_service_waiting_time(
    log: Union[EventLog, pd.DataFrame],
    service_time_column: str = "@@service_time",
    sojourn_time_column: str = "@@sojourn_time",
    waiting_time_column: str = "@@waiting_time",
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    start_timestamp_key: str = "time:timestamp",
) -> pd.DataFrame:
    """
    Inserts service time, waiting time, and sojourn time information for each case into a Pandas DataFrame.

    :param log: The event log or Pandas DataFrame to modify.
    :param service_time_column: The name of the column to store service times.
    :param sojourn_time_column: The name of the column to store sojourn times.
    :param waiting_time_column: The name of the column to store waiting times.
    :param activity_key: The attribute key used for activities.
    :param timestamp_key: The attribute key used for timestamps.
    :param case_id_key: The attribute key used to identify cases.
    :param start_timestamp_key: The attribute key used for the start timestamp of cases.
    :return: A Pandas DataFrame with the inserted service, waiting, and sojourn time columns.
    :rtype: pd.DataFrame

    .. code-block:: python3

        import pm4py

        dataframe = pm4py.insert_case_service_waiting_time(
            dataframe,
            activity_key='concept:name',
            timestamp_key='time:timestamp',
            case_id_key='case:concept:name',
            start_timestamp_key='time:timestamp'
        )
    """
    __event_log_deprecation_warning(log)

    properties = get_properties(
        log,
        activity_key=activity_key,
        case_id_key=case_id_key,
        timestamp_key=timestamp_key,
    )

    from pm4py.objects.conversion.log import converter as log_converter

    log_df = log_converter.apply(
        log,
        variant=log_converter.Variants.TO_DATA_FRAME,
        parameters=properties,
    )

    return pandas_utils.insert_case_service_waiting_time(
        log_df,
        case_id_column=case_id_key,
        timestamp_column=timestamp_key,
        start_timestamp_column=start_timestamp_key,
        service_time_column=service_time_column,
        waiting_time_column=waiting_time_column,
        sojourn_time_column=sojourn_time_column,
    )


def insert_case_arrival_finish_rate(
    log: Union[EventLog, pd.DataFrame],
    arrival_rate_column: str = "@@arrival_rate",
    finish_rate_column: str = "@@finish_rate",
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    start_timestamp_key: str = "time:timestamp",
) -> pd.DataFrame:
    """
    Inserts arrival and finish rate information for each case into a Pandas DataFrame.

    The arrival rate is computed as the time difference between the start of the current case and the start of the previous case to start.
    The finish rate is computed as the time difference between the end of the current case and the end of the next case to finish.

    :param log: The event log or Pandas DataFrame to modify.
    :param arrival_rate_column: The name of the column to store arrival rates.
    :param finish_rate_column: The name of the column to store finish rates.
    :param activity_key: The attribute key used for activities.
    :param timestamp_key: The attribute key used for timestamps.
    :param case_id_key: The attribute key used to identify cases.
    :param start_timestamp_key: The attribute key used for the start timestamp of cases.
    :return: A Pandas DataFrame with the inserted arrival and finish rate columns.
    :rtype: pd.DataFrame

    .. code-block:: python3

        import pm4py

        dataframe = pm4py.insert_case_arrival_finish_rate(
            dataframe,
            activity_key='concept:name',
            timestamp_key='time:timestamp',
            case_id_key='case:concept:name',
            start_timestamp_key='time:timestamp'
        )
    """
    __event_log_deprecation_warning(log)

    properties = get_properties(
        log,
        activity_key=activity_key,
        case_id_key=case_id_key,
        timestamp_key=timestamp_key,
    )

    from pm4py.objects.conversion.log import converter as log_converter

    log_df = log_converter.apply(
        log,
        variant=log_converter.Variants.TO_DATA_FRAME,
        parameters=properties,
    )

    return pandas_utils.insert_case_arrival_finish_rate(
        log_df,
        case_id_column=case_id_key,
        timestamp_column=timestamp_key,
        start_timestamp_column=start_timestamp_key,
        arrival_rate_column=arrival_rate_column,
        finish_rate_column=finish_rate_column,
    )


def check_is_workflow_net(net: PetriNet) -> bool:
    """
    Checks if the input Petri net satisfies the WF-net (Workflow net) conditions:
    1. It has a unique source place.
    2. It has a unique sink place.
    3. Every node is on a path from the source to the sink.

    :param net: The Petri net to check.
    :return: True if the Petri net is a WF-net, False otherwise.
    :rtype: bool

    .. code-block:: python3

        import pm4py

        net = pm4py.read_pnml('model.pnml')
        is_wfnet = pm4py.check_is_workflow_net(net)
    """
    from pm4py.algo.analysis.workflow_net import algorithm

    return algorithm.apply(net)


def maximal_decomposition(
    net: PetriNet, im: Marking, fm: Marking
) -> List[Tuple[PetriNet, Marking, Marking]]:
    """
    Calculates the maximal decomposition of an accepting Petri net into its maximal components.

    :param net: The Petri net to decompose.
    :param im: The initial marking of the Petri net.
    :param fm: The final marking of the Petri net.
    :return: A list of tuples, each containing a subnet Petri net, its initial marking, and its final marking.
    :rtype: List[Tuple[PetriNet, Marking, Marking]]

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('model.pnml')
        list_nets = pm4py.maximal_decomposition(net, im, fm)
        for subnet, subim, subfm in list_nets:
            pm4py.view_petri_net(subnet, subim, subfm, format='svg')
    """
    from pm4py.objects.petri_net.utils.decomposition import decompose

    return decompose(net, im, fm)


def simplicity_petri_net(
    net: PetriNet,
    im: Marking,
    fm: Marking,
    variant: Optional[str] = "arc_degree",
) -> float:
    """
    Computes the simplicity metric for a given Petri net model.

    Three available approaches are supported:
    - **Arc Degree Simplicity**: Described in the paper "ProDiGen: Mining complete, precise and minimal structure process models with a genetic algorithm." by Vázquez-Barreiros, Borja, Manuel Mucientes, and Manuel Lama. Information Sciences, 294 (2015): 315-333.
    - **Extended Cardoso Metric**: Described in the paper "Complexity Metrics for Workflow Nets" by Lassen, Kristian Bisgaard, and Wil MP van der Aalst.
    - **Extended Cyclomatic Metric**: Also described in the paper "Complexity Metrics for Workflow Nets" by Lassen, Kristian Bisgaard, and Wil MP van der Aalst.

    :param net: The Petri net for which to compute simplicity.
    :param im: The initial marking of the Petri net.
    :param fm: The final marking of the Petri net.
    :param variant: The simplicity metric variant to use ('arc_degree', 'extended_cardoso', 'extended_cyclomatic').
    :return: The computed simplicity value.
    :rtype: float

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        simplicity = pm4py.simplicity_petri_net(net, im, fm, variant='arc_degree')
    """
    if variant == "arc_degree":
        from pm4py.algo.evaluation.simplicity.variants import arc_degree

        return arc_degree.apply(net)
    elif variant == "extended_cardoso":
        from pm4py.algo.evaluation.simplicity.variants import extended_cardoso

        return extended_cardoso.apply(net)
    elif variant == "extended_cyclomatic":
        from pm4py.algo.evaluation.simplicity.variants import (
            extended_cyclomatic,
        )

        return extended_cyclomatic.apply(net, im)


def generate_marking(
    net: PetriNet,
    place_or_dct_places: Union[
        str, PetriNet.Place, Dict[str, int], Dict[PetriNet.Place, int]
    ],
) -> Marking:
    """
    Generates a marking for a given Petri net based on specified places and token counts.

    :param net: The Petri net for which to generate the marking.
    :param place_or_dct_places: Specifies the places and their token counts for the marking. It can be:
        - A single `PetriNet.Place` object, which will have one token.
        - A string representing the name of a place, which will have one token.
        - A dictionary mapping `PetriNet.Place` objects to their respective number of tokens.
        - A dictionary mapping place names (strings) to their respective number of tokens.
    :return: The generated Marking object.
    :rtype: Marking

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('model.pnml')
        marking = pm4py.generate_marking(net, {'source': 2})
    """
    dct_places = {x.name: x for x in net.places}
    if isinstance(place_or_dct_places, PetriNet.Place):
        # A single Place object is specified for the marking
        return Marking({place_or_dct_places: 1})
    elif isinstance(place_or_dct_places, str):
        # The name of a place is specified for the marking
        return Marking({dct_places[place_or_dct_places]: 1})
    elif isinstance(place_or_dct_places, dict):
        dct_keys = list(place_or_dct_places)
        if dct_keys:
            if isinstance(dct_keys[0], PetriNet.Place):
                # A dictionary mapping Place objects to token counts is
                # specified
                return Marking(place_or_dct_places)
            elif isinstance(dct_keys[0], str):
                # A dictionary mapping place names to token counts is specified
                return Marking(
                    {dct_places[x]: y for x, y in place_or_dct_places.items()}
                )


def reduce_petri_net_invisibles(net: PetriNet) -> PetriNet:
    """
    Reduces the number of invisible transitions in the provided Petri net.

    :param net: The Petri net to be reduced.
    :return: The reduced Petri net with fewer invisible transitions.
    :rtype: PetriNet

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('model.pnml')
        net = pm4py.reduce_petri_net_invisibles(net)
    """
    from pm4py.objects.petri_net.utils import reduction

    return reduction.apply_simple_reduction(net)


def reduce_petri_net_implicit_places(
    net: PetriNet, im: Marking, fm: Marking
) -> Tuple[PetriNet, Marking, Marking]:
    """
    Reduces the number of implicit places in the provided Petri net.

    :param net: The Petri net to be reduced.
    :param im: The initial marking of the Petri net.
    :param fm: The final marking of the Petri net.
    :return: A tuple containing the reduced Petri net, its initial marking, and its final marking.
    :rtype: Tuple[PetriNet, Marking, Marking]

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('model.pnml')
        net, im, fm = pm4py.reduce_petri_net_implicit_places(net, im, fm)
    """
    from pm4py.objects.petri_net.utils import murata

    return murata.apply_reduction(net, im, fm)


def get_enabled_transitions(
    net: PetriNet, marking: Marking
) -> Set[PetriNet.Transition]:
    """
    Retrieves the set of transitions that are enabled in a given marking of a Petri net.

    :param net: The Petri net.
    :param marking: The current marking of the Petri net.
    :return: A set of transitions that are enabled in the provided marking.
    :rtype: Set[PetriNet.Transition]

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('tests/input_data/running-example.pnml')
        # Gets the transitions enabled in the initial marking
        enabled_transitions = pm4py.get_enabled_transitions(net, im)
    """
    from pm4py.objects.petri_net import semantics

    return semantics.enabled_transitions(net, marking)


def get_activity_labels(*args) -> List[str]:
    """
    Gets the activity labels from the specified event log / process model.

    Returns
    ---------------
    activities
        Activity labels
    """
    import pm4py

    if isinstance(args[0], EventLog):
        labels = set(y["concept:name"] for x in args[0] for y in x)
    elif isinstance(args[0], pd.DataFrame):
        labels = set(args[0]["concept:name"].unique())
    else:
        net, im, fm = pm4py.convert_to_petri_net(*args)
        labels = {x.label for x in net.transitions if x.label is not None}
    return sorted(list(labels))


def replace_activity_labels(string_dictio, *args):
    """
    Replace the activity labels in the specified process model.
    The first argument is the dictionary, i.e., {"pay": "pay compensation", "reject": "reject request"}
    The rest is the specification of the process model
    """
    from pm4py.objects.powl.obj import POWL
    from pm4py.objects.bpmn.obj import BPMN

    if isinstance(args[0], POWL):
        from pm4py.objects.powl.utils import label_replacing
        return label_replacing.apply(args[0], string_dictio)
    elif isinstance(args[0], ProcessTree):
        from pm4py.objects.process_tree.utils import label_replacing
        return label_replacing.apply(args[0], string_dictio)
    elif isinstance(args[0], PetriNet):
        from pm4py.objects.petri_net.utils import label_replacing
        return label_replacing.apply(args[0], args[1], args[2], string_dictio)
    elif isinstance(args[0], BPMN):
        from pm4py.objects.bpmn.util import label_replacing
        return label_replacing.apply(args[0], string_dictio)
    else:
        raise Exception("unsupported.")


def __extract_models(*args) -> List[Any]:
    if len(args) < 2:
        raise Exception("Insufficient arguments provided.")

    counter = 0
    lst_models = []

    import pm4py
    for i in range(2):
        if type(args[counter]) is PetriNet:
            net, im, fm = args[counter:counter + 3]
            lst_models.append([net, im, fm])
            counter += 3
        elif isinstance(args[counter], dict):
            dfg, sa, ea = args[counter:counter + 3]
            net, im, fm = pm4py.convert_to_petri_net(dfg, sa, ea)
            lst_models.append([net, im, fm])
            counter += 3
        else:
            obj = args[counter]
            lst_models.append([obj])
            counter += 1

    return lst_models


def behavioral_similarity(*args) -> float:
    """
    Computes the behavioral similarity (footprints-based) between two process models.

    Examples:
    * pm4py.behavioral_similarity(petri_net, im, fm, process_tree)
    * pm4py.behavioral_similarity(bpmn1, bpmn2)
    * pm4py.behavioral_similarity(process_tree, powl)

    Returns
    --------------
    similarity
        Footprints-based behavioral similarity
    """
    lst_models = __extract_models(*args)

    import pm4py
    footprints = []
    for i in range(len(lst_models)):
        x = lst_models[i]
        if not (isinstance(x[0], PetriNet) or isinstance(x[0], ProcessTree)):
            x = [pm4py.convert_to_powl(*x)]

        footprints.append(pm4py.discover_footprints(*x))

    footprints1, footprints2 = footprints

    sequence_union = footprints1["sequence"].union(footprints2["sequence"])
    sequence_intersection = footprints1["sequence"].intersection(footprints2["sequence"])

    parallel_union = footprints1["parallel"].union(footprints2["parallel"])
    parallel_intersection = footprints1["parallel"].intersection(footprints2["parallel"])

    denominator = len(sequence_union) + len(parallel_union)

    if denominator == 0:
        return 0
    else:
        return (len(sequence_intersection) + len(parallel_intersection)) / denominator


def structural_similarity(*args) -> float:
    """
    Computes the structural similarity between two semi-block-structured process models,
    following an approach similar to:

    Dijkman, Remco, et al. "Similarity of business process models: Metrics and evaluation."
    Information Systems 36.2 (2011): 498-516.

    Examples:
    * pm4py.structural_similarity(petri_net, im, fm, process_tree)
    * pm4py.structural_similarity(bpmn1, bpmn2)
    * pm4py.structural_similarity(process_tree, powl)

    Returns
    --------------
    similarity
        Structural similarity
    """
    lst_models = __extract_models(*args)

    import pm4py
    i = 0
    while i < len(lst_models):
        lst_models[i] = pm4py.convert_to_process_tree(pm4py.convert_to_powl(*lst_models[i]))
        i = i + 1

    from pm4py.objects.process_tree.utils import struct_similarity
    return struct_similarity.structural_similarity(lst_models[0], lst_models[1])


def embeddings_similarity(*args) -> float:
    """
    Computes the embeddings similarity between two process models,
    following the approach described in:

    Colonna, Juan G., et al. "Process mining embeddings: Learning vector representations for Petri nets."
    Intelligent Systems with Applications 23 (2024): 200423.

    Examples:
    * pm4py.embeddings_similarity(petri_net, im, fm, process_tree)
    * pm4py.embeddings_similarity(bpmn1, bpmn2)
    * pm4py.embeddings_similarity(process_tree, powl)

    Returns
    --------------
    similarity
        Structural similarity
    """
    lst_models = __extract_models(*args)

    import pm4py
    i = 0
    while i < len(lst_models):
        lst_models[i] = pm4py.convert_to_petri_net(*lst_models[i])
        i = i + 1

    from pm4py.objects.petri_net.utils import embeddings_similarity
    return embeddings_similarity.apply(lst_models[0][0], lst_models[1][0])


def label_sets_similarity(*args, threshold=0.75) -> float:
    """
    Computes the label sets similarity between two process models.

    Examples:
    * pm4py.labels_similarity(petri_net, im, fm, process_tree)
    * pm4py.labels_similarity(bpmn1, bpmn2)
    * pm4py.labels_similarity(process_tree, powl)

    Returns
    --------------
    similarity
        Label sets similarity
    """
    lst_models = __extract_models(*args)
    labels = []
    i = 0
    while i < len(lst_models):
        labels.append(get_activity_labels(*lst_models[i]))
        i = i + 1

    return ls_util.label_sets_similarity(labels[0], labels[1], threshold=threshold)


def map_labels_from_second_model(*args, threshold=0.75):
    """
    Maps the labels from the second process model into the first.

    Example usages:
    * pm4py.map_labels_from_second_model(net, im, fm, process_tree)
    * pm4py.map_labels_from_second_model(process_tree, net, im, fm)
    * pm4py.map_labels_from_second_model(powl1, powl2)
    """
    lst_models = __extract_models(*args)
    labels = []
    i = 0
    while i < len(lst_models):
        labels.append(get_activity_labels(*lst_models[i]))
        i = i + 1

    label_mapping = ls_util.map_labels(labels[0], labels[1], threshold=threshold)
    return replace_activity_labels(label_mapping, *lst_models[0])
