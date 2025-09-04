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
The ``pm4py.conformance`` module contains the conformance checking algorithms implemented in ``pm4py``.
"""

from typing import List, Dict, Any, Union, Optional, Tuple, Set

from pm4py.objects.log.obj import EventLog, Trace, Event
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.process_tree.obj import ProcessTree
from pm4py.util import xes_constants, constants
from pm4py.utils import get_properties, __event_log_deprecation_warning
from pm4py.util.pandas_utils import (
    check_is_pandas_dataframe,
    check_pandas_dataframe_columns,
)
import pandas as pd
import deprecation


def conformance_diagnostics_token_based_replay(
    log: Union[EventLog, pd.DataFrame],
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    return_diagnostics_dataframe: bool = constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME,
    opt_parameters: Optional[Dict[Any, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Apply token-based replay for conformance checking analysis.
    This method returns the full token-based replay diagnostics.

    Token-based replay matches a trace against a Petri net model, starting from the initial marking, to discover which transitions are executed and in which places there are remaining or missing tokens for the given process instance. Token-based replay is useful for conformance checking: a trace fits the model if, during its execution, all transitions can be fired without the need to insert any missing tokens. If reaching the final marking is imposed, a trace fits if it reaches the final marking without any missing or remaining tokens.

    In PM4Py, the token replayer implementation can handle hidden transitions by calculating the shortest paths between places. It can be used with any Petri net model that has unique visible transitions and hidden transitions. When a visible transition needs to be fired and not all places in its preset have the correct number of tokens, the current marking is checked to see if any hidden transitions can be fired to enable the visible transition. The hidden transitions are then fired, reaching a marking that permits the firing of the visible transition.

    The approach is described in:
    Berti, Alessandro, and Wil MP van der Aalst. "Reviving Token-based Replay: Increasing Speed While Improving Diagnostics." ATAED@ Petri Nets/ACSD. 2019.

    The output of the token-based replay, stored in the variable `replayed_traces`, contains for each trace in the log:

    - **trace_is_fit**: Boolean value indicating whether the trace conforms to the model.
    - **activated_transitions**: List of transitions activated in the model by the token-based replay.
    - **reached_marking**: Marking reached at the end of the replay.
    - **missing_tokens**: Number of missing tokens.
    - **consumed_tokens**: Number of consumed tokens.
    - **remaining_tokens**: Number of remaining tokens.
    - **produced_tokens**: Number of produced tokens.

    :param log: Event log.
    :param petri_net: Petri net.
    :param initial_marking: Initial marking.
    :param final_marking: Final marking.
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :param return_diagnostics_dataframe: If possible, returns a dataframe with the diagnostics instead of the usual output (default is `constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME`).
    :param opt_parameters: Optional parameters for the token-based replay, including:
        * **reach_mark_through_hidden**: Boolean to decide if the final marking should be reached through hidden transitions.
        * **stop_immediately_unfit**: Boolean to decide if the replay should stop immediately when non-conformance is detected.
        * **walk_through_hidden_trans**: Boolean to decide if the replay should walk through hidden transitions to enable visible transitions.
        * **places_shortest_path_by_hidden**: Shortest paths between places using hidden transitions.
        * **is_reduction**: Indicates if the token-based replay is called in a reduction attempt.
        * **thread_maximum_ex_time**: Maximum allowed execution time for alignment threads.
        * **cleaning_token_flood**: Decides if token flood cleaning should be performed.
        * **disable_variants**: Disable variants grouping.
        * **return_object_names**: Decide whether to return names instead of object pointers.
    :return: A list of dictionaries containing diagnostics for each trace.
    :rtype: ``List[Dict[str, Any]]``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        tbr_diagnostics = pm4py.conformance_diagnostics_token_based_replay(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    if return_diagnostics_dataframe:
        from pm4py.convert import convert_to_event_log

        log = convert_to_event_log(log, case_id_key=case_id_key)
        case_id_key = None

    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    if opt_parameters is None:
        opt_parameters = {}

    for k, v in opt_parameters.items():
        properties[k] = v

    from pm4py.algo.conformance.tokenreplay import algorithm as token_replay

    result = token_replay.apply(
        log, petri_net, initial_marking, final_marking, parameters=properties
    )

    if return_diagnostics_dataframe:
        return token_replay.get_diagnostics_dataframe(
            log, result, parameters=properties
        )

    return result


def conformance_diagnostics_alignments(
    log: Union[EventLog, pd.DataFrame],
    *args,
    multi_processing: bool = constants.ENABLE_MULTIPROCESSING_DEFAULT,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    variant_str: Optional[str] = None,
    return_diagnostics_dataframe: bool = constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME,
    **kwargs,
) -> List[Dict[str, Any]]:
    """
    Apply the alignments algorithm between a log and a process model.
    This method returns the full alignment diagnostics.

    Alignment-based replay aims to find one of the best alignments between the trace and the model. For each trace, the output of an alignment is a list of pairs where the first element is an event (from the trace) or ``»`` and the second element is a transition (from the model) or ``»``. Each pair can be classified as follows:

    - **Sync move**: The event and transition labels correspond, advancing both the trace and the model simultaneously.
    - **Move on log**: The transition is ``»``, indicating a replay move in the trace that is not mirrored in the model. This move is unfit and signals a deviation.
    - **Move on model**: The event is ``»``, indicating a replay move in the model not mirrored in the trace. These can be further classified as:
        * **Moves on model involving hidden transitions**: Even if it's not a sync move, the move is fit.
        * **Moves on model not involving hidden transitions**: The move is unfit and signals a deviation.

    For each trace, a dictionary is associated containing, among other details:

    - **alignment**: The alignment pairs (sync moves, moves on log, moves on model).
    - **cost**: The cost of the alignment based on the provided cost function.
    - **fitness**: Equals 1 if the trace fits perfectly.

    :param log: Event log.
    :param args: Specifications of the process model.
    :param multi_processing: Boolean to enable multiprocessing (default is `constants.ENABLE_MULTIPROCESSING_DEFAULT`).
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :param variant_str: Variant specification (for Petri net alignments).
    :param return_diagnostics_dataframe: If possible, returns a dataframe with the diagnostics instead of the usual output (default is `constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME`).
    :return: A list of dictionaries containing diagnostics for each trace.
    :rtype: ``List[Dict[str, Any]]``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        alignments_diagnostics = pm4py.conformance_diagnostics_alignments(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    if return_diagnostics_dataframe:
        from pm4py.convert import convert_to_event_log

        log = convert_to_event_log(log, case_id_key=case_id_key)
        case_id_key = None

    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    if kwargs:
        for k, v in kwargs.items():
            properties[k] = v

    if len(args) == 3:
        if isinstance(args[0], PetriNet):
            # Petri net alignments
            from pm4py.algo.conformance.alignments.petri_net import (
                algorithm as alignments,
            )

            variant = alignments.DEFAULT_VARIANT
            if variant_str is not None:
                variant = variant_str
            if multi_processing:
                result = alignments.apply_multiprocessing(
                    log,
                    args[0],
                    args[1],
                    args[2],
                    parameters=properties,
                    variant=variant,
                )
            else:
                result = alignments.apply(
                    log,
                    args[0],
                    args[1],
                    args[2],
                    parameters=properties,
                    variant=variant,
                )

            if return_diagnostics_dataframe:
                return alignments.get_diagnostics_dataframe(
                    log, result, parameters=properties
                )

            return result
        elif isinstance(args[0], dict):
            # DFG alignments
            from pm4py.algo.conformance.alignments.dfg import (
                algorithm as dfg_alignment,
            )

            result = dfg_alignment.apply(
                log, args[0], args[1], args[2], parameters=properties
            )

            return result
    elif len(args) == 1:
        if isinstance(args[0], ProcessTree):
            # Process tree alignments
            from pm4py.algo.conformance.alignments.process_tree.variants import (
                search_graph_pt, )

            if multi_processing:
                result = search_graph_pt.apply_multiprocessing(
                    log, args[0], parameters=properties
                )
            else:
                result = search_graph_pt.apply(
                    log, args[0], parameters=properties
                )

            return result
        elif isinstance(args[0], (EventLog, pd.DataFrame)):
            # Edit distance alignments (log to log)
            from pm4py.algo.conformance.alignments.edit_distance import (
                algorithm as edit_distance_alignments,
            )

            result = edit_distance_alignments.apply(
                log, args[0], parameters=properties
            )

            return result
    # Try to convert to Petri net
    import pm4py
    from pm4py.algo.conformance.alignments.petri_net import (
        algorithm as alignments,
    )

    net, im, fm = pm4py.convert_to_petri_net(*args)
    if multi_processing:
        result = alignments.apply_multiprocessing(
            log, net, im, fm, parameters=properties
        )
    else:
        result = alignments.apply(log, net, im, fm, parameters=properties)

    if return_diagnostics_dataframe:
        return alignments.get_diagnostics_dataframe(
            log, result, parameters=properties
        )

    return result


def fitness_token_based_replay(
    log: Union[EventLog, pd.DataFrame],
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> Dict[str, float]:
    """
    Calculate the fitness using token-based replay.
    The fitness is calculated on a log-based level.
    The output dictionary contains the following keys:
    - **perc_fit_traces**: Percentage of fit traces (from 0.0 to 100.0).
    - **average_trace_fitness**: Average of the trace fitnesses (between 0.0 and 1.0).
    - **log_fitness**: Overall fitness of the log (between 0.0 and 1.0).
    - **percentage_of_fitting_traces**: Percentage of fit traces (from 0.0 to 100.0).

    Token-based replay matches a trace against a Petri net model, starting from the initial marking, to discover which transitions are executed and in which places there are remaining or missing tokens for the given process instance. Token-based replay is useful for conformance checking: a trace fits the model if, during its execution, all transitions can be fired without the need to insert any missing tokens. If reaching the final marking is imposed, a trace fits if it reaches the final marking without any missing or remaining tokens.

    In PM4Py, the token replayer implementation can handle hidden transitions by calculating the shortest paths between places. It can be used with any Petri net model that has unique visible transitions and hidden transitions. When a visible transition needs to be fired and not all places in its preset have the correct number of tokens, the current marking is checked to see if any hidden transitions can be fired to enable the visible transition. The hidden transitions are then fired, reaching a marking that permits the firing of the visible transition.

    The approach is described in:
    Berti, Alessandro, and Wil MP van der Aalst. "Reviving Token-based Replay: Increasing Speed While Improving Diagnostics." ATAED@ Petri Nets/ACSD. 2019.

    The calculation of replay fitness aims to assess how much of the behavior in the log is admitted by the process model. Two methods are proposed to calculate replay fitness, based on token-based replay and alignments respectively.

    For token-based replay, the percentage of traces that are completely fit is returned, along with a fitness value calculated as indicated in the referenced contribution.

    :param log: Event log.
    :param petri_net: Petri net.
    :param initial_marking: Initial marking.
    :param final_marking: Final marking.
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :return: A dictionary containing fitness metrics.
    :rtype: ``Dict[str, float]``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        fitness_tbr = pm4py.fitness_token_based_replay(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    from pm4py.algo.evaluation.replay_fitness import (
        algorithm as replay_fitness,
    )

    result = replay_fitness.apply(
        log,
        petri_net,
        initial_marking,
        final_marking,
        variant=replay_fitness.Variants.TOKEN_BASED,
        parameters=properties,
    )

    return result


def fitness_alignments(
    log: Union[EventLog, pd.DataFrame],
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
    multi_processing: bool = constants.ENABLE_MULTIPROCESSING_DEFAULT,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    variant_str: Optional[str] = None,
) -> Dict[str, float]:
    """
    Calculate the fitness using alignments.
    The output dictionary contains the following keys:
    - **average_trace_fitness**: Average of the trace fitnesses (between 0.0 and 1.0).
    - **log_fitness**: Overall fitness of the log (between 0.0 and 1.0).
    - **percentage_of_fitting_traces**: Percentage of fit traces (from 0.0 to 100.0).

    Alignment-based replay aims to find one of the best alignments between the trace and the model. For each trace, the output of an alignment is a list of pairs where the first element is an event (from the trace) or ``»`` and the second element is a transition (from the model) or ``»``. Each pair can be classified as follows:

    - **Sync move**: The event and transition labels correspond, advancing both the trace and the model simultaneously.
    - **Move on log**: The transition is ``»``, indicating a replay move in the trace that is not mirrored in the model. This move is unfit and signals a deviation.
    - **Move on model**: The event is ``»``, indicating a replay move in the model not mirrored in the trace. These can be further classified as:
        * **Moves on model involving hidden transitions**: Even if it's not a sync move, the move is fit.
        * **Moves on model not involving hidden transitions**: The move is unfit and signals a deviation.

    The calculation of replay fitness aims to assess how much of the behavior in the log is admitted by the process model. Two methods are proposed to calculate replay fitness, based on token-based replay and alignments respectively.

    For alignments, the percentage of traces that are completely fit is returned, along with a fitness value calculated as the average of the fitness values of the individual traces.

    :param log: Event log.
    :param petri_net: Petri net.
    :param initial_marking: Initial marking.
    :param final_marking: Final marking.
    :param multi_processing: Boolean to enable multiprocessing (default is `constants.ENABLE_MULTIPROCESSING_DEFAULT`).
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :param variant_str: Variant specification.
    :return: A dictionary containing fitness metrics.
    :rtype: ``Dict[str, float]``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        fitness_alignments = pm4py.fitness_alignments(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    from pm4py.algo.evaluation.replay_fitness import (
        algorithm as replay_fitness,
    )

    parameters = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    parameters["multiprocessing"] = multi_processing
    result = replay_fitness.apply(
        log,
        petri_net,
        initial_marking,
        final_marking,
        variant=replay_fitness.Variants.ALIGNMENT_BASED,
        align_variant=variant_str,
        parameters=parameters,
    )

    return result


def precision_token_based_replay(
    log: Union[EventLog, pd.DataFrame],
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> float:
    """
    Calculate precision using token-based replay.

    Token-based replay matches a trace against a Petri net model, starting from the initial marking, to discover which transitions are executed and in which places there are remaining or missing tokens for the given process instance. Token-based replay is useful for conformance checking: a trace fits the model if, during its execution, all transitions can be fired without the need to insert any missing tokens. If reaching the final marking is imposed, a trace fits if it reaches the final marking without any missing or remaining tokens.

    In PM4Py, the token replayer implementation can handle hidden transitions by calculating the shortest paths between places. It can be used with any Petri net model that has unique visible transitions and hidden transitions. When a visible transition needs to be fired and not all places in its preset have the correct number of tokens, the current marking is checked to see if any hidden transitions can be fired to enable the visible transition. The hidden transitions are then fired, reaching a marking that permits the firing of the visible transition.

    The approach is described in:
    Berti, Alessandro, and Wil MP van der Aalst. "Reviving Token-based Replay: Increasing Speed While Improving Diagnostics." ATAED@ Petri Nets/ACSD. 2019.

    The reference paper for the TBR-based precision (ETConformance) is:
    Muñoz-Gama, Jorge, and Josep Carmona. "A fresh look at precision in process conformance." International Conference on Business Process Management. Springer, Berlin, Heidelberg, 2010.

    In this approach, the different prefixes of the log are replayed (if possible) on the model. At the reached marking, the set of transitions that are enabled in the process model is compared with the set of activities that follow the prefix. The more the sets differ, the lower the precision value. The more the sets are similar, the higher the precision value.

    :param log: Event log.
    :param petri_net: Petri net.
    :param initial_marking: Initial marking.
    :param final_marking: Final marking.
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :return: The precision value.
    :rtype: ``float``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        precision_tbr = pm4py.precision_token_based_replay(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    from pm4py.algo.evaluation.precision import (
        algorithm as precision_evaluator,
    )

    result = precision_evaluator.apply(
        log,
        petri_net,
        initial_marking,
        final_marking,
        variant=precision_evaluator.Variants.ETCONFORMANCE_TOKEN,
        parameters=properties,
    )

    return result


def precision_alignments(
    log: Union[EventLog, pd.DataFrame],
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
    multi_processing: bool = constants.ENABLE_MULTIPROCESSING_DEFAULT,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> float:
    """
    Calculate the precision of the model with respect to the event log using alignments.

    Alignment-based replay aims to find one of the best alignments between the trace and the model. For each trace, the output of an alignment is a list of pairs where the first element is an event (from the trace) or ``»`` and the second element is a transition (from the model) or ``»``. Each pair can be classified as follows:

    - **Sync move**: The event and transition labels correspond, advancing both the trace and the model simultaneously.
    - **Move on log**: The transition is ``»``, indicating a replay move in the trace that is not mirrored in the model. This move is unfit and signals a deviation.
    - **Move on model**: The event is ``»``, indicating a replay move in the model not mirrored in the trace. These can be further classified as:
        * **Moves on model involving hidden transitions**: Even if it's not a sync move, the move is fit.
        * **Moves on model not involving hidden transitions**: The move is unfit and signals a deviation.

    The reference paper for the alignments-based precision (Align-ETConformance) is:
    Adriansyah, Arya, et al. "Measuring precision of modeled behavior." Information systems and e-Business Management 13.1 (2015): 37-67.

    In this approach, the different prefixes of the log are replayed (if possible) on the model. At the reached marking, the set of transitions that are enabled in the process model is compared with the set of activities that follow the prefix. The more the sets differ, the lower the precision value. The more the sets are similar, the higher the precision value.

    :param log: Event log.
    :param petri_net: Petri net.
    :param initial_marking: Initial marking.
    :param final_marking: Final marking.
    :param multi_processing: Boolean to enable multiprocessing (default is `constants.ENABLE_MULTIPROCESSING_DEFAULT`).
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :return: The precision value.
    :rtype: ``float``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        precision_alignments = pm4py.precision_alignments(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    from pm4py.algo.evaluation.precision import (
        algorithm as precision_evaluator,
    )

    parameters = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    parameters["multiprocessing"] = multi_processing
    result = precision_evaluator.apply(
        log,
        petri_net,
        initial_marking,
        final_marking,
        variant=precision_evaluator.Variants.ALIGN_ETCONFORMANCE,
        parameters=parameters,
    )

    return result


def generalization_tbr(
    log: Union[EventLog, pd.DataFrame],
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> float:
    """
    Compute the generalization of the model against the event log.
    The approach is described in the paper:

    Buijs, Joos CAM, Boudewijn F. van Dongen, and Wil MP van der Aalst. "Quality dimensions in process discovery: The importance of fitness, precision, generalization, and simplicity." International Journal of Cooperative Information Systems 23.01 (2014): 1440001.

    :param log: Event log.
    :param petri_net: Petri net.
    :param initial_marking: Initial marking.
    :param final_marking: Final marking.
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :return: The generalization value.
    :rtype: ``float``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        generalization_tbr = pm4py.generalization_tbr(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    from pm4py.algo.evaluation.generalization import (
        algorithm as generalization_evaluator,
    )

    parameters = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    result = generalization_evaluator.apply(
        log,
        petri_net,
        initial_marking,
        final_marking,
        variant=generalization_evaluator.Variants.GENERALIZATION_TOKEN,
        parameters=parameters,
    )

    return result


def replay_prefix_tbr(
    prefix: List[str],
    net: PetriNet,
    im: Marking,
    fm: Marking,
    activity_key: str = "concept:name",
) -> Marking:
    """
    Replay a prefix (list of activities) on a given accepting Petri net using Token-Based Replay.

    :param prefix: List of activities representing the prefix.
    :param net: Petri net.
    :param im: Initial marking.
    :param fm: Final marking.
    :param activity_key: Attribute to be used as the activity key (default is "concept:name").
    :return: The marking reached after replaying the prefix.
    :rtype: ``Marking``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.read_pnml('tests/input_data/running-example.pnml')
        marking = pm4py.replay_prefix_tbr(
            ['register request', 'check ticket'],
            net,
            im,
            fm,
            activity_key='concept:name'
        )
        ```
    """
    purpose_log = EventLog()
    trace = Trace()
    for act in prefix:
        trace.append(Event({activity_key: act}))
    purpose_log.append(trace)

    from pm4py.algo.conformance.tokenreplay.variants import token_replay

    parameters_tr = {
        token_replay.Parameters.CONSIDER_REMAINING_IN_FITNESS: False,
        token_replay.Parameters.TRY_TO_REACH_FINAL_MARKING_THROUGH_HIDDEN: False,
        token_replay.Parameters.STOP_IMMEDIATELY_UNFIT: True,
        token_replay.Parameters.WALK_THROUGH_HIDDEN_TRANS: True,
        token_replay.Parameters.ACTIVITY_KEY: activity_key,
    }
    res = token_replay.apply(
        purpose_log, net, im, fm, parameters=parameters_tr
    )[0]
    return res["reached_marking"]


@deprecation.deprecated(
    deprecated_in="2.3.0",
    removed_in="3.0.0",
    details="Conformance checking using footprints will not be exposed in a future release.",
)
def __convert_to_fp(*args) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Internal method to convert the provided event log or process model arguments
    to footprints using footprints discovery.

    :param args: Event log or process model.
    :return: Footprints representation.
    :rtype: ``Union[List[Dict[str, Any]], Dict[str, Any]]``

    Note:
        This is an internal method and is deprecated.
    """
    import pm4py

    while isinstance(args, tuple):
        if len(args) == 1:
            args = args[0]
        else:
            fp = pm4py.discover_footprints(*args)
            return fp
    if isinstance(args, list) or isinstance(args, dict):
        return args
    fp = pm4py.discover_footprints(args)
    return fp


@deprecation.deprecated(
    deprecated_in="2.3.0",
    removed_in="3.0.0",
    details="Conformance checking using footprints will not be exposed in a future release.",
)
def conformance_diagnostics_footprints(
    *args,
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Provide conformance checking diagnostics using footprints.

    :param args: Arguments where the first is an event log (or its footprints) and the others represent the process model (or its footprints).
    :return: Conformance diagnostics based on footprints.
    :rtype: ``Union[List[Dict[str, Any]], Dict[str, Any]]``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        footprints_diagnostics = pm4py.conformance_diagnostics_footprints(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    fp1 = __convert_to_fp(args[0])
    fp2 = __convert_to_fp(args[1:])
    from pm4py.algo.conformance.footprints import (
        algorithm as footprints_conformance,
    )

    if isinstance(fp1, list):
        result = footprints_conformance.apply(
            fp1, fp2, variant=footprints_conformance.Variants.TRACE_EXTENSIVE
        )
    else:
        result = footprints_conformance.apply(
            fp1, fp2, variant=footprints_conformance.Variants.LOG_EXTENSIVE
        )

    return result


@deprecation.deprecated(
    deprecated_in="2.3.0",
    removed_in="3.0.0",
    details="Conformance checking using footprints will not be exposed in a future release.",
)
def fitness_footprints(*args) -> Dict[str, float]:
    """
    Calculate fitness using footprints.
    The output is a dictionary containing two keys:
    - **perc_fit_traces**: Percentage of fit traces (over the log).
    - **log_fitness**: The fitness value over the log.

    :param args: Arguments where the first is an event log (or its footprints) and the others represent the process model (or its footprints).
    :return: A dictionary containing fitness metrics based on footprints.
    :rtype: ``Dict[str, float]``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        fitness_fp = pm4py.fitness_footprints(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    fp_conf = conformance_diagnostics_footprints(*args)
    fp1 = __convert_to_fp(args[0])
    fp2 = __convert_to_fp(args[1:])
    from pm4py.algo.conformance.footprints.util import evaluation

    result = evaluation.fp_fitness(fp1, fp2, fp_conf)

    return result


@deprecation.deprecated(
    deprecated_in="2.3.0",
    removed_in="3.0.0",
    details="Conformance checking using footprints will not be exposed in a future release.",
)
def precision_footprints(*args) -> float:
    """
    Calculate precision using footprints.

    :param args: Arguments where the first is an event log (or its footprints) and the others represent the process model (or its footprints).
    :return: The precision value based on footprints.
    :rtype: ``float``

    Example:
        ```python
        import pm4py

        net, im, fm = pm4py.discover_petri_net_inductive(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        precision_fp = pm4py.precision_footprints(
            dataframe,
            net,
            im,
            fm,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    fp1 = __convert_to_fp(args[0])
    fp2 = __convert_to_fp(args[1:])
    from pm4py.algo.conformance.footprints.util import evaluation

    result = evaluation.fp_precision(fp1, fp2)

    return result


@deprecation.deprecated(
    removed_in="2.3.0",
    deprecated_in="3.0.0",
    details="This method will be removed in a future release.",
)
def __check_is_fit_process_tree(trace, tree) -> bool:
    """
    Check if a trace object fits a process tree model.

    :param trace: Trace.
    :param tree: Process tree.
    :return: True if the trace fits the process tree, False otherwise.
    :rtype: ``bool``

    Note:
        This is an internal method and is deprecated.
    """
    __event_log_deprecation_warning(trace)

    from pm4py.discovery import discover_footprints

    log = EventLog()
    log.append(trace)
    fp_tree = discover_footprints(tree)
    fp_log = discover_footprints(log)
    fp_conf_res = conformance_diagnostics_footprints(fp_log, fp_tree)[0]
    # CHECK 1) If footprints indicate non-conformance, return False
    if not fp_conf_res["is_footprints_fit"]:
        return False
    else:
        from pm4py.convert import convert_to_petri_net

        net, im, fm = convert_to_petri_net(tree)
        tbr_conf_res = conformance_diagnostics_token_based_replay(
            log, net, im, fm, return_diagnostics_dataframe=False
        )[0]
        # CHECK 2) If TBR indicates fit, return True
        if tbr_conf_res["trace_is_fit"]:
            return True
        else:
            # CHECK 3) Use alignments for definitive fit assessment
            align_conf_res = conformance_diagnostics_alignments(
                log, tree, return_diagnostics_dataframe=False
            )[0]
            return align_conf_res["fitness"] == 1.0


@deprecation.deprecated(
    removed_in="2.3.0",
    deprecated_in="3.0.0",
    details="This method will be removed in a future release.",
)
def __check_is_fit_petri_net(
    trace,
    net: PetriNet,
    im: Marking,
    fm: Marking,
    activity_key=xes_constants.DEFAULT_NAME_KEY,
) -> bool:
    """
    Check if a trace object fits a Petri net model.

    :param trace: Trace.
    :param net: Petri net.
    :param im: Initial marking.
    :param fm: Final marking.
    :param activity_key: Attribute to be used as the activity key (default is defined in `xes_constants.DEFAULT_NAME_KEY`).
    :return: True if the trace fits the Petri net, False otherwise.
    :rtype: ``bool``

    Note:
        This is an internal method and is deprecated.
    """
    __event_log_deprecation_warning(trace)

    # Avoid checking footprints on Petri net (they are too slow)
    activities_model = set(
        trans.label for trans in net.transitions if trans.label is not None
    )
    activities_trace = set([x[activity_key] for x in trace])
    diff = activities_trace.difference(activities_model)
    if diff:
        # CHECK 1) If there are activities in the trace not present in the
        # model, return False
        return False
    else:
        log = EventLog()
        log.append(trace)
        tbr_conf_res = conformance_diagnostics_token_based_replay(
            log, net, im, fm, return_diagnostics_dataframe=False
        )[0]
        # CHECK 2) If TBR indicates fit, return True
        if tbr_conf_res["trace_is_fit"]:
            return True
        else:
            # CHECK 3) Use alignments for definitive fit assessment
            align_conf_res = conformance_diagnostics_alignments(
                log, net, im, fm, return_diagnostics_dataframe=False
            )[0]
            return align_conf_res["fitness"] == 1.0


@deprecation.deprecated(
    deprecated_in="2.3.0",
    removed_in="3.0.0",
    details="This method will be removed in a future release.",
)
def check_is_fitting(
    *args, activity_key=xes_constants.DEFAULT_NAME_KEY
) -> bool:
    """
    Check if a trace object fits a process model.

    :param args: Arguments where the first is a trace object and the others represent the process model (process tree, Petri net, BPMN).
    :param activity_key: Attribute to be used as the activity key (default is defined in `xes_constants.DEFAULT_NAME_KEY`).
    :return: True if the trace fits the process model, False otherwise.
    :rtype: ``bool``

    Note:
        This is an internal method and is deprecated.
    """
    from pm4py.util import variants_util
    from pm4py.convert import convert_to_process_tree, convert_to_petri_net

    trace = args[0]
    model = args[1:]

    try:
        model = convert_to_process_tree(*model)
    except BaseException:
        # If the model cannot be expressed as a process tree, attempt Petri net
        # conversion
        model = convert_to_petri_net(*model)

    if not isinstance(trace, Trace):
        activities = variants_util.get_activities_from_variant(trace)
        trace = Trace()
        for act in activities:
            trace.append(Event({activity_key: act}))

    if isinstance(model, ProcessTree):
        return __check_is_fit_process_tree(trace, model)
    elif isinstance(model, tuple) and isinstance(model[0], PetriNet):
        return __check_is_fit_petri_net(
            trace, model[0], model[1], model[2], activity_key=activity_key
        )


def conformance_temporal_profile(
    log: Union[EventLog, pd.DataFrame],
    temporal_profile: Dict[Tuple[str, str], Tuple[float, float]],
    zeta: float = 1.0,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    return_diagnostics_dataframe: bool = constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME,
) -> List[List[Tuple[float, float, float, float]]]:
    """
    Perform conformance checking on the provided log using the provided temporal profile.
    The result is a list of time-based deviations for every case.

    For example, consider a log with a single case:
    - A (timestamp: 2000-01)
    - B (timestamp: 2002-01)

    Given the temporal profile:
    ```python
    {
        ('A', 'B'): (1.5, 0.5),  # (mean, std)
        ('A', 'C'): (5.0, 0.0),
        ('A', 'D'): (2.0, 0.0)
    }
    ```
    and setting `zeta` to 1, the difference between the timestamps of A and B (2 years) exceeds the allowed time (1.5 months + 0.5 months), resulting in a deviation.

    :param log: Log object.
    :param temporal_profile: Temporal profile. For example, if the log has two cases:
        - Case 1: A (timestamp: 1980-01), B (timestamp: 1980-03), C (timestamp: 1980-06)
        - Case 2: A (timestamp: 1990-01), B (timestamp: 1990-02), D (timestamp: 1990-03)
      The temporal profile might look like:
        ```python
        {
            ('A', 'B'): (1.5, 0.5),  # (mean, std)
            ('A', 'C'): (5.0, 0.0),
            ('A', 'D'): (2.0, 0.0)
        }
        ```
    :param zeta: Number of standard deviations allowed from the average (default is 1.0). For example, `zeta=1` allows deviations within one standard deviation from the mean.
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :param return_diagnostics_dataframe: If possible, returns a dataframe with the diagnostics instead of the usual output (default is `constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME`).
    :return: A list containing lists of tuples representing time-based deviations for each case.
    :rtype: ``List[List[Tuple[float, float, float, float]]]``

    Example:
        ```python
        import pm4py

        temporal_profile = pm4py.discover_temporal_profile(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        conformance_temporal_profile = pm4py.conformance_temporal_profile(
            dataframe,
            temporal_profile,
            zeta=1,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    properties["zeta"] = zeta

    from pm4py.algo.conformance.temporal_profile import (
        algorithm as temporal_profile_conformance,
    )

    result = temporal_profile_conformance.apply(
        log, temporal_profile, parameters=properties
    )

    if return_diagnostics_dataframe:
        return temporal_profile_conformance.get_diagnostics_dataframe(
            log, result, parameters=properties
        )

    return result


def conformance_declare(
    log: Union[EventLog, pd.DataFrame],
    declare_model: Dict[str, Dict[Any, Dict[str, int]]],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    return_diagnostics_dataframe: bool = constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME,
) -> List[Dict[str, Any]]:
    """
    Apply conformance checking against a DECLARE model.

    Reference paper:
    F. M. Maggi, A. J. Mooij, and W. M. P. van der Aalst, "User-guided discovery of declarative process models," 2011 IEEE Symposium on Computational Intelligence and Data Mining (CIDM), Paris, France, 2011, pp. 192-199, doi: 10.1109/CIDM.2011.5949297.

    :param log: Event log.
    :param declare_model: DECLARE model represented as a nested dictionary.
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :param return_diagnostics_dataframe: If possible, returns a dataframe with the diagnostics instead of the usual output (default is `constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME`).
    :return: A list of dictionaries containing diagnostics for each trace.
    :rtype: ``List[Dict[str, Any]]``

    Example:
        ```python
        import pm4py

        log = pm4py.read_xes("C:/receipt.xes")
        declare_model = pm4py.discover_declare(log)
        conf_result = pm4py.conformance_declare(
            log,
            declare_model,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    if return_diagnostics_dataframe:
        from pm4py.convert import convert_to_event_log

        log = convert_to_event_log(log, case_id_key=case_id_key)
        case_id_key = None

    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    from pm4py.algo.conformance.declare import algorithm as declare_conformance

    result = declare_conformance.apply(
        log, declare_model, parameters=properties
    )

    if return_diagnostics_dataframe:
        return declare_conformance.get_diagnostics_dataframe(
            log, result, parameters=properties
        )

    return result


def conformance_log_skeleton(
    log: Union[EventLog, pd.DataFrame],
    log_skeleton: Dict[str, Any],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    return_diagnostics_dataframe: bool = constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME,
) -> List[Set[Any]]:
    """
    Perform conformance checking using the log skeleton.

    Reference paper:
    Verbeek, H. M. W., and R. Medeiros de Carvalho. "Log skeletons: A classification approach to process discovery." arXiv preprint arXiv:1806.08247 (2018).

    A log skeleton is a declarative model consisting of six different constraints:
    - **directly_follows**: Specifies strict bounds on activities directly following each other. For example, 'A should be directly followed by B' and 'B should be directly followed by C'.
    - **always_before**: Specifies that certain activities may only be executed if some other activities have been executed earlier in the case history. For example, 'C should always be preceded by A'.
    - **always_after**: Specifies that certain activities should always trigger the execution of other activities in the future history of the case. For example, 'A should always be followed by C'.
    - **equivalence**: Specifies that pairs of activities should occur the same number of times within a case. For example, 'B and C should always happen the same number of times'.
    - **never_together**: Specifies that certain pairs of activities should never occur together in the case history. For example, 'No case should contain both C and D'.
    - **activ_occurrences**: Specifies the allowed number of occurrences per activity. For example, 'A is allowed to be executed 1 or 2 times, and B is allowed to be executed 1 to 4 times'.

    :param log: Log object.
    :param log_skeleton: Log skeleton object, expressed as dictionaries of the six constraints along with the discovered rules.
    :param activity_key: Attribute to be used for the activity (default is "concept:name").
    :param timestamp_key: Attribute to be used for the timestamp (default is "time:timestamp").
    :param case_id_key: Attribute to be used as the case identifier (default is "case:concept:name").
    :param return_diagnostics_dataframe: If possible, returns a dataframe with the diagnostics instead of the usual output (default is `constants.DEFAULT_RETURN_DIAGNOSTICS_DATAFRAME`).
    :return: A list of sets containing deviations for each case.
    :rtype: ``List[Set[Any]]``

    Example:
        ```python
        import pm4py

        log_skeleton = pm4py.discover_log_skeleton(
            dataframe,
            noise_threshold=0.1,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        conformance_lsk = pm4py.conformance_log_skeleton(
            dataframe,
            log_skeleton,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
        ```
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    if return_diagnostics_dataframe:
        from pm4py.convert import convert_to_event_log

        log = convert_to_event_log(log, case_id_key=case_id_key)
        case_id_key = None

    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    from pm4py.algo.conformance.log_skeleton import (
        algorithm as log_skeleton_conformance,
    )

    result = log_skeleton_conformance.apply(
        log, log_skeleton, parameters=properties
    )

    if return_diagnostics_dataframe:
        return log_skeleton_conformance.get_diagnostics_dataframe(
            log, result, parameters=properties
        )

    return result
