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
The ``pm4py.stats`` module contains the statistical functionalities offered in ``pm4py``.
"""

import sys
from typing import Dict, Union, List, Tuple, Collection, Iterator
from typing import Set, Optional
from typing import Counter as TCounter
from collections import Counter

import pandas as pd

from pm4py.objects.log.obj import EventLog, Trace, EventStream
from pm4py.util.pandas_utils import (
    check_is_pandas_dataframe,
    check_pandas_dataframe_columns,
    insert_ev_in_tr_index,
)
from pm4py.utils import get_properties, __event_log_deprecation_warning
from pm4py.util import constants, pandas_utils
from pm4py.objects.petri_net.obj import PetriNet
from pm4py.objects.process_tree.obj import ProcessTree
import deprecation


def get_start_activities(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> Dict[str, int]:
    """
    Returns the start activities and their frequencies from a log object.

    :param log: Log object (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A dictionary mapping start activity names to their frequencies.

    .. code-block:: python3

        import pm4py

        start_activities = pm4py.get_start_activities(
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
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        from pm4py.statistics.start_activities.pandas import get

        return get.get_start_activities(log, parameters=properties)
    else:
        from pm4py.statistics.start_activities.log import get

        return get.get_start_activities(log, parameters=properties)


def get_end_activities(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> Dict[str, int]:
    """
    Returns the end activities and their frequencies from a log object.

    :param log: Log object (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A dictionary mapping end activity names to their frequencies.

    .. code-block:: python3

        import pm4py

        end_activities = pm4py.get_end_activities(
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
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        from pm4py.statistics.end_activities.pandas import get

        return get.get_end_activities(log, parameters=properties)
    else:
        from pm4py.statistics.end_activities.log import get

        return get.get_end_activities(log, parameters=properties)


def get_event_attributes(log: Union[EventLog, pd.DataFrame]) -> List[str]:
    """
    Returns the list of event-level attributes in the log.

    :param log: Log object (EventLog or pandas DataFrame).
    :return: A list of event attribute names.

    .. code-block:: python3

        import pm4py

        event_attributes = pm4py.get_event_attributes(dataframe)
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(log)
        return list(log.columns)
    else:
        from pm4py.statistics.attributes.log import get

        return list(get.get_all_event_attributes_from_log(log))


def get_trace_attributes(log: Union[EventLog, pd.DataFrame]) -> List[str]:
    """
    Returns the list of trace-level attributes in the log.

    :param log: Log object (EventLog or pandas DataFrame).
    :return: A list of trace attribute names.

    .. code-block:: python3

        import pm4py

        trace_attributes = pm4py.get_trace_attributes(dataframe)
    """
    __event_log_deprecation_warning(log)

    from pm4py.util import constants

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(log)
        return [
            x
            for x in list(log.columns)
            if x.startswith(constants.CASE_ATTRIBUTE_PREFIX)
        ]
    else:
        from pm4py.statistics.attributes.log import get

        return list(get.get_all_trace_attributes_from_log(log))


def get_event_attribute_values(
    log: Union[EventLog, pd.DataFrame],
    attribute: str,
    count_once_per_case: bool = False,
    case_id_key: str = "case:concept:name",
) -> Dict[str, int]:
    """
    Returns the values and their frequencies for a specified event attribute.

    :param log: Log object (EventLog or pandas DataFrame).
    :param attribute: The event attribute to analyze.
    :param count_once_per_case: If True, count each attribute value at most once per case.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A dictionary mapping attribute values to their frequencies.

    .. code-block:: python3

        import pm4py

        activities = pm4py.get_event_attribute_values(
            dataframe,
            'concept:name',
            case_id_key='case:concept:name'
        )
    """
    __event_log_deprecation_warning(log)

    parameters = get_properties(log, case_id_key=case_id_key)
    parameters["keep_once_per_case"] = count_once_per_case
    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(log, case_id_key=case_id_key)
        from pm4py.statistics.attributes.pandas import get

        return get.get_attribute_values(log, attribute, parameters=parameters)
    else:
        from pm4py.statistics.attributes.log import get

        return get.get_attribute_values(log, attribute, parameters=parameters)


def get_trace_attribute_values(
    log: Union[EventLog, pd.DataFrame],
    attribute: str,
    case_id_key: str = "case:concept:name",
) -> Dict[str, int]:
    """
    Returns the values and their frequencies for a specified trace attribute.

    :param log: Log object (EventLog or pandas DataFrame).
    :param attribute: The trace attribute to analyze.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A dictionary mapping trace attribute values to their frequencies.

    .. code-block:: python3

        import pm4py

        tr_attr_values = pm4py.get_trace_attribute_values(
            dataframe,
            'case:attribute',
            case_id_key='case:concept:name'
        )
    """
    __event_log_deprecation_warning(log)

    parameters = get_properties(log, case_id_key=case_id_key)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(log, case_id_key=case_id_key)
        from pm4py.statistics.attributes.pandas import get

        if (
            attribute not in log
            and constants.CASE_ATTRIBUTE_PREFIX + attribute in log
        ):
            # If "attribute" does not exist as a column, but "case:attribute"
            # exists, then use that.
            attribute = constants.CASE_ATTRIBUTE_PREFIX + attribute
        ret = get.get_attribute_values(log, attribute, parameters=parameters)
        return ret
    else:
        from pm4py.statistics.attributes.log import get

        ret = get.get_trace_attribute_values(
            log, attribute, parameters=parameters
        )

        if not ret:
            # If the provided attribute does not exist, but starts with "case:", try to get the attribute values
            # by removing the "case:" prefix.
            if attribute.startswith(constants.CASE_ATTRIBUTE_PREFIX):
                attribute = attribute.split(constants.CASE_ATTRIBUTE_PREFIX)[
                    -1
                ]
            ret = get.get_trace_attribute_values(
                log, attribute, parameters=parameters
            )

        return ret


def get_variants(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    max_repetitions: int = sys.maxsize,
) -> Union[Dict[Tuple[str], List[Trace]], Dict[Tuple[str], int]]:
    """
    Retrieves the variants from the log.

    :param log: Event log (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :param max_repetitions: Maximum number of consecutive repetitions for an activity.
                             Reduces variants by limiting consecutive activity repetitions.
    :return: A dictionary mapping activity tuples to their counts or lists of traces.

    .. code-block:: python3

        import pm4py

        variants = pm4py.get_variants(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
    """
    return get_variants_as_tuples(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
        max_repetitions=max_repetitions,
    )


def get_variants_as_tuples(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    max_repetitions: int = sys.maxsize,
) -> Union[Dict[Tuple[str], List[Trace]], Dict[Tuple[str], int]]:
    """
    Retrieves the variants from the log, where the variant keys are tuples.

    :param log: Event log (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :param max_repetitions: Maximum number of consecutive repetitions for an activity.
                             Reduces variants by limiting consecutive activity repetitions.
    :return: A dictionary mapping activity tuples to their counts or lists of traces.

    .. code-block:: python3

        import pm4py

        variants = pm4py.get_variants_as_tuples(
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
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        from pm4py.statistics.variants.pandas import get

        variants = get.get_variants_count(log, parameters=properties)
    else:
        from pm4py.statistics.variants.log import get

        variants = get.get_variants(log, parameters=properties)

    if max_repetitions < sys.maxsize:
        from pm4py.util import variants_util

        variants = variants_util.aggregate_consecutive_activities_in_variants(
            variants, max_repetitions=max_repetitions
        )

    return variants


def split_by_process_variant(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    variant_column: str = "@@variant_column",
    index_in_trace_column: str = "@@index_in_trace",
) -> Iterator[Tuple[Collection[str], pd.DataFrame]]:
    """
    Splits an event log into sub-dataframes for each process variant.
    The result is an iterator over the variants along with their corresponding sub-dataframes.

    :param log: Event log (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :param variant_column: Name of the utility column that stores the variant's tuple.
    :param index_in_trace_column: Name of the utility column that stores the index of the event in the case.
    :return: An iterator of tuples, each containing a variant and its corresponding sub-dataframe.

    .. code-block:: python3

        import pandas as pd
        import pm4py

        dataframe = pd.read_csv('tests/input_data/receipt.csv')
        dataframe = pm4py.format_dataframe(dataframe)
        for variant, subdf in pm4py.split_by_process_variant(dataframe):
            print(variant)
            print(subdf)
    """
    __event_log_deprecation_warning(log)

    import pm4py

    log = pm4py.convert_to_dataframe(log)
    check_pandas_dataframe_columns(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    from pm4py.util import pandas_utils

    log = pandas_utils.insert_ev_in_tr_index(
        log, case_id=case_id_key, column_name=index_in_trace_column
    )
    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    from pm4py.objects.log.util import pandas_numpy_variants

    variants_dict, case_variant = pandas_numpy_variants.apply(
        log, parameters=properties
    )

    log[variant_column] = log[case_id_key].map(case_variant)

    for variant, filtered_log in log.groupby(variant_column, sort=False):
        yield variant, filtered_log


def get_variants_paths_duration(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
    variant_column: str = "@@variant_column",
    variant_count: str = "@@variant_count",
    index_in_trace_column: str = "@@index_in_trace",
    cumulative_occ_path_column: str = "@@cumulative_occ_path_column",
    times_agg: str = "mean",
) -> pd.DataFrame:
    """
    Associates a pandas DataFrame aggregated by variants and their positions within each variant.
    Each row includes:
    - The variant
    - The position within the variant
    - The source activity of the path
    - The target activity of the path
    - An aggregation of the times between the two activities (e.g., mean)
    - The cumulative occurrences of the path within the case

    :param log: Event log (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :param variant_column: Name of the utility column that stores the variant's tuple.
    :param variant_count: Name of the utility column that stores the variant's occurrence count.
    :param index_in_trace_column: Name of the utility column that stores the index of the event in the case.
    :param cumulative_occ_path_column: Name of the column that stores the cumulative occurrences of the path within the case.
    :param times_agg: Aggregation function to be used for time differences (e.g., "mean", "median").
    :return: A pandas DataFrame with the aggregated variant paths and durations.

    .. code-block:: python3

        import pandas as pd
        import pm4py

        dataframe = pd.read_csv('tests/input_data/receipt.csv')
        dataframe = pm4py.format_dataframe(dataframe)

        var_paths_durs = pm4py.get_variants_paths_duration(dataframe)
        print(var_paths_durs)
    """
    __event_log_deprecation_warning(log)
    check_pandas_dataframe_columns(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    list_to_concat = []
    for variant, filtered_log in split_by_process_variant(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
        variant_column=variant_column,
        index_in_trace_column=index_in_trace_column,
    ):
        from pm4py.statistics.eventually_follows.pandas import (
            get as eventually_follows,
        )

        dir_follo_dataframe = eventually_follows.get_partial_order_dataframe(
            filtered_log.copy(),
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_glue=case_id_key,
            sort_caseid_required=False,
            sort_timestamp_along_case_id=False,
            reduce_dataframe=False,
        )
        dir_follo_dataframe[cumulative_occ_path_column] = (
            dir_follo_dataframe.groupby(
                [case_id_key, activity_key, activity_key + "_2"]
            ).cumcount()
        )
        dir_follo_dataframe = (
            dir_follo_dataframe[
                [
                    index_in_trace_column,
                    constants.DEFAULT_FLOW_TIME,
                    cumulative_occ_path_column,
                ]
            ]
            .groupby(index_in_trace_column)
            .agg(
                {
                    constants.DEFAULT_FLOW_TIME: times_agg,
                    cumulative_occ_path_column: "min",
                }
            )
            .reset_index()
        )
        dir_follo_dataframe[activity_key] = dir_follo_dataframe[
            index_in_trace_column
        ].apply(lambda x: variant[x])
        dir_follo_dataframe[activity_key + "_2"] = dir_follo_dataframe[
            index_in_trace_column
        ].apply(lambda x: variant[x + 1])
        dir_follo_dataframe[variant_column] = dir_follo_dataframe[
            index_in_trace_column
        ].apply(lambda x: variant)
        dir_follo_dataframe[variant_count] = filtered_log[
            case_id_key
        ].nunique()

        list_to_concat.append(dir_follo_dataframe)

    dataframe = pandas_utils.concat(list_to_concat)
    dataframe[index_in_trace_column] = -dataframe[index_in_trace_column]
    dataframe = dataframe.sort_values(
        [variant_count, variant_column, index_in_trace_column], ascending=False
    )
    dataframe[index_in_trace_column] = -dataframe[index_in_trace_column]

    return dataframe


def get_stochastic_language(*args, **kwargs) -> Dict[List[str], float]:
    """
    Retrieves the stochastic language from the provided object.

    The stochastic language represents the probabilities of different traces or sequences within the process.

    :param args: The input object, which can be a pandas DataFrame, EventLog, accepting Petri net, or ProcessTree.
    :param kwargs: Additional keyword arguments.
    :return: A dictionary mapping sequences of activities to their probabilities.

    .. code-block:: python3

        import pm4py

        # From an event log
        log = pm4py.read_xes('tests/input_data/running-example.xes')
        language_log = pm4py.get_stochastic_language(log)
        print(language_log)

        # From a Petri net
        net, im, fm = pm4py.read_pnml('tests/input_data/running-example.pnml')
        language_model = pm4py.get_stochastic_language(net, im, fm)
        print(language_model)
    """
    from pm4py.statistics.variants.log import get

    if (
        isinstance(args[0], EventLog)
        or isinstance(args[0], EventStream)
        or pandas_utils.check_is_pandas_dataframe(args[0])
    ):
        from pm4py.objects.conversion.log import converter as log_converter

        log = log_converter.apply(args[0])
        return get.get_language(log)
    elif (
        isinstance(args[0], PetriNet)
        or isinstance(args[0], ProcessTree)
        or isinstance(args[0], dict)
    ):
        import pm4py

        log = pm4py.play_out(*args, **kwargs)
        return get.get_language(log)
    else:
        raise Exception(
            "Unsupported input type for stochastic language extraction."
        )


def get_minimum_self_distances(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> Dict[str, int]:
    """
    Computes the minimum self-distance for each activity observed in an event log.

    The self-distance of an activity `a` in a trace is defined as follows:
    - In a trace <a>, it's infinity.
    - In a trace <a, a>, it's 0.
    - In a trace <a, b, a>, it's 1.
    - And so on.

    The minimum self-distance for an activity is the smallest self-distance observed across all traces.

    :param log: Event log (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A dictionary mapping each activity to its minimum self-distance.

    .. code-block:: python3

        import pm4py

        msd = pm4py.get_minimum_self_distances(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
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

    from pm4py.algo.discovery.minimum_self_distance import (
        algorithm as msd_algo,
    )

    return msd_algo.apply(log, parameters=properties)


def get_minimum_self_distance_witnesses(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> Dict[str, Set[str]]:
    """
    Derives the minimum self-distance witnesses for each activity.

    A 'witness' is an activity that occurs between two occurrences of the same activity at the minimum self-distance.
    For example, if the minimum self-distance of activity `a` is 2, then in a trace <a, b, c, a>,
    activities `b` and `c` are witnesses of `a`.

    :param log: Event log (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A dictionary mapping each activity to a set of its witness activities.

    .. code-block:: python3

        import pm4py

        msd_wit = pm4py.get_minimum_self_distance_witnesses(
            dataframe,
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    from pm4py.algo.discovery.minimum_self_distance import (
        algorithm as msd_algo,
    )
    from pm4py.algo.discovery.minimum_self_distance import utils as msdw_algo

    return msdw_algo.derive_msd_witnesses(
        log,
        msd_algo.apply(
            log,
            parameters=get_properties(
                log,
                activity_key=activity_key,
                timestamp_key=timestamp_key,
                case_id_key=case_id_key,
            ),
        ),
    )


def get_case_arrival_average(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> float:
    """
    Calculates the average time difference between the start times of two consecutive cases.

    This metric is based on the definition:
    Cycle time = Average time between completion of units.

    Example:
    In a manufacturing facility producing 100 units in a 40-hour week,
    the average throughput rate is 1 unit per 0.4 hours (24 minutes per unit).
    Therefore, the cycle time is 24 minutes on average.

    :param log: Event log (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: The average case arrival time in the same units as the timestamp.

    .. code-block:: python3

        import pm4py

        case_arr_avg = pm4py.get_case_arrival_average(
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
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        from pm4py.statistics.traces.generic.pandas import case_arrival

        return case_arrival.get_case_arrival_avg(log, parameters=properties)
    else:
        from pm4py.statistics.traces.generic.log import case_arrival

        return case_arrival.get_case_arrival_avg(log, parameters=properties)


def get_rework_cases_per_activity(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> Dict[str, int]:
    """
    Identifies activities that have rework occurrences, i.e., activities that occur more than once within the same case.
    The output is a dictionary mapping each such activity to the number of cases in which rework occurred.

    :param log: Log object (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A dictionary mapping each activity with rework to the number of cases where rework occurred.

    .. code-block:: python3

        import pm4py

        rework = pm4py.get_rework_cases_per_activity(
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
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        from pm4py.statistics.rework.pandas import get as rework_get

        return rework_get.apply(log, parameters=properties)
    else:
        from pm4py.statistics.rework.log import get as rework_get

        return rework_get.apply(log, parameters=properties)


@deprecation.deprecated(
    deprecated_in="2.3.0",
    removed_in="3.0.0",
    details="The get_case_overlap function will be removed in a future release.",
)
def get_case_overlap(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> List[int]:
    """
    Associates each case in the log with the number of cases that are concurrently open.

    :param log: Log object (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A list where each element corresponds to a case and indicates the number of overlapping cases.

    .. code-block:: python3

        import pm4py

        overlap = pm4py.get_case_overlap(
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
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        from pm4py.statistics.overlap.cases.pandas import get as cases_overlap

        return cases_overlap.apply(log, parameters=properties)
    else:
        from pm4py.statistics.overlap.cases.log import get as cases_overlap

        return cases_overlap.apply(log, parameters=properties)


def get_cycle_time(
    log: Union[EventLog, pd.DataFrame],
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> float:
    """
    Calculates the cycle time of the event log.

    Cycle time is defined as the average time between the completion of units.

    Example:
    In a manufacturing facility producing 100 units in a 40-hour week,
    the average throughput rate is 1 unit per 0.4 hours (24 minutes per unit).
    Therefore, the cycle time is 24 minutes on average.

    :param log: Event log (EventLog or pandas DataFrame).
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: The cycle time as a float.

    .. code-block:: python3

        import pm4py

        cycle_time = pm4py.get_cycle_time(
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
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        from pm4py.statistics.traces.cycle_time.pandas import get as cycle_time

        return cycle_time.apply(log, parameters=properties)
    else:
        from pm4py.statistics.traces.cycle_time.log import get as cycle_time

        return cycle_time.apply(log, parameters=properties)


def get_service_time(
    log: Union[EventLog, pd.DataFrame],
    aggregation_measure: str = "mean",
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    start_timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> Dict[str, float]:
    """
    Computes the service time for each activity in the event log using the specified aggregation measure.

    Service time refers to the duration an activity takes within a case.

    :param log: Event log (EventLog or pandas DataFrame).
    :param aggregation_measure: Aggregation function to apply (e.g., "mean", "median", "min", "max", "sum").
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param start_timestamp_key: Attribute to be used for the start timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A dictionary mapping each activity to its aggregated service time.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes('tests/input_data/interval_event_log.xes')
        mean_serv_time = pm4py.get_service_time(
            log,
            start_timestamp_key='start_timestamp',
            aggregation_measure='mean'
        )
        print(mean_serv_time)

        median_serv_time = pm4py.get_service_time(
            log,
            start_timestamp_key='start_timestamp',
            aggregation_measure='median'
        )
        print(median_serv_time)
    """
    __event_log_deprecation_warning(log)

    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
        start_timestamp_key=start_timestamp_key,
    )
    properties["aggregationMeasure"] = aggregation_measure

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
            start_timestamp_key=start_timestamp_key,
        )
        from pm4py.statistics.service_time.pandas import get as serv_time_get

        return serv_time_get.apply(log, parameters=properties)
    else:
        from pm4py.statistics.service_time.log import get as serv_time_get

        return serv_time_get.apply(log, parameters=properties)


def get_all_case_durations(
    log: Union[EventLog, pd.DataFrame],
    business_hours: bool = False,
    business_hour_slots=constants.DEFAULT_BUSINESS_HOUR_SLOTS,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> List[float]:
    """
    Retrieves the durations of all cases in the event log.

    :param log: Event log (EventLog or pandas DataFrame).
    :param business_hours: If True, computes durations based on business hours; otherwise, uses calendar time.
    :param business_hour_slots: Work schedule of the company as a list of tuples. Each tuple represents a time slot in seconds since the week start.
                                Example: [
                                    (7 * 60 * 60, 17 * 60 * 60),
                                    ((24 + 7) * 60 * 60, (24 + 12) * 60 * 60),
                                    ((24 + 13) * 60 * 60, (24 + 17) * 60 * 60),
                                ]
                                This example means:
                                - Monday 07:00 - 17:00
                                - Tuesday 07:00 - 12:00
                                - Tuesday 13:00 - 17:00
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A sorted list of case durations.

    .. code-block:: python3

        import pm4py

        case_durations = pm4py.get_all_case_durations(
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
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    properties["business_hours"] = business_hours
    properties["business_hour_slots"] = business_hour_slots

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        from pm4py.statistics.traces.generic.pandas import case_statistics

        cd = case_statistics.get_cases_description(log, parameters=properties)
        return sorted([x["caseDuration"] for x in cd.values()])
    else:
        from pm4py.statistics.traces.generic.log import case_statistics

        return case_statistics.get_all_case_durations(
            log, parameters=properties
        )


def get_case_duration(
    log: Union[EventLog, pd.DataFrame],
    case_id: str,
    business_hours: bool = False,
    business_hour_slots=constants.DEFAULT_BUSINESS_HOUR_SLOTS,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: Optional[str] = None,
) -> float:
    """
    Retrieves the duration of a specific case.

    :param log: Event log (EventLog or pandas DataFrame).
    :param case_id: Identifier of the case whose duration is to be retrieved.
    :param business_hours: If True, computes duration based on business hours; otherwise, uses calendar time.
    :param business_hour_slots: Work schedule of the company as a list of tuples. Each tuple represents a time slot in seconds since the week start.
                                Example: [
                                    (7 * 60 * 60, 17 * 60 * 60),
                                    ((24 + 7) * 60 * 60, (24 + 12) * 60 * 60),
                                    ((24 + 13) * 60 * 60, (24 + 17) * 60 * 60),
                                ]
                                This example means:
                                - Monday 07:00 - 17:00
                                - Tuesday 07:00 - 12:00
                                - Tuesday 13:00 - 17:00
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: The duration of the specified case.

    .. code-block:: python3

        import pm4py

        duration = pm4py.get_case_duration(
            dataframe,
            'case_1',
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
    """
    __event_log_deprecation_warning(log)

    properties = get_properties(
        log,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    properties["business_hours"] = business_hours
    properties["business_hour_slots"] = business_hour_slots

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        from pm4py.statistics.traces.generic.pandas import case_statistics

        cd = case_statistics.get_cases_description(log, parameters=properties)
        return cd[case_id]["caseDuration"]
    else:
        from pm4py.statistics.traces.generic.log import case_statistics

        cd = case_statistics.get_cases_description(log, parameters=properties)
        return cd[case_id]["caseDuration"]


def get_frequent_trace_segments(
    log: Union[EventLog, pd.DataFrame],
    min_occ: int,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> TCounter:
    """
    Retrieves frequent trace segments (sub-sequences of activities) from an event log.
    Each trace segment is preceded and followed by "...", indicating that it can be part of a larger sequence.

    :param log: Event log (EventLog or pandas DataFrame).
    :param min_occ: Minimum number of occurrences for a trace segment to be included.
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A Counter object mapping trace segments to their occurrence counts.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/receipt.xes")
        traces = pm4py.get_frequent_trace_segments(log, min_occ=100)
        print(traces)
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )

    import pm4py.utils
    from prefixspan import PrefixSpan

    projection = pm4py.utils.project_on_event_attribute(
        log, attribute_key=activity_key, case_id_key=case_id_key
    )
    traces0 = PrefixSpan(projection).frequent(min_occ)
    traces = {}
    for x in traces0:
        trace = ["..."]
        for i in range(len(x[1])):
            if i > 0:
                trace.append("...")
            trace.append(x[1][i])
        trace.append("...")
        trace = tuple(trace)
        traces[trace] = x[0]
    traces = Counter(traces)

    return traces


def get_activity_position_summary(
    log: Union[EventLog, pd.DataFrame],
    activity: str,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> Dict[int, int]:
    """
    Summarizes the positions of a specific activity across all cases in the event log.

    For each occurrence of the activity, records its position within the trace.
    For example, if 'A' occurs 1000 times in position 1 and 500 times in position 2,
    the returned dictionary will be {1: 1000, 2: 500}.

    :param log: Event log object (EventLog or pandas DataFrame).
    :param activity: The activity to analyze.
    :param activity_key: Attribute to be used for the activity.
    :param timestamp_key: Attribute to be used for the timestamp.
    :param case_id_key: Attribute to be used as the case identifier.
    :return: A dictionary mapping positions (0-based index) to the number of times the activity occurs in that position.

    .. code-block:: python3

        import pm4py

        act_pos = pm4py.get_activity_position_summary(
            dataframe,
            'Act. A',
            activity_key='concept:name',
            case_id_key='case:concept:name',
            timestamp_key='time:timestamp'
        )
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(
            log,
            activity_key=activity_key,
            timestamp_key=timestamp_key,
            case_id_key=case_id_key,
        )
        log = insert_ev_in_tr_index(log, case_id_key, "@@index_in_trace")
        ret = (
            log[log[activity_key] == activity]["@@index_in_trace"]
            .value_counts()
            .to_dict()
        )
        return ret
    else:
        ret = Counter()
        for trace in log:
            for i in range(len(trace)):
                this_act = trace[i][activity_key]
                if this_act == activity:
                    ret[i] += 1
        return dict(ret)
