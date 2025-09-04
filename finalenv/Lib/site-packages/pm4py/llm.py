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

import pandas as pd
from pm4py.objects.log.obj import EventLog, EventStream, Trace
from typing import Union, Optional, Dict, Tuple, List, Any
from pm4py.utils import get_properties, constants, check_is_pandas_dataframe
from pm4py.utils import __event_log_deprecation_warning
from pm4py.objects.ocel.obj import OCEL
from tempfile import NamedTemporaryFile
from copy import copy
from pm4py.objects.petri_net.obj import PetriNet, Marking


def openai_query(
    prompt: str,
    api_key: Optional[str] = None,
    openai_model: Optional[str] = None,
    api_url: Optional[str] = None,
    **kwargs
) -> str:
    """
    Executes the provided prompt, obtaining the answer from the OpenAI APIs.

    :param prompt: The prompt to be executed.
    :param api_key: (Optional) OpenAI API key.
    :param openai_model: (Optional) OpenAI model to be used (default: "gpt-3.5-turbo").
    :param api_url: (Optional) OpenAI API URL.
    :param **kwargs: Additional parameters to pass to the OpenAI API.
    :return: The response from the OpenAI API as a string.

    .. code-block:: python3

        import pm4py

        resp = pm4py.llm.openai_query('What is the result of 3+3?', api_key="sk-382393", openai_model="gpt-3.5-turbo")
        print(resp)
    """
    parameters = copy(kwargs) if kwargs is not None else {}
    if api_url is not None:
        parameters["api_url"] = api_url
    if api_key is not None:
        parameters["api_key"] = api_key
    if openai_model is not None:
        parameters["openai_model"] = openai_model

    from pm4py.algo.querying.llm.connectors import openai as perform_query

    return perform_query.apply(prompt, parameters=parameters)


def google_query(
    prompt: str,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    **kwargs
) -> str:
    """
    Executes the provided prompt, obtaining the answer from the Google APIs.

    :param prompt: prompt that should be executed
    :param api_key: API key
    :param model: Model to be used (default: gemini-1.5-flash-002)
    :rtype: ``str``

    .. code-block:: python3

        import pm4py

        resp = pm4py.llm.google_query('what is the result of 3+3?', api_key="sk-382393", model="gemini-1.5-flash-002")
        print(resp)
    """
    parameters = copy(kwargs) if kwargs is not None else {}
    if api_key is not None:
        parameters["api_key"] = api_key
    if model is not None:
        parameters["google_model"] = model

    from pm4py.algo.querying.llm.connectors import google as perform_query

    return perform_query.apply(prompt, parameters=parameters)


def anthropic_query(
    prompt: str,
    api_key: Optional[str] = None,
    model: Optional[str] = None,
    **kwargs
) -> str:
    """
    Executes the provided prompt, obtaining the answer from the Google APIs.

    :param prompt: prompt that should be executed
    :param api_key: API key
    :param model: Model to be used (default: claude-3-5-sonnet-20241022)
    :rtype: ``str``

    .. code-block:: python3

        import pm4py

        resp = pm4py.llm.anthropic_query('what is the result of 3+3?', api_key="sk-382393", model="claude-3-5-sonnet-20241022")
        print(resp)
    """
    parameters = copy(kwargs) if kwargs is not None else {}
    if api_key is not None:
        parameters["api_key"] = api_key
    if model is not None:
        parameters["anthropic_model"] = model

    from pm4py.algo.querying.llm.connectors import anthropic as perform_query

    return perform_query.apply(prompt, parameters=parameters)


def abstract_dfg(
    log_obj: Union[pd.DataFrame, EventLog, EventStream],
    max_len: int = constants.OPENAI_MAX_LEN,
    include_performance: bool = True,
    relative_frequency: bool = False,
    response_header: bool = True,
    primary_performance_aggregation: str = "mean",
    secondary_performance_aggregation: Optional[str] = None,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> str:
    """
    Obtains the DFG (Directly-Follows Graph) abstraction of a traditional event log.

    :param log_obj: The log object to abstract.
    :param max_len: Maximum length of the string abstraction (default: constants.OPENAI_MAX_LEN).
    :param include_performance: Whether to include the performance of the paths in the abstraction.
    :param relative_frequency: Whether to use relative instead of absolute frequency of the paths.
    :param response_header: Whether to include a short header before the paths, describing the abstraction.
    :param primary_performance_aggregation: Primary aggregation method for the arc's performance (default: "mean"). Other options: "median", "min", "max", "sum", "stdev".
    :param secondary_performance_aggregation: (Optional) Secondary aggregation method for the arc's performance (default: None). Other options: "mean", "median", "min", "max", "sum", "stdev".
    :param activity_key: The column name to be used as activity.
    :param timestamp_key: The column name to be used as timestamp.
    :param case_id_key: The column name to be used as case identifier.
    :return: The DFG abstraction as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/roadtraffic100traces.xes")
        print(pm4py.llm.abstract_dfg(log))
    """
    __event_log_deprecation_warning(log_obj)

    parameters = get_properties(
        log_obj,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    parameters["max_len"] = max_len
    parameters["include_performance"] = include_performance
    parameters["relative_frequency"] = relative_frequency
    parameters["response_header"] = response_header
    parameters["primary_performance_aggregation"] = (
        primary_performance_aggregation
    )
    parameters["secondary_performance_aggregation"] = (
        secondary_performance_aggregation
    )

    from pm4py.algo.querying.llm.abstractions import log_to_dfg_descr

    return log_to_dfg_descr.apply(log_obj, parameters=parameters)


def abstract_variants(
    log_obj: Union[pd.DataFrame, EventLog, EventStream],
    max_len: int = constants.OPENAI_MAX_LEN,
    include_performance: bool = True,
    relative_frequency: bool = False,
    response_header: bool = True,
    primary_performance_aggregation: str = "mean",
    secondary_performance_aggregation: Optional[str] = None,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> str:
    """
    Obtains the variants abstraction of a traditional event log.

    :param log_obj: The log object to abstract.
    :param max_len: Maximum length of the string abstraction (default: constants.OPENAI_MAX_LEN).
    :param include_performance: Whether to include the performance of the variants in the abstraction.
    :param relative_frequency: Whether to use relative instead of absolute frequency of the variants.
    :param response_header: Whether to include a short header before the variants, describing the abstraction.
    :param primary_performance_aggregation: Primary aggregation method for the variants' performance (default: "mean"). Other options: "median", "min", "max", "sum", "stdev".
    :param secondary_performance_aggregation: (Optional) Secondary aggregation method for the variants' performance (default: None). Other options: "mean", "median", "min", "max", "sum", "stdev".
    :param activity_key: The column name to be used as activity.
    :param timestamp_key: The column name to be used as timestamp.
    :param case_id_key: The column name to be used as case identifier.
    :return: The variants abstraction as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/roadtraffic100traces.xes")
        print(pm4py.llm.abstract_variants(log))
    """
    __event_log_deprecation_warning(log_obj)

    parameters = get_properties(
        log_obj,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    parameters["max_len"] = max_len
    parameters["include_performance"] = include_performance
    parameters["relative_frequency"] = relative_frequency
    parameters["response_header"] = response_header
    parameters["primary_performance_aggregation"] = (
        primary_performance_aggregation
    )
    parameters["secondary_performance_aggregation"] = (
        secondary_performance_aggregation
    )

    from pm4py.algo.querying.llm.abstractions import log_to_variants_descr

    return log_to_variants_descr.apply(log_obj, parameters=parameters)


def abstract_ocel(ocel: OCEL, include_timestamps: bool = True) -> str:
    """
    Obtains the abstraction of an object-centric event log, including the list of events and the objects of the OCEL.

    :param ocel: The object-centric event log to abstract.
    :param include_timestamps: Whether to include timestamp information in the abstraction.
    :return: The OCEL abstraction as a string.

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel("tests/input_data/ocel/example_log.jsonocel")
        print(pm4py.llm.abstract_ocel(ocel))
    """
    parameters = {}
    parameters["include_timestamps"] = include_timestamps

    from pm4py.algo.transformation.ocel.description import (
        algorithm as ocel_description,
    )

    return ocel_description.apply(ocel, parameters=parameters)


def abstract_ocel_ocdfg(
    ocel: OCEL,
    include_header: bool = True,
    include_timestamps: bool = True,
    max_len: int = constants.OPENAI_MAX_LEN,
) -> str:
    """
    Obtains the abstraction of an object-centric event log, representing the object-centric directly-follows graph in text.

    :param ocel: The object-centric event log to abstract.
    :param include_header: Whether to include a header in the abstraction.
    :param include_timestamps: Whether to include timestamp information in the abstraction.
    :param max_len: Maximum length of the abstraction (default: constants.OPENAI_MAX_LEN).
    :return: The object-centric DFG abstraction as a string.

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel("tests/input_data/ocel/example_log.jsonocel")
        print(pm4py.llm.abstract_ocel_ocdfg(ocel))
    """
    parameters = {}
    parameters["include_header"] = include_header
    parameters["include_timestamps"] = include_timestamps
    parameters["max_len"] = max_len

    from pm4py.algo.querying.llm.abstractions import ocel_ocdfg_descr

    return ocel_ocdfg_descr.apply(ocel, parameters=parameters)


def abstract_ocel_features(
    ocel: OCEL,
    obj_type: str,
    include_header: bool = True,
    max_len: int = constants.OPENAI_MAX_LEN,
    debug: bool = False,
    enable_object_lifecycle_paths: bool = True,
) -> str:
    """
    Obtains the abstraction of an object-centric event log, representing the features and their values in text.

    :param ocel: The object-centric event log to abstract.
    :param obj_type: The object type to consider in feature extraction.
    :param include_header: Whether to include a header in the abstraction.
    :param max_len: Maximum length of the abstraction (default: constants.OPENAI_MAX_LEN).
    :param debug: Enables debugging mode, providing insights into feature extraction steps.
    :param enable_object_lifecycle_paths: Enables the "lifecycle paths" feature in the abstraction.
    :return: The OCEL features abstraction as a string.

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel("tests/input_data/ocel/example_log.jsonocel")
        print(pm4py.llm.abstract_ocel_features(ocel, obj_type="Resource"))
    """
    parameters = {}
    parameters["include_header"] = include_header
    parameters["max_len"] = max_len
    parameters["debug"] = debug
    parameters["enable_object_lifecycle_paths"] = enable_object_lifecycle_paths

    from pm4py.algo.querying.llm.abstractions import ocel_fea_descr

    return ocel_fea_descr.apply(ocel, obj_type, parameters=parameters)


def abstract_event_stream(
    log_obj: Union[pd.DataFrame, EventLog, EventStream],
    max_len: int = constants.OPENAI_MAX_LEN,
    response_header: bool = True,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> str:
    """
    Obtains the event stream abstraction of a traditional event log.

    :param log_obj: The log object to abstract.
    :param max_len: Maximum length of the string abstraction (default: constants.OPENAI_MAX_LEN).
    :param response_header: Whether to include a short header before the event stream, describing the abstraction.
    :param activity_key: The column name to be used as activity.
    :param timestamp_key: The column name to be used as timestamp.
    :param case_id_key: The column name to be used as case identifier.
    :return: The event stream abstraction as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/roadtraffic100traces.xes")
        print(pm4py.llm.abstract_event_stream(log))
    """
    __event_log_deprecation_warning(log_obj)

    parameters = get_properties(
        log_obj,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    parameters["max_len"] = max_len
    parameters["response_header"] = response_header

    from pm4py.algo.querying.llm.abstractions import stream_to_descr

    return stream_to_descr.apply(log_obj, parameters=parameters)


def abstract_petri_net(
    net: PetriNet, im: Marking, fm: Marking, response_header: bool = True
) -> str:
    """
    Obtains an abstraction of a Petri net.

    :param net: The Petri net to abstract.
    :param im: The initial marking of the Petri net.
    :param fm: The final marking of the Petri net.
    :param response_header: Whether to include a header in the abstraction.
    :return: The Petri net abstraction as a string.

    .. code-block:: python3

        import pm4py

        net, im, fm = pm4py.read_pnml('tests/input_data/running-example.pnml')
        print(pm4py.llm.abstract_petri_net(net, im, fm))
    """
    parameters = {}
    parameters["response_header"] = response_header

    from pm4py.algo.querying.llm.abstractions import net_to_descr

    return net_to_descr.apply(net, im, fm, parameters=parameters)


def abstract_log_attributes(
    log_obj: Union[pd.DataFrame, EventLog, EventStream],
    max_len: int = constants.OPENAI_MAX_LEN,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> str:
    """
    Abstracts the attributes of a log by reporting their names, types, and top values.

    :param log_obj: The log object whose attributes are to be abstracted.
    :param max_len: Maximum length of the string abstraction (default: constants.OPENAI_MAX_LEN).
    :param activity_key: The column name to be used as activity.
    :param timestamp_key: The column name to be used as timestamp.
    :param case_id_key: The column name to be used as case identifier.
    :return: The log attributes abstraction as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/roadtraffic100traces.xes")
        print(pm4py.llm.abstract_log_attributes(log))
    """
    __event_log_deprecation_warning(log_obj)

    parameters = get_properties(
        log_obj,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    parameters["max_len"] = max_len

    from pm4py.algo.querying.llm.abstractions import log_to_cols_descr

    return log_to_cols_descr.apply(log_obj, parameters=parameters)


def abstract_log_features(
    log_obj: Union[pd.DataFrame, EventLog, EventStream],
    max_len: int = constants.OPENAI_MAX_LEN,
    include_header: bool = True,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
    case_id_key: str = "case:concept:name",
) -> str:
    """
    Abstracts the machine learning features obtained from a log by reporting the top features until the desired length is achieved.

    :param log_obj: The log object from which to extract features.
    :param max_len: Maximum length of the string abstraction (default: constants.OPENAI_MAX_LEN).
    :param include_header: Whether to include a header in the abstraction.
    :param activity_key: The column name to be used as activity.
    :param timestamp_key: The column name to be used as timestamp.
    :param case_id_key: The column name to be used as case identifier.
    :return: The log features abstraction as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/roadtraffic100traces.xes")
        print(pm4py.llm.abstract_log_features(log))
    """
    __event_log_deprecation_warning(log_obj)

    parameters = get_properties(
        log_obj,
        activity_key=activity_key,
        timestamp_key=timestamp_key,
        case_id_key=case_id_key,
    )
    parameters["max_len"] = max_len
    parameters["include_header"] = include_header

    from pm4py.algo.querying.llm.abstractions import log_to_fea_descr

    return log_to_fea_descr.apply(log_obj, parameters=parameters)


def abstract_temporal_profile(
    temporal_profile: Dict[Tuple[str, str], Tuple[float, float]],
    include_header: bool = True,
) -> str:
    """
    Abstracts a temporal profile model into a descriptive string.

    :param temporal_profile: The temporal profile model to abstract.
    :param include_header: Whether to include a header in the abstraction describing the temporal profile.
    :return: The temporal profile abstraction as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/roadtraffic100traces.xes", return_legacy_log_object=True)
        temporal_profile = pm4py.discover_temporal_profile(log)
        text_abstr = pm4py.llm.abstract_temporal_profile(temporal_profile, include_header=True)
        print(text_abstr)
    """
    parameters = {}
    parameters["include_header"] = include_header

    from pm4py.algo.querying.llm.abstractions import tempprofile_to_descr

    return tempprofile_to_descr.apply(temporal_profile, parameters=parameters)


def abstract_case(
    case: Trace,
    include_case_attributes: bool = True,
    include_event_attributes: bool = True,
    include_timestamp: bool = True,
    include_header: bool = True,
    activity_key: str = "concept:name",
    timestamp_key: str = "time:timestamp",
) -> str:
    """
    Textually abstracts a single case from an event log.

    :param case: The case object to abstract.
    :param include_case_attributes: Whether to include attributes at the case level.
    :param include_event_attributes: Whether to include attributes at the event level.
    :param include_timestamp: Whether to include event timestamps in the abstraction.
    :param include_header: Whether to include a header in the abstraction.
    :param activity_key: The column name to be used as activity.
    :param timestamp_key: The column name to be used as timestamp.
    :return: The case abstraction as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/roadtraffic100traces.xes", return_legacy_log_object=True)
        print(pm4py.llm.abstract_case(log[0]))
    """
    parameters = {}
    parameters["include_case_attributes"] = include_case_attributes
    parameters["include_event_attributes"] = include_event_attributes
    parameters["include_timestamp"] = include_timestamp
    parameters["include_header"] = include_header
    parameters[constants.PARAMETER_CONSTANT_ACTIVITY_KEY] = activity_key
    parameters[constants.PARAMETER_CONSTANT_TIMESTAMP_KEY] = timestamp_key

    from pm4py.algo.querying.llm.abstractions import case_to_descr

    return case_to_descr.apply(case, parameters=parameters)


def abstract_declare(declare_model, include_header: bool = True) -> str:
    """
    Textually abstracts a DECLARE model.

    :param declare_model: The DECLARE model to abstract.
    :param include_header: Whether to include a header in the abstraction.
    :return: The DECLARE model abstraction as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/roadtraffic100traces.xes", return_legacy_log_object=True)
        log_ske = pm4py.discover_declare(log)
        print(pm4py.llm.abstract_declare(log_ske))
    """
    parameters = {}
    parameters["include_header"] = include_header

    from pm4py.algo.querying.llm.abstractions import declare_to_descr

    return declare_to_descr.apply(declare_model, parameters=parameters)


def abstract_log_skeleton(log_skeleton, include_header: bool = True) -> str:
    """
    Textually abstracts a log skeleton process model.

    :param log_skeleton: The log skeleton to abstract.
    :param include_header: Whether to include a header in the abstraction.
    :return: The log skeleton abstraction as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/roadtraffic100traces.xes", return_legacy_log_object=True)
        log_ske = pm4py.discover_log_skeleton(log)
        print(pm4py.llm.abstract_log_skeleton(log_ske))
    """
    parameters = {}
    parameters["include_header"] = include_header

    from pm4py.algo.querying.llm.abstractions import logske_to_descr

    return logske_to_descr.apply(log_skeleton, parameters=parameters)


def __execute_prompt_to_db_query(
    log: pd.DataFrame,
    prompt: str,
    executor=openai_query,
    execute_query: bool = True,
    **kwargs
) -> Union[str, pd.DataFrame]:
    """
    Internal method to retrieve (and execute) a SQL query corresponding to a prompt
    """
    import duckdb

    if check_is_pandas_dataframe(log):
        kwargs["table_name"] = "dataframe"
        dataframe = log

    response = executor(prompt, **kwargs)

    sql_query = ""
    if "```sql" in response:
        sql_query = response.split("```sql")[1].split("```")[0]
    sql_query = sql_query.strip()

    if not sql_query:
        raise Exception(
            "The response does not contain a valid SQL query: " + response
        )

    if execute_query:
        result = duckdb.query(sql_query).to_df()

        if check_is_pandas_dataframe(log):
            return result

    return sql_query


def nlp_to_log_query(
    log: pd.DataFrame,
    query: str,
    executor=openai_query,
    obtain_query: bool = True,
    execute_query: bool = True,
    **kwargs
) -> Union[str, pd.DataFrame]:
    """
    Translates a natural language statement into a database (SQL) query executable against the event log.

    :param log: event log object
    :param query: query expressed in natural language
    :param executor: the connector to the LLM (e.g., pm4py.llm.openai_query)
    :param obtain_query: executes the prompt, to transform the natural statements into a database (SQL) query
    :param execute_query: executes the database (SQL) query against the event data
    :param kwargs: additional keyword arguments to the method
    :rtype: ``Union[str, pd.DataFrame]``

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/running-example.xes")
        resp = pm4py.llm.nlp_to_log_query(log, "How many cases are contained in the event log?", api_key="sk-5HNn")
        print(resp)
    """
    from pm4py.algo.querying.llm.injection import (
        algorithm as domain_knowledge_injector,
    )

    prompt = (
        "Could you provide a database query for the following question?\n\n"
        + query
    )
    prompt += domain_knowledge_injector.apply(log, parameters=kwargs)
    prompt += "\n\nPlease include the SQL query between the ```sql and the ``` tags.\n\n"

    if obtain_query:
        return __execute_prompt_to_db_query(
            log,
            prompt,
            executor=executor,
            execute_query=execute_query,
            **kwargs
        )

    return prompt


def nlp_to_log_filter(
    log: pd.DataFrame,
    filter_query: str,
    executor=openai_query,
    obtain_query: bool = True,
    execute_query: bool = True,
    **kwargs
) -> Union[str, pd.DataFrame]:
    """
    Translates a filtering query expressed in natural language into a database (SQL) query that is used to filter the
    event log.

    :param log: event log object
    :param filter_query: filtering query expressed in natural language
    :param executor: the connector to the LLM (e.g., pm4py.llm.openai_query)
    :param obtain_query: executes the prompt, to transform the natural statements into a database (SQL) query
    :param execute_query: executes the database (SQL) query against the event data
    :param kwargs: additional keyword arguments to the method
    :rtype: ``Union[str, pd.DataFrame]``

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/running-example.xes")
        resp = pm4py.llm.nlp_to_log_filter(log, "There is an event with activity: pay compensation", api_key="sk-5HNn")
        print(resp)
    """
    from pm4py.algo.querying.llm.injection import (
        algorithm as domain_knowledge_injector,
    )

    prompt = (
        "Could you provide a database query to filter all the events of the cases for which at least an event (row) is satisfying the following filtering query?\n\n" +
        filter_query)
    prompt += domain_knowledge_injector.apply(log, parameters=kwargs)
    prompt += "\n\nPlease include the SQL query between the ```sql and the ``` tags.\n\n"

    if obtain_query:
        return __execute_prompt_to_db_query(
            log,
            prompt,
            executor=executor,
            execute_query=execute_query,
            **kwargs
        )

    return prompt


def automated_hypotheses_formulation(
    dataframe: pd.DataFrame,
    executor=openai_query,
    obtain_query: bool = True,
    execute_query: bool = True,
    max_len=10000,
    **kwargs
) -> Union[str, List[Tuple[str, str, Union[str, None]]]]:
    """
    Automatically formulate some hypotheses on the event data.
    The result of this method is either:
    - The prompt (to be executed manually against the LLM) when obtain_query=False
    - A list of different hypotheses. Each hypothesis comes with a description (position 0), the SQL query (position 1),
    and (if execute_query=True) the result of the execution of the query in position 2.

    :param dataframe: event log object
    :param executor: the connector to the LLM (e.g., pm4py.llm.openai_query)
    :param obtain_query: executes the prompt and get the hypotheses along with the corresponding SQL queries
    :param execute_query: executes the obtained SQL queries and stores the results
    :param kwargs: additional keyword arguments to the method
    :param max_len: maximum length of the obtained prompt
    :rtype: ``Union[str, List[Tuple[str, str, Union[str, None]]]]``

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/running-example.xes")
        result = pm4py.llm.automated_hypotheses_formulation(log, api_key="sk-5HN")
        print(result)
    """
    import duckdb
    from pm4py.algo.querying.llm.abstractions import log_to_cols_descr

    from pm4py.algo.querying.llm.injection import (
        algorithm as domain_knowledge_injector,
    )

    prompt = "Could you formulate some hypotheses over the event data? The directly-follows graph, the list of attributes, and additional process mining/database knowledge follow.\n\n"
    prompt += abstract_dfg(dataframe, max_len=int(max_len / 3))
    prompt += "\n\n"
    prompt += log_to_cols_descr.apply(
        dataframe, parameters={"max_len": int(max_len / 3)}
    )
    prompt += "\n\n"
    prompt += domain_knowledge_injector.apply(dataframe, parameters=kwargs)
    prompt += "Please provide each hypothesis along with a SQL query. Please include the SQL queries between the ```sql and the ``` tags.\n"
    prompt += "Can you please include also a description of each hypothesis between some <description> and </description> tags?\n\n"

    if obtain_query:
        response = executor(prompt, **kwargs)

        obtained_queries = response.split("```sql")
        obtained_descriptions = response.split("<description>")

        if (
            len(obtained_queries) > 1
            and len(obtained_descriptions) > 1
            and len(obtained_queries) == len(obtained_descriptions)
        ):
            del obtained_queries[0]
            del obtained_descriptions[0]

            obtained_queries = [x.split("```")[0] for x in obtained_queries]
            obtained_descriptions = [
                x.split("</description>")[0].strip()
                for x in obtained_descriptions
            ]
            obtained_results = [None] * len(obtained_descriptions)

            if execute_query:
                for i in range(len(obtained_queries)):
                    try:
                        obtained_results[i] = duckdb.sql(
                            obtained_queries[i]
                        ).to_df()
                    except BaseException:
                        print("Exception executing query nr. %d" % i)

            return [
                (
                    obtained_descriptions[i],
                    obtained_queries[i],
                    obtained_results[i],
                )
                for i in range(len(obtained_queries))
            ]

    return prompt


def explain_visualization(
    vis_saver, *args, connector=openai_query, **kwargs
) -> str:
    """
    Explains a process mining visualization using LLMs by saving it as a .png image and providing the image to the Large Language Model along with a description.

    :param vis_saver: The visualizer function used to save the visualization to disk.
    :param args: Positional arguments required by the visualizer function.
    :param connector: (Optional) The connector method to communicate with the large language model (default: openai_query).
    :param **kwargs: Additional keyword arguments for the visualizer function or the connector (e.g., annotations, API key).
    :return: The explanation of the visualization as a string.

    .. code-block:: python3

        import pm4py

        log = pm4py.read_xes("tests/input_data/running-example.xes")
        descr = pm4py.llm.explain_visualization(pm4py.save_vis_dotted_chart, log, api_key="sk-5HN", show_legend=False)
        print(descr)
    """
    F = NamedTemporaryFile(suffix=".png")
    image_path = F.name
    F.close()

    description = vis_saver(*args, image_path, **kwargs)

    parameters = copy(kwargs) if kwargs is not None else {}
    parameters["image_path"] = image_path

    return connector(
        "Could you explain the included process mining visualization?\n\n"
        + description,
        **parameters
    )
