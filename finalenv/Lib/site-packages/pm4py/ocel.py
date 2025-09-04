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
The ``pm4py.ocel`` module contains the object-centric process mining features offered in ``pm4py``.
"""

from typing import List, Dict, Collection, Any, Optional, Set, Tuple

import pandas as pd

from pm4py.objects.ocel.obj import OCEL
from pm4py.util import constants, pandas_utils
import sys
import random


def ocel_get_object_types(ocel: OCEL) -> List[str]:
    """
    Returns the list of object types contained in the object-centric event log
    (e.g., ["order", "item", "delivery"]).

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: List of object types.
    :rtype: List[str]

    .. code-block:: python3

        import pm4py

        object_types = pm4py.ocel_get_object_types(ocel)
    """
    return pandas_utils.format_unique(
        ocel.objects[ocel.object_type_column].unique()
    )


def ocel_get_attribute_names(ocel: OCEL) -> List[str]:
    """
    Returns the list of attributes at the event and object levels of an object-centric event log
    (e.g., ["cost", "amount", "name"]).

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: List of attribute names.
    :rtype: List[str]

    .. code-block:: python3

        import pm4py

        attribute_names = pm4py.ocel_get_attribute_names(ocel)
    """
    from pm4py.objects.ocel.util import attributes_names

    return attributes_names.get_attribute_names(ocel)


def ocel_flattening(ocel: OCEL, object_type: str) -> pd.DataFrame:
    """
    Flattens the object-centric event log to a traditional event log based on a chosen object type.
    In the flattened log, the objects of the specified type are treated as cases, and each case
    contains the set of events related to that object.
    The flattened log follows the XES notations for case identifier, activity, and timestamp. Specifically:
    - "case:concept:name" is used for the case ID.
    - "concept:name" is used for the activity.
    - "time:timestamp" is used for the timestamp.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param object_type: The object type to use as cases.
    :type object_type: str
    :return: Flattened traditional event log.
    :rtype: pd.DataFrame

    .. code-block:: python3

        import pm4py

        event_log = pm4py.ocel_flattening(ocel, 'items')
    """
    from pm4py.objects.ocel.util import flattening

    return flattening.flatten(ocel, object_type)


def ocel_object_type_activities(ocel: OCEL) -> Dict[str, Collection[str]]:
    """
    Returns the set of activities performed for each object type.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: Dictionary mapping object types to their associated activities.
    :rtype: Dict[str, Collection[str]]

    .. code-block:: python3

        import pm4py

        ot_activities = pm4py.ocel_object_type_activities(ocel)
    """
    from pm4py.statistics.ocel import ot_activities

    return ot_activities.get_object_type_activities(ocel)


def ocel_objects_ot_count(ocel: OCEL) -> Dict[str, Dict[str, int]]:
    """
    Returns the count of related objects per type for each event.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: Nested dictionary mapping events to object types and their counts.
    :rtype: Dict[str, Dict[str, int]]

    .. code-block:: python3

        import pm4py

        objects_ot_count = pm4py.ocel_objects_ot_count(ocel)
    """
    from pm4py.statistics.ocel import objects_ot_count

    return objects_ot_count.get_objects_ot_count(ocel)


def ocel_temporal_summary(ocel: OCEL) -> pd.DataFrame:
    """
    Returns the temporal summary of an object-centric event log.
    The temporal summary aggregates all events that occur at the same timestamp
    and reports the list of activities and involved objects.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: Temporal summary DataFrame.
    :rtype: pd.DataFrame

    .. code-block:: python3

        import pm4py

        temporal_summary = pm4py.ocel_temporal_summary(ocel)
    """
    gdf = ocel.relations.groupby(ocel.event_timestamp)
    act_comb = gdf[ocel.event_activity].agg(list).to_frame()
    obj_comb = gdf[ocel.object_id_column].agg(list).to_frame()
    temporal_summary = act_comb.join(obj_comb).reset_index()
    return temporal_summary


def ocel_objects_summary(ocel: OCEL) -> pd.DataFrame:
    """
    Returns the objects summary of an object-centric event log.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: Objects summary DataFrame containing lifecycle information and interacting objects.
    :rtype: pd.DataFrame

    .. code-block:: python3

        import pm4py

        objects_summary = pm4py.ocel_objects_summary(ocel)
    """
    gdf = ocel.relations.groupby(ocel.object_id_column)
    act_comb = (
        gdf[ocel.event_activity]
        .agg(list)
        .to_frame()
        .rename(columns={ocel.event_activity: "activities_lifecycle"})
    )
    lif_start_tim = (
        gdf[ocel.event_timestamp]
        .min()
        .to_frame()
        .rename(columns={ocel.event_timestamp: "lifecycle_start"})
    )
    lif_end_tim = (
        gdf[ocel.event_timestamp]
        .max()
        .to_frame()
        .rename(columns={ocel.event_timestamp: "lifecycle_end"})
    )
    objects_summary = act_comb.join(lif_start_tim)
    objects_summary = objects_summary.join(lif_end_tim)
    objects_summary = objects_summary.reset_index()
    objects_summary["lifecycle_duration"] = pandas_utils.get_total_seconds(
        objects_summary["lifecycle_end"] - objects_summary["lifecycle_start"]
    )
    ev_rel_obj = (
        ocel.relations.groupby(ocel.event_id_column)[ocel.object_id_column]
        .agg(list)
        .to_dict()
    )
    objects_ids = pandas_utils.format_unique(
        ocel.objects[ocel.object_id_column].unique()
    )
    graph = {o: set() for o in objects_ids}
    for ev in ev_rel_obj:
        rel_obj = ev_rel_obj[ev]
        for o1 in rel_obj:
            for o2 in rel_obj:
                if o1 != o2:
                    graph[o1].add(o2)
    objects_summary["interacting_objects"] = objects_summary[
        ocel.object_id_column
    ].map(graph)
    return objects_summary


def ocel_objects_interactions_summary(ocel: OCEL) -> pd.DataFrame:
    """
    Returns the objects interactions summary of an object-centric event log.
    The summary includes a row for every combination of (event, related object, other related object).
    Properties such as the activity of the event and the object types of the two related objects are included.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: Objects interactions summary DataFrame.
    :rtype: pd.DataFrame

    .. code-block:: python3

        import pm4py

        interactions_summary = pm4py.ocel_objects_interactions_summary(ocel)
    """
    obj_types = (
        ocel.objects.groupby(ocel.object_id_column)[ocel.object_type_column]
        .first()
        .to_dict()
    )
    eve_activities = (
        ocel.events.groupby(ocel.event_id_column)[ocel.event_activity]
        .first()
        .to_dict()
    )
    ev_rel_obj = (
        ocel.relations.groupby(ocel.event_id_column)[ocel.object_id_column]
        .agg(list)
        .to_dict()
    )
    stream = []
    for ev in ev_rel_obj:
        rel_obj = ev_rel_obj[ev]
        for o1 in rel_obj:
            for o2 in rel_obj:
                if o1 != o2:
                    stream.append(
                        {
                            ocel.event_id_column: ev,
                            ocel.event_activity: eve_activities[ev],
                            ocel.object_id_column: o1,
                            ocel.object_type_column: obj_types[o1],
                            f"{ocel.object_id_column}_2": o2,
                            f"{ocel.object_type_column}_2": obj_types[o2],
                        }
                    )

    return pandas_utils.instantiate_dataframe(stream)


def discover_ocdfg(
    ocel: OCEL,
    business_hours: bool = False,
    business_hour_slots: Optional[
        List[Tuple[int, int]]
    ] = constants.DEFAULT_BUSINESS_HOUR_SLOTS,
) -> Dict[str, Any]:
    """
    Discovers an Object-Centric Directly-Follows Graph (OC-DFG) from an object-centric event log.

    Object-centric directly-follows multigraphs are a composition of directly-follows graphs for each object type.
    These graphs can be annotated with different metrics considering the entities of an object-centric event log
    (i.e., events, unique objects, total objects).

    Reference paper:
    Berti, Alessandro, and Wil van der Aalst. "Extracting multiple viewpoint models from relational databases."
    Data-Driven Process Discovery and Analysis. Springer, Cham, 2018. 24-51.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param business_hours: Enable the usage of business hours if set to True.
    :type business_hours: bool
    :param business_hour_slots: Work schedule of the company, provided as a list of tuples where each tuple
                                 represents one time slot of business hours. Each tuple consists of a start
                                 and an end time given in seconds since week start, e.g.,
                                 [(25200, 61200), (9072, 43200), (46800, 61200)] meaning that business hours
                                 are Mondays 07:00 - 17:00, Tuesdays 02:32 - 12:00, and Wednesdays 13:00 - 17:00.
    :type business_hour_slots: Optional[List[Tuple[int, int]]]
    :return: OC-DFG discovery result.
    :rtype: Dict[str, Any]

    .. code-block:: python3

        import pm4py

        ocdfg = pm4py.discover_ocdfg(ocel)
    """
    parameters = {
        "business_hours": business_hours,
        "business_hour_slots": business_hour_slots,
    }
    from pm4py.algo.discovery.ocel.ocdfg import algorithm as ocdfg_discovery

    return ocdfg_discovery.apply(ocel, parameters=parameters)


def discover_oc_petri_net(
    ocel: OCEL,
    inductive_miner_variant: str = "im",
    diagnostics_with_tbr: bool = False,
) -> Dict[str, Any]:
    """
    Discovers an object-centric Petri net from the provided object-centric event log.

    Reference paper: van der Aalst, Wil MP, and Alessandro Berti.
    "Discovering object-centric Petri nets." Fundamenta Informaticae 175.1-4 (2020): 1-40.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param inductive_miner_variant: Variant of the inductive miner to use ("im" for traditional; "imd" for the faster inductive miner directly-follows).
    :type inductive_miner_variant: str
    :param diagnostics_with_tbr: Enable the computation of diagnostics using token-based replay if set to True.
    :type diagnostics_with_tbr: bool
    :return: Discovered object-centric Petri net.
    :rtype: Dict[str, Any]

    .. code-block:: python3

        import pm4py

        ocpn = pm4py.discover_oc_petri_net(ocel)
    """
    from pm4py.algo.discovery.ocel.ocpn import algorithm as ocpn_discovery

    parameters = {
        "inductive_miner_variant": inductive_miner_variant,
        "diagnostics_with_token_based_replay": diagnostics_with_tbr,
    }

    return ocpn_discovery.apply(ocel, parameters=parameters)


def discover_objects_graph(
    ocel: OCEL, graph_type: str = "object_interaction"
) -> Set[Tuple[str, str]]:
    """
    Discovers an object graph from the provided object-centric event log.

    Available graph types:
    - "object_interaction"
    - "object_descendants"
    - "object_inheritance"
    - "object_cobirth"
    - "object_codeath"

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param graph_type: Type of graph to consider.
                       Options include "object_interaction", "object_descendants",
                       "object_inheritance", "object_cobirth", "object_codeath".
    :type graph_type: str
    :return: Discovered object graph as a set of tuples.
    :rtype: Set[Tuple[str, str]]

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        obj_graph = pm4py.discover_objects_graph(ocel, graph_type='object_interaction')
    """
    if graph_type == "object_interaction":
        from pm4py.algo.transformation.ocel.graphs import (
            object_interaction_graph,
        )

        return object_interaction_graph.apply(ocel)
    elif graph_type == "object_descendants":
        from pm4py.algo.transformation.ocel.graphs import (
            object_descendants_graph,
        )

        return object_descendants_graph.apply(ocel)
    elif graph_type == "object_inheritance":
        from pm4py.algo.transformation.ocel.graphs import (
            object_inheritance_graph,
        )

        return object_inheritance_graph.apply(ocel)
    elif graph_type == "object_cobirth":
        from pm4py.algo.transformation.ocel.graphs import object_cobirth_graph

        return object_cobirth_graph.apply(ocel)
    elif graph_type == "object_codeath":
        from pm4py.algo.transformation.ocel.graphs import object_codeath_graph

        return object_codeath_graph.apply(ocel)
    else:
        raise ValueError(f"Unsupported graph_type: {graph_type}")


def ocel_o2o_enrichment(
    ocel: OCEL, included_graphs: Optional[Collection[str]] = None
) -> OCEL:
    """
    Enriches the OCEL with information inferred from graph computations by inserting them into the O2O relations.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param included_graphs: Types of graphs to include, provided as a list or set of strings.
                            Options include "object_interaction_graph", "object_descendants_graph",
                            "object_inheritance_graph", "object_cobirth_graph", "object_codeath_graph".
    :type included_graphs: Optional[Collection[str]]
    :return: Enriched object-centric event log.
    :rtype: OCEL

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        ocel = pm4py.ocel_o2o_enrichment(ocel)
        print(ocel.o2o)
    """
    from pm4py.algo.transformation.ocel.graphs import ocel20_computation

    return ocel20_computation.apply(
        ocel, parameters={"included_graphs": included_graphs}
    )


def ocel_e2o_lifecycle_enrichment(ocel: OCEL) -> OCEL:
    """
    Enriches the OCEL with lifecycle-based information, indicating when an object is created, terminated,
    or has other types of relations, by updating the E2O relations.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: Enriched object-centric event log with lifecycle information.
    :rtype: OCEL

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        ocel = pm4py.ocel_e2o_lifecycle_enrichment(ocel)
        print(ocel.relations)
    """
    from pm4py.objects.ocel.util import e2o_qualification

    ocel = e2o_qualification.apply(ocel, "termination")
    ocel = e2o_qualification.apply(ocel, "creation")
    ocel = e2o_qualification.apply(ocel, "other")
    return ocel


def sample_ocel_objects(ocel: OCEL, num_objects: int) -> OCEL:
    """
    Returns a sampled object-centric event log containing a random subset of objects.
    Only events related to at least one of the sampled objects are included in the returned log.
    Note that this sampling may disrupt the relationships between different objects.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param num_objects: Number of objects to include in the sampled event log.
    :type num_objects: int
    :return: Sampled object-centric event log.
    :rtype: OCEL

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        sampled_ocel = pm4py.sample_ocel_objects(ocel, 50)  # Keeps only 50 random objects
    """
    from pm4py.objects.ocel.util import sampling

    return sampling.sample_ocel_objects(
        ocel, parameters={"num_entities": num_objects}
    )


def sample_ocel_connected_components(
    ocel: OCEL,
    connected_components: int = 1,
    max_num_events_per_cc: int = sys.maxsize,
    max_num_objects_per_cc: int = sys.maxsize,
    max_num_e2o_relations_per_cc: int = sys.maxsize,
) -> OCEL:
    """
    Returns a sampled object-centric event log containing a specified number of connected components.
    Users can also set maximum limits on the number of events, objects, and E2O relations per connected component.

    Reference paper:
    Adams, Jan Niklas, et al. "Defining cases and variants for object-centric event data."
    2022 4th International Conference on Process Mining (ICPM). IEEE, 2022.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param connected_components: Number of connected components to include in the sampled event log.
    :type connected_components: int
    :param max_num_events_per_cc: Maximum number of events allowed per connected component (default: sys.maxsize).
    :type max_num_events_per_cc: int
    :param max_num_objects_per_cc: Maximum number of objects allowed per connected component (default: sys.maxsize).
    :type max_num_objects_per_cc: int
    :param max_num_e2o_relations_per_cc: Maximum number of event-to-object relationships allowed per connected component (default: sys.maxsize).
    :type max_num_e2o_relations_per_cc: int
    :return: Sampled object-centric event log containing the specified connected components.
    :rtype: OCEL

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        sampled_ocel = pm4py.sample_ocel_connected_components(ocel, 5)  # Keeps only 5 connected components
    """
    from pm4py.algo.transformation.ocel.split_ocel import algorithm

    ocel_splits = algorithm.apply(
        ocel, variant=algorithm.Variants.CONNECTED_COMPONENTS
    )
    events = None
    objects = None
    relations = None
    ocel_splits = [
        x
        for x in ocel_splits
        if len(x.events) <= max_num_events_per_cc
        and len(x.objects) <= max_num_objects_per_cc
        and len(x.relations) <= max_num_e2o_relations_per_cc
    ]

    if len(ocel_splits) > 0:
        ocel_splits = random.sample(
            ocel_splits, min(connected_components, len(ocel_splits))
        )

    for cc in ocel_splits:
        if events is None:
            events = cc.events
            objects = cc.objects
            relations = cc.relations
        else:
            events = pandas_utils.concat([events, cc.events])
            objects = pandas_utils.concat([objects, cc.objects])
            relations = pandas_utils.concat([relations, cc.relations])

    return OCEL(events, objects, relations)


def ocel_drop_duplicates(ocel: OCEL) -> OCEL:
    """
    Removes duplicate relations between events and objects that occur at the same time,
    have the same activity, and are linked to the same object identifier.
    This effectively cleans the OCEL by eliminating duplicate events.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: Cleaned object-centric event log without duplicate relations.
    :rtype: OCEL

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        ocel = pm4py.ocel_drop_duplicates(ocel)
    """
    from pm4py.objects.ocel.util import filtering_utils

    ocel.relations = ocel.relations.drop_duplicates(
        subset=[
            ocel.event_activity,
            ocel.event_timestamp,
            ocel.object_id_column,
        ]
    )
    ocel = filtering_utils.propagate_relations_filtering(ocel)
    return ocel


def ocel_merge_duplicates(
    ocel: OCEL, have_common_object: Optional[bool] = False
) -> OCEL:
    """
    Merges events in the OCEL that have the same activity and timestamp. Optionally, ensures that
    the events being merged share a common object.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param have_common_object: If set to True, only merges events that share a common object. Defaults to False.
    :type have_common_object: Optional[bool]
    :return: Object-centric event log with merged duplicate events.
    :rtype: OCEL

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        ocel = pm4py.ocel_merge_duplicates(ocel)
    """
    import copy
    import uuid

    relations = copy.copy(ocel.relations)
    if have_common_object:
        relations["@@groupn"] = relations.groupby(
            [ocel.object_id_column, ocel.event_activity, ocel.event_timestamp]
        ).ngroup()
    else:
        relations["@@groupn"] = relations.groupby(
            [ocel.event_activity, ocel.event_timestamp]
        ).ngroup()

    group_size = relations["@@groupn"].value_counts().to_dict()
    relations["@@groupsize"] = relations["@@groupn"].map(group_size)
    relations = relations.sort_values(
        ["@@groupsize", "@@groupn"], ascending=False
    )
    val_corr = {
        x: str(uuid.uuid4())
        for x in pandas_utils.format_unique(relations["@@groupn"].unique())
    }
    relations = (
        relations.groupby(ocel.event_id_column).first()["@@groupn"].to_dict()
    )
    relations = {x: val_corr[y] for x, y in relations.items()}

    ocel.events[ocel.event_id_column] = ocel.events[ocel.event_id_column].map(
        relations
    )
    ocel.relations[ocel.event_id_column] = ocel.relations[
        ocel.event_id_column
    ].map(relations)

    ocel.events = ocel.events.drop_duplicates(subset=[ocel.event_id_column])
    ocel.relations = ocel.relations.drop_duplicates(
        subset=[ocel.event_id_column, ocel.object_id_column]
    )

    return ocel


def ocel_sort_by_additional_column(
    ocel: OCEL, additional_column: str, primary_column: str = "ocel:timestamp"
) -> OCEL:
    """
    Sorts the OCEL based on the primary timestamp column and an additional column to determine
    the order of events occurring at the same timestamp.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param additional_column: Additional column to use for sorting.
    :type additional_column: str
    :param primary_column: Primary column to use for sorting (default: "ocel:timestamp").
                           Typically the timestamp column.
    :type primary_column: str
    :return: Sorted object-centric event log.
    :rtype: OCEL

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        ocel = pm4py.ocel_sort_by_additional_column(ocel, 'ordering')
    """
    ocel.events = pandas_utils.insert_index(
        ocel.events, "@@index", reset_index=False, copy_dataframe=False
    )
    ocel.events = ocel.events.sort_values(
        [primary_column, additional_column, "@@index"]
    )
    del ocel.events["@@index"]
    ocel.events = ocel.events.reset_index(drop=True)
    return ocel


def ocel_add_index_based_timedelta(ocel: OCEL) -> OCEL:
    """
    Adds a small time delta to the timestamp column based on the event index to ensure the correct ordering
    of events within any object-centric process mining solution.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :return: Object-centric event log with index-based time deltas added.
    :rtype: OCEL

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        ocel = pm4py.ocel_add_index_based_timedelta(ocel)
    """
    from datetime import timedelta

    eids = ocel.events[ocel.event_id_column].to_numpy().tolist()
    eids = {eids[i]: timedelta(milliseconds=i) for i in range(len(eids))}
    ocel.events["@@timedelta"] = ocel.events[ocel.event_id_column].map(eids)
    ocel.relations["@@timedelta"] = ocel.relations[ocel.event_id_column].map(
        eids
    )
    ocel.events[ocel.event_timestamp] = (
        ocel.events[ocel.event_timestamp] + ocel.events["@@timedelta"]
    )
    ocel.relations[ocel.event_timestamp] = (
        ocel.relations[ocel.event_timestamp] + ocel.relations["@@timedelta"]
    )
    del ocel.events["@@timedelta"]
    del ocel.relations["@@timedelta"]
    return ocel


def cluster_equivalent_ocel(
    ocel: OCEL, object_type: str, max_objs: int = sys.maxsize
) -> Dict[str, Collection[OCEL]]:
    """
    Clusters the object-centric event log based on the 'executions' of a single object type.
    Equivalent 'executions' are grouped together in the output dictionary.

    :param ocel: Object-centric event log.
    :type ocel: OCEL
    :param object_type: Reference object type for clustering.
    :type object_type: str
    :param max_objs: Maximum number of objects (of the specified object type) to include per cluster.
                    Defaults to sys.maxsize.
    :type max_objs: int
    :return: Dictionary mapping cluster descriptions to collections of equivalent OCELs.
    :rtype: Dict[str, Collection[OCEL]]

    .. code-block:: python3

        import pm4py

        ocel = pm4py.read_ocel('trial.ocel')
        clusters = pm4py.cluster_equivalent_ocel(ocel, "order")
    """
    from pm4py.algo.transformation.ocel.split_ocel import (
        algorithm as split_ocel_algorithm,
    )
    from pm4py.objects.ocel.util import rename_objs_ot_tim_lex
    from pm4py.algo.transformation.ocel.description import (
        algorithm as ocel_description,
    )

    lst_ocels = split_ocel_algorithm.apply(
        ocel,
        variant=split_ocel_algorithm.Variants.ANCESTORS_DESCENDANTS,
        parameters={"object_type": object_type, "max_objs": max_objs},
    )
    ret = {}
    for index, oc in enumerate(lst_ocels):
        oc_ren = rename_objs_ot_tim_lex.apply(oc)
        descr = ocel_description.apply(
            oc_ren, parameters={"include_timestamps": False}
        )
        if descr not in ret:
            ret[descr] = []
        ret[descr].append(oc)
    return ret
