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
from typing import Optional, Dict, Any
from enum import Enum
from pm4py.util import exec_utils, pandas_utils
from pm4py.objects.ocel import constants as ocel_constants
from pm4py.objects.ocel.obj import OCEL
from pm4py.statistics.ocel import act_ot_dependent, act_utils, edge_metrics


class Parameters(Enum):
    EVENT_ACTIVITY = ocel_constants.PARAM_EVENT_ACTIVITY
    OBJECT_TYPE = ocel_constants.PARAM_OBJECT_TYPE
    COMPUTE_EDGES_PERFORMANCE = "compute_edges_performance"


def apply(
    ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None
) -> Dict[str, Any]:
    """
    Discovers an OC-DFG model from an object-centric event log.
    Reference paper:
    Berti, Alessandro, and Wil van der Aalst. "Extracting multiple viewpoint models from relational databases." Data-Driven Process Discovery and Analysis. Springer, Cham, 2018. 24-51.

    Parameters
    -----------------
    ocel
        Object-centric event log
    parameters
        Parameters of the algorithm, including:
        - Parameters.EVENT_ACTIVITY => the attribute to be used as activity
        - Parameters.OBJECT_TYPE => the attribute to be used as object type
        - Parameters.COMPUTE_EDGES_PERFORMANCE => (boolean) enables/disables the computation of the performance on the edges

    Returns
    -----------------
    ocdfg
        Object-centric directly-follows graph, expressed as a dictionary containing the following properties:
        - activities: complete set of activities derived from the object-centric event log
        - object_types: complete set of object types derived from the object-centric event log
        - edges: dictionary connecting each object type to a set of directly-followed arcs between activities
        - activities_indep: dictionary linking each activity, regardless of the object type
        - activities_ot: dictionary linking each object type to another dictionary
        - start_activities: dictionary linking each object type to start activities
        - end_activities: dictionary linking each object type to end activities
    """
    if parameters is None:
        parameters = {}

    # Extract parameter values once
    object_type = exec_utils.get_param_value(
        Parameters.OBJECT_TYPE, parameters, ocel.object_type_column
    )
    event_activity = exec_utils.get_param_value(
        Parameters.EVENT_ACTIVITY, parameters, ocel.event_activity
    )
    compute_edges_performance = exec_utils.get_param_value(
        Parameters.COMPUTE_EDGES_PERFORMANCE, parameters, True
    )

    # Pre-compute activities and object types (used multiple times)
    activities = set(pandas_utils.format_unique(ocel.events[event_activity].unique()))
    object_types = set(pandas_utils.format_unique(ocel.objects[object_type].unique()))

    # Initialize result dictionary with pre-computed values
    ret = {
        "activities": activities,
        "object_types": object_types,
        "edges": {},
        "activities_indep": {},
        "activities_ot": {},
        "start_activities": {},
        "end_activities": {},
        "edges_performance": {"event_couples": {}, "total_objects": {}}
    }

    # Process object-type independent associations (shared computation)
    ot_independent = act_utils.find_associations_from_ocel(
        ocel, parameters=parameters
    )
    ret["activities_indep"] = {
        "events": act_utils.aggregate_events(ot_independent),
        "unique_objects": act_utils.aggregate_unique_objects(ot_independent),
        "total_objects": act_utils.aggregate_total_objects(ot_independent)
    }

    # Process object-type dependent associations (shared computation)
    ot_dependent = act_ot_dependent.find_associations_from_ocel(
        ocel, parameters=parameters
    )
    ret["activities_ot"] = {
        "events": act_ot_dependent.aggregate_events(ot_dependent),
        "unique_objects": act_ot_dependent.aggregate_unique_objects(ot_dependent),
        "total_objects": act_ot_dependent.aggregate_total_objects(ot_dependent)
    }

    # Process start activities
    start_parameters = {**parameters, "prefiltering": "start"}
    ot_dependent_start = act_ot_dependent.find_associations_from_ocel(
        ocel, parameters=start_parameters
    )
    ret["start_activities"] = {
        "events": act_ot_dependent.aggregate_events(ot_dependent_start),
        "unique_objects": act_ot_dependent.aggregate_unique_objects(ot_dependent_start),
        "total_objects": act_ot_dependent.aggregate_total_objects(ot_dependent_start)
    }

    # Process end activities
    end_parameters = {**parameters, "prefiltering": "end"}
    ot_dependent_end = act_ot_dependent.find_associations_from_ocel(
        ocel, parameters=end_parameters
    )
    ret["end_activities"] = {
        "events": act_ot_dependent.aggregate_events(ot_dependent_end),
        "unique_objects": act_ot_dependent.aggregate_unique_objects(ot_dependent_end),
        "total_objects": act_ot_dependent.aggregate_total_objects(ot_dependent_end)
    }

    # Process edges
    edges = edge_metrics.find_associations_per_edge(
        ocel, parameters=end_parameters
    )
    ret["edges"] = {
        "event_couples": edge_metrics.aggregate_ev_couples(edges),
        "unique_objects": edge_metrics.aggregate_unique_objects(edges),
        "total_objects": edge_metrics.aggregate_total_objects(edges)
    }

    # Only compute performance metrics if needed
    if compute_edges_performance:
        ret["edges_performance"] = {
            "event_couples": edge_metrics.performance_calculation_ocel_aggregation(
                ocel, ret["edges"]["event_couples"], parameters=parameters
            ),
            "total_objects": edge_metrics.performance_calculation_ocel_aggregation(
                ocel, ret["edges"]["total_objects"], parameters=parameters
            )
        }

    return ret
