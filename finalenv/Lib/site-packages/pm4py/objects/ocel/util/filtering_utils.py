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
from pm4py.objects.ocel import constants
from enum import Enum
from pm4py.objects.ocel.obj import OCEL
from typing import Optional, Dict, Any, Set
from pm4py.util import exec_utils, pandas_utils
import pandas as pd


class Parameters(Enum):
    EVENT_ID = constants.PARAM_EVENT_ID
    OBJECT_ID = constants.PARAM_OBJECT_ID
    OBJECT_TYPE = constants.PARAM_OBJECT_TYPE


def _get_unique_ids(df: pd.DataFrame, column: str) -> Set:
    """Helper function to get unique IDs from a DataFrame column."""
    if df.empty:
        return set()
    return set(pandas_utils.format_unique(df[column].unique()))


def propagate_event_filtering(
        ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None
) -> OCEL:
    """
    Propagates the filtering at the event level to the remaining parts of the OCEL structure
    (objects, relations)

    Parameters
    ----------------
    ocel
        Object-centric event log
    parameters
        Parameters of the algorithm, including:
        - Parameters.EVENT_ID => the column to be used as case identifier
        - Parameters.OBJECT_ID => the column to be used as object identifier
        - Parameters.OBJECT_TYPE => the column to be used as object type

    Returns
    ----------------
    ocel
        Object-centric event log with propagated filter
    """
    if parameters is None:
        parameters = {}

    event_id = exec_utils.get_param_value(
        Parameters.EVENT_ID, parameters, ocel.event_id_column
    )
    object_id = exec_utils.get_param_value(
        Parameters.OBJECT_ID, parameters, ocel.object_id_column
    )
    event_id_2 = event_id + "_2"
    object_id_2 = object_id + "_2"

    # Get unique event IDs efficiently
    selected_event_ids = _get_unique_ids(ocel.events, event_id)
    if not selected_event_ids:
        # If no events are selected, clear all components
        for df_attr in ['events', 'objects', 'relations', 'e2e', 'o2o', 'object_changes']:
            setattr(ocel, df_attr, getattr(ocel, df_attr).iloc[0:0])
        return ocel

    # Filter relations and get unique object IDs in one pass
    ocel.relations = ocel.relations[ocel.relations[event_id].isin(selected_event_ids)]
    selected_object_ids = _get_unique_ids(ocel.relations, object_id)

    # Filter remaining components efficiently
    if selected_object_ids:
        # Apply filters to object-related components
        ocel.objects = ocel.objects[ocel.objects[object_id].isin(selected_object_ids)]
        ocel.object_changes = ocel.object_changes[ocel.object_changes[object_id].isin(selected_object_ids)]

        # Combined filtering for o2o with single mask
        o2o_mask = (ocel.o2o[object_id].isin(selected_object_ids) &
                    ocel.o2o[object_id_2].isin(selected_object_ids))
        ocel.o2o = ocel.o2o[o2o_mask]
    else:
        # If no objects are selected, clear objects-related components
        for df_attr in ['objects', 'o2o', 'object_changes']:
            setattr(ocel, df_attr, getattr(ocel, df_attr).iloc[0:0])

    # Filter e2e with single mask
    e2e_mask = (ocel.e2e[event_id].isin(selected_event_ids) &
                ocel.e2e[event_id_2].isin(selected_event_ids))
    ocel.e2e = ocel.e2e[e2e_mask]

    return ocel


def propagate_object_filtering(
        ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None
) -> OCEL:
    """
    Propagates the filtering at the object level to the remaining parts of the OCEL structure
    (events, relations)

    Parameters
    ----------------
    ocel
        Object-centric event log
    parameters
        Parameters of the algorithm, including:
        - Parameters.EVENT_ID => the column to be used as case identifier
        - Parameters.OBJECT_ID => the column to be used as object identifier
        - Parameters.OBJECT_TYPE => the column to be used as object type

    Returns
    ----------------
    ocel
        Object-centric event log with propagated filter
    """
    if parameters is None:
        parameters = {}

    event_id = exec_utils.get_param_value(
        Parameters.EVENT_ID, parameters, ocel.event_id_column
    )
    object_id = exec_utils.get_param_value(
        Parameters.OBJECT_ID, parameters, ocel.object_id_column
    )
    event_id_2 = event_id + "_2"
    object_id_2 = object_id + "_2"

    # Get unique object IDs efficiently
    selected_object_ids = _get_unique_ids(ocel.objects, object_id)
    if not selected_object_ids:
        # If no objects are selected, clear all components
        for df_attr in ['events', 'objects', 'relations', 'e2e', 'o2o', 'object_changes']:
            setattr(ocel, df_attr, getattr(ocel, df_attr).iloc[0:0])
        return ocel

    # Filter relations and get unique event IDs in one pass
    ocel.relations = ocel.relations[ocel.relations[object_id].isin(selected_object_ids)]
    selected_event_ids = _get_unique_ids(ocel.relations, event_id)

    # Filter event-related components
    if selected_event_ids:
        ocel.events = ocel.events[ocel.events[event_id].isin(selected_event_ids)]

        # Combined filtering for e2e with single mask
        e2e_mask = (ocel.e2e[event_id].isin(selected_event_ids) &
                    ocel.e2e[event_id_2].isin(selected_event_ids))
        ocel.e2e = ocel.e2e[e2e_mask]
    else:
        # If no events are selected, clear events-related components
        for df_attr in ['events', 'e2e']:
            setattr(ocel, df_attr, getattr(ocel, df_attr).iloc[0:0])

    # Filter object-related components
    o2o_mask = (ocel.o2o[object_id].isin(selected_object_ids) &
                ocel.o2o[object_id_2].isin(selected_object_ids))
    ocel.o2o = ocel.o2o[o2o_mask]

    ocel.object_changes = ocel.object_changes[
        ocel.object_changes[object_id].isin(selected_object_ids)
    ]

    return ocel


def propagate_relations_filtering(
        ocel: OCEL, parameters: Optional[Dict[Any, Any]] = None
) -> OCEL:
    """
    Propagates the filtering at the relations level to the remaining parts of the OCEL structure
    (events, objects)

    Parameters
    ----------------
    ocel
        Object-centric event log
    parameters
        Parameters of the algorithm, including:
        - Parameters.EVENT_ID => the column to be used as case identifier
        - Parameters.OBJECT_ID => the column to be used as object identifier
        - Parameters.OBJECT_TYPE => the column to be used as object type

    Returns
    ----------------
    ocel
        Object-centric event log with propagated filter
    """
    if parameters is None:
        parameters = {}

    event_id = exec_utils.get_param_value(
        Parameters.EVENT_ID, parameters, ocel.event_id_column
    )
    object_id = exec_utils.get_param_value(
        Parameters.OBJECT_ID, parameters, ocel.object_id_column
    )
    event_id_2 = event_id + "_2"
    object_id_2 = object_id + "_2"

    # Efficiently get unique IDs from relations
    relation_event_ids = _get_unique_ids(ocel.relations, event_id)
    relation_object_ids = _get_unique_ids(ocel.relations, object_id)

    if not relation_event_ids or not relation_object_ids:
        # If no relations are selected, clear all components
        for df_attr in ['events', 'objects', 'relations', 'e2e', 'o2o', 'object_changes']:
            setattr(ocel, df_attr, getattr(ocel, df_attr).iloc[0:0])
        return ocel

    # Compute intersections with existing components
    existing_event_ids = _get_unique_ids(ocel.events, event_id)
    existing_object_ids = _get_unique_ids(ocel.objects, object_id)

    selected_event_ids = relation_event_ids.intersection(existing_event_ids)
    selected_object_ids = relation_object_ids.intersection(existing_object_ids)

    if not selected_event_ids or not selected_object_ids:
        # If no valid intersections, clear all components
        for df_attr in ['events', 'objects', 'relations', 'e2e', 'o2o', 'object_changes']:
            setattr(ocel, df_attr, getattr(ocel, df_attr).iloc[0:0])
        return ocel

    # Filter all components in a single operation each
    ocel.events = ocel.events[ocel.events[event_id].isin(selected_event_ids)]
    ocel.objects = ocel.objects[ocel.objects[object_id].isin(selected_object_ids)]

    # Filter relations with combined condition
    relations_mask = (ocel.relations[event_id].isin(selected_event_ids) &
                      ocel.relations[object_id].isin(selected_object_ids))
    ocel.relations = ocel.relations[relations_mask]

    # Filter e2e and o2o with combined conditions
    e2e_mask = (ocel.e2e[event_id].isin(selected_event_ids) &
                ocel.e2e[event_id_2].isin(selected_event_ids))
    ocel.e2e = ocel.e2e[e2e_mask]

    o2o_mask = (ocel.o2o[object_id].isin(selected_object_ids) &
                ocel.o2o[object_id_2].isin(selected_object_ids))
    ocel.o2o = ocel.o2o[o2o_mask]

    ocel.object_changes = ocel.object_changes[
        ocel.object_changes[object_id].isin(selected_object_ids)
    ]

    return ocel
