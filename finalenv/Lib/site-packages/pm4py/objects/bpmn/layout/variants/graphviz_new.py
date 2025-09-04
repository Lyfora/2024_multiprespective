'''
    PM4Py â€“ A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschrÃ¤nkt)

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
from pm4py.objects.bpmn.obj import BPMN
from typing import Optional, Dict, Any

from copy import copy
import tempfile


def apply(
    bpmn_graph: BPMN, parameters: Optional[Dict[Any, Any]] = None
) -> BPMN:
    """
    Layouts the BPMN graphviz using directly the information about node positioning
    and edges waypoints provided in the SVG obtained from Graphviz.

    Parameters
    -----------------
    bpmn_graph
        BPMN graph
    parameters
        Optional parameters of the method

    Returns
    ----------------
    layouted_bpmn
        Layouted BPMN
    """
    if parameters is None:
        parameters = {}

    from pm4py.visualization.bpmn.variants import classic as bpmn_visualizer
    from pm4py.visualization.common import svg_pos_parser

    layout = bpmn_graph.get_layout()

    filename_svg = tempfile.NamedTemporaryFile(suffix=".svg")
    filename_svg.close()

    vis_parameters = copy(parameters)
    vis_parameters["format"] = "svg"
    vis_parameters["include_name_in_events"] = False
    vis_parameters["endpoints_shape"] = "box"

    gviz = bpmn_visualizer.apply(bpmn_graph, parameters=vis_parameters)
    bpmn_visualizer.save(gviz, filename_svg.name)

    # print(filename_svg.name)

    nodes_p, edges_p = svg_pos_parser.apply(filename_svg.name)

    for node in list(bpmn_graph.get_nodes()):
        node_id = str(id(node))
        if node_id in nodes_p:
            node_info = nodes_p[node_id]
            if node_info["polygon"] is not None:
                min_x = min(x[0] for x in node_info["polygon"])
                max_x = max(x[0] for x in node_info["polygon"])
                min_y = min(x[1] for x in node_info["polygon"])
                max_y = max(x[1] for x in node_info["polygon"])

                width = max_x - min_x
                height = max_y - min_y

                layout.get(node).set_width(width)
                layout.get(node).set_height(height)
                layout.get(node).set_x(min_x)
                layout.get(node).set_y(min_y)

    for flow in list(bpmn_graph.get_flows()):
        flow_id = (str(id(flow.source)), str(id(flow.target)))
        if flow_id in edges_p:
            flow_info = edges_p[flow_id]
            if flow_info["waypoints"] is not None:
                flow.del_waypoints()

                for wayp in flow_info["waypoints"]:
                    flow.add_waypoint(wayp)

    return bpmn_graph
