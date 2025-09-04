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
from typing import Optional, Dict, Any
from pm4py.util import exec_utils, constants
from enum import Enum
import shutil
import json
import tempfile
import importlib.resources
from pm4py.util import vis_utils


class Parameters(Enum):
    ENCODING = "encoding"
    IFRAME_WIDTH = "iframe_width"
    IFRAME_HEIGHT = "iframe_height"
    LOCAL_JUPYTER_FILE_NAME = "local_jupyter_file_name"
    PERFORMANCE_AGGREGATION_MEASURE = "aggregationMeasure"
    ANNOTATION = "annotation"
    ACT_METRIC = "act_metric"
    EDGE_METRIC = "edge_metric"
    ACT_THRESHOLD = "act_threshold"
    EDGE_THRESHOLD = "edge_threshold"


def wrap_text(text: str, max_length: int = 15) -> str:
    words = text.split()
    current_line = []
    current_length = 0
    result_lines = []

    for word in words:
        # If adding this word to the current line would exceed max_length
        if (
            current_length + len(word) + (1 if current_line else 0)
            > max_length
        ):
            # Join the current line into a string and append to results
            result_lines.append(" ".join(current_line))
            # Start a new line with the current word
            current_line = [word]
            current_length = len(word)
        else:
            # Add this word to the current line
            if current_line:
                current_length += len(word) + 1  # plus one for the space
            else:
                current_length += len(word)
            current_line.append(word)

    # Append the last line if there is any leftover
    if current_line:
        result_lines.append(" ".join(current_line))

    return "\n".join(result_lines)


def get_html_file_contents():
    with importlib.resources.path(
        "pm4py.visualization.ocel.ocdfg.util", "elkjs_ocdfg.html"
    ) as p:
        with open(str(p), "r") as html_file:
            return html_file.read()


def apply(
    ocdfg: Dict[str, Any], parameters: Optional[Dict[Any, Any]] = None
) -> str:
    """
    Visualizes an OC-DFG using ELK.JS

    Parameters
    ---------------
    ocdfg
        OC-DFG
    parameters
        Parameters of the algorithm:
        - Parameters.ACT_METRIC => the metric to use for the activities. Available values:
            - "events" => number of events (default)
            - "unique_objects" => number of unique objects
            - "total_objects" => number of total objects
        - Parameters.EDGE_METRIC => the metric to use for the edges. Available values:
            - "event_couples" => number of event couples (default)
            - "unique_objects" => number of unique objects
            - "total_objects" => number of total objects
        - Parameters.ANNOTATION => the annotation to use for the visualization. Values:
            - "frequency": frequency annotation
            - "performance": performance annotation
        - Parameters.PERFORMANCE_AGGREGATION_MEASURE => the aggregation measure to use for the performance:
            - mean
            - median
            - min
            - max
            - sum

    Returns
    ---------------
    viz
        Visualization file
    """
    if parameters is None:
        parameters = {}

    from statistics import mean, median

    encoding = exec_utils.get_param_value(
        Parameters.ENCODING, parameters, constants.DEFAULT_ENCODING
    )
    aggregation_measure = exec_utils.get_param_value(
        Parameters.PERFORMANCE_AGGREGATION_MEASURE, parameters, "mean"
    )
    act_key = exec_utils.get_param_value(
        Parameters.ACT_METRIC, parameters, "events"
    )
    edge_key = exec_utils.get_param_value(
        Parameters.EDGE_METRIC, parameters, "event_couples"
    )
    annotation = exec_utils.get_param_value(
        Parameters.ANNOTATION, parameters, "frequency"
    )
    act_threshold = exec_utils.get_param_value(
        Parameters.ACT_THRESHOLD, parameters, 0
    )
    edge_threshold = exec_utils.get_param_value(
        Parameters.EDGE_THRESHOLD, parameters, 0
    )

    pref_act = (
        " E="
        if act_key == "events"
        else " UO=" if act_key == "unique_objects" else " TO="
    )
    pref_edge = (
        " EC="
        if edge_key == "event_couples"
        else " UO=" if edge_key == "unique_objects" else " TO="
    )

    data = {"objectTypes": [], "overallActivityStats": {}}
    added_activities = set()

    for act, ent in ocdfg["activities_indep"][act_key].items():
        if len(ent) >= act_threshold:
            data["overallActivityStats"][wrap_text(act)] = {
                "totalFrequency": pref_act + str(len(ent))
            }
            added_activities.add(act)

    counter = 0

    for ot, content in ocdfg["activities_ot"][act_key].items():
        counter += 1
        list_item = {}
        list_item["objType"] = str(counter)
        list_item["headerLabel"] = wrap_text(ot)
        list_item["activities"] = []
        for act, ent in content.items():
            if act in added_activities:
                list_item["activities"].append(
                    {"name": wrap_text(act), "frequency": pref_act + str(len(ent))}
                )

        content2 = ocdfg["edges"][edge_key][ot]
        content3 = ocdfg["edges_performance"][edge_key][ot]
        content4 = ocdfg["start_activities"][act_key][ot]
        content5 = ocdfg["end_activities"][act_key][ot]

        list_edges = []
        for tup, ent in content2.items():
            perf = content3[tup]
            if aggregation_measure == "median":
                perf = median(perf)
            elif aggregation_measure == "min":
                perf = min(perf)
            elif aggregation_measure == "max":
                perf = max(perf)
            elif aggregation_measure == "sum":
                perf = sum(perf)
            else:
                perf = mean(perf)

            if tup[0] in added_activities and tup[1] in added_activities:
                if len(ent) >= edge_threshold:
                    list_edges.append(
                        {
                            "source": wrap_text(tup[0]),
                            "target": wrap_text(tup[1]),
                            "frequency": pref_edge + str(len(ent)),
                            "performance": vis_utils.human_readable_stat(perf),
                        }
                    )

        for act, ent in content4.items():
            if act in added_activities:
                if len(ent) >= edge_threshold:
                    list_edges.append(
                        {
                            "source": "Start",
                            "target": wrap_text(act),
                            "frequency": pref_edge + str(len(ent)),
                            "performance": vis_utils.human_readable_stat(0.0),
                        }
                    )

        for act, ent in content5.items():
            if act in added_activities:
                if len(ent) >= edge_threshold:
                    list_edges.append(
                        {
                            "source": wrap_text(act),
                            "target": "End",
                            "frequency": pref_edge + str(len(ent)),
                            "performance": vis_utils.human_readable_stat(0.0),
                        }
                    )

        list_item["edges"] = list_edges

        data["objectTypes"].append(list_item)

    stru = json.dumps(data, indent=2)

    if annotation == "frequency":
        suffix = (
            "drawGraph(data, {showFrequency: true, showPerformance: false});\n"
        )
    else:
        suffix = (
            "drawGraph(data, {showFrequency: false, showPerformance: true});\n"
        )

    stru = "const data = " + stru + ";\n\n" + suffix

    F = tempfile.NamedTemporaryFile(suffix=".html")
    F.close()

    F = open(F.name, "w", encoding=encoding)
    F.write(get_html_file_contents().replace("REPLACE", stru))
    F.close()

    return F.name


def view(temp_file_name, parameters=None):
    """
    View the SNA visualization on the screen

    Parameters
    -------------
    temp_file_name
        Temporary file name
    parameters
        Possible parameters of the algorithm
    """
    if parameters is None:
        parameters = {}

    if constants.DEFAULT_ENABLE_VISUALIZATIONS_VIEW:
        iframe_width = exec_utils.get_param_value(
            Parameters.IFRAME_WIDTH, parameters, 900
        )
        iframe_height = exec_utils.get_param_value(
            Parameters.IFRAME_HEIGHT, parameters, 600
        )
        local_jupyter_file_name = exec_utils.get_param_value(
            Parameters.LOCAL_JUPYTER_FILE_NAME,
            parameters,
            "jupyter_bpmn_vis.html",
        )

        if vis_utils.check_visualization_inside_jupyter():
            from IPython.display import IFrame

            shutil.copyfile(temp_file_name, local_jupyter_file_name)
            iframe = IFrame(
                local_jupyter_file_name,
                width=iframe_width,
                height=iframe_height,
            )
            from IPython.display import display

            return display(iframe)
        else:
            vis_utils.open_opsystem_image_viewer(temp_file_name)


def save(temp_file_name, dest_file, parameters=None):
    """
    Save the SNA visualization from a temporary file to a well-defined destination file

    Parameters
    -------------
    temp_file_name
        Temporary file name
    dest_file
        Destination file
    parameters
        Possible parameters of the algorithm
    """
    if parameters is None:
        parameters = {}

    shutil.copyfile(temp_file_name, dest_file)
