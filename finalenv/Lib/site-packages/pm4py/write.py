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
The ``pm4py.write`` module contains all functionality related to writing files/objects to disk.
"""

import importlib.util

from pm4py.objects.bpmn.obj import BPMN
from pm4py.objects.log.obj import EventLog
from pm4py.objects.ocel.obj import OCEL
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.objects.process_tree.obj import ProcessTree
from pm4py.utils import __event_log_deprecation_warning
import pandas as pd
from typing import Union, Tuple, Dict, Optional
from pm4py.util import constants
from pm4py.utils import __rustxes_usage_warning, __rustxes_non_usage_warning
from pm4py.util.pandas_utils import (
    check_is_pandas_dataframe,
    check_pandas_dataframe_columns,
)


def write_xes(
    log: Union[EventLog, pd.DataFrame],
    file_path: str,
    case_id_key: str = "case:concept:name",
    extensions=None,
    encoding: str = constants.DEFAULT_ENCODING,
    variant_str: Optional[str] = None,
    **kwargs
) -> None:
    """
    Writes an event log to disk in the XES format (see `xes-standard <https://xes-standard.org/>`_).

    :param log: Log object (``EventLog`` or ``pandas.DataFrame``) that needs to be written to disk.
    :param file_path: Target file path of the event log (``.xes`` file) on disk.
    :param case_id_key: Column key that identifies the case identifier.
    :param extensions: Extensions defined for the event log.
    :param variant_str: Variant to be used (default: line-by-line, rustxes)
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_xes(log, '<path_to_export_to>', case_id_key='case:concept:name')
    """
    __event_log_deprecation_warning(log)

    if check_is_pandas_dataframe(log):
        check_pandas_dataframe_columns(log, case_id_key=case_id_key)

    file_path = str(file_path)
    if not (
        file_path.lower().endswith("xes")
        or file_path.lower().endswith("xes.gz")
    ):
        file_path = file_path + ".xes"

    parameters = {}
    for k, v in kwargs.items():
        parameters[k] = v
    parameters[constants.PARAMETER_CONSTANT_CASEID_KEY] = case_id_key
    parameters["extensions"] = extensions
    parameters["encoding"] = encoding

    if variant_str is None and importlib.util.find_spec("rustxes"):
        __rustxes_usage_warning()
        variant_str = "rustxes"

    if variant_str is None or variant_str == "line_by_line":
        __rustxes_non_usage_warning()
        from pm4py.objects.log.exporter.xes import exporter as xes_exporter
        xes_exporter.apply(log, file_path, variant=xes_exporter.Variants.LINE_BY_LINE, parameters=parameters)
    else:
        import pm4py, rustxes, polars
        log = pm4py.convert_to_dataframe(log)
        log = polars.DataFrame(log)
        rustxes.export_xes(log, file_path)


def write_pnml(
    petri_net: PetriNet,
    initial_marking: Marking,
    final_marking: Marking,
    file_path: str,
    encoding: str = constants.DEFAULT_ENCODING,
) -> None:
    """
    Writes a Petri net object to disk in the ``.pnml`` format (see `pnml-standard <https://www.pnml.org/>`_).

    :param petri_net: Petri net object that needs to be written to disk.
    :param initial_marking: Initial marking of the Petri net.
    :param final_marking: Final marking of the Petri net.
    :param file_path: Target file path on disk of the ``.pnml`` file.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_pnml(petri_net, initial_marking, final_marking, '<path_to_export_to>')
    """
    file_path = str(file_path)
    if not file_path.lower().endswith("pnml"):
        file_path = file_path + ".pnml"

    from pm4py.objects.petri_net.exporter import exporter as petri_exporter

    petri_exporter.apply(
        petri_net,
        initial_marking,
        file_path,
        final_marking=final_marking,
        parameters={"encoding": encoding},
    )


def write_ptml(
    tree: ProcessTree,
    file_path: str,
    auto_layout: bool = True,
    encoding: str = constants.DEFAULT_ENCODING,
) -> None:
    """
    Writes a process tree object to disk in the ``.ptml`` format.

    :param tree: ProcessTree object that needs to be written to disk.
    :param file_path: Target file path on disk of the ``.ptml`` file.
    :param auto_layout: Boolean indicating whether the model should get an auto layout (which is written to disk).
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ptml(tree, '<path_to_export_to>', auto_layout=True)
    """
    file_path = str(file_path)
    if not file_path.lower().endswith("ptml"):
        file_path = file_path + ".ptml"

    from pm4py.objects.process_tree.exporter import exporter as tree_exporter

    tree_exporter.apply(tree, file_path, parameters={"encoding": encoding})


def write_dfg(
    dfg: Dict[Tuple[str, str], int],
    start_activities: Dict[str, int],
    end_activities: Dict[str, int],
    file_path: str,
    encoding: str = constants.DEFAULT_ENCODING,
) -> None:
    """
    Writes a directly follows graph (DFG) object to disk in the ``.dfg`` format.

    :param dfg: Directly follows relation (multiset of activity-activity pairs).
    :param start_activities: Multiset tracking the number of occurrences of start activities.
    :param end_activities: Multiset tracking the number of occurrences of end activities.
    :param file_path: Target file path on disk to write the DFG object to.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_dfg(dfg, start_activities, end_activities, '<path_to_export_to>')
    """
    file_path = str(file_path)
    if not file_path.lower().endswith("dfg"):
        file_path = file_path + ".dfg"

    from pm4py.objects.dfg.exporter import exporter as dfg_exporter

    dfg_exporter.apply(
        dfg,
        file_path,
        parameters={
            dfg_exporter.Variants.CLASSIC.value.Parameters.START_ACTIVITIES: start_activities,
            dfg_exporter.Variants.CLASSIC.value.Parameters.END_ACTIVITIES: end_activities,
            "encoding": encoding,
        },
    )


def write_bpmn(
    model: BPMN,
    file_path: str,
    auto_layout: bool = True,
    encoding: str = constants.DEFAULT_ENCODING,
) -> None:
    """
    Writes a BPMN model object to disk in the ``.bpmn`` format.

    :param model: BPMN model to export.
    :param file_path: Target file path on disk to write the BPMN object to.
    :param auto_layout: Boolean indicating whether the model should get an auto layout (which is written to disk).
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_bpmn(model, '<path_to_export_to>', auto_layout=True)
    """
    file_path = str(file_path)
    if not file_path.lower().endswith("bpmn"):
        file_path = file_path + ".bpmn"

    if auto_layout:
        from pm4py.objects.bpmn.layout import layouter

        model = layouter.apply(model)
    from pm4py.objects.bpmn.exporter import exporter

    exporter.apply(model, file_path, parameters={"encoding": encoding})


def write_ocel(
    ocel: OCEL,
    file_path: str,
    objects_path: str = None,
    encoding: str = constants.DEFAULT_ENCODING,
) -> None:
    """
    Writes an OCEL object to disk in various formats.
    Supported formats include CSV (flat table), JSON-OCEL, XML-OCEL, and SQLite
    (described on the site https://www.ocel-standard.org/).

    :param ocel: OCEL object to write to disk.
    :param file_path: Target file path on disk to write the OCEL object to.
    :param objects_path: Location of the objects table (only applicable in case of .csv exporting).
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ocel(ocel, '<path_to_export_to>')
    """
    file_path = str(file_path)

    if file_path.lower().endswith("csv"):
        write_ocel_csv(ocel, file_path, objects_path, encoding=encoding)
    elif file_path.lower().endswith("jsonocel"):
        write_ocel_json(ocel, file_path, encoding=encoding)
    elif file_path.lower().endswith("xmlocel"):
        write_ocel_xml(ocel, file_path, encoding=encoding)
    elif file_path.lower().endswith("sqlite"):
        write_ocel_sqlite(ocel, file_path, encoding=encoding)
    else:
        raise Exception("Unsupported file format.")


def write_ocel_csv(
    ocel: OCEL,
    file_path: str,
    objects_path: str,
    encoding: str = constants.DEFAULT_ENCODING,
) -> None:
    """
    Writes an OCEL object to disk in the ``.csv`` file format.
    The OCEL object is exported into two separate files, i.e., one event table and one objects table.
    Both file paths should be specified.

    :param ocel: OCEL object.
    :param file_path: Target file path on disk to write the event table to.
    :param objects_path: Target file path on disk to write the objects table to.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ocel_csv(ocel, '<path_to_export_events_to>', '<path_to_export_objects_to>')
    """
    file_path = str(file_path)
    if not file_path.lower().endswith("csv"):
        file_path = file_path + ".csv"

    from pm4py.objects.ocel.exporter.csv import exporter as csv_exporter

    csv_exporter.apply(
        ocel,
        file_path,
        objects_path=objects_path,
        parameters={"encoding": encoding},
    )


def write_ocel_json(
    ocel: OCEL, file_path: str, encoding: str = constants.DEFAULT_ENCODING
) -> None:
    """
    Writes an OCEL object to disk in the ``.jsonocel`` file format.

    :param ocel: OCEL object.
    :param file_path: Target file path on disk to write the OCEL object to.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ocel_json(ocel, '<path_to_export_to>')
    """
    file_path = str(file_path)
    if not file_path.lower().endswith("jsonocel"):
        file_path = file_path + ".jsonocel"

    from pm4py.objects.ocel.exporter.jsonocel import (
        exporter as jsonocel_exporter,
    )

    is_ocel20 = ocel.is_ocel20()
    variant = (
        jsonocel_exporter.Variants.OCEL20
        if is_ocel20
        else jsonocel_exporter.Variants.CLASSIC
    )

    jsonocel_exporter.apply(
        ocel, file_path, variant=variant, parameters={"encoding": encoding}
    )


def write_ocel_xml(
    ocel: OCEL, file_path: str, encoding: str = constants.DEFAULT_ENCODING
) -> None:
    """
    Writes an OCEL object to disk in the ``.xmlocel`` file format.

    :param ocel: OCEL object.
    :param file_path: Target file path on disk to write the OCEL object to.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ocel_xml(ocel, '<path_to_export_to>')
    """
    file_path = str(file_path)
    if not file_path.lower().endswith("xmlocel"):
        file_path = file_path + ".xmlocel"

    from pm4py.objects.ocel.exporter.xmlocel import (
        exporter as xmlocel_exporter,
    )

    xmlocel_exporter.apply(
        ocel,
        file_path,
        variant=xmlocel_exporter.Variants.CLASSIC,
        parameters={"encoding": encoding},
    )


def write_ocel_sqlite(
    ocel: OCEL, file_path: str, encoding: str = constants.DEFAULT_ENCODING
) -> None:
    """
    Writes an OCEL object to disk to a ``SQLite`` database (exported as ``.sqlite`` file).

    :param ocel: OCEL object.
    :param file_path: Target file path to the SQLite database.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ocel_sqlite(ocel, '<path_to_export_to>')
    """
    file_path = str(file_path)
    if not file_path.lower().endswith("sqlite"):
        file_path = file_path + ".sqlite"

    from pm4py.objects.ocel.exporter.sqlite import exporter as sqlite_exporter

    sqlite_exporter.apply(
        ocel,
        file_path,
        variant=sqlite_exporter.Variants.PANDAS_EXPORTER,
        parameters={"encoding": encoding},
    )


def write_ocel2(
    ocel: OCEL, file_path: str, encoding: str = constants.DEFAULT_ENCODING
) -> None:
    """
    Writes an OCEL2.0 object to disk in various formats.
    Supported formats include JSON-OCEL, XML-OCEL, and SQLite.

    :param ocel: OCEL object.
    :param file_path: Target file path to write the OCEL2.0 object to.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ocel2(ocel, '<path_to_export_to>')
    """
    file_path = str(file_path)

    if file_path.lower().endswith("sqlite"):
        write_ocel2_sqlite(ocel, file_path, encoding=encoding)
    elif file_path.lower().endswith("xml") or file_path.lower().endswith(
        "xmlocel"
    ):
        write_ocel2_xml(ocel, file_path, encoding=encoding)
    elif file_path.lower().endswith("jsonocel"):
        write_ocel2_json(ocel, file_path, encoding=encoding)
    else:
        raise Exception("Unsupported file format for OCEL2.0 export.")


def write_ocel2_json(
    ocel: OCEL, file_path: str, encoding: str = constants.DEFAULT_ENCODING
) -> None:
    """
    Writes an OCEL2.0 object to disk in the ``.jsonocel`` file format.

    :param ocel: OCEL object.
    :param file_path: Target file path to the JSON-OCEL file.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ocel2_json(ocel, '<path_to_export_to>')
    """
    file_path = str(file_path)
    if not (file_path.lower().endswith("jsonocel") or file_path.lower().endswith("json")):
        file_path = file_path + ".json"

    from pm4py.objects.ocel.exporter.jsonocel import (
        exporter as jsonocel_exporter,
    )

    jsonocel_exporter.apply(
        ocel,
        file_path,
        variant=jsonocel_exporter.Variants.OCEL20_STANDARD,
        parameters={"encoding": encoding},
    )


def write_ocel2_sqlite(
    ocel: OCEL, file_path: str, encoding: str = constants.DEFAULT_ENCODING
) -> None:
    """
    Writes an OCEL2.0 object to disk to a ``SQLite`` database (exported as ``.sqlite`` file).

    :param ocel: OCEL object.
    :param file_path: Target file path to the SQLite database.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ocel2_sqlite(ocel, '<path_to_export_to>')
    """
    file_path = str(file_path)
    if not file_path.lower().endswith("sqlite"):
        file_path = file_path + ".sqlite"

    from pm4py.objects.ocel.exporter.sqlite import exporter as sqlite_exporter

    sqlite_exporter.apply(
        ocel,
        file_path,
        variant=sqlite_exporter.Variants.OCEL20,
        parameters={"encoding": encoding},
    )


def write_ocel2_xml(
    ocel: OCEL, file_path: str, encoding: str = constants.DEFAULT_ENCODING
) -> None:
    """
    Writes an OCEL2.0 object to disk in the ``.xmlocel`` file format.

    :param ocel: OCEL object.
    :param file_path: Target file path to the XML-OCEL file.
    :param encoding: The encoding to be used (default: utf-8).

    .. code-block:: python3

        import pm4py

        pm4py.write_ocel2_xml(ocel, '<path_to_export_to>')
    """
    file_path = str(file_path)
    if not (file_path.lower().endswith("xmlocel") or file_path.lower().endswith("xml")):
        file_path = file_path + ".xml"

    from pm4py.objects.ocel.exporter.xmlocel import exporter as xml_exporter

    xml_exporter.apply(
        ocel,
        file_path,
        variant=xml_exporter.Variants.OCEL20,
        parameters={"encoding": encoding},
    )
