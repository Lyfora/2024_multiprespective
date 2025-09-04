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
from pm4py.objects.process_tree import semantics
from pm4py.util import exec_utils
from typing import Optional, Dict, Any, Union
from pm4py.objects.log.obj import EventLog
from pm4py.objects.process_tree.obj import ProcessTree


class Parameters:
    NO_TRACES = "num_traces"


def apply(
    tree: ProcessTree,
    parameters: Optional[Dict[Union[str, Parameters], Any]] = None,
) -> EventLog:
    """
    Generate a log by a playout operation

    Parameters
    ---------------
    tree
        Process tree
    parameters
        Parameters of the algorithm, including:
        - Parameters.NO_TRACES: number of traces of the playout

    Returns
    --------------
    log
        Simulated log
    """
    if parameters is None:
        parameters = {}

    no_traces = exec_utils.get_param_value(
        Parameters.NO_TRACES, parameters, 1000
    )

    log = semantics.generate_log(tree, no_traces=no_traces)

    return log
