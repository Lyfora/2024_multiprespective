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
from enum import Enum
from pm4py.algo.conformance.footprints.variants import (
    log_model,
    log_extensive,
    trace_extensive,
)
from pm4py.util import exec_utils
from typing import Optional, Dict, Any, Union, List


class Variants(Enum):
    LOG_MODEL = log_model
    LOG_EXTENSIVE = log_extensive
    TRACE_EXTENSIVE = trace_extensive


def apply(
    log_footprints: Union[Dict[str, Any], List[Dict[str, Any]]],
    model_footprints: Dict[str, Any],
    variant=Variants.LOG_MODEL,
    parameters: Optional[Dict[Any, Any]] = None,
) -> Union[List[Dict[str, Any]], Dict[str, Any]]:
    """
    Apply footprints conformance between a log footprints object
    and a model footprints object

    Parameters
    -----------------
    log_footprints
        Footprints of the log
    model_footprints
        Footprints of the model
    parameters
        Parameters of the algorithm, including:
            - Parameters.STRICT => strict check of the footprints

    Returns
    ------------------
    violations
        Set/dictionary of all the violations between the log footprints
        and the model footprints, OR list of case-per-case violations
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(
        log_footprints, model_footprints, parameters=parameters
    )
