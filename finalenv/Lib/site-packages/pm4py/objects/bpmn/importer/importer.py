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

from pm4py.objects.bpmn.importer.variants import lxml
from pm4py.util import exec_utils


class Variants(Enum):
    LXML = lxml


DEFAULT_VARIANT = Variants.LXML


def apply(path, variant=DEFAULT_VARIANT, parameters=None):
    """
    Imports a BPMN diagram from a file

    Parameters
    -------------
    path
        Path to the file
    variant
        Variant of the algorithm to use, possible values:
        - Variants.LXML
    parameters
        Parameters of the algorithm

    Returns
    -------------
    bpmn_graph
        BPMN graph
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(path, parameters=parameters)


def deserialize(bpmn_string, variant=DEFAULT_VARIANT, parameters=None):
    """
    Deserialize a text/binary string representing a BPMN 2.0

    Parameters
    -------------
    bpmn_string
        BPMN string
    variant
        Variant of the algorithm to use, possible values:
        - Variants.LXML
    parameters
        Parameters of the algorithm

    Returns
    -------------
    bpmn_graph
        BPMN graph
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).import_from_string(
        bpmn_string, parameters=parameters
    )
