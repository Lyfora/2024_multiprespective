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
from pm4py.objects.process_tree.exporter.variants import ptml
from pm4py.util import exec_utils
from enum import Enum


class Variants(Enum):
    PTML = ptml


DEFAULT_VARIANT = Variants.PTML


def apply(tree, output_path, variant=DEFAULT_VARIANT, parameters=None):
    """
    Exports the process tree to a file

    Parameters
    ----------------
    tree
        Process tree
    output_path
        Output path
    variant
        Variant of the algorithm:
            - Variants.PTML
    parameters
        Parameters
    """
    return exec_utils.get_variant(variant).apply(
        tree, output_path, parameters=parameters
    )


def serialize(tree, variant=DEFAULT_VARIANT, parameters=None):
    """
    Serializes the process tree into a binary string

    Parameters
    ----------------
    tree
        Process tree
    variant
        Variant of the algorithm:
            - Variants.PTML
    parameters
        Parameters

    Returns
    ---------------
    serialization
        Serialized string
    """
    return exec_utils.get_variant(variant).export_tree_as_string(
        tree, parameters=parameters
    )
