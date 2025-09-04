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
from typing import Optional, Dict, Any

from pm4py.objects.ocel.importer.csv.variants import pandas
from pm4py.objects.ocel.obj import OCEL
from pm4py.util import exec_utils


class Variants(Enum):
    PANDAS = pandas


def apply(
    file_path: str,
    objects_path: str = None,
    variant=Variants.PANDAS,
    parameters: Optional[Dict[Any, Any]] = None,
) -> OCEL:
    """
    Imports an object-centric event log from a CSV file

    Parameters
    -----------------
    file_path
        Path to the object-centric event log
    objects_path
        Optional path to a CSV file containing the objects dataframe
    variant
        Variant of the algorithm that should be used, possible values:
        - Variants.PANDAS
    parameters
        Parameters of the algorithm

    Returns
    ------------------
    ocel
        Object-centric event log
    """
    return exec_utils.get_variant(variant).apply(
        file_path, objects_path, parameters
    )
