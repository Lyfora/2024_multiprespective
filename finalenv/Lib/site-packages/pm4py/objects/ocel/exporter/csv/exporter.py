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

from pm4py.objects.ocel.exporter.csv.variants import pandas
from pm4py.objects.ocel.obj import OCEL
from pm4py.util import exec_utils


class Variants(Enum):
    PANDAS = pandas


def apply(
    ocel: OCEL,
    output_path: str,
    variant=Variants.PANDAS,
    objects_path=None,
    parameters: Optional[Dict[Any, Any]] = None,
):
    """
    Exports an object-centric event log in a CSV file

    Parameters
    -----------------
    ocel
        Object-centric event log
    output_path
        Destination file
    variant
        Variant of the algorithm that should be used, possible values:
        - Variants.PANDAS
    objects_path
        Optional path, where the objects dataframe is stored
    parameters
        Parameters of the algorithm
    """
    return exec_utils.get_variant(variant).apply(
        ocel, output_path, objects_path=objects_path, parameters=parameters
    )
