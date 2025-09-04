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
from pm4py.util import exec_utils
from pm4py.objects.ocel.exporter.sqlite.variants import pandas_exporter, ocel20
from pm4py.objects.ocel.obj import OCEL
from typing import Optional, Dict, Any


class Variants(Enum):
    PANDAS_EXPORTER = pandas_exporter
    OCEL20 = ocel20


def apply(
    ocel: OCEL,
    target_path: str,
    variant=Variants.PANDAS_EXPORTER,
    parameters: Optional[Dict[Any, Any]] = None,
):
    """
    Exports an OCEL to a SQLite database

    Parameters
    -------------
    ocel
        Object-centric event log
    target_path
        Path to the SQLite database
    variant
        Variant to use. Possible values:
        - Variants.PANDAS_EXPORTER => Pandas exporter
    parameters
        Variant-specific parameters
    """
    if parameters is None:
        parameters = {}

    return exec_utils.get_variant(variant).apply(
        ocel, target_path, parameters=parameters
    )
