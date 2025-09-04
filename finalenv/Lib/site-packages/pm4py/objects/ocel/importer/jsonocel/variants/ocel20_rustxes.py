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
from typing import Optional, Dict, Any
from pm4py.objects.ocel.obj import OCEL


def apply(file_path: str, parameters: Optional[Dict[Any, Any]] = None) -> OCEL:
    """
    Imports an OCEL 2.0 JSON using the RUSTXES parser.

    Parameters
    ---------------
    file_path
        Path to the OCEL 2.0 JSON
    parameters
        Optional parameters.

    Returns
    ---------------
    ocel
        Object-centric event log
    """
    if parameters is None:
        parameters = {}

    import rustxes

    return rustxes.import_ocel_json_pm4py(file_path)
