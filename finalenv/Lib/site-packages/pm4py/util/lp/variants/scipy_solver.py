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

import numpy as np
from scipy.optimize import linprog, OptimizeResult
from typing import Optional, Dict, Any, List
from pm4py.util import exec_utils


class Parameters:
    INTEGRALITY = "integrality"
    METHOD = "method"
    BOUNDS = "bounds"


def apply(
    c: list,
    Aub: np.ndarray,
    bub: np.matrix,
    Aeq: np.matrix,
    beq: np.matrix,
    parameters: Optional[Dict[Any, Any]] = None,
) -> OptimizeResult:
    if parameters is None:
        parameters = {}

    integrality = exec_utils.get_param_value(
        Parameters.INTEGRALITY, parameters, None
    )
    bounds = exec_utils.get_param_value(Parameters.BOUNDS, parameters, None)

    sol = linprog(
        c,
        A_ub=Aub,
        b_ub=bub,
        A_eq=Aeq,
        b_eq=beq,
        integrality=integrality,
        bounds=bounds,
    )

    return sol


def get_prim_obj_from_sol(
    sol: OptimizeResult, parameters: Optional[Dict[Any, Any]] = None
) -> int:
    if sol.fun is not None:
        return round(sol.fun)


def get_points_from_sol(
    sol: OptimizeResult, parameters: Optional[Dict[Any, Any]] = None
) -> List[int]:
    if sol.x is not None:
        return [round(y) for y in sol.x]
