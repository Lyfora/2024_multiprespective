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
import sys
from enum import Enum

import numpy as np
import pulp
from pulp import LpProblem, LpMinimize, LpVariable, LpStatus, value, lpSum


class Parameters(Enum):
    REQUIRE_ILP = "require_ilp"
    INTEGRALITY = "integrality"
    BOUNDS = "bounds"


MIN_THRESHOLD = 1e-12
MAX_NUM_CONSTRAINTS = 7  # Maximum safe number of constraints (log10)

# Solver function to maintain compatibility with different versions of PuLP
if hasattr(pulp, "__version__"):
    # New interface
    from pulp import PULP_CBC_CMD

    def solver(prob):
        return prob.solve(PULP_CBC_CMD(msg=0))

else:
    # Old interface
    def solver(prob):
        return prob.solve()


def get_variable_name(index):
    """Generates a variable name with leading zeros to ensure consistent length."""
    return str(index).zfill(MAX_NUM_CONSTRAINTS)


def apply(c, Aub, bub, Aeq=None, beq=None, parameters=None):
    """
    Solves a linear programming problem using PuLP with all inputs as Python lists or lists of lists.
    """
    if type(Aub) is np.matrix:
        Aub = Aub.tolist()

    if type(bub) is np.matrix:
        bub = bub.tolist()

    if type(Aeq) is np.matrix:
        Aeq = Aeq.tolist()

    if type(beq) is np.matrix:
        beq = beq.tolist()

    if parameters is None:
        parameters = {}

    # Get parameters
    require_ilp = parameters.get("require_ilp", False)
    integrality = parameters.get("integrality", None)
    bounds = parameters.get("bounds", None)

    # Initialize the problem
    prob = LpProblem("LP_Problem", LpMinimize)

    # Define decision variables
    num_vars = len(c)

    # Validate integrality and bounds lists
    if integrality is not None and len(integrality) != num_vars:
        raise ValueError(
            "Length of 'integrality' list must be equal to the number of variables."
        )
    if bounds is not None and len(bounds) != num_vars:
        raise ValueError(
            "Length of 'bounds' list must be equal to the number of variables."
        )

    x_vars = []

    for i in range(num_vars):
        var_name = f"x_{get_variable_name(i)}"

        # Determine variable bounds
        lb = None
        ub = None
        if bounds is not None:
            lb, ub = bounds[i]
            # Convert 'None' strings to actual None
            lb = None if lb == "None" else lb
            ub = None if ub == "None" else ub

        # Determine variable category (continuous or integer)
        if integrality is not None:
            # Use integrality list
            cat = "Integer" if integrality[i] else "Continuous"
        elif require_ilp:
            # All variables are integer
            cat = "Integer"
        else:
            # All variables are continuous
            cat = "Continuous"

        x_vars.append(LpVariable(var_name, lowBound=lb, upBound=ub, cat=cat))

    # Build the objective function
    objective_expr = lpSum(
        c[j] * x_vars[j] for j in range(num_vars) if abs(c[j]) >= MIN_THRESHOLD
    )
    prob += objective_expr, "Objective"

    # Add inequality constraints
    for i in range(len(Aub)):
        constraint_expr = lpSum(
            Aub[i][j] * x_vars[j]
            for j in range(num_vars)
            if abs(Aub[i][j]) >= MIN_THRESHOLD
        )
        constraint_name = f"Inequality_Constraint_{get_variable_name(i)}"
        prob += constraint_expr <= bub[i], constraint_name

    # Add equality constraints, if any
    if Aeq is not None and beq is not None:
        for i in range(len(Aeq)):
            constraint_expr = lpSum(
                Aeq[i][j] * x_vars[j]
                for j in range(num_vars)
                if abs(Aeq[i][j]) >= MIN_THRESHOLD
            )
            constraint_name = (
                f"Equality_Constraint_{get_variable_name(i + len(Aub))}"
            )
            prob += constraint_expr == beq[i], constraint_name

    # Solve the problem
    solver(prob)

    return prob


def get_prim_obj_from_sol(sol, parameters=None):
    """
    Retrieves the objective value from the solved LP problem.
    """
    return value(sol.objective)


def get_points_from_sol(sol, parameters=None):
    """
    Retrieves the values of the decision variables from the solved LP problem.
    """
    if parameters is None:
        parameters = {}

    maximize = parameters.get("maximize", False)
    return_when_none = parameters.get("return_when_none", False)
    var_corr = parameters.get("var_corr", {})

    if LpStatus[sol.status] == "Optimal":
        # Extract variable values from the solution
        return [v.varValue for v in sol.variables()]
    elif return_when_none:
        # Return a list of default values if no solution is found
        default_value = sys.float_info.max if maximize else sys.float_info.min
        return [default_value] * len(var_corr)
    else:
        return None
