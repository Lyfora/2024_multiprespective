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
def replace_values(dfg1, dfg2):
    """
    Replace edge values specified in a DFG by values from a (potentially bigger) DFG

    Parameters
    -----------
    dfg1
        First specified DFG (where values of edges should be replaces)
    dfg2
        Second specified DFG (from which values should be taken)

    Returns
    -----------
    dfg1
        First specified DFG with overrided values
    """
    for edge in dfg1:
        if edge in dfg2:
            dfg1[edge] = dfg2[edge]
    return dfg1
