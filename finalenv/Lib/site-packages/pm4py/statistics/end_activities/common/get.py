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
def get_sorted_end_activities_list(end_activities):
    """
    Gets sorted end attributes list

    Parameters
    ----------
    end_activities
        Dictionary of end attributes associated with their count

    Returns
    ----------
    listact
        Sorted end attributes list
    """
    listact = []
    for ea in end_activities:
        listact.append([ea, end_activities[ea]])
    listact = sorted(listact, key=lambda x: x[1], reverse=True)
    return listact


def get_end_activities_threshold(ealist, decreasing_factor):
    """
    Get end attributes cutting threshold

    Parameters
    ----------
    ealist
        Sorted end attributes list
    decreasing_factor
        Decreasing factor of the algorithm

    Returns
    ---------
    threshold
        End attributes cutting threshold
    """

    threshold = ealist[0][1]
    for i in range(1, len(ealist)):
        value = ealist[i][1]
        if value > threshold * decreasing_factor:
            threshold = value
    return threshold
