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
def get_sorted_start_activities_list(start_activities):
    """
    Gets sorted start attributes list

    Parameters
    ----------
    start_activities
        Dictionary of start attributes associated with their count

    Returns
    ----------
    listact
        Sorted start attributes list
    """
    listact = []
    for sa in start_activities:
        listact.append([sa, start_activities[sa]])
    listact = sorted(listact, key=lambda x: x[1], reverse=True)
    return listact


def get_start_activities_threshold(salist, decreasing_factor):
    """
    Get start attributes cutting threshold

    Parameters
    ----------
    salist
        Sorted start attributes list
    decreasing_factor
        Decreasing factor of the algorithm

    Returns
    ---------
    threshold
        Start attributes cutting threshold
    """
    threshold = salist[0][1]
    for i in range(1, len(salist)):
        value = salist[i][1]
        if value > threshold * decreasing_factor:
            threshold = value
    return threshold
