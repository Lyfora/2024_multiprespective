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
def pick_chosen_points(m, n):
    """
    Pick chosen points in a list

    Parameters
    ------------
    m
        Number of wanted points
    n
        Number of current points

    Returns
    ------------
    indexes
        Indexes of chosen points
    """
    return [i * n // m + n // (2 * m) for i in range(m)]


def pick_chosen_points_list(m, lst, include_extremes=True):
    """
    Pick a chosen number of points from a list

    Parameters
    -------------
    m
        Number of wanted points
    lst
        List

    Returns
    -------------
    reduced_lst
        Reduced list
    """
    n = len(lst)
    points = pick_chosen_points(m, n)

    if include_extremes:
        if 0 not in points:
            points = [0] + points

        if len(lst)-1 not in points:
            points = points + [len(lst)-1]

    ret = []
    for i in points:
        ret.append(lst[i])

    return ret
