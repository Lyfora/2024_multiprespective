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
def merge_groups_based_on_activities(a, b, groups):
    group_a = None
    group_b = None
    for group in groups:
        if a in group:
            group_a = group
        if b in group:
            group_b = group
    groups = [
        group for group in groups if group != group_a and group != group_b
    ]
    groups.append(group_a.union(group_b))
    return groups


def merge_lists_based_on_activities(a, b, groups):
    group_a = []
    group_b = []
    for group in groups:
        if a in group:
            group_a = group
        if b in group:
            group_b = group
    if group_a is group_b:
        return groups
    groups = [
        group for group in groups if group != group_a and group != group_b
    ]
    groups.append(group_a + group_b)
    return groups
