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
def derive_end_activities_from_log(log, activity_key):
    """
    Derive end activities from log

    Parameters
    -----------
    log
        Log object
    activity_key
        Activity key

    Returns
    -----------
    e
        End activities
    """
    e = set()
    for t in log:
        if len(t) > 0:
            if activity_key in t[len(t) - 1]:
                e.add(t[len(t) - 1][activity_key])
    return e


def derive_start_activities_from_log(log, activity_key):
    """
    Derive start activities from log

    Parameters
    -----------
    log
        Log object
    activity_key
        Activity key

    Returns
    -----------
    s
        Start activities
    """
    s = set()
    for t in log:
        if len(t) > 0:
            if activity_key in t[0]:
                s.add(t[0][activity_key])
    return s
