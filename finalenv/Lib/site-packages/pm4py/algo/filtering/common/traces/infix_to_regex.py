'''
    PM4Py â€“ A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschrÃ¤nkt)

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
from re import escape


def translate_infix_to_regex(infix):
    regex = "^"
    for i, act in enumerate(infix):
        is_last_activity = i == (len(infix) - 1)
        if act == "...":
            if is_last_activity:
                regex = f"{regex[:-1]}(,[^,]*)*"
            else:
                regex = f"{regex}([^,]*,)*"
        else:
            if act:
                act = escape(act)

            if is_last_activity:
                regex = f"{regex}{act}"
            else:
                regex = f"{regex}{act},"

    regex = f"{regex}$"
    return regex
