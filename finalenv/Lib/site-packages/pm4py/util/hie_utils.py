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


def indent_representation(model_string, max_indent=sys.maxsize):
    model_string = (
        model_string.replace("\n", "")
        .replace("\r", "")
        .replace("\t", "")
        .strip()
    )

    indent_level = 0
    list_strs = []
    formatted_str = ""
    for char in model_string:
        if char in "({":
            formatted_str += char
            indent_level += 1
            if indent_level <= max_indent:
                list_strs.append(formatted_str)
                formatted_str = "\t" * indent_level
        elif char in ")}":
            indent_level -= 1
            if indent_level < max_indent:
                if formatted_str[-1] not in "({":
                    list_strs.append(formatted_str)
                    formatted_str = "\t" * indent_level
            formatted_str += char
        elif char == ",":
            formatted_str += char
            if indent_level <= max_indent:
                list_strs.append(formatted_str)
                formatted_str = "\t" * indent_level
        else:
            formatted_str += char
    list_strs.append(formatted_str)
    return list_strs
