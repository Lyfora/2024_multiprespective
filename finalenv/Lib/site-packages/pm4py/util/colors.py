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
def get_corr_hex(num):
    """
    Gets correspondence between a number
    and an hexadecimal string

    Parameters
    -------------
    num
        Number

    Returns
    -------------
    hex_string
        Hexadecimal string
    """
    if num < 10:
        return str(int(num))
    elif num < 11:
        return "A"
    elif num < 12:
        return "B"
    elif num < 13:
        return "C"
    elif num < 14:
        return "D"
    elif num < 15:
        return "E"
    elif num < 16:
        return "F"


def get_transitions_color(count_move_on_model, count_fit):
    """
    Gets the color associated to the transition

    Parameters
    ------------
    count_move_on_model
        Number of move on models
    count_fit
        Number of fit moves

    Returns
    -----------
    color
        Color associated to the transition
    """
    factor = int(
        255.0
        * float(count_fit)
        / float(count_move_on_model + count_fit + 0.00001)
    )
    first = get_corr_hex(int(factor / 16))
    second = get_corr_hex(factor % 16)
    return "#FF" + first + second + first + second


def get_string_from_int_below_255(factor):
    """
    Gets a string from an integer below 255

    Parameters
    ---------------
    factor
        Factor

    Returns
    ---------------
    stru
        Length 2 string
    """
    first = get_corr_hex(int(factor / 16))
    second = get_corr_hex(factor % 16)
    return first + second
