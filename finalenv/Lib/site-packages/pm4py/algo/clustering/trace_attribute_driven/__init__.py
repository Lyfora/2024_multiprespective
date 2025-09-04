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
from pm4py.algo.clustering.trace_attribute_driven import (
    algorithm,
    dfg,
    leven_dist,
    linkage_method,
    merge_log,
    util,
    variants,
)

import warnings
from pm4py.util import constants

if constants.SHOW_INTERNAL_WARNINGS:
    warnings.warn(
        "The clustering.trace_attribute_driven package will be removed in a future release."
    )
