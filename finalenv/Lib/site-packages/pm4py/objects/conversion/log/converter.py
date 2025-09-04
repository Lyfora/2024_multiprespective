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
from enum import Enum

from pm4py.objects.conversion.log.variants import (
    to_event_stream,
    to_event_log,
    to_data_frame,
    to_nx,
)


class Variants(Enum):
    TO_EVENT_LOG = to_event_log
    TO_EVENT_STREAM = to_event_stream
    TO_DATA_FRAME = to_data_frame
    TO_NX = to_nx


TO_EVENT_LOG = Variants.TO_EVENT_LOG
TO_EVENT_STREAM = Variants.TO_EVENT_STREAM
TO_DATA_FRAME = Variants.TO_DATA_FRAME


def apply(log, parameters=None, variant=None):
    if variant is None:
        variant = Variants.TO_EVENT_LOG
    return variant.value.apply(log, parameters=parameters)
