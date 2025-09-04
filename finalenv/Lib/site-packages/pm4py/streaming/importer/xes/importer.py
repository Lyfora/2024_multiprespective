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
from pm4py.streaming.importer.xes.variants import (
    xes_trace_stream,
    xes_event_stream,
)
from enum import Enum
from pm4py.util import exec_utils


class Variants(Enum):
    XES_EVENT_STREAM = xes_event_stream
    XES_TRACE_STREAM = xes_trace_stream


DEFAULT_VARIANT = Variants.XES_EVENT_STREAM


def apply(path, variant=DEFAULT_VARIANT, parameters=None):
    """
    Imports a stream from a XES log

    Parameters
    ---------------
    path
        Path to the XES log
    variant
        Variant of the importer:
         - Variants.XES_EVENT_STREAM
         - Variants.XES_TRACE_STREAM

    Returns
    ---------------
    streaming_reader
        Streaming XES reader
    """
    return exec_utils.get_variant(variant).apply(path, parameters=parameters)
