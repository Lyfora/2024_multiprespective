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
from pm4py.util import exec_utils
from pm4py.streaming.importer.csv.variants import csv_event_stream


class Variants(Enum):
    CSV_EVENT_STREAM = csv_event_stream


DEFAULT_VARIANT = Variants.CSV_EVENT_STREAM


def apply(path, variant=DEFAULT_VARIANT, parameters=None):
    """
    Reads a stream object from a CSV file

    Parameters
    ---------------
    path
        Path to the CSV file
    variant
        Variant of the importer, possible values:
         - Variants.CSV_EVENT_STREAM
    parameters
        Parameters of the importer

    Returns
    --------------
    stream_obj
        Stream object
    """
    return exec_utils.get_variant(variant).apply(path, parameters=parameters)
