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
from typing import Optional, Dict, Any, Union

import pandas as pd

from pm4py.algo.discovery.temporal_profile.variants import log, dataframe
from pm4py.objects.log.obj import EventLog
from pm4py.util import typing, pandas_utils


def apply(
    elog: Union[EventLog, pd.DataFrame],
    parameters: Optional[Dict[Any, Any]] = None,
) -> typing.TemporalProfile:
    """
    Discovers the temporal profile out of the provided log object.

    Implements the approach described in:
    Stertz, Florian, Jürgen Mangler, and Stefanie Rinderle-Ma. "Temporal Conformance Checking at Runtime based on Time-infused Process Models." arXiv preprint arXiv:2008.07262 (2020).

    Parameters
    ----------
    elog
        Event log
    parameters
        Parameters, including:
        - Parameters.ACTIVITY_KEY => the attribute to use as activity
        - Parameters.START_TIMESTAMP_KEY => the attribute to use as start timestamp
        - Parameters.TIMESTAMP_KEY => the attribute to use as timestamp

    Returns
    -------
    temporal_profile
        Temporal profile of the log
    """
    if pandas_utils.check_is_pandas_dataframe(elog):
        return dataframe.apply(elog, parameters=parameters)

    return log.apply(elog, parameters=parameters)
