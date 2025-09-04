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
from typing import Union, Optional, Dict, Any
import pandas as pd
from sqlite3 import Connection as SQ3_Connection
from pm4py.objects.ocel.obj import OCEL
from pm4py.util import pandas_utils, exec_utils
from pm4py.algo.querying.llm.injection.db_knowledge.variants import (
    pandas_duckdb,
    sqlite3_traditional,
)


def apply(
    db: Union[pd.DataFrame, SQ3_Connection, OCEL],
    variant=None,
    parameters: Optional[Dict[Any, Any]] = None,
) -> str:
    """
    Provides a string containing the required database knowledge for database querying
    (in order for the LLM to produce meaningful queries).

    Parameters
    ---------------
    db
        Database
    variant
        Variant of the method to be used (pandas_duckdb, sqlite3_traditional)
    parameters
        Variant-specific parameters

    Returns
    ---------------
    db_knowledge
        String containing the required database knowledge.
    """
    if parameters is None:
        parameters = {}

    if variant is None:
        if pandas_utils.check_is_pandas_dataframe(db):
            variant = pandas_duckdb
        elif isinstance(db, SQ3_Connection):
            variant = sqlite3_traditional

    if variant is None:
        return "\n\n"

    return exec_utils.get_variant(variant).apply(db, parameters)
