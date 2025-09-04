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
from pm4py.algo.conformance.alignments.decomposed.variants import (
    recompos_maximal,
)
from enum import Enum
from pm4py.util import exec_utils
from typing import Optional, Dict, Any, Union
from pm4py.objects.log.obj import EventLog
from pm4py.objects.petri_net.obj import PetriNet, Marking
from pm4py.util import typing
import pandas as pd


class Variants(Enum):
    RECOMPOS_MAXIMAL = recompos_maximal


VERSIONS = {Variants.RECOMPOS_MAXIMAL}


def apply(
    log: Union[EventLog, pd.DataFrame],
    net: PetriNet,
    im: Marking,
    fm: Marking,
    variant=Variants.RECOMPOS_MAXIMAL,
    parameters: Optional[Dict[Any, Any]] = None,
) -> typing.ListAlignments:
    """
    Apply the recomposition alignment approach
    to a log and a Petri net performing decomposition

    Parameters
    --------------
    log
        Event log
    net
        Petri net
    im
        Initial marking
    fm
        Final marking
    variant
        Variant of the algorithm, possible values:
            - Variants.RECOMPOS_MAXIMAL
    parameters
        Parameters of the algorithm

    Returns
    --------------
    aligned_traces
        For each trace, return its alignment
    """
    return exec_utils.get_variant(variant).apply(
        log, net, im, fm, parameters=parameters
    )
