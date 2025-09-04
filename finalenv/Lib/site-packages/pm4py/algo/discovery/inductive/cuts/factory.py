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
from typing import List, Optional, Tuple, TypeVar, Dict, Any

from pm4py.algo.discovery.inductive.cuts.abc import Cut
from pm4py.algo.discovery.inductive.cuts.concurrency import (
    ConcurrencyCutUVCL,
    ConcurrencyCutDFG,
)
from pm4py.algo.discovery.inductive.cuts.loop import LoopCutUVCL, LoopCutDFG
from pm4py.algo.discovery.inductive.cuts.sequence import (
    StrictSequenceCutUVCL,
    StrictSequenceCutDFG,
    SequenceCutUVCL,
    SequenceCutDFG,
)
from pm4py.algo.discovery.inductive.cuts.xor import (
    ExclusiveChoiceCutUVCL,
    ExclusiveChoiceCutDFG,
)
from pm4py.algo.discovery.inductive.dtypes.im_ds import (
    IMDataStructure,
    IMDataStructureUVCL,
    IMDataStructureDFG,
)
from pm4py.algo.discovery.inductive.variants.instances import IMInstance
from pm4py.objects.process_tree.obj import ProcessTree
from pm4py.util import exec_utils
from enum import Enum


T = TypeVar("T", bound=IMDataStructure)
S = TypeVar("S", bound=Cut)


class Parameters(Enum):
    DISABLE_STRICT_SEQUENCE_CUT = "disable_strict_sequence_cut"


class CutFactory:

    @classmethod
    def get_cuts(
        cls,
        obj: T,
        inst: IMInstance,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[S]:
        if parameters is None:
            parameters = {}

        disable_strict_sequence_cut = exec_utils.get_param_value(
            Parameters.DISABLE_STRICT_SEQUENCE_CUT, parameters, False
        )

        if inst is IMInstance.IM or inst is IMInstance.IMf:
            if type(obj) is IMDataStructureUVCL:
                sequence_cut = StrictSequenceCutUVCL
                if disable_strict_sequence_cut:
                    sequence_cut = SequenceCutUVCL
                return [
                    ExclusiveChoiceCutUVCL,
                    sequence_cut,
                    ConcurrencyCutUVCL,
                    LoopCutUVCL,
                ]
        if inst is IMInstance.IMd:
            if type(obj) is IMDataStructureDFG:
                sequence_cut = StrictSequenceCutDFG
                if disable_strict_sequence_cut:
                    sequence_cut = SequenceCutDFG
                return [
                    ExclusiveChoiceCutDFG,
                    sequence_cut,
                    ConcurrencyCutDFG,
                    LoopCutDFG,
                ]
        return list()

    @classmethod
    def find_cut(
        cls,
        obj: IMDataStructure,
        inst: IMInstance,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[ProcessTree, List[T]]]:
        for c in CutFactory.get_cuts(obj, inst, parameters):
            r = c.apply(obj, parameters)
            if r is not None:
                return r
        return None
