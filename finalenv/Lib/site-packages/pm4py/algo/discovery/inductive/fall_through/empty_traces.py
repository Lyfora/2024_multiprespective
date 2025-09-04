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
from collections import Counter
from typing import Tuple, List, Optional, Dict, Any

from pm4py.algo.discovery.inductive.dtypes.im_ds import (
    IMDataStructureUVCL,
    IMDataStructureDFG,
)
from pm4py.algo.discovery.inductive.fall_through.abc import FallThrough
from pm4py.objects.process_tree.obj import ProcessTree, Operator
from pm4py.objects.dfg.obj import DFG
from pm4py.algo.discovery.inductive.dtypes.im_dfg import InductiveDFG
from copy import copy


class EmptyTracesUVCL(FallThrough[IMDataStructureUVCL]):

    @classmethod
    def apply(
            cls,
            obj: IMDataStructureUVCL,
            pool=None,
            manager=None,
            parameters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[ProcessTree, List[IMDataStructureUVCL]]]:
        if cls.holds(obj, parameters):
            data_structure = copy(obj.data_structure)
            del data_structure[()]
            if data_structure:
                return ProcessTree(operator=Operator.XOR), [
                    IMDataStructureUVCL(Counter()),
                    IMDataStructureUVCL(data_structure),
                ]
            else:
                return ProcessTree(), []
        else:
            return None

    @classmethod
    def holds(
            cls,
            obj: IMDataStructureUVCL,
            parameters: Optional[Dict[str, Any]] = None,
    ) -> bool:
        return len(list(filter(lambda t: len(t) == 0, obj.data_structure))) > 0


class EmptyTracesDFG(FallThrough[IMDataStructureDFG]):
    @classmethod
    def apply(
            cls,
            obj: IMDataStructureDFG,
            pool=None,
            manager=None,
            parameters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[ProcessTree, List[IMDataStructureDFG]]]:
        if cls.holds(obj, parameters):
            # If the DFG itself is empty, the log contains only empty traces -> just τ
            dfg = obj.data_structure.dfg
            is_empty = (
                    len(dfg.graph) == 0
                    and len(dfg.start_activities) == 0
                    and len(dfg.end_activities) == 0
            )

            if is_empty:
                return ProcessTree(), []
                # Otherwise, split: XOR(τ, non-empty part)

            return ProcessTree(operator=Operator.XOR), [
                IMDataStructureDFG(InductiveDFG(DFG())),
                # note: skip intentionally False here; τ is already handled on the XOR
                IMDataStructureDFG(InductiveDFG(obj.data_structure.dfg)),
            ]
        return None

    @classmethod
    def holds(
            cls,
            obj: IMDataStructureDFG,
            parameters: Optional[Dict[str, Any]] = None,
    ) -> bool:
        return obj.data_structure.skip
