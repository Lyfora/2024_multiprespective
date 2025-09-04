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
from typing import Optional, Tuple, List, Dict, Any

from pm4py.algo.discovery.inductive.dtypes.im_ds import IMDataStructureUVCL
from pm4py.algo.discovery.inductive.fall_through.abc import FallThrough
from pm4py.objects.process_tree.obj import ProcessTree, Operator
from pm4py.util.compression import util as comut
from pm4py.util.compression.dtypes import UVCL


class StrictTauLoopUVCL(FallThrough[IMDataStructureUVCL]):

    @classmethod
    def _get_projected_log(
        cls, log: UVCL, parameters: Optional[Dict[str, Any]] = None
    ) -> UVCL:
        start_activities = comut.get_start_activities(log)
        end_activities = comut.get_end_activities(log)
        proj = Counter()
        for t in log:
            x = 0
            for i in range(1, len(t)):
                if t[i] in start_activities and t[i - 1] in end_activities:
                    proj.update({t[x:i]: log[t]})
                    x = i
            proj.update({t[x: len(t)]: log[t]})
        return proj

    @classmethod
    def holds(
        cls,
        obj: IMDataStructureUVCL,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> bool:
        log = obj.data_structure
        return sum(cls._get_projected_log(log).values()) > sum(log.values())

    @classmethod
    def apply(
        cls,
        obj: IMDataStructureUVCL,
        pool=None,
        manager=None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[ProcessTree, List[IMDataStructureUVCL]]]:
        log = obj.data_structure
        proj = cls._get_projected_log(log)
        if sum(proj.values()) > sum(log.values()):
            return ProcessTree(operator=Operator.LOOP), [
                IMDataStructureUVCL(proj),
                IMDataStructureUVCL(Counter()),
            ]
