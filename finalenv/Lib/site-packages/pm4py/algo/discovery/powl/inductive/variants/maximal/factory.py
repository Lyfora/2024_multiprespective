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

from typing import List, Optional, Dict, Any, Tuple, Type

from pm4py.algo.discovery.powl.inductive.cuts.concurrency import (
    POWLConcurrencyCutUVCL,
)
from pm4py.algo.discovery.powl.inductive.cuts.factory import S, T, CutFactory
from pm4py.algo.discovery.powl.inductive.cuts.loop import POWLLoopCutUVCL
from pm4py.algo.discovery.powl.inductive.cuts.sequence import (
    POWLStrictSequenceCutUVCL,
)
from pm4py.algo.discovery.powl.inductive.cuts.xor import (
    POWLExclusiveChoiceCutUVCL,
)
from pm4py.algo.discovery.inductive.dtypes.im_ds import IMDataStructureUVCL
from pm4py.algo.discovery.powl.inductive.variants.maximal.maximal_partial_order_cut import (
    MaximalPartialOrderCutUVCL, )
from pm4py.objects.powl.obj import POWL
from pm4py.objects.dfg import util as dfu


class CutFactoryPOWLMaximal(CutFactory):

    @classmethod
    def get_cuts(
        cls, obj: T, parameters: Optional[Dict[str, Any]] = None
    ) -> List[Type[S]]:
        if type(obj) is IMDataStructureUVCL:
            return [
                POWLExclusiveChoiceCutUVCL,
                POWLStrictSequenceCutUVCL,
                POWLConcurrencyCutUVCL,
                POWLLoopCutUVCL,
                MaximalPartialOrderCutUVCL,
            ]
        return list()

    @classmethod
    def find_cut(
        cls,
        obj: IMDataStructureUVCL,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[POWL, List[T]]]:

        alphabet = sorted(dfu.get_vertices(obj.dfg), key=lambda g: g.__str__())
        if len(alphabet) < 2:
            return None
        for c in CutFactoryPOWLMaximal.get_cuts(obj):
            r = c.apply(obj, parameters)
            if r is not None:
                return r
        return None
