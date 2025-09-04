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

from multiprocessing import Pool, Manager
from typing import Optional, Tuple, List, Dict, Any

from pm4py.algo.discovery.inductive.dtypes.im_ds import IMDataStructureUVCL
from pm4py.algo.discovery.inductive.fall_through.flower import FlowerModelUVCL
from pm4py.objects.powl.obj import OperatorPOWL
from pm4py.objects.process_tree.obj import Operator
from pm4py.util.compression import util as comut
from pm4py.util.compression.dtypes import UVCL


class POWLFlowerModelUVCL(FlowerModelUVCL):

    @classmethod
    def apply(
        cls,
        obj: IMDataStructureUVCL,
        pool: Pool = None,
        manager: Manager = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Tuple[OperatorPOWL, List[IMDataStructureUVCL]]]:
        log = obj.data_structure
        uvcl_do = UVCL()
        for a in comut.get_alphabet(log):
            uvcl_do[(a,)] = 1
        uvcl_redo = UVCL()
        im_uvcl_do = IMDataStructureUVCL(uvcl_do)
        im_uvcl_redo = IMDataStructureUVCL(uvcl_redo)
        children = [im_uvcl_do, im_uvcl_redo]
        return OperatorPOWL(Operator.LOOP, children), children
