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

from abc import ABC
from typing import Any, Optional, Dict, Tuple, List, Generic

from pm4py.algo.discovery.inductive.base_case.abc import T
from pm4py.algo.discovery.inductive.cuts.sequence import (
    SequenceCut,
    SequenceCutUVCL,
    StrictSequenceCutUVCL,
    StrictSequenceCut,
)
from pm4py.algo.discovery.inductive.dtypes.im_ds import IMDataStructureUVCL
from pm4py.objects.powl.obj import Sequence


class POWLSequenceCut(SequenceCut, ABC, Generic[T]):

    @classmethod
    def operator(cls, parameters: Optional[Dict[str, Any]] = None) -> Sequence:
        raise Exception("This function should not be called!")

    @classmethod
    def apply(
        cls, obj: T, parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[Tuple[Sequence, List[T]]]:
        g = cls.holds(obj, parameters)
        if g is None:
            return g
        children = cls.project(obj, g, parameters)
        po = Sequence(children)
        return po, children


class POWLStrictSequenceCut(POWLSequenceCut[T], StrictSequenceCut, ABC):
    pass


class POWLSequenceCutUVCL(
    SequenceCutUVCL, POWLSequenceCut[IMDataStructureUVCL]
):
    pass


class POWLStrictSequenceCutUVCL(
    StrictSequenceCutUVCL,
    StrictSequenceCut[IMDataStructureUVCL],
    POWLSequenceCutUVCL,
):
    pass
