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
from typing import TypeVar, Optional, Dict, Any, Type, List as TList

from pm4py.algo.discovery.powl.inductive.base_case.abc import BaseCase
from pm4py.algo.discovery.powl.inductive.base_case.empty_log import (
    EmptyLogBaseCaseUVCL,
)
from pm4py.algo.discovery.powl.inductive.base_case.single_activity import (
    SingleActivityBaseCaseUVCL,
)
from pm4py.algo.discovery.inductive.dtypes.im_ds import (
    IMDataStructure,
    IMDataStructureUVCL,
)

from pm4py.objects.powl.obj import POWL

T = TypeVar("T", bound=IMDataStructure)
S = TypeVar("S", bound=BaseCase)


class BaseCaseFactory:

    @classmethod
    def get_base_cases(
        cls, obj: T, parameters: Optional[Dict[str, Any]] = None
    ) -> TList[Type[S]]:
        if type(obj) is IMDataStructureUVCL:
            return [EmptyLogBaseCaseUVCL, SingleActivityBaseCaseUVCL]
        return []

    @classmethod
    def apply_base_cases(
        cls, obj: T, parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[POWL]:
        for b in BaseCaseFactory.get_base_cases(obj):
            r = b.apply(obj, parameters)
            if r is not None:
                return r
        return None
