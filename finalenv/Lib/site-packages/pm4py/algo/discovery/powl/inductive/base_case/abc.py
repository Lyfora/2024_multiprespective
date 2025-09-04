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

from abc import ABC, abstractmethod
from typing import Union, TypeVar, Generic, Optional, Dict, Any

from pm4py.algo.discovery.inductive.dtypes.im_ds import IMDataStructure
from pm4py.objects.powl.obj import POWL

T = TypeVar("T", bound=Union[IMDataStructure])


class BaseCase(ABC, Generic[T]):

    @classmethod
    def apply(
        cls, obj=T, parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[POWL]:
        return (
            cls.leaf(obj, parameters) if cls.holds(obj, parameters) else None
        )

    @classmethod
    @abstractmethod
    def holds(cls, obj=T, parameters: Optional[Dict[str, Any]] = None) -> bool:
        pass

    @classmethod
    @abstractmethod
    def leaf(cls, obj=T, parameters: Optional[Dict[str, Any]] = None) -> POWL:
        pass
