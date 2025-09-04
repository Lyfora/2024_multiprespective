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
from typing import Optional, Tuple, List, TypeVar, Dict, Any

from pm4py.algo.discovery.inductive.dtypes.im_ds import IMDataStructureLog
from pm4py.algo.discovery.powl.inductive.variants.dynamic_clustering_frequency.factory import (
    CutFactoryPOWLDynamicClusteringFrequency, )
from pm4py.algo.discovery.powl.inductive.variants.im_tree import IMBasePOWL
from pm4py.algo.discovery.powl.inductive.variants.powl_discovery_varaints import (
    POWLDiscoveryVariant, )
from pm4py.objects.powl.obj import POWL

T = TypeVar("T", bound=IMDataStructureLog)


class POWLInductiveMinerDynamicClusteringFrequency(IMBasePOWL):

    def instance(self) -> POWLDiscoveryVariant:
        return POWLDiscoveryVariant.DYNAMIC_CLUSTERING

    def find_cut(
        self, obj: T, parameters: Optional[Dict[str, Any]] = None
    ) -> Optional[Tuple[POWL, List[T]]]:
        res = CutFactoryPOWLDynamicClusteringFrequency.find_cut(
            obj, parameters=parameters
        )
        return res
