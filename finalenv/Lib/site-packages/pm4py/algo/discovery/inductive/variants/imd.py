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
from typing import Optional, Dict, Any
from pm4py.algo.discovery.inductive.dtypes.im_ds import IMDataStructureDFG
from pm4py.algo.discovery.inductive.variants.abc import InductiveMinerFramework
from pm4py.algo.discovery.inductive.variants.instances import IMInstance
from pm4py.algo.discovery.inductive.fall_through.empty_traces import EmptyTracesDFG
from pm4py.objects.process_tree.obj import ProcessTree

class IMD(InductiveMinerFramework[IMDataStructureDFG]):

    def instance(self) -> IMInstance:
        return IMInstance.IMd


    def apply(
        self,
        obj: IMDataStructureDFG,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> ProcessTree:
        # Handle empty traces FIRST (so τ is not lost if a base case/cut triggers)
        empty_traces = EmptyTracesDFG.apply(obj, parameters=parameters)
        if empty_traces is not None:
            return self._recurse(empty_traces[0], empty_traces[1], parameters)
        return super().apply(obj, parameters)
