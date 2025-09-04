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
from collections import Counter
from typing import Any, Optional, Dict
from pm4py.algo.discovery.inductive.dtypes.im_ds import IMDataStructureUVCL
from pm4py.algo.discovery.inductive.fall_through.activity_concurrent import (
    ActivityConcurrentUVCL,
)
from pm4py.util.compression import util as comut


class ActivityOncePerTraceUVCL(ActivityConcurrentUVCL):
    @classmethod
    def _get_candidate(
        cls,
        obj: IMDataStructureUVCL,
        pool=None,
        manager=None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> Optional[Any]:
        # Initialize candidates as a set of all activities
        candidates = set(comut.get_alphabet(obj.data_structure))

        for t in obj.data_structure:
            # Use a Counter to count occurrences of each activity in the trace
            activity_counts = Counter(t)
            # Create a set of activities that occur exactly once in the trace
            activities_once = {
                activity
                for activity, count in activity_counts.items()
                if count == 1
            }
            # Intersect with the existing candidates
            candidates &= activities_once
            # Early exit if no candidates remain
            if not candidates:
                return None

        # Return any one of the remaining candidates
        return next(iter(candidates)) if candidates else None
