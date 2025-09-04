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
from typing import Tuple, Any, Counter as TCounter


class DirectlyFollowsGraph:

    def __init__(self, graph=None, start_activities=None, end_activities=None):
        if graph is None:
            graph = {}
        if start_activities is None:
            start_activities = {}
        if end_activities is None:
            end_activities = {}
        self._graph = Counter(graph)
        self._start_activities = Counter(start_activities)
        self._end_activities = Counter(end_activities)

    @property
    def graph(self) -> TCounter[Tuple[Any, Any]]:
        return self._graph

    @property
    def start_activities(self) -> TCounter[Any]:
        return self._start_activities

    @property
    def end_activities(self) -> TCounter[Any]:
        return self._end_activities

    def __repr__(self):
        return repr(self._graph)

    def __str__(self):
        return str(self._graph)

    def __iter__(self):
        yield dict(self.graph)
        yield dict(self.start_activities)
        yield dict(self.end_activities)


DFG = DirectlyFollowsGraph
