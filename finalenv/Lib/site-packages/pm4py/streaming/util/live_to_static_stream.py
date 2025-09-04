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
from pm4py.streaming.algo.interface import StreamingAlgorithm
from pm4py.objects.log.obj import EventStream


class LiveToStaticStream(StreamingAlgorithm):
    static_stream = None

    def __init__(self):
        self.static_stream = EventStream()
        StreamingAlgorithm.__init__(self)

    def _process(self, event):
        self.static_stream.append(event)

    def _current_result(self):
        return self.static_stream
