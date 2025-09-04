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
import abc
from threading import Lock
import traceback


class StreamingAlgorithm(abc.ABC):
    def __init__(self, parameters=None):
        self._lock = Lock()

    @abc.abstractmethod
    def _process(self, event):
        pass

    @abc.abstractmethod
    def _current_result(self):
        pass

    def get(self):
        self._lock.acquire()
        try:
            ret = self._current_result()
        except BaseException:
            traceback.print_exc()
            ret = None
        self._lock.release()
        return ret

    def receive(self, event):
        self._lock.acquire()
        try:
            self._process(event)
        except BaseException:
            traceback.print_exc()
        self._lock.release()
