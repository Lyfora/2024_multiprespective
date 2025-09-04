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

import deprecation


class Semantics(object):
    @deprecation.deprecated(
        "2.3.0",
        "3.0.0",
        details="this method will be removed, use PetriNetSemantics.is_enabled() instead",
    )
    def is_enabled(self, t, pn, m, **kwargs):
        pass

    @deprecation.deprecated(
        "2.3.0",
        "3.0.0",
        details="this method will be removed, use PetriNetSemantics.fire() instead",
    )
    def execute(self, t, pn, m, **kwargs):
        pass

    @deprecation.deprecated(
        "2.3.0",
        "3.0.0",
        details="this method will be removed, use PetriNetSemantics.fire() instead",
    )
    def weak_execute(self, t, pn, m, **kwargs):
        pass

    @deprecation.deprecated(
        "2.3.0", "3.0.0", details="this method will be removed"
    )
    def enabled_transitions(self, pn, m, **kwargs):
        pass
