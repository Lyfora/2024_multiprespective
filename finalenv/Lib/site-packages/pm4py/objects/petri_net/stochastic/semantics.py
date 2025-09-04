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

import random
from typing import Counter, Generic, TypeVar

from pm4py.objects.petri_net.semantics import PetriNetSemantics
from pm4py.objects.petri_net.stochastic.obj import StochasticPetriNet

N = TypeVar("N", bound=StochasticPetriNet)
T = TypeVar("T", bound=StochasticPetriNet.Transition)
P = TypeVar("P", bound=StochasticPetriNet.Place)


class StochasticPetriNetSemantics(PetriNetSemantics[N], Generic[N]):

    @classmethod
    def sample_enabled_transition(
        cls, pn: N, marking: Counter[P], seed: int = None
    ) -> T:
        """
        Randomly samples a transition from all enabled transitions

        Parameters
        ----------
        :param pn: Petri net
        :param marking: marking to use

        Returns
        -------
        :return: a transition sampled from the enabled transitions
        """
        if seed is not None:
            random.seed(seed)
        enabled = list(
            filter(
                lambda t: cls.is_enabled(pn, t, marking),
                [t for t in pn.transitions],
            )
        )
        weights = list(
            map(
                lambda t: cls.probability_of_transition(pn, t, marking),
                enabled,
            )
        )
        return random.choices(enabled, weights)[0]

    @classmethod
    def probability_of_transition(
        cls, pn: N, transition: T, marking: Counter[P]
    ) -> float:
        """
        Compute the probability of firing a transition in the net and marking.

        Args:
            pn (N): Stochastic net
            transition (T): transition to fire
            marking (Counter[P]): marking to use

        Returns:
            float: _description_
        """
        if transition not in pn.transitions or not cls.is_enabled(
            pn, transition, marking
        ):
            return 0.0
        return transition.weight / sum(
            list(
                map(
                    lambda t: t.weight,
                    list(
                        filter(
                            lambda t: cls.is_enabled(pn, t, marking),
                            [t for t in pn.transitions],
                        )
                    ),
                )
            )
        )
