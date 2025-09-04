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
import sys

import numpy as np

from pm4py.objects.random_variables.basic_structure import (
    BasicStructureRandomVariable,
)
from pm4py.util import constants
import warnings


class Gamma(BasicStructureRandomVariable):
    """
    Describes a normal variable
    """

    def __init__(self, a=1, loc=0, scale=1):
        """
        Constructor
        """
        self.a = a
        self.loc = loc
        self.scale = scale
        BasicStructureRandomVariable.__init__(self)

    def read_from_string(self, distribution_parameters):
        """
        Initialize distribution parameters from string

        Parameters
        -----------
        distribution_parameters
            Current distribution parameters as exported on the Petri net
        """
        self.a = float(distribution_parameters.split(";")[0])
        self.loc = float(distribution_parameters.split(";")[1])
        self.scale = float(distribution_parameters.split(";")[2])

    def get_distribution_type(self):
        """
        Get current distribution type

        Returns
        -----------
        distribution_type
            String representing the distribution type
        """
        return "GAMMA"

    def get_distribution_parameters(self):
        """
        Get a string representing distribution parameters

        Returns
        -----------
        distribution_parameters
            String representing distribution parameters
        """
        return str(self.a) + ";" + str(self.loc) + ";" + str(self.scale)

    def calculate_loglikelihood(self, values):
        """
        Calculate log likelihood

        Parameters
        ------------
        values
            Empirical values to work on

        Returns
        ------------
        likelihood
            Log likelihood that the values follows the distribution
        """
        from scipy.stats import gamma

        if len(values) > 1:
            somma = 0
            for value in values:
                somma = somma + np.log(
                    gamma.pdf(value, self.a, self.loc, self.scale)
                )
            return somma
        return -sys.float_info.max

    def calculate_parameters(self, values):
        """
        Calculate parameters of the current distribution

        Parameters
        -----------
        values
            Empirical values to work on
        """
        from scipy.stats import gamma

        if len(values) > 1:
            try:
                self.a, self.loc, self.scale = gamma.fit(values)
            except BaseException:
                if constants.SHOW_INTERNAL_WARNINGS:
                    warnings.warn(
                        "Gamma fitting: Optimization converged to parameters that are outside the range allowed by the distribution"
                    )

    def get_value(self):
        """
        Get a random value following the distribution

        Returns
        -----------
        value
            Value obtained following the distribution
        """
        from scipy.stats import gamma

        return gamma.rvs(self.a, self.loc, self.scale)
