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
from typing import Optional, Dict, Any, Union, List
import numpy as np
from pm4py.util.regex import SharedObj, get_new_char
from pm4py.util import string_distance
from pm4py.util import exec_utils
from scipy.optimize import linprog
import importlib.util


class Parameters:
    STRING_DISTANCE = "string_distance"
    USE_FAST_EMD = "use_fast_emd"  # New parameter to choose between implementations


def normalized_levensthein(s1, s2):
    return float(string_distance.levenshtein(s1, s2)) / float(
        max(len(s1), len(s2))
    )


def get_act_correspondence(activities, parameters=None):
    if parameters is None:
        parameters = {}

    shared_obj = SharedObj()
    ret = {}
    for act in activities:
        get_new_char(act, shared_obj)
        ret[act] = shared_obj.mapping_dictio[act]

    return ret


def encode_two_languages(lang1, lang2, parameters=None):
    if parameters is None:
        parameters = {}

    all_activities = sorted(
        list(
            set(y for x in lang1 for y in x).union(
                set(y for x in lang2 for y in x)
            )
        )
    )
    acts_corresp = get_act_correspondence(
        all_activities, parameters=parameters
    )

    enc1 = {}
    enc2 = {}

    for k in lang1:
        new_key = "".join(acts_corresp[i] for i in k)
        enc1[new_key] = lang1[k]

    for k in lang2:
        new_key = "".join(acts_corresp[i] for i in k)
        enc2[new_key] = lang2[k]

    for x in enc1:
        if x not in enc2:
            enc2[x] = 0.0

    for x in enc2:
        if x not in enc1:
            enc1[x] = 0.0

    enc1 = [(x, y) for x, y in enc1.items()]
    enc2 = [(x, y) for x, y in enc2.items()]

    enc1 = sorted(enc1, reverse=True, key=lambda x: x[0])
    enc2 = sorted(enc2, reverse=True, key=lambda x: x[0])

    return enc1, enc2


class EMDCalculator:
    """
    A class that provides an EMD (Earth Mover's Distance) computation similar to what `pyemd` offers.
    It uses linear programming via `scipy.optimize.linprog` to solve the underlying flow problem.

    Usage:
    ------
    emd_value = EMDCalculator.emd(first_histogram, second_histogram, distance_matrix)
    """

    @staticmethod
    def emd(
            first_histogram: np.ndarray,
            second_histogram: np.ndarray,
            distance_matrix: np.ndarray,
    ) -> float:
        """
        Compute the Earth Mover's Distance given two histograms and a distance matrix.

        Parameters
        ----------
        first_histogram : np.ndarray
            The first distribution (array of nonnegative numbers).
        second_histogram : np.ndarray
            The second distribution (array of nonnegative numbers).
        distance_matrix : np.ndarray
            Matrix of distances between points of the two distributions.

        Returns
        -------
        float
            The computed EMD value.
        """
        # Ensure the histograms sum to the same total
        sum1 = np.sum(first_histogram)
        sum2 = np.sum(second_histogram)
        if not np.isclose(sum1, sum2):
            raise ValueError(
                "Histograms must sum to the same total for EMD calculation."
            )

        n = len(first_histogram)
        m = len(second_histogram)

        # Flatten the distance matrix
        c = distance_matrix.flatten()

        # Constraints:
        # sum_j F_ij = first_histogram[i] for each i
        # sum_i F_ij = second_histogram[j] for each j
        # F_ij >= 0

        # We have n "row sum" constraints and m "column sum" constraints.
        A_eq = []
        b_eq = []

        # Row constraints
        for i in range(n):
            row_constraint = np.zeros(n * m)
            for j in range(m):
                row_constraint[i * m + j] = 1
            A_eq.append(row_constraint)
            b_eq.append(first_histogram[i])

        # Column constraints
        for j in range(m):
            col_constraint = np.zeros(n * m)
            for i in range(n):
                col_constraint[i * m + j] = 1
            A_eq.append(col_constraint)
            b_eq.append(second_histogram[j])

        A_eq = np.array(A_eq)
        b_eq = np.array(b_eq)

        # Bounds: F_ij >= 0
        bounds = [(0, None) for _ in range(n * m)]

        # Solve the LP:
        # minimize c^T x subject to A_eq x = b_eq and x >= 0
        res = linprog(c, A_eq=A_eq, b_eq=b_eq, bounds=bounds, method="highs")

        if res.status != 0:
            raise ValueError(
                f"Linear programming failed. Status: {
                res.status}, Message: {
                res.message}")

        # The optimal value is the EMD
        return res.fun


class POTEMDCalculator:
    """
    A faster implementation of EMD using the POT (Python Optimal Transport) library.
    Falls back to the SciPy implementation if POT is not available.

    Usage:
    ------
    emd_value = POTEMDCalculator.emd(first_histogram, second_histogram, distance_matrix)
    """

    @staticmethod
    def is_pot_available():
        """Check if POT (Python Optimal Transport) is available."""
        return importlib.util.find_spec("ot") is not None

    @staticmethod
    def emd(
            first_histogram: np.ndarray,
            second_histogram: np.ndarray,
            distance_matrix: np.ndarray,
    ) -> float:
        """
        Compute the Earth Mover's Distance using POT if available, otherwise fall back to SciPy.

        Parameters
        ----------
        first_histogram : np.ndarray
            The first distribution (array of nonnegative numbers).
        second_histogram : np.ndarray
            The second distribution (array of nonnegative numbers).
        distance_matrix : np.ndarray
            Matrix of distances between points of the two distributions.

        Returns
        -------
        float
            The computed EMD value.
        """
        # Check if POT is available
        if POTEMDCalculator.is_pot_available():
            try:
                import ot

                # Normalize histograms to sum to 1.0 if they don't sum to the same value
                sum1 = np.sum(first_histogram)
                sum2 = np.sum(second_histogram)

                if not np.isclose(sum1, sum2):
                    # Normalize both histograms to sum to 1.0
                    a = first_histogram / sum1
                    b = second_histogram / sum2
                else:
                    a = first_histogram
                    b = second_histogram

                # Ensure arrays have the right type
                a = np.asarray(a, dtype=np.float64)
                b = np.asarray(b, dtype=np.float64)
                M = np.asarray(distance_matrix, dtype=np.float64)

                # Use POT's EMD computation
                # Regularized version can be used with reg=1e-3 for efficiency when needed
                emd_value = ot.emd2(a, b, M)

                return emd_value

            except ImportError:
                # Fall back to SciPy implementation
                return EMDCalculator.emd(first_histogram, second_histogram, distance_matrix)
            except Exception as e:
                # If POT fails for any reason, fall back to SciPy implementation
                print(f"POT EMD calculation failed: {str(e)}. Falling back to SciPy implementation.")
                return EMDCalculator.emd(first_histogram, second_histogram, distance_matrix)
        else:
            # Fall back to SciPy implementation
            return EMDCalculator.emd(first_histogram, second_histogram, distance_matrix)


def apply(
        lang1: Dict[List[str], float],
        lang2: Dict[List[str], float],
        parameters: Optional[Dict[Union[str, Parameters], Any]] = None,
) -> float:
    if parameters is None:
        parameters = {}

    distance_function = exec_utils.get_param_value(
        Parameters.STRING_DISTANCE, parameters, normalized_levensthein
    )

    # New parameter to choose between implementations
    use_fast_emd = exec_utils.get_param_value(
        Parameters.USE_FAST_EMD, parameters, True
    )

    enc1, enc2 = encode_two_languages(lang1, lang2, parameters=parameters)

    first_histogram = np.array([x[1] for x in enc1])
    second_histogram = np.array([x[1] for x in enc2])

    distance_matrix = []
    for x in enc1:
        row = []
        for y in enc2:
            dist = distance_function(x[0], y[0])
            row.append(float(dist))
        distance_matrix.append(row)

    distance_matrix = np.array(distance_matrix)

    # Choose which EMD implementation to use
    if use_fast_emd and POTEMDCalculator.is_pot_available():
        ret = POTEMDCalculator.emd(first_histogram, second_histogram, distance_matrix)
    else:
        # Use the original SciPy implementation
        ret = EMDCalculator.emd(first_histogram, second_histogram, distance_matrix)

    return ret
