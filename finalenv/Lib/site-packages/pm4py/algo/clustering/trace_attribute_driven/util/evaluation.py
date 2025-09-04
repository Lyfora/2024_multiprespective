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
from scipy.spatial.distance import squareform
import numpy as np
from pm4py.algo.clustering.trace_attribute_driven.variants import act_dist_calc
from pm4py.algo.clustering.trace_attribute_driven.variants import suc_dist_calc
from pm4py.algo.clustering.trace_attribute_driven.leven_dist import (
    leven_dist_calc,
)
from pm4py.algo.clustering.trace_attribute_driven.dfg import dfg_dist


def dfg_dis(loglist, percent, alpha):
    size = len(loglist)
    dist_mat = np.zeros((size, size))

    for i in range(0, size - 1):
        for j in range(i + 1, size):
            (dist_act, dist_dfg) = dfg_dist.dfg_dist_calc(
                loglist[i], loglist[j]
            )
            dist_mat[i][j] = dist_act * alpha + dist_dfg * (1 - alpha)
            dist_mat[j][i] = dist_mat[i][j]
    y = squareform(dist_mat)
    return y


def eval_avg_variant(loglist, percent, alpha):
    size = len(loglist)
    dist_mat = np.zeros((size, size))

    for i in range(0, size - 1):
        for j in range(i + 1, size):
            dist_act = act_dist_calc.act_sim_percent_avg(
                loglist[i], loglist[j], percent, percent
            )
            dist_suc = suc_dist_calc.suc_sim_percent_avg(
                loglist[i], loglist[j], percent, percent
            )
            dist_mat[i][j] = dist_act * alpha + dist_suc * (1 - alpha)
            dist_mat[j][i] = dist_mat[i][j]
    y = squareform(dist_mat)

    return y


def eval_DMM_variant(loglist, percent, alpha):
    size = len(loglist)
    dist_mat = np.zeros((size, size))

    for i in range(0, size - 1):
        for j in range(i + 1, size):
            dist_act = act_dist_calc.act_sim_percent(
                loglist[i], loglist[j], percent, percent
            )
            dist_suc = suc_dist_calc.suc_sim_percent(
                loglist[i], loglist[j], percent, percent
            )
            dist_mat[i][j] = dist_act * alpha + dist_suc * (1 - alpha)
            dist_mat[j][i] = dist_mat[i][j]
    y = squareform(dist_mat)
    return y


def eval_avg_leven(loglist, percent, alpha):
    size = len(loglist)
    dist_mat = np.zeros((size, size))

    for i in range(0, size - 1):
        for j in range(i + 1, size):
            dist_mat[i][j] = leven_dist_calc.leven_dist_avg(
                loglist[i], loglist[j], percent, percent
            )
            dist_mat[j][i] = dist_mat[i][j]
    y = squareform(dist_mat)
    return y


def eval_DMM_leven(loglist, percent, alpha):
    size = len(loglist)
    dist_mat = np.zeros((size, size))

    for i in range(0, size - 1):
        for j in range(i + 1, size):
            dist_mat[i][j] = leven_dist_calc.leven_dist(
                loglist[i], loglist[j], percent, percent
            )
            dist_mat[j][i] = dist_mat[i][j]
    y = squareform(dist_mat)
    return y
