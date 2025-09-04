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
import shutil
import os
from pm4py.visualization.common import dot_util, html


def save(gviz, output_file_path, parameters=None):
    """
    Save the diagram

    Parameters
    -----------
    gviz
        GraphViz diagram
    output_file_path
        Path where the GraphViz output should be saved
    """
    format = os.path.splitext(output_file_path)[1][1:].lower()
    is_dot_installed = dot_util.check_dot_installed()

    if format.startswith("html"):
        html.save(gviz, output_file_path, parameters=parameters)
    elif format == "gv":
        F = open(output_file_path, "w")
        F.write(str(gviz))
        F.close()
    else:
        render = gviz.render(cleanup=True)
        shutil.copyfile(render, output_file_path)
    """elif not is_dot_installed:
        raise Exception("impossible to save formats different from HTML without the Graphviz binary")"""
