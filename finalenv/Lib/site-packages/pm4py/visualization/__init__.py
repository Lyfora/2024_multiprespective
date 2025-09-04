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
from pm4py.util import constants as pm4_constants

if pm4_constants.ENABLE_INTERNAL_IMPORTS:
    import importlib.util

    if importlib.util.find_spec("graphviz"):
        # imports the visualizations only if graphviz is installed
        from pm4py.visualization import (
            common,
            dfg,
            petri_net,
            process_tree,
            transition_system,
            bpmn,
            trie,
            ocel,
            network_analysis,
        )

        if importlib.util.find_spec("matplotlib") and importlib.util.find_spec(
            "pyvis"
        ):
            # SNA requires both packages matplotlib and pyvis. These are included in the default installation;
            # however, they may lead to problems in some platforms/deployments
            from pm4py.visualization import sna, performance_spectrum
        if importlib.util.find_spec("pydotplus"):
            # heuristics net visualization requires pydotplus. This is included in the default installation;
            # however, they may lead to problems in some platforms/deployments
            from pm4py.visualization import heuristics_net

    if importlib.util.find_spec("matplotlib"):
        # graphs require matplotlib. This is included in the default installation;
        # however, they may lead to problems in some platforms/deployments
        from pm4py.visualization import graphs
