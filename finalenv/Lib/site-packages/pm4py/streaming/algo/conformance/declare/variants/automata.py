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
import logging
from pm4py.streaming.algo.interface import StreamingAlgorithm


class DeclareStreamingConformance(StreamingAlgorithm):
    """
    Streaming Conformance Checking Algorithm for DECLARE models.
    Attempts to implement state-based checks for all Declare constraint types.
    When a violation occurs, prints out which constraints are violated.

    Implementation of:
    Maggi, Fabrizio Maria, et al. "Monitoring business constraints with linear temporal logic: An approach based on colored automata." Business Process Management: 9th International Conference, BPM 2011, Clermont-Ferrand, France, August 30-September 2, 2011. Proceedings 9. Springer Berlin Heidelberg, 2011.
    """

    def __init__(self, declare_model, parameters=None):
        super().__init__(parameters=parameters)
        self.declare_model = declare_model
        self._cases = {}
        self._total_events = 0
        self._total_deviations = 0
        self._deviations_per_time = []
        # Parse and build automata for all constraints
        self._constraints = self._parse_declare_model(declare_model)

    def _parse_declare_model(self, model):
        constraints = {}
        for template_type, activities_dict in model.items():
            constraints[template_type] = {}
            for activities, _params in activities_dict.items():
                if isinstance(activities, str):
                    activities = (activities,)
                constraints[template_type][activities] = (
                    self._create_automaton_for_constraint(
                        template_type, activities
                    )
                )
        return constraints

    # Automaton construction methods start here

    def _dummy_automaton(self):
        return {
            "initial_state": {"name": "init"},
            "states": {"init": {"on_event": lambda a, e, s: ("init", False)}},
        }

    def _existence_automaton(self, A):
        def on_event_init(a, e, s):
            if e.get("concept:name") == A:
                return ("seen", False)
            return ("init", False)

        def on_event_seen(a, e, s):
            return ("seen", False)

        return {
            "initial_state": {"name": "init"},
            "states": {
                "init": {"on_event": on_event_init},
                "seen": {"on_event": on_event_seen},
            },
        }

    def _absence_automaton(self, A):
        def on_event_init(a, e, s):
            if e.get("concept:name") == A:
                return ("violated", True)
            return ("init", False)

        return {
            "initial_state": {"name": "init"},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _exactly_one_automaton(self, A):
        def on_event_init(a, e, s):
            if e.get("concept:name") == A:
                return ("seen_once", False)
            return ("init", False)

        def on_event_seen_once(a, e, s):
            if e.get("concept:name") == A:
                return ("violated", True)
            return ("seen_once", False)

        return {
            "initial_state": {"name": "init"},
            "states": {
                "init": {"on_event": on_event_init},
                "seen_once": {"on_event": on_event_seen_once},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _init_automaton(self, A):
        def on_event_init(a, e, s):
            if s.get("received_first", False) is False:
                s["received_first"] = True
                if e.get("concept:name") != A:
                    return ("violated", True)
                else:
                    return ("ok", False)
            else:
                return ("ok", False)

        return {
            "initial_state": {"name": "init", "received_first": False},
            "states": {
                "init": {"on_event": on_event_init},
                "ok": {"on_event": lambda a, e, s: ("ok", False)},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _responded_existence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            if act == A:
                s["A_seen"] = True
            if act == B:
                s["B_seen"] = True
            # No immediate violation, since we can't confirm end of trace
            return ("init", False)

        return {
            "initial_state": {
                "name": "init",
                "A_seen": False,
                "B_seen": False,
            },
            "states": {"init": {"on_event": on_event_init}},
        }

    def _coexistence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            if act == A:
                s["A_seen"] = True
            if act == B:
                s["B_seen"] = True
            return ("init", False)

        return {
            "initial_state": {
                "name": "init",
                "A_seen": False,
                "B_seen": False,
            },
            "states": {"init": {"on_event": on_event_init}},
        }

    def _response_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            pA = s.get("pending_As", 0)
            if act == A:
                pA += 1
            if act == B and pA > 0:
                pA -= 1
            s["pending_As"] = pA
            return ("init", False)

        return {
            "initial_state": {"name": "init", "pending_As": 0},
            "states": {"init": {"on_event": on_event_init}},
        }

    def _precedence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            if act == B and not s.get("A_occurred", False):
                return ("violated", True)
            if act == A:
                s["A_occurred"] = True
            return ("init", False)

        return {
            "initial_state": {"name": "init", "A_occurred": False},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _succession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            pA = s.get("pending_As", 0)
            if act == B and not s.get("A_seen", False):
                return ("violated", True)
            if act == A:
                s["A_seen"] = True
                pA += 1
            elif act == B and pA > 0:
                pA -= 1
            s["pending_As"] = pA
            return ("init", False)

        return {
            "initial_state": {
                "name": "init",
                "A_seen": False,
                "pending_As": 0,
            },
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _altresponse_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            if act == A:
                if s.get("waiting_for_B", False):
                    return ("violated", True)
                s["waiting_for_B"] = True
            if act == B and s.get("waiting_for_B", False):
                s["waiting_for_B"] = False
            return ("init", False)

        return {
            "initial_state": {"name": "init", "waiting_for_B": False},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _altprecedence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            if act == B:
                if s.get("waiting_for_A", True):
                    return ("violated", True)
                s["waiting_for_A"] = True
            if act == A:
                s["waiting_for_A"] = False
            return ("init", False)

        return {
            "initial_state": {"name": "init", "waiting_for_A": True},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _altsuccession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            wB = s.get("waiting_for_B", False)
            wA = s.get("waiting_for_A", True)
            if act == A:
                if wB:
                    return ("violated", True)
                wB = True
                wA = False
            elif act == B:
                if wA:
                    return ("violated", True)
                wA = True
                wB = False
            s["waiting_for_A"] = wA
            s["waiting_for_B"] = wB
            return ("init", False)

        return {
            "initial_state": {
                "name": "init",
                "waiting_for_B": False,
                "waiting_for_A": True,
            },
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _chainresponse_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            expB = s.get("expecting_B", False)
            if expB:
                if act != B:
                    return ("violated", True)
                s["expecting_B"] = False
            if act == A:
                s["expecting_B"] = True
            return ("init", False)

        return {
            "initial_state": {"name": "init", "expecting_B": False},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _chainprecedence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            last = s.get("last", None)
            if act == B and last != A:
                return ("violated", True)
            s["last"] = act
            return ("init", False)

        return {
            "initial_state": {"name": "init", "last": None},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _chainsuccession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            expB = s.get("expecting_B", False)
            last = s.get("last", None)
            if expB:
                if act != B:
                    return ("violated", True)
                expB = False
            if act == B and last != A:
                return ("violated", True)
            if act == A:
                expB = True
            s["expecting_B"] = expB
            s["last"] = act
            return ("init", False)

        return {
            "initial_state": {
                "name": "init",
                "expecting_B": False,
                "last": None,
            },
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _noncoexistence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            if act == A:
                s["A_seen"] = True
                if s.get("B_seen", False):
                    return ("violated", True)
            if act == B:
                s["B_seen"] = True
                if s.get("A_seen", False):
                    return ("violated", True)
            return ("init", False)

        return {
            "initial_state": {
                "name": "init",
                "A_seen": False,
                "B_seen": False,
            },
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _nonsuccession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            if act == A:
                s["A_occurred"] = True
            if act == B and s.get("A_occurred", False):
                return ("violated", True)
            return ("init", False)

        return {
            "initial_state": {"name": "init", "A_occurred": False},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _nonchainsuccession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get("concept:name")
            lA = s.get("last_was_A", False)
            if lA and act == B:
                return ("violated", True)
            lA = act == A
            s["last_was_A"] = lA
            return ("init", False)

        return {
            "initial_state": {"name": "init", "last_was_A": False},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)},
            },
        }

    def _create_automaton_for_constraint(self, template_type, activities):
        if template_type == "existence":
            return self._existence_automaton(activities[0])
        elif template_type == "absence":
            return self._absence_automaton(activities[0])
        elif template_type == "exactly_one":
            return self._exactly_one_automaton(activities[0])
        elif template_type == "init":
            return self._init_automaton(activities[0])
        elif template_type == "responded_existence":
            return self._responded_existence_automaton(
                activities[0], activities[1]
            )
        elif template_type == "coexistence":
            return self._coexistence_automaton(activities[0], activities[1])
        elif template_type == "response":
            return self._response_automaton(activities[0], activities[1])
        elif template_type == "precedence":
            return self._precedence_automaton(activities[0], activities[1])
        elif template_type == "succession":
            return self._succession_automaton(activities[0], activities[1])
        elif template_type == "altresponse":
            return self._altresponse_automaton(activities[0], activities[1])
        elif template_type == "altprecedence":
            return self._altprecedence_automaton(activities[0], activities[1])
        elif template_type == "altsuccession":
            return self._altsuccession_automaton(activities[0], activities[1])
        elif template_type == "chainresponse":
            return self._chainresponse_automaton(activities[0], activities[1])
        elif template_type == "chainprecedence":
            return self._chainprecedence_automaton(
                activities[0], activities[1]
            )
        elif template_type == "chainsuccession":
            return self._chainsuccession_automaton(
                activities[0], activities[1]
            )
        elif template_type == "noncoexistence":
            return self._noncoexistence_automaton(activities[0], activities[1])
        elif template_type == "nonsuccession":
            return self._nonsuccession_automaton(activities[0], activities[1])
        elif template_type == "nonchainsuccession":
            return self._nonchainsuccession_automaton(
                activities[0], activities[1]
            )
        else:
            return self._dummy_automaton()

    def _process(self, event):
        case_id = event.get("case:concept:name", "undefined_case")
        self._total_events += 1

        if case_id not in self._cases:
            case_data = {"constraints_state": {}, "deviations": 0, "events": 0}
            for template_type, constraints_dict in self._constraints.items():
                for activities, automaton in constraints_dict.items():
                    state_data = dict(automaton["initial_state"])
                    case_data["constraints_state"][
                        (template_type, activities)
                    ] = (state_data["name"], state_data)
            self._cases[case_id] = case_data

        self._cases[case_id]["events"] += 1
        current_case_data = self._cases[case_id]
        deviations_in_this_event = 0
        violated_constraints = []

        for (template_type, activities), (
            state_name,
            state_data,
        ) in current_case_data["constraints_state"].items():
            automaton = self._constraints[template_type][activities]
            current_state = automaton["states"][state_name]
            on_event = current_state["on_event"]
            new_state_name, violated = on_event(automaton, event, state_data)
            current_case_data["constraints_state"][
                (template_type, activities)
            ] = (new_state_name, state_data)
            if violated:
                deviations_in_this_event += 1
                violated_constraints.append((template_type, activities))

        if deviations_in_this_event > 0:
            current_case_data["deviations"] += deviations_in_this_event
            self._total_deviations += deviations_in_this_event
            # Print types of violated constraints
            violated_types = [vt for vt, _acts in violated_constraints]
            logging.error(
                f"Case {case_id} - Deviations detected: {deviations_in_this_event}. Violated constraint types: {violated_types}")

        timestamp = event.get("time:timestamp", self._total_events)
        self._deviations_per_time.append((timestamp, deviations_in_this_event))

    def _current_result(self):
        result = {
            "total_events_processed": self._total_events,
            "total_deviations": self._total_deviations,
            "deviations_per_time": self._deviations_per_time,
            "cases": {},
        }
        for c_id, c_data in self._cases.items():
            constraints_state = {}
            for k, (st_name, st_data) in c_data["constraints_state"].items():
                constraints_state[str(k)] = st_name
            result["cases"][c_id] = {
                "events": c_data["events"],
                "deviations": c_data["deviations"],
                "constraints_state": constraints_state,
            }
        return result


def apply(declare_model, parameters=None):
    return DeclareStreamingConformance(declare_model, parameters)
