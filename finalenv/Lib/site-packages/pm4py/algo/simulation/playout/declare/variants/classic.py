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
import random
from pm4py.objects.log.obj import EventLog, Trace, Event
from pm4py.util.xes_constants import DEFAULT_NAME_KEY
from enum import Enum
from pm4py.util import exec_utils


class Parameters(Enum):
    N_TRACES = "n_traces"
    MIN_LENGTH = "min_length"
    MAX_LENGTH = "max_length"


class DeclarePlayout:
    """
    Simple random playout generator for DECLARE models.
    It reuses the same constraint-to-automaton mappings
    but in a generation mode instead of a conformance-checking mode.
    """

    def __init__(self, declare_model, parameters=None):
        self.declare_model = declare_model
        self.parameters = parameters if parameters is not None else {}
        self._constraints = self._parse_declare_model(declare_model)

        # Get set of all activities mentioned in constraints
        self.activities = set()
        for template_type, acts_dict in self.declare_model.items():
            for activities_tuple in acts_dict.keys():
                if isinstance(activities_tuple, str):
                    self.activities.add(activities_tuple)
                else:
                    for a in activities_tuple:
                        self.activities.add(a)

        # Number of traces to produce
        self.n_traces = exec_utils.get_param_value(Parameters.N_TRACES, parameters, 1000)

        # Limits on trace length (to avoid infinite loops)
        # You can adjust min_length, max_length, or force constraints
        self.min_length = exec_utils.get_param_value(Parameters.MIN_LENGTH, parameters, 3)
        self.max_length = exec_utils.get_param_value(Parameters.MAX_LENGTH, parameters, 15)

    def _parse_declare_model(self, model):
        """
        Create the constraints dictionary:
        { template_type -> { (activities) -> automaton } }
        """
        constraints = {}
        for template_type, activities_dict in model.items():
            constraints[template_type] = {}
            for activities, _params in activities_dict.items():
                if isinstance(activities, str):
                    activities = (activities,)
                constraints[template_type][activities] = self._create_automaton_for_constraint(
                    template_type, activities
                )
        return constraints

    # ----------------------------------------------------------------
    # The same automaton definitions as in your conformance checker:
    # ----------------------------------------------------------------

    def _dummy_automaton(self):
        return {
            "initial_state": {"name": "init"},
            "states": {
                "init": {"on_event": lambda a, e, s: ("init", False)}
            }
        }

    def _existence_automaton(self, A):
        def on_event_init(a, e, s):
            if e.get(DEFAULT_NAME_KEY) == A:
                return ("seen", False)
            return ("init", False)

        def on_event_seen(a, e, s):
            return ("seen", False)

        return {
            "initial_state": {"name": "init"},
            "states": {
                "init": {"on_event": on_event_init},
                "seen": {"on_event": on_event_seen}
            }
        }

    def _absence_automaton(self, A):
        def on_event_init(a, e, s):
            if e.get(DEFAULT_NAME_KEY) == A:
                return ("violated", True)
            return ("init", False)

        return {
            "initial_state": {"name": "init"},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _exactly_one_automaton(self, A):
        def on_event_init(a, e, s):
            if e.get(DEFAULT_NAME_KEY) == A:
                return ("seen_once", False)
            return ("init", False)

        def on_event_seen_once(a, e, s):
            if e.get(DEFAULT_NAME_KEY) == A:
                return ("violated", True)
            return ("seen_once", False)

        return {
            "initial_state": {"name": "init"},
            "states": {
                "init": {"on_event": on_event_init},
                "seen_once": {"on_event": on_event_seen_once},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _init_automaton(self, A):
        def on_event_init(a, e, s):
            if s.get("received_first", False) is False:
                s["received_first"] = True
                if e.get(DEFAULT_NAME_KEY) != A:
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
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _responded_existence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
            if act == A:
                s["A_seen"] = True
            if act == B:
                s["B_seen"] = True
            return ("init", False)

        return {
            "initial_state": {"name": "init", "A_seen": False, "B_seen": False},
            "states": {"init": {"on_event": on_event_init}}
        }

    def _coexistence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
            if act == A:
                s["A_seen"] = True
            if act == B:
                s["B_seen"] = True
            return ("init", False)

        return {
            "initial_state": {"name": "init", "A_seen": False, "B_seen": False},
            "states": {"init": {"on_event": on_event_init}}
        }

    def _response_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
            pA = s.get("pending_As", 0)
            if act == A:
                pA += 1
            if act == B and pA > 0:
                pA -= 1
            s["pending_As"] = pA
            return ("init", False)

        return {
            "initial_state": {"name": "init", "pending_As": 0},
            "states": {"init": {"on_event": on_event_init}}
        }

    def _precedence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
            if act == B and not s.get("A_occurred", False):
                return ("violated", True)
            if act == A:
                s["A_occurred"] = True
            return ("init", False)

        return {
            "initial_state": {"name": "init", "A_occurred": False},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _succession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
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
            "initial_state": {"name": "init", "A_seen": False, "pending_As": 0},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _altresponse_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
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
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _altprecedence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
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
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _altsuccession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
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
            "initial_state": {"name": "init", "waiting_for_B": False, "waiting_for_A": True},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _chainresponse_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
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
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _chainprecedence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
            last = s.get("last", None)
            if act == B and last != A:
                return ("violated", True)
            s["last"] = act
            return ("init", False)

        return {
            "initial_state": {"name": "init", "last": None},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _chainsuccession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
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
            "initial_state": {"name": "init", "expecting_B": False, "last": None},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _noncoexistence_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
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
            "initial_state": {"name": "init", "A_seen": False, "B_seen": False},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _nonsuccession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
            if act == A:
                s["A_occurred"] = True
            if act == B and s.get("A_occurred", False):
                return ("violated", True)
            return ("init", False)

        return {
            "initial_state": {"name": "init", "A_occurred": False},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _nonchainsuccession_automaton(self, A, B):
        def on_event_init(a, e, s):
            act = e.get(DEFAULT_NAME_KEY)
            lA = s.get("last_was_A", False)
            if lA and act == B:
                return ("violated", True)
            lA = (act == A)
            s["last_was_A"] = lA
            return ("init", False)

        return {
            "initial_state": {"name": "init", "last_was_A": False},
            "states": {
                "init": {"on_event": on_event_init},
                "violated": {"on_event": lambda a, e, s: ("violated", False)}
            }
        }

    def _create_automaton_for_constraint(self, template_type, activities):
        """
        Reuse the same constraint->automaton logic.
        """
        if template_type == "existence":
            return self._existence_automaton(activities[0])
        elif template_type == "absence":
            return self._absence_automaton(activities[0])
        elif template_type == "exactly_one":
            return self._exactly_one_automaton(activities[0])
        elif template_type == "init":
            return self._init_automaton(activities[0])
        elif template_type == "responded_existence":
            return self._responded_existence_automaton(activities[0], activities[1])
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
            return self._chainprecedence_automaton(activities[0], activities[1])
        elif template_type == "chainsuccession":
            return self._chainsuccession_automaton(activities[0], activities[1])
        elif template_type == "noncoexistence":
            return self._noncoexistence_automaton(activities[0], activities[1])
        elif template_type == "nonsuccession":
            return self._nonsuccession_automaton(activities[0], activities[1])
        elif template_type == "nonchainsuccession":
            return self._nonchainsuccession_automaton(activities[0], activities[1])
        else:
            return self._dummy_automaton()

    # ---------------------------------------------------------
    #  Playout generation logic
    # ---------------------------------------------------------

    def _new_constraints_state(self):
        """
        For each constraint, return the initial state of its automaton
        (state_name, state_data).
        """
        constraints_state = {}
        for template_type, constraints_dict in self._constraints.items():
            for activities, automaton in constraints_dict.items():
                init_data = dict(automaton["initial_state"])
                constraints_state[(template_type, activities)] = (
                    init_data["name"],
                    init_data,
                )
        return constraints_state

    def _try_event(self, activity, constraints_state):
        """
        Try an event with the given 'activity' on the current constraints_state.
        Returns:
          (new_constraints_state, violated)
          - new_constraints_state: updated states after applying activity
          - violated: True if some constraint was violated by this event
        """
        new_state = {}
        violated_any = False

        # Craft a mock event for the automaton logic
        event = {DEFAULT_NAME_KEY: activity}

        for (template_type, acts), (state_name, s_data) in constraints_state.items():
            automaton = self._constraints[template_type][acts]
            current_state_data = dict(s_data)  # clone or keep reference
            on_event = automaton["states"][state_name]["on_event"]
            next_state_name, violated = on_event(automaton, event, current_state_data)
            new_state[(template_type, acts)] = (next_state_name, current_state_data)
            if violated:
                violated_any = True

        return new_state, violated_any

    def _generate_trace(self, case_id):
        """
        Generate a single random trace for a given case_id,
        trying not to violate constraints.
        """
        constraints_state = self._new_constraints_state()
        trace = Trace(attributes={"concept:name": str(case_id)})

        # Random length in [min_length, max_length]
        length = random.randint(self.min_length, self.max_length)

        for _ in range(length):
            # Collect all candidate activities that do NOT lead to violation
            candidates = []
            for act in self.activities:
                new_state, violated = self._try_event(act, constraints_state)
                if not violated:
                    candidates.append((act, new_state))

            if not candidates:
                # No activity can be chosen without violation => stop
                break

            # Randomly pick an activity among the non-violating ones
            chosen_act, chosen_state = random.choice(candidates)
            # Update constraints_state
            constraints_state = chosen_state

            # Append event to the trace
            e = Event({DEFAULT_NAME_KEY: chosen_act})
            trace.append(e)

        return trace

    def generate_log(self):
        """
        Generate an EventLog with self.n_traces traces.
        """
        log = EventLog()
        for i in range(self.n_traces):
            trace = self._generate_trace(case_id=i)
            log.append(trace)
        return log


def apply(declare_model, parameters=None) -> EventLog:
    """
    Produce a playout EventLog of a given DECLARE model.
    Optional parameters:
      - n_traces (int): number of traces to generate. Default = 1000
      - min_length (int): minimal length of each trace. Default = 3
      - max_length (int): maximal length of each trace. Default = 15
    """
    playout = DeclarePlayout(declare_model, parameters=parameters)
    return playout.generate_log()
