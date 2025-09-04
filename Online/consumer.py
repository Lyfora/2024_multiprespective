# consumer.py
from kafka import KafkaConsumer
import json
from pm4py.objects.log.obj import EventLog, Trace, Event

# Start empty log
online_log = EventLog()

consumer = KafkaConsumer(
    "event_log",
    bootstrap_servers="localhost:9092",
    value_deserializer=lambda m: json.loads(m.decode("utf-8"))
)

for msg in consumer:
    event_data = msg.value
    
    # Use case_id (or equivalent) to find trace
    case_id = event_data.get("concept:name", "unknown_case")
    matching_trace = None
    for t in online_log:
        if t.attributes.get("concept:name") == case_id:
            matching_trace = t
            break

    if not matching_trace:
        matching_trace = Trace()
        matching_trace.attributes["concept:name"] = case_id
        online_log.append(matching_trace)

    # Append event
    ev = Event(event_data)
    matching_trace.append(ev)

    print(f"Added event: {event_data}")

    # 👉 Here you can do online mining, conformance checking, etc.
