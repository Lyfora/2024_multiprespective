from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import time
import random
from pm4py.objects.log.importer.xes import importer as xes_importer
from datetime import datetime

# Configuration matching the paper
TOPIC_NAME = "event-topic"
NUM_PARTITIONS = 3
REPLICATION_FACTOR = 2
BROKERS = ['localhost:9092', 'localhost:9093', 'localhost:9094']

def create_topic_if_not_exists():
    """Create topic with 3 partitions and replication factor 2"""
    admin_client = KafkaAdminClient(
        bootstrap_servers=BROKERS,
        client_id='event-producer-admin'
    )
    
    topic_list = []
    topic = NewTopic(
        name=TOPIC_NAME,
        num_partitions=NUM_PARTITIONS,
        replication_factor=REPLICATION_FACTOR
    )
    topic_list.append(topic)
    
    try:
        admin_client.create_topics(new_topics=topic_list, validate_only=False)
        print(f"Topic '{TOPIC_NAME}' created with {NUM_PARTITIONS} partitions and RF={REPLICATION_FACTOR}")
    except TopicAlreadyExistsError:
        print(f"Topic '{TOPIC_NAME}' already exists")
    finally:
        admin_client.close()

def leader_info(admin_client, topic, partition):
    try:
        metadata = admin_client.describe_topics([topic])[0]  # dict
        partition_info = next(
            (p for p in metadata["partitions"] if p["partition"] == partition),
            None
        )
        if partition_info is None:
            return {"broker_id": None, "host": None, "port": None}

        leader_id = partition_info["leader"]

        # Fetch broker metadata
        cluster_metadata = admin_client.describe_cluster()
        brokers = cluster_metadata["brokers"]

        broker = next((b for b in brokers if b["node_id"] == leader_id), None)

        if broker is None:
            return {"broker_id": leader_id, "host": None, "port": None}

        return {
            "broker_id": leader_id,
            "host": broker["host"],
            "port": broker["port"]
        }
    except Exception as e:
        print(f"Leader info error: {e}")
        return {"broker_id": None, "host": None, "port": None}

def main():
    # Create topic first
    create_topic_if_not_exists()
    admin_client = KafkaAdminClient(bootstrap_servers=BROKERS, client_id="event-producer-admin")

    # Load XES file
    print("Loading XES file...")
    log = xes_importer.apply("new_log.xes")
    log = log[:1]  # Process first 5 traces as in your example
    
    # Create producer with configuration for distributed setup
    producer = KafkaProducer(
        bootstrap_servers=BROKERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        acks='all',  # Wait for all in-sync replicas
        retries=5,
        max_in_flight_requests_per_connection=1,
        enable_idempotence=True,  # Ensure exactly-once semantics
        linger_ms=10  # Small batching for better throughput
    )
     # Force initial metadata refresh

    print(f"\nStreaming events to Kafka topic '{TOPIC_NAME}'...")
    print("=" * 60)
    
    event_count = 0

    for trace in log:
        case_id = trace.attributes.get("concept:name", "unknown_case")
        
        for idx, event in enumerate(trace):
            # Convert to your format
            event_dict = {
                'case_id': case_id,
                'activity': event.get("concept:name", "unknown"),
                'resource': event.get("org:resource", "unknown"),
                'timestamp': str(event.get("time:timestamp", "")),
                'product_type': event.get("product_type", "unknown"),
                'concept:name': event.get("concept:name", "unknown"),
                'time:timestamp': str(event.get("time:timestamp", "")),
                '@@index': str(idx),
                '@@case_index': case_id
            }
            
            # Add any additional attributes
            for k, v in event.items():
                if k not in event_dict:
                    event_dict[k] = str(v)
            
            future = producer.send(
                topic=TOPIC_NAME,
                key=case_id,
                value=event_dict
            )
            print(f"Sent: {event_dict}")
            try:
                md = future.get(timeout=10)  # RecordMetadata
                li = leader_info(admin_client, TOPIC_NAME, md.partition)

                print(json.dumps({
                    "event": "produced",
                    "topic": md.topic,
                    "partition": md.partition,
                    "offset": md.offset,
                    "leader": li,                  # {broker_id, host, port}
                    "key": case_id,
                    "activity": event_dict["activity"],
                    "timestamp": event_dict.get("timestamp")
                }))
            except Exception as e:
                print(json.dumps({
                    "event": "produce_error",
                    "error": str(e),
                    "key": case_id
                }))
            # time.sleep(random.uniform(0.5, 2))

    producer.flush()
    producer.close()

if __name__ == '__main__':
    main()