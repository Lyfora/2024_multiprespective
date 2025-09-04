import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from pm4py.objects.log.importer.xes import importer as xes_importer
import logging

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class Event:
    """Normalized event structure"""
    event_id: str
    trace_id: str
    activity: str
    lifecycle: Optional[str]
    resource: Optional[str]
    timestamp: datetime
    event_index: int
    case_attrs: Dict[str, Any]
    event_attrs: Dict[str, Any]
    source: Dict[str, Any]
    inter_event_delta: float = 0.0  # in seconds

@dataclass
class SimulationMeta:
    """Simulation metadata"""
    replay_mode: str
    speed: float
    scheduled_at: datetime
    is_late: bool
    watermark: datetime

class EventStreamSimulator:
    """
    Event Stream Simulator for Online Conformance Checking
    Reads XES files and replays events to Kafka with realistic timing
    """
    
    def __init__(self, brokers: List[str], config: Dict[str, Any] = None):
        self.brokers = brokers
        self.config = config or {}
        self.producer = None
        self.admin_client = None
        
        # Simulation state
        self.is_running = False
        self.is_paused = False
        self.speed_factor = 1.0
        self.replay_mode = "exact"  # exact, scaled, fixed_interval, burst
        self.events = []
        self.current_event_index = 0
        self.start_time = None
        self.start_event_time = None
        self.last_event_time = None
        
        # Topics configuration
        self.topics = {
            'events': 'pm.test.events.raw',
            'watermark': 'pm.test.events.watermark',
            'dlq': 'pm.test.events.dlq'
        }
        
        # Metrics
        self.metrics = {
            'events_total': 0,
            'events_inflight': 0,
            'events_rate': 0.0,
            'late_events': 0,
            'last_watermark': None
        }
    
    def create_producer(self) -> KafkaProducer:
        """Create Kafka producer with optimized settings"""
        producer_config = {
            'bootstrap_servers': self.brokers,
            'value_serializer': lambda v: json.dumps(v, default=str).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': 'all',
            'enable_idempotence': True,
            'retries': 5,
            'max_in_flight_requests_per_connection': 1,
            'linger_ms': 10,
            'batch_size': 65536,  # 64KB
            'request_timeout_ms': 30000,
            'delivery_timeout_ms': 120000
        }
        producer_config.update(self.config.get('producer', {}))
        return KafkaProducer(**producer_config)
    
    def create_admin_client(self) -> KafkaAdminClient:
        """Create Kafka admin client"""
        return KafkaAdminClient(
            bootstrap_servers=self.brokers,
            client_id='event-simulator-admin'
        )
    
    def create_topics_if_not_exist(self):
        """Create required topics with proper configuration"""
        if not self.admin_client:
            self.admin_client = self.create_admin_client()
        
        topics_config = [
            NewTopic(
                name=self.topics['events'],
                num_partitions=self.config.get('partitions', 3),
                replication_factor=self.config.get('replication_factor', 2)
            ),
            NewTopic(
                name=self.topics['watermark'],
                num_partitions=1,
                replication_factor=self.config.get('replication_factor', 3)
            ),
            NewTopic(
                name=self.topics['dlq'],
                num_partitions=self.config.get('partitions', 6),
                replication_factor=self.config.get('replication_factor', 3)
            )
        ]
        
        try:
            self.admin_client.create_topics(new_topics=topics_config, validate_only=False)
            logger.info(f"Topics created: {[t.name for t in topics_config]}")
        except TopicAlreadyExistsError:
            logger.info("Topics already exist")
        except Exception as e:
            logger.error(f"Error creating topics: {e}")
    
    def load_xes_files(self, file_paths: List[str]) -> List[Event]:
        """Load and normalize XES files to Event objects"""
        all_events = []
        
        for file_path in file_paths:
            logger.info(f"Loading XES file: {file_path}")
            try:
                log = xes_importer.apply(file_path)
                
                for trace in log:
                    trace_id = trace.attributes.get("concept:name", str(uuid.uuid4()))
                    
                    # Extract case attributes
                    case_attrs = {}
                    for key, value in trace.attributes.items():
                        if key != "concept:name":
                            case_attrs[key] = str(value)
                    
                    trace_events = []
                    for idx, event in enumerate(trace):
                        # Extract event attributes
                        event_attrs = {}
                        for key, value in event.items():
                            if key not in ['concept:name', 'org:resource', 'lifecycle:transition', 'time:timestamp']:
                                event_attrs[key] = str(value)
                        
                        normalized_event = Event(
                            event_id=str(uuid.uuid4()),
                            trace_id=trace_id,
                            activity=event.get("concept:name", "unknown"),
                            lifecycle=event.get("lifecycle:transition"),
                            resource=event.get("resource"),
                            timestamp=event.get("time:timestamp", datetime.now(timezone.utc)),
                            event_index=idx,
                            case_attrs=case_attrs,
                            event_attrs=event_attrs,
                            source={"file": file_path, "trace_index": len(all_events)}
                        )
                        trace_events.append(normalized_event)
                    
                    # Calculate inter-event deltas for this trace
                    for i in range(1, len(trace_events)):
                        delta = (trace_events[i].timestamp - trace_events[i-1].timestamp).total_seconds()
                        trace_events[i].inter_event_delta = max(0, delta)
                    
                    all_events.extend(trace_events)
                    
            except Exception as e:
                logger.error(f"Error loading XES file {file_path}: {e}")
        
        # Global sorting by timestamp
        all_events.sort(key=lambda x: x.timestamp)
        
        # Recalculate global inter-event deltas
        for i in range(1, len(all_events)):
            delta = (all_events[i].timestamp - all_events[i-1].timestamp).total_seconds()
            all_events[i].inter_event_delta = max(0, delta)
        
        logger.info(f"Loaded {len(all_events)} events from {len(file_paths)} files")
        return all_events
    
    def create_event_message(self, event: Event, sim_meta: SimulationMeta) -> Dict[str, Any]:
        """Create Kafka message from Event and simulation metadata"""
        return {
            "event_id": event.event_id,
            "trace_id": event.trace_id,
            "activity": event.activity,
            "lifecycle": event.lifecycle,
            "resource": event.resource,
            "timestamp": event.timestamp.isoformat(),
            "event_index": event.event_index,
            "case_attrs": event.case_attrs,
            "event_attrs": event.event_attrs,
            "source": event.source,
            "sim": {
                "replay_mode": sim_meta.replay_mode,
                "speed": sim_meta.speed,
                "scheduled_at": sim_meta.scheduled_at.isoformat(),
                "is_late": sim_meta.is_late,
                "watermark": sim_meta.watermark.isoformat()
            }
        }
    
    def create_watermark_message(self, event_time: datetime, events_emitted: int) -> Dict[str, Any]:
        """Create watermark message"""
        percent_complete = (self.current_event_index / len(self.events)) * 100 if self.events else 0
        
        return {
            "watermark_event_time": event_time.isoformat(),
            "emitted_at": datetime.now(timezone.utc).isoformat(),
            "events_emitted": events_emitted,
            "percent_complete": round(percent_complete, 2),
            "late_events": self.metrics['late_events'],
            "current_speed": self.speed_factor,
            "replay_mode": self.replay_mode
        }
    
    def calculate_delay(self, event: Event) -> float:
        """Calculate delay based on replay mode and speed factor"""
        if self.replay_mode == "exact":
            return event.inter_event_delta
        elif self.replay_mode == "scaled":
            return event.inter_event_delta * self.speed_factor
        elif self.replay_mode == "fixed_interval":
            return self.config.get('fixed_interval_ms', 1000) / 1000.0
        elif self.replay_mode == "burst":
            return 0.0  # No delay in burst mode
        else:
            return event.inter_event_delta
    
    async def emit_watermark(self, event_time: datetime, events_emitted: int):
        """Emit watermark message"""
        try:
            watermark_msg = self.create_watermark_message(event_time, events_emitted)
            
            future = self.producer.send(
                topic=self.topics['watermark'],
                key="watermark",
                value=watermark_msg
            )
            
            future.get(timeout=5)
            self.metrics['last_watermark'] = event_time
            logger.debug(f"Watermark emitted: {event_time}")
            
        except Exception as e:
            logger.error(f"Error emitting watermark: {e}")
    
    async def replay_events(self):
        """Main replay loop"""
        if not self.events:
            logger.warning("No events to replay")
            return
        
        self.start_time = datetime.now(timezone.utc)
        self.start_event_time = self.events[0].timestamp
        self.last_event_time = self.start_event_time
        self.is_running = True
        
        logger.info(f"Starting replay of {len(self.events)} events")
        logger.info(f"Mode: {self.replay_mode}, Speed: {self.speed_factor}x")
        
        watermark_interval = self.config.get('watermark_interval_events', 1000)
        
        for i in range(self.current_event_index, len(self.events)):
            if not self.is_running:
                break
            
            while self.is_paused:
                await asyncio.sleep(0.1)
                if not self.is_running:
                    break
            
            event = self.events[i]
            self.current_event_index = i
            
            # Calculate and apply delay
            if i > 0:
                delay = self.calculate_delay(event)
                if delay > 0:
                    await asyncio.sleep(delay)
            
            # Check if event is late (simplified check)
            current_sim_time = datetime.now(timezone.utc)
            is_late = False
            if hasattr(event, 'expected_time'):
                is_late = current_sim_time > event.expected_time
                if is_late:
                    self.metrics['late_events'] += 1
            
            # Create simulation metadata
            sim_meta = SimulationMeta(
                replay_mode=self.replay_mode,
                speed=self.speed_factor,
                scheduled_at=current_sim_time,
                is_late=is_late,
                watermark=event.timestamp
            )
            
            # Create and send message
            try:
                message = self.create_event_message(event, sim_meta)
                
                future = self.producer.send(
                    topic=self.topics['events'],
                    key=event.trace_id,
                    value=message
                )
                
                # Get result for monitoring
                record_metadata = future.get(timeout=10)
                
                self.metrics['events_total'] += 1
                self.last_event_time = event.timestamp
                
                logger.info(f"Event sent - Trace: {event.trace_id}, Activity: {event.activity}, "
                           f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
                
                # Emit watermark periodically
                if self.metrics['events_total'] % watermark_interval == 0:
                    await self.emit_watermark(event.timestamp, self.metrics['events_total'])
                
            except Exception as e:
                logger.error(f"Error sending event {event.event_id}: {e}")
                # Optionally send to DLQ
                try:
                    dlq_message = {"error": str(e), "original_event": asdict(event)}
                    self.producer.send(
                        topic=self.topics['dlq'],
                        key=event.trace_id,
                        value=dlq_message
                    )
                except Exception as dlq_error:
                    logger.error(f"Error sending to DLQ: {dlq_error}")
        
        # Final watermark
        if self.events:
            await self.emit_watermark(self.events[-1].timestamp, self.metrics['events_total'])
        
        self.is_running = False
        logger.info(f"Replay completed. Total events: {self.metrics['events_total']}")
    
    def start_replay(self, file_paths: List[str], mode: str = "exact", speed: float = 1.0):
        """Start the replay process"""
        try:
            self.replay_mode = mode
            self.speed_factor = speed
            
            # Initialize components
            self.create_topics_if_not_exist()
            self.producer = self.create_producer()
            
            # Load events
            self.events = self.load_xes_files(file_paths)
            
            # Start replay
            asyncio.run(self.replay_events())
            
        finally:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            if self.admin_client:
                self.admin_client.close()
    
    def pause(self):
        """Pause the replay"""
        self.is_paused = True
        logger.info("Replay paused")
    
    def resume(self):
        """Resume the replay"""
        self.is_paused = False
        logger.info("Replay resumed")
    
    def stop(self):
        """Stop the replay"""
        self.is_running = False
        logger.info("Replay stopped")
    
    def set_speed(self, speed: float):
        """Change replay speed on the fly"""
        self.speed_factor = speed
        logger.info(f"Speed changed to {speed}x")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current simulation status"""
        progress = (self.current_event_index / len(self.events)) * 100 if self.events else 0
        
        return {
            "is_running": self.is_running,
            "is_paused": self.is_paused,
            "progress_percent": round(progress, 2),
            "current_event_index": self.current_event_index,
            "total_events": len(self.events),
            "replay_mode": self.replay_mode,
            "speed_factor": self.speed_factor,
            "metrics": self.metrics.copy()
        }

def main():
    """Main function demonstrating usage"""
    # Configuration
    brokers = ['localhost:9092', 'localhost:9093', 'localhost:9094']
    
    config = {
        'partitions': 3,
        'replication_factor': 2,  # Reduced for development
        'watermark_interval_events': 100,
        'fixed_interval_ms': 100,
        'producer': {
            'linger_ms': 20,
            'batch_size': 131072  # 128KB
        }
    }
    
    # Create simulator
    simulator = EventStreamSimulator(brokers, config)
    
    # Start replay
    try:
        simulator.start_replay(
            file_paths=["output_logv2.xes"],
            mode="fixed_interval",  # Options: exact, scaled, fixed_interval, burst
            speed=0.25      # 4x faster than original
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        simulator.stop()
    except Exception as e:
        logger.error(f"Error in simulation: {e}")

if __name__ == '__main__':
    main()