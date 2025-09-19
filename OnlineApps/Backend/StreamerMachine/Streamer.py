import asyncio
import json
import time
import uuid
import aiohttp
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, asdict
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

class XESEventProducer:
    """
    XES Event Producer - Loads XES files and sends events via webhooks
    Simulates a real system generating events
    """
    
    def __init__(self, webhook_url: str, config: Dict[str, Any] = None):
        self.webhook_url = webhook_url
        self.config = config or {}
        
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
        
        # HTTP session for webhook calls
        self.session = None
        
        # Metrics
        self.metrics = {
            'events_total': 0,
            'events_sent': 0,
            'events_failed': 0,
            'events_rate': 0.0,
            'late_events': 0,
            'last_watermark': None
        }
    
    async def create_session(self):
        """Create aiohttp session with proper configuration"""
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        connector = aiohttp.TCPConnector(
            limit=100,
            limit_per_host=20,
            ttl_dns_cache=300,
            use_dns_cache=True
        )
        
        self.session = aiohttp.ClientSession(
            timeout=timeout,
            connector=connector,
            headers={
                'Content-Type': 'application/json',
                'User-Agent': 'XES-Event-Producer/1.0'
            }
        )
    
    async def close_session(self):
        """Close aiohttp session"""
        if self.session:
            await self.session.close()
    
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
                            source={"file": file_path, "trace_index": len(all_events), "producer": "xes-loader"}
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
        """Create webhook payload from Event and simulation metadata"""
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
            },
            "producer_metrics": {
                "events_sent": self.metrics['events_sent'],
                "events_total": self.metrics['events_total']
            }
        }
    
    def calculate_delay(self, event: Event) -> float:
        """Calculate delay based on replay mode and speed factor"""
        if self.replay_mode == "exact":
            return event.inter_event_delta / self.speed_factor
        elif self.replay_mode == "scaled":
            return event.inter_event_delta * self.speed_factor
        elif self.replay_mode == "fixed_interval":
            return self.config.get('fixed_interval_ms', 1000) / 1000.0
        elif self.replay_mode == "burst":
            return 0.0  # No delay in burst mode
        else:
            return event.inter_event_delta / self.speed_factor
    
    async def send_event_webhook(self, event: Event, sim_meta: SimulationMeta) -> bool:
        """Send event via webhook to Kafka producer service"""
        try:
            message = self.create_event_message(event, sim_meta)
            
            async with self.session.post(
                self.webhook_url,
                json=message,
                headers={'X-Event-ID': event.event_id, 'X-Trace-ID': event.trace_id}
            ) as response:
                
                if response.status == 200:
                    self.metrics['events_sent'] += 1
                    logger.debug(f"Event sent successfully - ID: {event.event_id}, "
                               f"Trace: {event.trace_id}, Activity: {event.activity}")
                    return True
                else:
                    error_text = await response.text()
                    logger.error(f"Webhook failed - Status: {response.status}, "
                               f"Event: {event.event_id}, Error: {error_text}")
                    self.metrics['events_failed'] += 1
                    return False
                    
        except asyncio.TimeoutError:
            logger.error(f"Webhook timeout for event {event.event_id}")
            self.metrics['events_failed'] += 1
            return False
        except Exception as e:
            logger.error(f"Webhook error for event {event.event_id}: {e}")
            self.metrics['events_failed'] += 1
            return False
    
    async def send_watermark_webhook(self, event_time: datetime, events_emitted: int):
        """Send watermark via webhook"""
        try:
            percent_complete = (self.current_event_index / len(self.events)) * 100 if self.events else 0
            
            watermark_msg = {
                "type": "watermark",
                "watermark_event_time": event_time.isoformat(),
                "emitted_at": datetime.now(timezone.utc).isoformat(),
                "events_emitted": events_emitted,
                "percent_complete": round(percent_complete, 2),
                "late_events": self.metrics['late_events'],
                "current_speed": self.speed_factor,
                "replay_mode": self.replay_mode,
                "producer_id": "xes-event-producer"
            }
            
            async with self.session.post(
                f"{self.webhook_url}/watermark",
                json=watermark_msg
            ) as response:
                
                if response.status == 200:
                    self.metrics['last_watermark'] = event_time
                    logger.debug(f"Watermark sent: {event_time}")
                else:
                    logger.error(f"Watermark webhook failed: {response.status}")
                    
        except Exception as e:
            logger.error(f"Error sending watermark webhook: {e}")
    
    async def replay_events(self):
        """Main replay loop - sends events via webhooks"""
        if not self.events:
            logger.warning("No events to replay")
            return
        
        await self.create_session()
        
        try:
            self.start_time = datetime.now(timezone.utc)
            self.start_event_time = self.events[0].timestamp
            self.last_event_time = self.start_event_time
            self.is_running = True
            
            logger.info(f"Starting event production of {len(self.events)} events")
            logger.info(f"Mode: {self.replay_mode}, Speed: {self.speed_factor}x")
            logger.info(f"Webhook URL: {self.webhook_url}")
            
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
                
                # Send event via webhook
                success = await self.send_event_webhook(event, sim_meta)
                
                if success:
                    self.metrics['events_total'] += 1
                    self.last_event_time = event.timestamp
                    
                    logger.info(f"Event produced - Trace: {event.trace_id}, "
                               f"Activity: {event.activity}, Total: {self.metrics['events_total']}")
                
                # Send watermark periodically
                if self.metrics['events_total'] % watermark_interval == 0:
                    await self.send_watermark_webhook(event.timestamp, self.metrics['events_total'])
            
            # Final watermark
            if self.events:
                await self.send_watermark_webhook(self.events[-1].timestamp, self.metrics['events_total'])
            
            self.is_running = False
            logger.info(f"Event production completed. Total events: {self.metrics['events_total']}, "
                       f"Sent: {self.metrics['events_sent']}, Failed: {self.metrics['events_failed']}")
        
        finally:
            await self.close_session()
    
    def start_production(self, file_paths: List[str], mode: str = "exact", speed: float = 1.0):
        """Start the event production process"""
        self.replay_mode = mode
        self.speed_factor = speed
        
        # Load events
        self.events = self.load_xes_files(file_paths)
        
        # Start production
        asyncio.run(self.replay_events())
    
    def pause(self):
        """Pause the production"""
        self.is_paused = True
        logger.info("Event production paused")
    
    def resume(self):
        """Resume the production"""
        self.is_paused = False
        logger.info("Event production resumed")
    
    def stop(self):
        """Stop the production"""
        self.is_running = False
        logger.info("Event production stopped")
    
    def set_speed(self, speed: float):
        """Change replay speed on the fly"""
        self.speed_factor = speed
        logger.info(f"Speed changed to {speed}x")
    
    def get_status(self) -> Dict[str, Any]:
        """Get current production status"""
        progress = (self.current_event_index / len(self.events)) * 100 if self.events else 0
        
        return {
            "is_running": self.is_running,
            "is_paused": self.is_paused,
            "progress_percent": round(progress, 2),
            "current_event_index": self.current_event_index,
            "total_events": len(self.events),
            "replay_mode": self.replay_mode,
            "speed_factor": self.speed_factor,
            "webhook_url": self.webhook_url,
            "metrics": self.metrics.copy()
        }

def main():
    """Main function demonstrating usage"""
    # Configuration
    webhook_url = "http://localhost:8100/events"  # Kafka producer webhook endpoint
    
    config = {
        'watermark_interval_events': 5000,
        'fixed_interval_ms': 5000
    }
    
    # Create event producer
    producer = XESEventProducer(webhook_url, config)
    
    # Start production
    try:
        producer.start_production(
            file_paths=["output_logv2.xes"],
            mode="fixed_interval",  # Options: exact, scaled, fixed_interval, burst
            speed=1      # 4x faster than original
        )
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        producer.stop()
    except Exception as e:
        logger.error(f"Error in event production: {e}")

if __name__ == '__main__':
    main()