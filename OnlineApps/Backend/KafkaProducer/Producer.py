import asyncio
import json
import time
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from fastapi.responses import JSONResponse
import uvicorn
import logging

# Setup structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class KafkaWebhookProducer:
    """
    Kafka Webhook Producer Service
    Receives events via webhooks and publishes to Kafka topics
    """
    
    def __init__(self, brokers: List[str], config: Dict[str, Any] = None):
        self.brokers = brokers
        self.config = config or {}
        self.producer = None
        self.admin_client = None
        
        # Topics configuration
        self.topics = {
            'events': 'pm.test.events.raw',
            'watermark': 'pm.test.events.watermark',
            'dlq': 'pm.test.events.dlq'
        }
        
        # Metrics
        self.metrics = {
            'webhooks_received': 0,
            'events_produced': 0,
            'events_failed': 0,
            'watermarks_produced': 0,
            'dlq_messages': 0,
            'last_event_time': None,
            'last_watermark': None,
            'start_time': datetime.now(timezone.utc)
        }
        
        # Initialize Kafka components
        self.initialize_kafka()
    
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
            client_id='kafka-webhook-producer-admin'
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
                replication_factor=self.config.get('replication_factor', 2)
            ),
            NewTopic(
                name=self.topics['dlq'],
                num_partitions=self.config.get('partitions', 3),
                replication_factor=self.config.get('replication_factor', 2)
            )
        ]  
        try:
            self.admin_client.create_topics(new_topics=topics_config, validate_only=False)
            logger.info(f"Topics created: {[t.name for t in topics_config]}")
        except TopicAlreadyExistsError:
            logger.info("Topics already exist")
        except Exception as e:
            logger.error(f"Error creating topics: {e}")
    
    def initialize_kafka(self):
        """Initialize Kafka producer and topics"""
        try:
            self.create_topics_if_not_exist()
            self.producer = self.create_producer()
            logger.info("Kafka producer initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Kafka producer: {e}")
            raise
    
    async def produce_event(self, event_data: Dict[str, Any]) -> Dict[str, Any]:
        """Produce event to Kafka topic"""
        try:
            # Extract trace_id for partitioning
            trace_id = event_data.get('trace_id', 'unknown')
            event_id = event_data.get('event_id', str(uuid.uuid4()))
            
            # Add producer metadata
            event_data['kafka_producer'] = {
                'received_at': datetime.now(timezone.utc).isoformat(),
                'producer_id': 'kafka-webhook-producer',
                'webhook_metrics': {
                    'webhooks_received': self.metrics['webhooks_received'],
                    'events_produced': self.metrics['events_produced']
                }
            }
            
            # Send to Kafka
            future = self.producer.send(
                topic=self.topics['events'],
                key=trace_id,
                value=event_data,
                headers=[
                    ('event_id', event_id.encode('utf-8')),
                    ('trace_id', trace_id.encode('utf-8')),
                    ('producer', 'webhook-producer'.encode('utf-8'))
                ]
            )
            
            # Get result for confirmation
            record_metadata = future.get(timeout=10)
            
            self.metrics['events_produced'] += 1
            self.metrics['last_event_time'] = datetime.now(timezone.utc)
            
            logger.info(f"Event produced - ID: {event_id}, Trace: {trace_id}, "
                       f"Topic: {record_metadata.topic}, Partition: {record_metadata.partition}, "
                       f"Offset: {record_metadata.offset}")
            
            return {
                'status': 'success',
                'event_id': event_id,
                'trace_id': trace_id,
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset,
                'timestamp': record_metadata.timestamp
            }
            
        except Exception as e:
            self.metrics['events_failed'] += 1
            logger.error(f"Failed to produce event {event_data.get('event_id', 'unknown')}: {e}")
            
            # Send to DLQ
            await self.send_to_dlq(event_data, str(e))
            
            raise HTTPException(status_code=500, detail=f"Failed to produce event: {str(e)}")
    
    async def produce_watermark(self, watermark_data: Dict[str, Any]) -> Dict[str, Any]:
        """Produce watermark to Kafka topic"""
        try:
            # Add producer metadata
            watermark_data['kafka_producer'] = {
                'received_at': datetime.now(timezone.utc).isoformat(),
                'producer_id': 'kafka-webhook-producer'
            }
            
            # Send watermark to Kafka
            future = self.producer.send(
                topic=self.topics['watermark'],
                key="watermark",
                value=watermark_data,
                headers=[
                    ('type', 'watermark'.encode('utf-8')),
                    ('producer', 'webhook-producer'.encode('utf-8'))
                ]
            )
            
            # Get result
            record_metadata = future.get(timeout=5)
            
            self.metrics['watermarks_produced'] += 1
            self.metrics['last_watermark'] = datetime.now(timezone.utc)
            
            logger.info(f"Watermark produced - Topic: {record_metadata.topic}, "
                       f"Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
            return {
                'status': 'success',
                'topic': record_metadata.topic,
                'partition': record_metadata.partition,
                'offset': record_metadata.offset,
                'timestamp': record_metadata.timestamp
            }
            
        except Exception as e:
            logger.error(f"Failed to produce watermark: {e}")
            raise HTTPException(status_code=500, detail=f"Failed to produce watermark: {str(e)}")
    
    async def send_to_dlq(self, original_data: Dict[str, Any], error: str):
        """Send failed message to Dead Letter Queue"""
        try:
            dlq_message = {
                'error': error,
                'error_timestamp': datetime.now(timezone.utc).isoformat(),
                'original_event': original_data,
                'producer_id': 'kafka-webhook-producer'
            }
            
            # Send to DLQ topic
            future = self.producer.send(
                topic=self.topics['dlq'],
                key=original_data.get('trace_id', 'unknown'),
                value=dlq_message
            )
            
            future.get(timeout=5)
            self.metrics['dlq_messages'] += 1
            
            logger.info(f"Message sent to DLQ for event {original_data.get('event_id', 'unknown')}")
            
        except Exception as dlq_error:
            logger.error(f"Failed to send message to DLQ: {dlq_error}")
    
    def get_health(self) -> Dict[str, Any]:
        """Get service health status"""
        try:
            # Test Kafka connection
            metadata = self.producer.bootstrap_connected()
            
            return {
                'status': 'healthy',
                'kafka_connected': metadata,
                'topics': self.topics,
                'metrics': self.metrics.copy(),
                'uptime_seconds': (datetime.now(timezone.utc) - self.metrics['start_time']).total_seconds()
            }
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'metrics': self.metrics.copy()
            }
    
    def shutdown(self):
        """Gracefully shutdown the producer"""
        try:
            if self.producer:
                self.producer.flush()
                self.producer.close()
            if self.admin_client:
                self.admin_client.close()
            logger.info("Kafka producer shut down gracefully")
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")

# Global producer instance
kafka_producer = None

# FastAPI application
app = FastAPI(
    title="Kafka Webhook Producer",
    description="Receives events via webhooks and produces them to Kafka topics",
    version="1.0.0"
)

@app.on_event("startup")
async def startup_event():
    """Initialize Kafka producer on startup"""
    global kafka_producer
    
    # Configuration
    brokers = ['localhost:29092', 'localhost:29093', 'localhost:29094']
    config = {
        'partitions': 3,
        'replication_factor': 2,
        'producer': {
            'linger_ms': 20,
            'batch_size': 131072  # 128KB
        }
    }
    
    try:
        kafka_producer = KafkaWebhookProducer(brokers, config)
        logger.info("Kafka Webhook Producer service started")
    except Exception as e:
        logger.error(f"Failed to start Kafka producer: {e}")
        raise

@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    global kafka_producer
    if kafka_producer:
        kafka_producer.shutdown()

@app.post("/events")
async def receive_event(request: Request, background_tasks: BackgroundTasks):
    """Receive event via webhook and produce to Kafka"""
    global kafka_producer
    
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka producer not initialized")
    
    try:
        # Parse JSON payload
        event_data = await request.json()
        
        # Update metrics
        kafka_producer.metrics['webhooks_received'] += 1
        
        # Log received event
        logger.info(f"Webhook received - Event ID: {event_data.get('event_id', 'unknown')}, "
                   f"Trace ID: {event_data.get('trace_id', 'unknown')}")
        
        # Produce to Kafka
        result = await kafka_producer.produce_event(event_data)
        
        return JSONResponse(content=result, status_code=200)
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Error processing webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/events/watermark")
async def receive_watermark(request: Request):
    """Receive watermark via webhook and produce to Kafka"""
    global kafka_producer
    
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka producer not initialized")
    
    try:
        # Parse JSON payload
        watermark_data = await request.json()
        
        # Log received watermark
        logger.info(f"Watermark webhook received - "
                   f"Event time: {watermark_data.get('watermark_event_time', 'unknown')}")
        
        # Produce to Kafka
        result = await kafka_producer.produce_watermark(watermark_data)
        
        return JSONResponse(content=result, status_code=200)
        
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Error processing watermark webhook: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    global kafka_producer
    
    if not kafka_producer:
        return JSONResponse(
            content={"status": "unhealthy", "error": "Kafka producer not initialized"}, 
            status_code=503
        )
    
    health = kafka_producer.get_health()
    status_code = 200 if health['status'] == 'healthy' else 503
    
    return JSONResponse(content=health, status_code=status_code)

@app.get("/metrics")
async def get_metrics():
    """Get service metrics"""
    global kafka_producer
    
    if not kafka_producer:
        return JSONResponse(
            content={"error": "Kafka producer not initialized"}, 
            status_code=503
        )
    
    return JSONResponse(content=kafka_producer.metrics)

@app.get("/topics")
async def get_topics():
    """Get configured topics"""
    global kafka_producer
    
    if not kafka_producer:
        return JSONResponse(
            content={"error": "Kafka producer not initialized"}, 
            status_code=503
        )
    
    return JSONResponse(content=kafka_producer.topics)

@app.post("/flush")
async def flush_producer():
    """Flush Kafka producer buffer"""
    global kafka_producer
    
    if not kafka_producer:
        raise HTTPException(status_code=503, detail="Kafka producer not initialized")
    
    try:
        kafka_producer.producer.flush()
        return JSONResponse(content={"status": "flushed", "timestamp": datetime.now(timezone.utc).isoformat()})
    except Exception as e:
        logger.error(f"Error flushing producer: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def main():
    """Run the FastAPI server"""
    uvicorn.run(
        "Producer:app",
        host="0.0.0.0",
        port=8100,
        reload=False,
        access_log=True,
        log_level="info"
    )

if __name__ == '__main__':
    main()