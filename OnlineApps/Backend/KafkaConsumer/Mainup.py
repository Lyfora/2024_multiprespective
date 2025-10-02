# consumer.py
from kafka import KafkaConsumer
import json
import time
from datetime import datetime
import pandas as pd
import pm4py
from neo4j import GraphDatabase

# Import separation algorithm file
import Algorithm.GO_TR as GO_TR
import Algorithm.Neo4jFunc as neo4j_func

from pm4py.objects.petri_net.utils import reachability_graph
import threading
from collections import defaultdict
import queue

# New Import for FastAPI
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from threading import Thread, Lock
from typing import List, Dict
import uuid
from queue import Queue
from collections import deque

class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.connection_ids: Dict[WebSocket, str] = {}

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        connection_id = str(uuid.uuid4())
        self.active_connections.append(websocket)
        self.connection_ids[websocket] = connection_id
        print(f"Client {connection_id} connected")
        return connection_id

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            connection_id = self.connection_ids.get(websocket, "Unknown")
            self.active_connections.remove(websocket)
            del self.connection_ids[websocket]
            print(f"Client {connection_id} disconnected")

    async def send_personal_message(self, message: dict, websocket: WebSocket):
        await websocket.send_json(message)

    async def broadcast(self, message: dict):
        disconnected = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                print(f"Error sending message: {e}")
                disconnected.append(connection)
        
        for conn in disconnected:
            self.disconnect(conn)


class GOTRKafkaConsumer:
    def __init__(self, kafka_config=None, neo4j_config=None):
        # Kafka configuration
        self.kafka_config = kafka_config or {
            'bootstrap_servers': ['localhost:29092', 'localhost:29093', 'localhost:29094'],
            'group_id': 'gotr_consumer_group_7',
            'value_deserializer': lambda m: json.loads(m.decode('utf-8')),
            'key_deserializer': lambda m: m.decode('utf-8') if m else None
        }
        
        # Neo4j configuration
        self.neo4j_config = neo4j_config or {
            'uri': "neo4j://127.0.0.1:7687",
            'user': "neo4j",
            'password': "12345678"
        }

        # FastAPI and WebSocket components
        self.manager = ConnectionManager()
        self.alert_queue = Queue()
        self.recent_alerts = deque(maxlen=200)
        self.recent_alerts_lock = threading.Lock()
        
        self.app = self.create_app()
        self.websocket_thread = None
      
        # Event buffering
        self.event_buffer = defaultdict(list)
        self.processing_queue = queue.Queue()
        self.is_running = False

        # Thread safety locks
        self.consumer_lock = Lock()
        self.neo4j_lock = Lock()
        
        self.consumer = None
        self.driver = None
        self.session = None

        # Master Model Properties
        self.trans_name = []
        self.states = []
        self.places = []
        
        # Configuration
        self.check = None
        self.is_configured = False
        self.configuration_event = threading.Event()
        self.consumer_thread = None
        self.stop_event = threading.Event()

        # State Management
        self.active_cases = set()
        self.anomaly_scores = defaultdict(float)
        self.case_event_history = defaultdict(list)
        self.unknown_activities = defaultdict(list)

    def create_app(self):
        """Create and configure FastAPI app"""
        app = FastAPI()

        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

        manager = self.manager
        alert_queue = self.alert_queue
        recent_alerts = self.recent_alerts
        recent_alerts_lock = self.recent_alerts_lock

        consumer_instance = self

        @app.post("/api/configure")
        async def configure_consumer(config: dict):
            """Configure the consumer before starting"""
            if 'mode' not in config:
                return {"status": "error", "message": "Missing 'mode' field in configuration"}

            mode = config['mode']
            conformance = config.get('conformance', 'continue')

            if mode not in ['online', 'multi']:
                return {"status": "error", "message": f"Invalid mode '{mode}'. Must be 'online' or 'multi'"}
            if conformance not in ['continue', 'reset']:
                return {"status": "error", "message": f"Invalid conformance '{conformance}'. Must be 'continue' or 'reset'"}

            try:
                # Set the configuration
                consumer_instance.check = mode

                if conformance == 'reset':
                    print("Reset mode detected - performing full reset...")
                    
                    # 1. Stop consumer thread safely
                    await consumer_instance.safe_stop_consumer()
                    
                    # 2. Close and cleanup Kafka consumer
                    with consumer_instance.consumer_lock:
                        if consumer_instance.consumer:
                            try:
                                print("Closing Kafka consumer...")
                                consumer_instance.consumer.close()
                                consumer_instance.consumer = None
                            except Exception as e:
                                print(f"Error closing Kafka consumer: {e}")

                    # 3. Reinitialize Neo4j session
                    with consumer_instance.neo4j_lock:
                        consumer_instance.initialize_neo4j_session()

                    # 4. Clear all state
                    consumer_instance.active_cases.clear()
                    consumer_instance.anomaly_scores.clear()
                    consumer_instance.case_event_history.clear()
                    consumer_instance.unknown_activities.clear()
                    consumer_instance.event_buffer.clear()

                    # 5. Reinitialize consumer with reset
                    with consumer_instance.consumer_lock:
                        consumer_instance.initialize_kafka_consumer('reset')

                    # 6. Mark as configured and start new thread
                    consumer_instance.is_configured = True
                    consumer_instance.configuration_event.set()
                    consumer_instance.start_consumer_thread()
                    
                    print("Reset completed successfully!")
                else:
                    # Continue mode - just initialize and start
                    print("Continue mode detected - starting consumer...")
                    
                    with consumer_instance.consumer_lock:
                        if not consumer_instance.consumer:
                            consumer_instance.initialize_kafka_consumer('continue')
                    
                    consumer_instance.is_configured = True
                    consumer_instance.configuration_event.set()
                    
                    if not consumer_instance.consumer_thread or not consumer_instance.consumer_thread.is_alive():
                        consumer_instance.start_consumer_thread()
                
                print(f"Consumer configured with mode: {mode} and conformance: {conformance}")

                # Notify connected clients
                await manager.broadcast({
                    "type": "configured",
                    "timestamp": datetime.now().isoformat(),
                    "mode": mode,
                    "conformance": conformance,
                    "message": f"Consumer configured in {mode} mode with {conformance} conformance"
                })

                return {
                    "status": "success",
                    "mode": mode,
                    "conformance": conformance,
                    "timestamp": datetime.now().isoformat(),
                    "message": "Consumer configured successfully. Ready to start processing."
                }
            
            except Exception as e:
                print(f"Error during configuration: {e}")
                import traceback
                traceback.print_exc()
                return {
                    "status": "error",
                    "message": f"Configuration failed: {str(e)}"
                }
    
        @app.get("/api/configuration")
        async def get_configuration():
            """Get current configuration status"""
            return {
                "is_configured": consumer_instance.is_configured,
                "mode": consumer_instance.check,
                "timestamp": datetime.now().isoformat()
            }
    
        @app.post("/api/start")
        async def start_processing():
            """Manually trigger the start of processing"""
            if not consumer_instance.is_configured:
                return {
                    "status": "error",
                    "message": "Consumer not configured. Please configure first using /api/configure"
                }

            return {
                "status": "success",
                "message": "Processing started",
                "mode": consumer_instance.check
            }
    
        @app.get("/api/status")
        async def get_status():
            """Get overall system status"""
            with recent_alerts_lock:
                alert_count = len(recent_alerts)

            return {
                "is_configured": consumer_instance.is_configured,
                "mode": consumer_instance.check,
                "is_running": consumer_instance.is_running,
                "active_cases": len(consumer_instance.active_cases),
                "active_connections": len(manager.active_connections),
                "total_alerts": alert_count,
                "timestamp": datetime.now().isoformat()
            }

        async def process_alert_queue():
            """Background task to process alerts from the queue"""
            while True:
                try:
                    if not alert_queue.empty():
                        alert_data = alert_queue.get_nowait()
                        alert_data['alert_id'] = f"{alert_data['timestamp']}_{alert_data['case_id']}"

                        with recent_alerts_lock:
                            recent_alerts.append(alert_data)

                        await manager.broadcast(alert_data)
                    else:
                        await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"Error processing alert: {e}")

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            connection_id = await manager.connect(websocket)

            async def heartbeat():
                try:
                    while True:
                        await asyncio.sleep(30)
                        await websocket.send_json({"type": "heartbeat", "timestamp": datetime.now().isoformat()})
                except:
                    pass
                
            heartbeat_task = asyncio.create_task(heartbeat())

            try:
                await manager.send_personal_message({
                    "type": "connection",
                    "message": "Connected to GO-TR monitoring",
                    "connection_id": connection_id
                }, websocket)

                while True:
                    try:
                        data = await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
                        if data == "ping":
                            await websocket.send_json({"type": "pong"})
                    except asyncio.TimeoutError:
                        await websocket.send_json({"type": "ping"})
                        continue       
            except WebSocketDisconnect:
                manager.disconnect(websocket)
            finally:
                heartbeat_task.cancel()

        @app.get("/api/alerts/recent")
        async def get_recent_alerts(limit: int = 100, since_timestamp: str = None):
            """Get recent alerts with optional timestamp filter"""
            try:
                with recent_alerts_lock:
                    all_alerts = list(recent_alerts)
        
                if since_timestamp:
                    try:
                        since_dt = datetime.fromisoformat(since_timestamp.replace('Z', '+00:00'))
                        
                        if since_dt > datetime.now(since_dt.tzinfo):
                            print(f"Warning: Future timestamp provided: {since_timestamp}")
                            since_dt = datetime.now(since_dt.tzinfo)
                        
                        filtered_alerts = [
                            alert for alert in all_alerts 
                            if datetime.fromisoformat(alert['timestamp']) > since_dt
                        ]
                        all_alerts = filtered_alerts
                    except Exception as e:
                        print(f"Error parsing timestamp: {e}")
                    
                alerts_to_return = all_alerts[-limit:]
        
                return {
                    "alerts": alerts_to_return,
                    "count": len(alerts_to_return),
                    "total_stored": len(recent_alerts)
                }
            except Exception as e:
                print(f"Error in get_recent_alerts: {e}")
                return {
                    "alerts": [],
                    "count": 0,
                    "total_stored": 0,
                    "error": str(e)
                }

        @app.get("/api/stats")
        async def get_stats():
            """Get current statistics"""
            with recent_alerts_lock:
                all_alerts = list(recent_alerts)

            critical_count = sum(1 for a in all_alerts if a.get('severity') == 'critical')
            case_ids = set(a.get('case_id') for a in all_alerts if a.get('case_id'))

            return {
                "total_alerts": len(all_alerts),
                "critical_count": critical_count,
                "unique_cases": len(case_ids),
                "active_connections": len(manager.active_connections)
            }

        @app.get("/health")
        async def health_check():
            return {
                "status": "healthy",
                "active_connections": len(manager.active_connections),
                "active_cases": len(consumer_instance.active_cases)
            }

        @app.on_event("startup")
        async def startup_event():
            asyncio.create_task(process_alert_queue())

        return app
    
    def start_websocket_server(self):
        """Start the WebSocket server in a separate thread"""
        def run_server():
            uvicorn.run(self.app, host="0.0.0.0", port=8000)

        self.websocket_thread = Thread(target=run_server, daemon=True)
        self.websocket_thread.start()
        print("WebSocket server started on http://0.0.0.0:8000")

    def send_deviation_alert(self, alert_data):
        """Send deviation alert through WebSocket"""
        self.alert_queue.put(alert_data)

    def initialize_neo4j_session(self):
        """Initialize or reinitialize Neo4j session"""
        print("Initializing Neo4j session...")
        
        # Close existing session/driver if they exist
        if self.session:
            try:
                self.session.close()
            except Exception as e:
                print(f"Error closing Neo4j session: {e}")
            
        if self.driver:
            try:
                self.driver.close()
            except Exception as e:
                print(f"Error closing Neo4j driver: {e}")
            
        # Create fresh connections
        self.driver = GraphDatabase.driver(
            uri=self.neo4j_config['uri'], 
            auth=(self.neo4j_config['user'], self.neo4j_config['password'])
        )
        self.session = self.driver.session()
        print("Neo4j session initialized successfully!")

    def initialize_master_model(self):
        """Initialize the master model components"""
        print("Initializing master model...")
        # Load your existing CSV data to create the master model
        dataframe = pd.read_csv('G:/Project/AI/2024_multiprespective/2024_multiprespective/process_mining/media/datacsv_repair.csv', sep=';')
        # Format dataframe
        start_time = datetime.now()
        dataframe['timestamp'] = pd.date_range(start=start_time, periods=len(dataframe), freq='15s')
        dataframe = pm4py.format_dataframe(dataframe, case_id='case_id', activity_key='activity', timestamp_key='timestamp')
         # Create event log and discover process model
        event_log = pm4py.convert_to_event_log(dataframe)
        net, initial_marking, final_marking = pm4py.discover_petri_net_inductive(event_log)
        ts = reachability_graph.construct_reachability_graph(net, initial_marking)
        # Get transaction names
        self.trans_name = []
        for t in net.transitions:
            self.trans_name.append(t.label)
        self.trans_name.extend(['START', 'END'])
         # Get states and places
        self.states, self.places = GO_TR.reachabilityGraphProperties(ts, net)
        # Initialize Neo4j connection
        self.initialize_neo4j_session()
        
        neo4j_func.start_environtmen(net, ts, self.trans_name, initial_marking, final_marking, self.session)
        neo4j_func.generate_organizational_model(self.session)
        
        print("Master model initialized successfully!")
        
    def initialize_kafka_consumer(self, status):
        """Initialize Kafka consumer"""
        print(f"Initializing Kafka consumer with {status}")
        
        try:
            self.consumer = KafkaConsumer(
                'pm.test.events.raw',
                **self.kafka_config,
                auto_offset_reset='earliest' if status == 'reset' else 'latest',
                enable_auto_commit=False if status == 'reset' else True
            )
            
            if status == 'reset':
                print("Waiting for partition assignment...")
                # Trigger partition assignment
                self.consumer.poll(timeout_ms=1000)
                
                while not self.consumer.assignment():
                    self.consumer.poll(timeout_ms=100)
                print(f"Assigned partitions: {self.consumer.assignment()}")
                self.consumer.seek_to_beginning()
                print('Kafka Consumer resetted succesfully!')
            else:
                print("Kafka consumer initialized in continue mode!")
                
        except Exception as e:
            print(f"Error initializing Kafka consumer: {e}")
            raise
        
    def convert_kafka_event_to_gotr_format(self, kafka_event):
        """Convert Kafka event message to GO-TR expected format"""
        return [
            kafka_event['trace_id'],
            kafka_event['activity'],
            kafka_event['resource'],
            kafka_event.get('product_type', 'unknown')
        ]
    
    async def safe_stop_consumer(self):
        """Safely stop the consumer thread with proper synchronization"""
        if self.consumer_thread and self.consumer_thread.is_alive():
            print("Stopping consumer thread safely...")
            
            # Signal the thread to stop
            self.stop_event.set()
            self.is_running = False
            
            # Wait for thread to finish (with timeout)
            self.consumer_thread.join(timeout=10)
            
            if self.consumer_thread.is_alive():
                print("Warning: Consumer thread did not stop in time.")
            else:
                print("Consumer thread stopped successfully.")
                
            self.consumer_thread = None
            
        # Reset the stop event for next start
        self.stop_event.clear()

    def start_consumer_thread(self):
        """Starts a new consumer thread"""
        if self.consumer_thread and self.consumer_thread.is_alive():
            print("Consumer thread is already running.")
            return
            
        print("Starting new consumer thread...")
        self.stop_event.clear()
        self.consumer_thread = Thread(target=self.run_consumer, daemon=True)
        self.consumer_thread.start()
    
    def run_consumer(self):
        """The main consumer loop"""
        print("Waiting for configuration from frontend...")
        self.configuration_event.wait()

        print(f"Configuration received! Starting GO-TR Kafka consumer in '{self.check}' mode...")
        self.is_running = True
        print("Starting stateful GO-TR Kafka consumer...")

        try:
            while not self.stop_event.is_set():
                try:
                    with self.consumer_lock:
                        if not self.consumer:
                            print("Consumer not initialized, waiting...")
                            time.sleep(1)
                            continue
                        
                        # Poll with timeout
                        records = self.consumer.poll(timeout_ms=1000)

                    if not records:
                        continue

                    for topic_partition, messages in records.items():
                        # Check stop event between batches
                        if self.stop_event.is_set():
                            break
                            
                        for message in messages:
                            # Check stop event for each message
                            if self.stop_event.is_set():
                                break
                                
                            kafka_event = message.value

                            p_id = kafka_event.get('trace_id')
                            activity = kafka_event.get('activity')
                            
                            if not p_id or not activity:
                                print(f"Skipping malformed message: {kafka_event}")
                                continue

                            print(f"Received event: Case {p_id} - Activity '{activity}'")

                            # Process event
                            if p_id not in self.active_cases:
                                with self.neo4j_lock:
                                    GO_TR.initialize_case_in_db(p_id, self.session)
                                self.active_cases.add(p_id)

                            gotr_event = self.convert_kafka_event_to_gotr_format(kafka_event)

                            with self.neo4j_lock:
                                result = GO_TR.process_single_event(
                                    p_id,
                                    gotr_event,
                                    self.trans_name,
                                    self.states,
                                    self.places,
                                    self.check,
                                    self.session
                                )

                            self.case_event_history[p_id].append(activity)
                            
                            if result['status'] == 'deviation':
                                self.handle_deviation(p_id, result)

                            if activity == 'Return the item':
                                print(f"Detected end of case for {p_id}. Finalizing...")
                                with self.neo4j_lock:
                                    final_stats = GO_TR.finalize_case(p_id, self.session)
                                if p_id in self.active_cases:
                                    self.active_cases.remove(p_id)

                except Exception as e:
                    if self.stop_event.is_set():
                        break
                    print(f"Error in consumer loop: {e}")
                    import traceback
                    traceback.print_exc()
                    time.sleep(1)  # Brief pause before retrying

        except KeyboardInterrupt:
            print("\nShutting down consumer...")
        except Exception as e:
            print(f"Error in consumer: {e}")
            import traceback
            traceback.print_exc()
        finally:
            print("Consumer thread exiting, cleaning up...")
            self.is_running = False

    def handle_deviation(self, p_id, deviation_details):
        """Handles the logic for when a deviation is detected"""
        deviation_type = deviation_details.get('type')
        activity_did = deviation_details.get('activity')
        
        if deviation_type == 'missing_token':
            self.anomaly_scores[p_id] += 1.0
        elif deviation_type == 'organizational':
            if 'wrong_structure' in deviation_details.get('org_issues', []):
                self.anomaly_scores[p_id] += 0.8
            if 'wrong_team' in deviation_details.get('org_issues', []):
                self.anomaly_scores[p_id] += 0.5
        elif deviation_type == 'unknown_activity':
            self.unknown_activities[p_id].append(deviation_details.get('activity'))

        print(f"Anomaly score for case {p_id} is now: {self.anomaly_scores[p_id]}")

        alert_data = {
            "type": "deviation_alert",
            "timestamp": datetime.now().isoformat(),
            "case_id": p_id,
            "deviation_type": deviation_type,
            "details": deviation_details,
            "cumulative_score": self.anomaly_scores[p_id],
            "event_history": self.case_event_history[p_id][-5:],
            "message": f"Deviation detected in case {p_id}: {deviation_type} {activity_did}"
        }
    
        if self.anomaly_scores[p_id] >= 1.5:
            print(f"🔥 WARNING! Inspection needed on Case ID: {p_id}")
            critical_alert = {
                **alert_data,
                "type": "critical_alert",
                "severity": "critical",
                "message": f"CRITICAL: Case {p_id} requires immediate inspection! with activity {activity_did} {deviation_type}"
            }
            self.send_deviation_alert(critical_alert)
        else:
            self.send_deviation_alert(alert_data)
            
    def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        
        with self.consumer_lock:
            if self.consumer:
                try:
                    self.consumer.close()
                    print("Kafka consumer closed.")
                except Exception as e:
                    print(f"Error closing consumer: {e}")
                self.consumer = None
        
        with self.neo4j_lock:
            if self.session:
                try:
                    self.session.close()
                    print("Neo4j session closed.")
                except Exception as e:
                    print(f"Error closing session: {e}")
                    
            if self.driver:
                try:
                    self.driver.close()
                    print("Neo4j driver closed.")
                except Exception as e:
                    print(f"Error closing driver: {e}")


def main():
    """Main function to run the consumer"""
    consumer = GOTRKafkaConsumer()
    
    try:
        consumer.initialize_master_model()
        consumer.start_websocket_server()
        time.sleep(2)

        # Keep main thread alive
        while True:
            time.sleep(1)
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error running consumer: {e}")
        import traceback
        traceback.print_exc()
    finally:
        consumer.cleanup()


if __name__ == "__main__":
    main()