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
from threading import Thread
from typing import List, Dict
import uuid
from queue import Queue
from collections import deque

# Connection of Kafka
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
        
        # Clean up disconnected clients
        for conn in disconnected:
            self.disconnect(conn)


class GOTRKafkaConsumer:
    def __init__(self, kafka_config=None, neo4j_config=None):
        # Kafka configuration
        self.kafka_config = kafka_config or {
            'bootstrap_servers': ['localhost:9092', 'localhost:9093', 'localhost:9094'],
            'auto_offset_reset': 'earliest',
            'enable_auto_commit': False,  #Set False to Reindex each run, set True to stay to the last index
            'auto_offset_reset':"earliest",
            'group_id': 'gotr_consumer_group_4',
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
        self.alert_queue = Queue()  # Thread-safe queue for alerts
        
        self.recent_alerts = deque(maxlen=200)  # Store last 200 alerts
        self.recent_alerts_lock = threading.Lock()
        
        self.app = self.create_app()
        self.websocket_thread = None
      
        # Event buffering for processing
        self.event_buffer = defaultdict(list)
        self.processing_queue = queue.Queue()
        self.is_running = False

        self.consumer = None
        self.driver = None
        self.session = None

        # --- Global Model Properties (loaded once) ---
        self.trans_name = []
        self.states = []
        self.places = []
        
        # -- Check the Case Set Up --
        self.check = None  # Will be set via API before starting
        self.is_configured = False
        self.configuration_event = threading.Event()  # To signal when config is ready

        # ✅ STATE MANAGEMENT: This is what was extracted from tokenBasedReplay
        # These variables now live here to track state across all events.
        self.active_cases = set()
        self.anomaly_scores = defaultdict(float)
        self.case_event_history = defaultdict(list)
        self.unknown_activities = defaultdict(list)

    #-------------- Web Socket Fast API func ------------------ 
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
        get_active_cases = lambda: len(self.active_cases)

        consumer_instance = self

        @app.post("/api/configure")
        async def configure_consumer(config: dict):
            """
            Configure the consumer before starting
            Expected JSON: {
                "mode": "online" or "multi",
                "webhook": "optional_webhook_url"  # for future use
            }
            """
            if 'mode' not in config:
                return {
                    "status": "error",
                    "message": "Missing 'mode' field in configuration"
                }
            
            mode = config['mode']
            if mode not in ['online', 'multi']:
                return {
                    "status": "error",
                    "message": f"Invalid mode '{mode}'. Must be 'online' or 'multi'"
                }
            
            # Set the configuration
            consumer_instance.check = mode
            consumer_instance.is_configured = True
            consumer_instance.configuration_event.set()  # Signal that config is ready
            
            print(f"Consumer configured with mode: {mode}")
            
            # Notify connected clients
            await manager.broadcast({
                "type": "configured",
                "timestamp": datetime.now().isoformat(),
                "mode": mode,
                "message": f"Consumer configured in {mode} mode"
            })
            
            return {
                "status": "success",
                "mode": mode,
                "timestamp": datetime.now().isoformat(),
                "message": "Consumer configured successfully. Ready to start processing."
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
            """Manually trigger the start of processing (if needed)"""
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

                        # Add unique ID to each alert for de-duplication
                        alert_data['alert_id'] = f"{alert_data['timestamp']}_{alert_data['case_id']}"

                        # Store in recent alerts
                        with recent_alerts_lock:
                            recent_alerts.append(alert_data)

                        # Broadcast to connected clients
                        await manager.broadcast(alert_data)
                    else:
                        await asyncio.sleep(0.1)
                except Exception as e:
                    print(f"Error processing alert: {e}")
        
        

        @app.websocket("/ws")
        async def websocket_endpoint(websocket: WebSocket):
            connection_id = await manager.connect(websocket)

            # Create a heartbeat task
            async def heartbeat():
                try:
                    while True:
                        await asyncio.sleep(30)  # Send heartbeat every 30 seconds
                        await websocket.send_json({"type": "heartbeat", "timestamp": datetime.now().isoformat()})
                except:
                    pass  # Connection closed
                
            # Start heartbeat
            heartbeat_task = asyncio.create_task(heartbeat())

            try:
                await manager.send_personal_message({
                    "type": "connection",
                    "message": "Connected to GO-TR monitoring",
                    "connection_id": connection_id
                }, websocket)

                while True:
                    # Add timeout to prevent hanging
                    try:
                        data = await asyncio.wait_for(websocket.receive_text(), timeout=60.0)
                        if data == "ping":
                            await websocket.send_json({"type": "pong"})
                    except asyncio.TimeoutError:
                        # Send a ping to check if connection is alive
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
        
                # Filter by timestamp if provided
                if since_timestamp:
                    try:
                        since_dt = datetime.fromisoformat(since_timestamp.replace('Z', '+00:00'))
                        
                        # Validate timestamp is not in the future
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
                        # Don't filter if timestamp is invalid
                    
                # Return most recent alerts up to limit
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
                "active_cases": get_active_cases()
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
        print("data out sended")
    #-------------- Web Socket Fast API end -------------------

    def initialize_master_model(self):
        """Initialize the master model components"""
        print("Initializing master model...")
        
        # Load your existing CSV data to create the master model
        dataframe = pd.read_csv('G:/Project/AI/2024_multiprespective/2024_multiprespective/process_mining/media/datacsv_repair.csv', sep=';')
        
        # Format dataframe
        start_time = datetime.now()
        dataframe['timestamp'] = pd.date_range(start=start_time, periods=len(dataframe), freq='15S')
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
        self.driver = GraphDatabase.driver(
            uri=self.neo4j_config['uri'], 
            auth=(self.neo4j_config['user'], self.neo4j_config['password'])
        )
        self.session = self.driver.session()
        
        # Initialize Neo4j environment
        neo4j_func.start_environtmen(net, ts, self.trans_name, initial_marking, final_marking, self.session)
        neo4j_func.generate_organizational_model(self.session)
        
        print("Master model initialized successfully!")
        
    def initialize_kafka_consumer(self):
        """Initialize Kafka consumer"""
        print("Initializing Kafka consumer...")
        self.consumer = KafkaConsumer(
            'pm.test.events.raw',
            **self.kafka_config
        )
        print("Kafka consumer initialized successfully!")
        
    def convert_kafka_event_to_gotr_format(self, kafka_event):
        """Convert Kafka event message to GO-TR expected format"""
        # GO-TR expects: [case_id, activity, resource, product_type_value]
        return [
            kafka_event['trace_id'],
            kafka_event['activity'],
            kafka_event['resource'],
            kafka_event.get('product_type', 'unknown')
        ]
        
    def process_event_stream(self, event_streams):
        """Process events using GO-TR algorithm"""
        print(f"Processing {len(event_streams)} events with GO-TR...")
        
        try:
            # Call the modified tokenBasedReplay function
            activate_activities, activities_coming, unknownActivities = GO_TR.tokenBasedReplay(
                event_streams, 
                self.trans_name, 
                self.states, 
                self.places, 
                self.session
            )
            
            return {
                'activate_activities': activate_activities,
                'activities_coming': activities_coming,
                'unknownActivities': unknownActivities
            }
            
        except Exception as e:
            print(f"Error processing event stream: {e}")
            return None
            
    def buffer_events_by_case(self, event, buffer_size=10, timeout_seconds=30):
        """Buffer events by case ID for batch processing"""
        case_id = event[0]  # case_id is first element
        self.event_buffer[case_id].append(event)
        
        # Check if we should process this case's events
        should_process = (
            len(self.event_buffer[case_id]) >= buffer_size or
            self._is_case_timeout(case_id, timeout_seconds)
        )
        
        if should_process:
            events_to_process = self.event_buffer[case_id].copy()
            self.event_buffer[case_id].clear()
            return events_to_process
        
        return None
        
    def _is_case_timeout(self, case_id, timeout_seconds):
        """Check if case has timed out (simplified implementation)"""
        # You might want to implement more sophisticated timeout logic
        return len(self.event_buffer[case_id]) > 0 and time.time() % timeout_seconds < 1
        
    def process_single_event(self, kafka_event):
        """Process a single event immediately"""
        gotr_event = self.convert_kafka_event_to_gotr_format(kafka_event)
        result = self.process_event_stream([gotr_event])
        
        if result:
            self.log_conformance_results(kafka_event['case_id'], result)
            
    def log_conformance_results(self, case_id, results):
        """Log conformance checking results"""
        print(f"\n=== Conformance Results for Case {case_id} ===")
        
        if results['unknownActivities'].get(case_id):
            print(f"Unknown Activities: {results['unknownActivities'][case_id]}")
            
        if results['activate_activities'].get(case_id):
            activities = results['activate_activities'][case_id]
            for activity_info in activities:
                if len(activity_info) >= 3 and activity_info[1] == 'MISSING_TOKEN':
                    print(f"ANOMALY DETECTED: Missing token for activity {activity_info[0]}")
                elif len(activity_info) >= 4 and 'wrong' in str(activity_info[2]):
                    print(f"ANOMALY DETECTED: Organizational issue for activity {activity_info[0]}: {activity_info[2]}")
                    
        print("=" * 50)
        
    def run_consumer(self):
        """
        The main consumer loop that manages case state and calls the GO-TR logic.
        """
        print("Waiting for configuration from frontend...")

        # Wait until configured via API
        self.configuration_event.wait()

        print(f"Configuration received! Starting GO-TR Kafka consumer in '{self.check}' mode...")
        self.is_running = True

        print("Starting stateful GO-TR Kafka consumer...")

        try:
            # Use poll() instead of iterator to keep running even without messages
            while self.is_running:
                # Poll for messages with a timeout
                records = self.consumer.poll(timeout_ms=1000)  # 1 second timeout

                if not records:
                    # No messages, but keep running
                    continue

                # Process messages
                for topic_partition, messages in records.items():
                    for message in messages:
                        kafka_event = message.value

                        # Use the corrected schema mapping
                        p_id = kafka_event.get('trace_id')
                        activity = kafka_event.get('activity')
                        if not p_id or not activity:
                            print(f"Skipping malformed message: {kafka_event}")
                            continue

                        print(f"Received event: Case {p_id} - Activity '{activity}'")

                        # --- Rest of your processing logic stays the same ---
                        if p_id not in self.active_cases:
                            GO_TR.initialize_case_in_db(p_id, self.session)
                            self.active_cases.add(p_id)

                        gotr_event = self.convert_kafka_event_to_gotr_format(kafka_event)

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
                            final_stats = GO_TR.finalize_case(p_id, self.session)
                            if p_id in self.active_cases:
                                self.active_cases.remove(p_id)

        except KeyboardInterrupt:
            print("\nShutting down consumer...")
        except Exception as e:
            print(f"Error in consumer: {e}")
            import traceback
            traceback.print_exc()
        finally:
            # Only cleanup if explicitly stopped
            if not self.is_running:
                self.cleanup()

    def handle_deviation(self, p_id, deviation_details):
        """
        Handles the logic for when a deviation is detected.
        Updates anomaly scores and sends alerts.
        """
        deviation_type = deviation_details.get('type')
        
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

        # Prepare alert data
        alert_data = {
            "type": "deviation_alert",
            "timestamp": datetime.now().isoformat(),
            "case_id": p_id,
            "deviation_type": deviation_type,
            "details": deviation_details,
            "cumulative_score": self.anomaly_scores[p_id],
            "event_history": self.case_event_history[p_id][-5:],  # Last 5 events
            "message": f"Deviation detected in case {p_id}: {deviation_type}"
        }
    
        # Send WebSocket alert
        self.send_deviation_alert(alert_data)

        if self.anomaly_scores[p_id] >= 1.0:
            print(f"🔥 WARNING! Inspection needed on Case ID: {p_id}")
            # THIS IS WHERE YOU WOULD SEND A WEBSOCKET/API ALERT TO THE FRONTEND
            critical_alert = {
                **alert_data,
                "type": "critical_alert",
                "message": f"CRITICAL: Case {p_id} requires immediate inspection!"
            }
            self.send_deviation_alert(critical_alert)
            
    def cleanup(self):
        """Clean up resources"""
        self.is_running = False
        
        if self.consumer:
            self.consumer.close()
            print("Kafka consumer closed.")
            
        if self.session:
            self.session.close()
            print("Neo4j session closed.")
            
        if self.driver:
            self.driver.close()
            print("Neo4j driver closed.")


def main():
    """Main function to run the consumer"""
    consumer = GOTRKafkaConsumer()
    
    try:
        # Initialize master model and Kafka consumer
        consumer.initialize_master_model()
        consumer.initialize_kafka_consumer()
        
        # Start WebSocket server
        consumer.start_websocket_server()
        time.sleep(2)  # Give server time to start

        # Run consumer (this will keep running)
        consumer.run_consumer()
        
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        print(f"Error running consumer: {e}")
    finally:
        consumer.cleanup()


if __name__ == "__main__":
    main()