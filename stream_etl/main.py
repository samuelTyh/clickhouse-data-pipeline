import os
import sys
import json
import time
import logging
import signal
import requests
from dotenv import load_dotenv
from core.config import AppConfig
from core.consumer import KafkaConsumerManager
from core.processor import DataProcessor
from utils.logging_config import configure_logging

# Load environment variables
load_dotenv()

# Configure logging
configure_logging()
logger = logging.getLogger('stream-etl')

# Signal handler for graceful shutdown
def signal_handler(sig, frame):
    logger.info("Shutdown signal received. Closing connections...")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def setup_debezium_connector():
    """Set up Debezium connector for PostgreSQL."""
    connector_url = os.environ.get('DEBEZIUM_CONNECTOR_URL')
    
    # Define the connector configuration
    connector_config = {
        "name": "postgres-connector",
        "config": {
            "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
            "tasks.max": "1",
            "database.hostname": os.environ.get('DEBEZIUM_PG_HOST'),
            "database.port": os.environ.get('DEBEZIUM_PG_PORT'),
            "database.user": os.environ.get('DEBEZIUM_PG_USER'),
            "database.password": os.environ.get('DEBEZIUM_PG_PASSWORD'),
            "database.dbname": os.environ.get('DEBEZIUM_PG_DBNAME'),
            "database.server.name": "postgres",
            "topic.prefix": "postgres",  # Critical parameter that was missing
            "table.include.list": "public.advertiser,public.campaign,public.impressions,public.clicks",
            "plugin.name": "pgoutput",   # Using pgoutput instead of wal2json
            "transforms": "unwrap",
            "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
            "transforms.unwrap.drop.tombstones": "false",
            "transforms.unwrap.delete.handling.mode": "rewrite",
            "transforms.unwrap.add.fields": "op,db,table",
            "key.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter": "org.apache.kafka.connect.json.JsonConverter",
            "value.converter.schemas.enable": "false",
            "key.converter.schemas.enable": "false"
        }
    }
    
    try:
        # Check if connector already exists
        response = requests.get(f"{connector_url}")
        if response.status_code == 200:
            connectors = response.json()
            if "postgres-connector" in connectors:
                logger.info("Debezium connector 'postgres-connector' already exists")
                return True
        
        # Create connector
        response = requests.post(
            f"{connector_url}",
            headers={"Content-Type": "application/json"},
            data=json.dumps(connector_config)
        )
        
        if response.status_code in [201, 200]:
            logger.info("Successfully created Debezium connector 'postgres-connector'")
            return True
        else:
            logger.error(f"Failed to create connector: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"Error setting up Debezium connector: {e}")
        return False

def wait_for_services():
    """Wait for dependent services to be available."""
    # Wait for Kafka
    kafka_ready = False
    retries = 0
    max_retries = 30
    
    while not kafka_ready and retries < max_retries:
        try:
            from kafka import KafkaAdminClient
            admin_client = KafkaAdminClient(
                bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
                client_id='stream-etl-admin'
            )
            admin_client.list_topics()
            kafka_ready = True
            admin_client.close()
            logger.info("Kafka is available")
        except Exception as e:
            logger.warning(f"Waiting for Kafka to be available... ({retries}/{max_retries})")
            retries += 1
            time.sleep(10)
    
    if not kafka_ready:
        logger.error("Kafka service is not available after maximum retries")
        return False
    
    # Wait for Debezium Connect
    debezium_ready = False
    retries = 0
    
    while not debezium_ready and retries < max_retries:
        try:
            response = requests.get(os.environ.get('DEBEZIUM_CONNECTOR_URL'))
            if response.status_code == 200:
                debezium_ready = True
                logger.info("Debezium Connect is available")
            else:
                raise Exception(f"Debezium responded with status code {response.status_code}")
        except Exception as e:
            logger.warning(f"Waiting for Debezium Connect to be available... ({retries}/{max_retries})")
            retries += 1
            time.sleep(10)
    
    if not debezium_ready:
        logger.error("Debezium Connect service is not available after maximum retries")
        return False
    
    return True

def main():
    """Main entry point for the streaming ETL service."""
    logger.info("Starting Streaming ETL service")
    
    # Wait for dependent services
    if not wait_for_services():
        logger.error("Failed to connect to dependent services. Exiting.")
        sys.exit(1)
    
    # Set up Debezium connector
    if not setup_debezium_connector():
        logger.error("Failed to set up Debezium connector. Exiting.")
        sys.exit(1)
    
    # Initialize configuration
    config = AppConfig()
    
    # Initialize data processor
    processor = DataProcessor(config)
    
    # Initialize Kafka consumer manager
    topics = [
        os.environ.get('KAFKA_TOPICS_ADVERTISER'),
        os.environ.get('KAFKA_TOPICS_CAMPAIGN'),
        os.environ.get('KAFKA_TOPICS_IMPRESSIONS'),
        os.environ.get('KAFKA_TOPICS_CLICKS')
    ]
    
    consumer_manager = KafkaConsumerManager(
        bootstrap_servers=os.environ.get('KAFKA_BOOTSTRAP_SERVERS'),
        group_id=os.environ.get('KAFKA_GROUP_ID'),
        topics=topics,
        processor=processor
    )
    
    try:
        # Start consuming messages
        logger.info("Starting to consume messages from Kafka")
        consumer_manager.start_consuming()
    except KeyboardInterrupt:
        logger.info("Interrupted by user. Shutting down...")
    except Exception as e:
        logger.error(f"Error in streaming ETL service: {e}")
    finally:
        # Cleanup
        consumer_manager.stop_consuming()
        processor.close_connections()
        logger.info("Streaming ETL service stopped")

if __name__ == "__main__":
    main()
