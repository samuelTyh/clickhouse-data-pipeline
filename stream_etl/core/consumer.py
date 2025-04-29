import json
import logging
import threading
import time
from typing import List, Dict, Any, Optional
from kafka import KafkaConsumer
from .processor import DataProcessor

logger = logging.getLogger('stream-etl.consumer')


class KafkaConsumerManager:
    """Manages Kafka consumers for multiple topics."""
    
    def __init__(
        self,
        bootstrap_servers: str,
        group_id: str,
        topics: List[str],
        processor: DataProcessor,
        auto_offset_reset: str = 'earliest'
    ):
        """Initialize the Kafka consumer manager.
        
        Args:
            bootstrap_servers: Kafka bootstrap servers
            group_id: Consumer group ID
            topics: List of topics to consume
            processor: Data processor instance
            auto_offset_reset: Auto offset reset strategy
        """
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topics = topics
        self.processor = processor
        self.auto_offset_reset = auto_offset_reset
        self.consumer = None
        self.running = False
        self.consumer_thread = None
    
    def _create_consumer(self) -> KafkaConsumer:
        """Create and configure a Kafka consumer."""
        return KafkaConsumer(
            *self.topics,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
            value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None,
            enable_auto_commit=False,
            max_poll_interval_ms=300000,  # 5 minutes
            session_timeout_ms=90000,     # 1.5 minutes
            request_timeout_ms=120000,    # 2 minutes
            max_poll_records=500
        )
    
    def _process_message(self, topic: str, message: Dict[str, Any]) -> None:
        """Process a message from Kafka.
        
        Args:
            topic: The topic the message was received from
            message: The message payload
        """
        try:
            # Extract table name from topic, e.g., 'postgres.public.advertiser' -> 'advertiser'
            table_name = topic.split('.')[-1]
            
            if message is None:
                logger.warning(f"Received null message from topic {topic}")
                return
            
            # Process based on table name
            if table_name == 'advertiser':
                self.processor.process_advertiser(message)
            elif table_name == 'campaign':
                self.processor.process_campaign(message)
            elif table_name == 'impressions':
                self.processor.process_impression(message)
            elif table_name == 'clicks':
                self.processor.process_click(message)
            else:
                logger.warning(f"Unknown table name: {table_name}")
        
        except Exception as e:
            logger.error(f"Error processing message from topic {topic}: {e}")
    
    def _consume_messages(self) -> None:
        """Continuously consume and process messages from Kafka."""
        self.consumer = self._create_consumer()
        
        logger.info(f"Started consuming from topics: {', '.join(self.topics)}")
        
        while self.running:
            try:
                # Poll for messages
                message_batch = self.consumer.poll(timeout_ms=1000, max_records=100)
                
                for topic_partition, messages in message_batch.items():
                    topic = topic_partition.topic
                    
                    for message in messages:
                        self._process_message(topic, message.value)
                
                # Commit offsets
                if message_batch:
                    self.consumer.commit()
            
            except Exception as e:
                logger.error(f"Error consuming messages: {e}")
                time.sleep(5)  # Wait before retrying
    
    def start_consuming(self) -> None:
        """Start consuming messages in a separate thread."""
        if self.running:
            logger.warning("Consumer is already running")
            return
        
        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_messages)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()
        
        # Keep the main thread running
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            self.stop_consuming()
    
    def stop_consuming(self) -> None:
        """Stop consuming messages."""
        self.running = False
        
        if self.consumer:
            self.consumer.close()
            
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.consumer_thread.join(timeout=30)
            
        logger.info("Stopped consuming messages")
