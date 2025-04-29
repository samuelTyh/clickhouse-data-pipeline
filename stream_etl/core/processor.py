import logging
from datetime import datetime
from typing import Dict, Any, Optional
import clickhouse_driver
import psycopg
from .config import AppConfig

logger = logging.getLogger('stream-etl.processor')


class DataProcessor:
    """Processes data from Kafka and loads it into ClickHouse."""
    
    def __init__(self, config: AppConfig):
        """Initialize the data processor.
        
        Args:
            config: Application configuration
        """
        self.config = config
        self.clickhouse_client = self._create_clickhouse_client()
        
        # Initialize statistics
        self.stats = {
            'advertiser': 0,
            'campaign': 0,
            'impressions': 0,
            'clicks': 0
        }
    
    def _create_clickhouse_client(self) -> clickhouse_driver.Client:
        """Create and configure a ClickHouse client."""
        return clickhouse_driver.Client(
            host=self.config.clickhouse.host,
            port=self.config.clickhouse.port,
            user=self.config.clickhouse.user,
            password=self.config.clickhouse.password,
            database=self.config.clickhouse.database
        )
    
    def _handle_operation(self, operation: str, table: str, data: Dict[str, Any]) -> None:
        """Handle different database operations.
        
        Args:
            operation: Database operation (c=create, u=update, d=delete)
            table: Table name
            data: Data payload
        """
        if operation in ('c', 'r'):  # Create or Read
            if table == 'advertiser':
                self._insert_advertiser(data)
            elif table == 'campaign':
                self._insert_campaign(data)
            elif table == 'impressions':
                self._insert_impression(data)
            elif table == 'clicks':
                self._insert_click(data)
        
        elif operation == 'u':  # Update
            if table == 'advertiser':
                self._update_advertiser(data)
            elif table == 'campaign':
                self._update_campaign(data)
            # For impressions and clicks, we don't need to handle updates
            # as they are append-only in our data model
        
        elif operation == 'd':  # Delete
            # For a streaming warehousing solution, we typically don't delete data
            # but you could implement soft deletes or specific handling if needed
            logger.info(f"Delete operation for {table} not implemented. Data: {data}")
    
    def _insert_advertiser(self, data: Dict[str, Any]) -> None:
        """Insert advertiser data into ClickHouse.
        
        Args:
            data: Advertiser data
        """
        try:
            adv_id = data.get('id')
            name = data.get('name')
            updated_at = data.get('updated_at') or datetime.now()
            created_at = data.get('created_at') or datetime.now()
            
            self.clickhouse_client.execute(
                """
                INSERT INTO analytics.dim_advertiser
                (advertiser_id, name, updated_at, created_at)
                VALUES
                """,
                [(adv_id, name, updated_at, created_at)]
            )
            
            self.stats['advertiser'] += 1
            logger.info(f"Inserted advertiser: {adv_id} - {name}")
        
        except Exception as e:
            logger.error(f"Error inserting advertiser: {e}")
    
    def _update_advertiser(self, data: Dict[str, Any]) -> None:
        """Update advertiser data in ClickHouse.
        
        Args:
            data: Advertiser data
        """
        try:
            # For ClickHouse ReplacingMergeTree, we just insert the updated record
            # with a newer updated_at timestamp, and it will replace the old one
            # during the next merge operation
            self._insert_advertiser(data)
            logger.info(f"Updated advertiser: {data.get('id')} - {data.get('name')}")
        
        except Exception as e:
            logger.error(f"Error updating advertiser: {e}")
    
    def _insert_campaign(self, data: Dict[str, Any]) -> None:
        """Insert campaign data into ClickHouse.
        
        Args:
            data: Campaign data
        """
        try:
            campaign_id = data.get('id')
            name = data.get('name')
            bid = float(data.get('bid') or 0.0)
            budget = float(data.get('budget') or 0.0)
            start_date = data.get('start_date') or datetime.now().date()
            end_date = data.get('end_date') or datetime.now().date()
            advertiser_id = data.get('advertiser_id')
            updated_at = data.get('updated_at') or datetime.now()
            created_at = data.get('created_at') or datetime.now()
            
            self.clickhouse_client.execute(
                """
                INSERT INTO analytics.dim_campaign
                (campaign_id, name, bid, budget, start_date, end_date, advertiser_id, updated_at, created_at)
                VALUES
                """,
                [(campaign_id, name, bid, budget, start_date, end_date, advertiser_id, updated_at, created_at)]
            )
            
            self.stats['campaign'] += 1
            logger.info(f"Inserted campaign: {campaign_id} - {name}")
        
        except Exception as e:
            logger.error(f"Error inserting campaign: {e}")
    
    def _update_campaign(self, data: Dict[str, Any]) -> None:
        """Update campaign data in ClickHouse.
        
        Args:
            data: Campaign data
        """
        try:
            # For ClickHouse ReplacingMergeTree, we just insert the updated record
            # with a newer updated_at timestamp, and it will replace the old one
            # during the next merge operation
            self._insert_campaign(data)
            logger.info(f"Updated campaign: {data.get('id')} - {data.get('name')}")
        
        except Exception as e:
            logger.error(f"Error updating campaign: {e}")
    
    def _insert_impression(self, data: Dict[str, Any]) -> None:
        """Insert impression data into ClickHouse.
        
        Args:
            data: Impression data
        """
        try:
            impression_id = data.get('id')
            campaign_id = data.get('campaign_id')
            created_at = data.get('created_at') or datetime.now()
            event_time = created_at
            event_date = event_time.date() if hasattr(event_time, 'date') else event_time
            
            self.clickhouse_client.execute(
                """
                INSERT INTO analytics.fact_impressions
                (impression_id, campaign_id, event_date, event_time, created_at)
                VALUES
                """,
                [(impression_id, campaign_id, event_date, event_time, created_at)]
            )
            
            self.stats['impressions'] += 1
            logger.debug(f"Inserted impression: {impression_id}")
        
        except Exception as e:
            logger.error(f"Error inserting impression: {e}")
    
    def _insert_click(self, data: Dict[str, Any]) -> None:
        """Insert click data into ClickHouse.
        
        Args:
            data: Click data
        """
        try:
            click_id = data.get('id')
            campaign_id = data.get('campaign_id')
            created_at = data.get('created_at') or datetime.now()
            event_time = created_at
            event_date = event_time.date() if hasattr(event_time, 'date') else event_time
            
            self.clickhouse_client.execute(
                """
                INSERT INTO analytics.fact_clicks
                (click_id, campaign_id, event_date, event_time, created_at)
                VALUES
                """,
                [(click_id, campaign_id, event_date, event_time, created_at)]
            )
            
            self.stats['clicks'] += 1
            logger.debug(f"Inserted click: {click_id}")
        
        except Exception as e:
            logger.error(f"Error inserting click: {e}")
    
    def process_advertiser(self, message: Dict[str, Any]) -> None:
        """Process advertiser data from Kafka.
        
        Args:
            message: Kafka message payload
        """
        try:
            operation = message.get('op', 'c')  # Default to create
            self._handle_operation(operation, 'advertiser', message)
        except Exception as e:
            logger.error(f"Error processing advertiser message: {e}")
    
    def process_campaign(self, message: Dict[str, Any]) -> None:
        """Process campaign data from Kafka.
        
        Args:
            message: Kafka message payload
        """
        try:
            operation = message.get('op', 'c')  # Default to create
            self._handle_operation(operation, 'campaign', message)
        except Exception as e:
            logger.error(f"Error processing campaign message: {e}")
    
    def process_impression(self, message: Dict[str, Any]) -> None:
        """Process impression data from Kafka.
        
        Args:
            message: Kafka message payload
        """
        try:
            operation = message.get('op', 'c')  # Default to create
            self._handle_operation(operation, 'impressions', message)
        except Exception as e:
            logger.error(f"Error processing impression message: {e}")
    
    def process_click(self, message: Dict[str, Any]) -> None:
        """Process click data from Kafka.
        
        Args:
            message: Kafka message payload
        """
        try:
            operation = message.get('op', 'c')  # Default to create
            self._handle_operation(operation, 'clicks', message)
        except Exception as e:
            logger.error(f"Error processing click message: {e}")
    
    def close_connections(self) -> None:
        """Close all connections."""
        # ClickHouse driver handles connection pooling,
        # so we just need to set the client to None
        self.clickhouse_client = None
        logger.info("Closed all connections")
