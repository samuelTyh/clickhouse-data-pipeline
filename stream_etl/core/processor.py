import logging
from datetime import datetime, date
from typing import Dict, Any, Union
import clickhouse_driver
import re
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
    
    def _parse_numeric(self, value: Any) -> float:
        """Parse a numeric value from various formats.
        
        Args:
            value: Numeric value in string, int, float, or encoded format
            
        Returns:
            A float value
        """
        if value is None:
            return 0.0
        
        if isinstance(value, (int, float)):
            return float(value)
            
        if isinstance(value, str):
            # First try direct conversion
            try:
                return float(value)
            except ValueError:
                # If it looks like a decimal or numeric string with non-numeric chars
                clean_value = re.sub(r'[^\d\.\-]', '', value)  # Escape the dot and dash
                if clean_value:
                    return float(clean_value)
        
        # Default to 1.0 if parsing fails - a reasonable value that won't break things
        logger.warning(f"Could not parse numeric value: {value}, using 1.0")
        return 1.0
    
    def _parse_date(self, date_value: Any) -> Union[datetime, date]:
        """Parse a date value from various formats.
        
        Args:
            date_value: Date value in string, int, or datetime format
            
        Returns:
            A datetime object or date object
        """
        if date_value is None:
            return datetime.now()
            
        # If already a datetime or date, return as is
        if isinstance(date_value, datetime):
            return date_value
        if isinstance(date_value, date) and not isinstance(date_value, datetime):
            return date_value
            
        # If it's an integer, it might be a timestamp
        if isinstance(date_value, int):
            # Convert timestamp to datetime
            try:
                # If it's a standard Unix timestamp (seconds since epoch)
                if date_value < 32503680000:  # Max reasonable timestamp in seconds (year ~3000)
                    return datetime.fromtimestamp(date_value)
                # If it's in milliseconds
                elif date_value < 32503680000000:
                    return datetime.fromtimestamp(date_value / 1000)
                # If it's in microseconds
                else:
                    return datetime.fromtimestamp(date_value / 1000000)
            except (ValueError, OverflowError, OSError) as e:
                logger.warning(f"Failed to parse timestamp {date_value}: {e}")
                return datetime.now()
        
        # If it's a float, treat it as a timestamp
        if isinstance(date_value, float):
            try:
                # Assume it's seconds since epoch
                return datetime.fromtimestamp(date_value)
            except (ValueError, OverflowError, OSError) as e:
                logger.warning(f"Failed to parse timestamp {date_value}: {e}")
                return datetime.now()
        
        # If it's a string, try various date formats
        if isinstance(date_value, str):
            # Try ISO format
            try:
                cleaned_iso = date_value.replace('Z', '+00:00')
                if 'T' in cleaned_iso:
                    return datetime.fromisoformat(cleaned_iso)
            except ValueError:
                pass
                
            # Try various date formats
            for fmt in ['%Y-%m-%d', '%Y-%m-%d %H:%M:%S', '%Y/%m/%d', '%d-%m-%Y', '%d/%m/%Y']:
                try:
                    return datetime.strptime(date_value, fmt)
                except ValueError:
                    continue
            
            # Try extracting and cleaning the string
            try:
                clean_date = re.sub(r'[^\d\-:T\.Z\+]', '', date_value)  # Escape special characters
                if clean_date:
                    return datetime.fromisoformat(clean_date.replace('Z', '+00:00'))
            except (ValueError, re.error):
                pass
        
        # Default to current datetime if all parsing attempts fail
        logger.warning(f"Could not parse date value: {date_value} (type: {type(date_value)}), using current time")
        return datetime.now()
    
    def _ensure_datetime(self, dt_value: Any) -> datetime:
        """Ensure a value is a datetime object.
        
        Args:
            dt_value: Value that should be a datetime
            
        Returns:
            A datetime object
        """
        if isinstance(dt_value, datetime):
            return dt_value
        
        if isinstance(dt_value, date) and not isinstance(dt_value, datetime):
            # Convert date to datetime at midnight
            return datetime.combine(dt_value, datetime.min.time())
        
        # Parse any other value type
        try:
            return self._parse_date(dt_value)
        except Exception as e:
            logger.warning(f"Failed to convert to datetime: {dt_value}, using current time. Error: {e}")
            return datetime.now()
    
    def _ensure_date(self, dt_value: Any) -> date:
        """Ensure a value is a date object.
        
        Args:
            dt_value: Value that should be a date
            
        Returns:
            A date object
        """
        if isinstance(dt_value, date):
            return dt_value
        
        # If it's a datetime, extract the date
        if isinstance(dt_value, datetime):
            return dt_value.date()
        
        # Parse any other value type to datetime first, then extract date
        dt = self._ensure_datetime(dt_value)
        return dt.date()
    
    def _handle_operation(self, operation: str, table: str, data: Dict[str, Any]) -> None:
        """Handle different database operations.
        
        Args:
            operation: Database operation (c=create, u=update, d=delete)
            table: Table name
            data: Data payload
        """
        logger.debug(f"Processing {operation} operation for {table}: {data}")
        
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
            updated_at = self._ensure_datetime(data.get('updated_at'))
            created_at = self._ensure_datetime(data.get('created_at'))
            
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
            # Debug the raw data
            logger.debug(f"Raw campaign data: {data}")
            
            campaign_id = data.get('id')
            name = data.get('name')
            
            # Debug the bid and budget values
            bid_raw = data.get('bid')
            budget_raw = data.get('budget')
            logger.debug(f"Raw bid value: {bid_raw} (type: {type(bid_raw)})")
            logger.debug(f"Raw budget value: {budget_raw} (type: {type(budget_raw)})")
            
            # Parse numeric values safely
            bid = self._parse_numeric(bid_raw)
            budget = self._parse_numeric(budget_raw)
            logger.debug(f"Parsed bid: {bid}, budget: {budget}")
            
            # Parse dates
            start_date = self._ensure_date(data.get('start_date'))
            end_date = self._ensure_date(data.get('end_date'))
            
            advertiser_id = data.get('advertiser_id')
            updated_at = self._ensure_datetime(data.get('updated_at'))
            created_at = self._ensure_datetime(data.get('created_at'))
            
            # Debug final values
            logger.debug(f"Final values - campaign_id: {campaign_id}, name: {name}, "
                        f"bid: {bid}, budget: {budget}, start_date: {start_date}, "
                        f"end_date: {end_date}, advertiser_id: {advertiser_id}")
            
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
            # Log the full stack trace for debugging
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
    
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
            # Debug the raw data
            logger.debug(f"Raw impression data: {data}")
            
            impression_id = data.get('id')
            campaign_id = data.get('campaign_id')
            
            # Debug timestamp before parsing
            timestamp_raw = data.get('created_at')
            logger.debug(f"Raw created_at value: {timestamp_raw} (type: {type(timestamp_raw)})")
            
            # Handle timestamp parsing with more robustness
            created_at = self._ensure_datetime(timestamp_raw)
            event_time = created_at
            event_date = created_at.date()
            
            logger.debug(f"Parsed timestamps - created_at: {created_at}, event_time: {event_time}, event_date: {event_date}")
            
            # Debug final values before insert
            logger.debug(f"Final values - impression_id: {impression_id}, campaign_id: {campaign_id}, "
                        f"event_date: {event_date}, event_time: {event_time}, created_at: {created_at}")
            
            self.clickhouse_client.execute(
                """
                INSERT INTO analytics.fact_impressions
                (impression_id, campaign_id, event_date, event_time, created_at)
                VALUES
                """,
                [(impression_id, campaign_id, event_date, event_time, created_at)]
            )
            
            self.stats['impressions'] += 1
            logger.info(f"Inserted impression: {impression_id}")
        
        except Exception as e:
            logger.error(f"Error inserting impression: {e}")
            # Log the full stack trace for debugging
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
    
    def _insert_click(self, data: Dict[str, Any]) -> None:
        """Insert click data into ClickHouse.
        
        Args:
            data: Click data
        """
        try:
            # Debug the raw data
            logger.debug(f"Raw click data: {data}")
            
            click_id = data.get('id')
            campaign_id = data.get('campaign_id')
            
            # Debug timestamp before parsing
            timestamp_raw = data.get('created_at')
            logger.debug(f"Raw created_at value: {timestamp_raw} (type: {type(timestamp_raw)})")
            
            # Handle timestamp parsing with more robustness
            created_at = self._ensure_datetime(timestamp_raw)
            event_time = created_at
            event_date = created_at.date()
            
            logger.debug(f"Parsed timestamps - created_at: {created_at}, event_time: {event_time}, event_date: {event_date}")
            
            # Debug final values before insert
            logger.debug(f"Final values - click_id: {click_id}, campaign_id: {campaign_id}, "
                        f"event_date: {event_date}, event_time: {event_time}, created_at: {created_at}")
            
            self.clickhouse_client.execute(
                """
                INSERT INTO analytics.fact_clicks
                (click_id, campaign_id, event_date, event_time, created_at)
                VALUES
                """,
                [(click_id, campaign_id, event_date, event_time, created_at)]
            )
            
            self.stats['clicks'] += 1
            logger.info(f"Inserted click: {click_id}")
        
        except Exception as e:
            logger.error(f"Error inserting click: {e}")
            # Log the full stack trace for debugging
            import traceback
            logger.error(f"Stack trace: {traceback.format_exc()}")
    
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
