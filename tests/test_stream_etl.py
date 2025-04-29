import sys
import os
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Import stream ETL modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../stream_etl')))
from stream_etl.core.config import AppConfig
from stream_etl.core.processor import DataProcessor
from stream_etl.core.consumer import KafkaConsumerManager


@pytest.fixture
def app_config():
    """Fixture for application configuration."""
    config = AppConfig()
    return config


@pytest.fixture
def mock_clickhouse_client():
    """Fixture for mocked ClickHouse client."""
    mock_client = MagicMock()
    return mock_client


@pytest.fixture
def mock_processor(mock_clickhouse_client):
    """Fixture for mocked data processor."""
    processor = MagicMock(spec=DataProcessor)
    processor.clickhouse_client = mock_clickhouse_client
    return processor


@pytest.mark.unit
class TestDataProcessor:
    """Tests for DataProcessor."""

    @patch('clickhouse_driver.Client')
    def test_init(self, mock_client_class, app_config):
        """Test processor initialization."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        processor = DataProcessor(app_config)
        
        assert processor.config == app_config
        assert processor.clickhouse_client == mock_client_instance
        assert processor.stats == {
            'advertiser': 0,
            'campaign': 0,
            'impressions': 0,
            'clicks': 0
        }

    @patch('clickhouse_driver.Client')
    def test_process_advertiser(self, mock_client_class, app_config):
        """Test processing advertiser message."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        processor = DataProcessor(app_config)
        processor._handle_operation = MagicMock()
        
        message = {
            'op': 'c',
            'id': 1,
            'name': 'Test Advertiser',
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        }
        
        processor.process_advertiser(message)
        
        processor._handle_operation.assert_called_once_with('c', 'advertiser', message)

    @patch('clickhouse_driver.Client')
    def test_process_campaign(self, mock_client_class, app_config):
        """Test processing campaign message."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        processor = DataProcessor(app_config)
        processor._handle_operation = MagicMock()
        
        message = {
            'op': 'c',
            'id': 1,
            'name': 'Test Campaign',
            'bid': "1.5",  # Simulating string from decimal.handling.mode: string
            'budget': "100.0",
            'start_date': datetime.now().date().isoformat(),
            'end_date': (datetime.now() + timedelta(days=30)).date().isoformat(),
            'advertiser_id': 1,
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        processor.process_campaign(message)
        
        processor._handle_operation.assert_called_once_with('c', 'campaign', message)

    @patch('clickhouse_driver.Client')
    def test_process_impression(self, mock_client_class, app_config):
        """Test processing impression message."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        processor = DataProcessor(app_config)
        processor._handle_operation = MagicMock()
        
        message = {
            'op': 'c',
            'id': 1,
            'campaign_id': 1,
            'created_at': int(datetime.now().timestamp())  # Simulating integer timestamp
        }
        
        processor.process_impression(message)
        
        processor._handle_operation.assert_called_once_with('c', 'impressions', message)

    @patch('clickhouse_driver.Client')
    def test_process_click(self, mock_client_class, app_config):
        """Test processing click message."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        processor = DataProcessor(app_config)
        processor._handle_operation = MagicMock()
        
        message = {
            'op': 'c',
            'id': 1,
            'campaign_id': 1,
            'created_at': datetime.now().isoformat()
        }
        
        processor.process_click(message)
        
        processor._handle_operation.assert_called_once_with('c', 'clicks', message)

    @patch('clickhouse_driver.Client')
    def test_parse_numeric(self, mock_client_class, app_config):
        """Test numeric parsing with various formats."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        processor = DataProcessor(app_config)
        
        # Test with various input types
        assert processor._parse_numeric(None) == 0.0
        assert processor._parse_numeric(42) == 42.0
        assert processor._parse_numeric(3.14) == 3.14
        assert processor._parse_numeric("2.5") == 2.5
        assert processor._parse_numeric("$3.99") == 3.99
        
        # Test with invalid input
        assert processor._parse_numeric("invalid") == 1.0
    
    @patch('clickhouse_driver.Client')
    def test_parse_date(self, mock_client_class, app_config):
        """Test date parsing with various formats."""
        mock_client_instance = MagicMock()
        mock_client_class.return_value = mock_client_instance
        
        processor = DataProcessor(app_config)
        
        # Test with datetime object
        now = datetime.now()
        assert processor._parse_date(now) == now
        
        # Test with date string
        date_str = "2023-04-15"
        parsed = processor._parse_date(date_str)
        assert isinstance(parsed, datetime)
        assert parsed.year == 2023
        assert parsed.month == 4
        assert parsed.day == 15
        
        # Test with timestamp
        timestamp = int(now.timestamp())
        parsed = processor._parse_date(timestamp)
        assert isinstance(parsed, datetime)
        
        # Test with ISO format
        iso_str = "2023-04-15T14:30:45Z"
        parsed = processor._parse_date(iso_str)
        assert isinstance(parsed, datetime)
        assert parsed.year == 2023
        assert parsed.month == 4
        assert parsed.day == 15
        assert parsed.hour == 14
        assert parsed.minute == 30
        assert parsed.second == 45
        
        # Test with invalid input
        assert isinstance(processor._parse_date("invalid"), datetime)


@pytest.mark.unit
class TestKafkaConsumerManager:
    """Tests for KafkaConsumerManager."""

    def test_init(self, mock_processor):
        """Test consumer manager initialization."""
        bootstrap_servers = 'kafka:9092'
        group_id = 'test-group'
        topics = ['topic1', 'topic2']
        
        consumer_manager = KafkaConsumerManager(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topics=topics,
            processor=mock_processor
        )
        
        assert consumer_manager.bootstrap_servers == bootstrap_servers
        assert consumer_manager.group_id == group_id
        assert consumer_manager.topics == topics
        assert consumer_manager.processor == mock_processor
        assert consumer_manager.auto_offset_reset == 'earliest'
        assert consumer_manager.consumer is None
        assert consumer_manager.running is False
        assert consumer_manager.consumer_thread is None

    # Properly mocked KafkaConsumer test
    @patch('stream_etl.core.consumer.KafkaConsumer')
    def test_create_consumer(self, mock_kafka_consumer):
        """Test creating Kafka consumer."""
        bootstrap_servers = 'kafka:9092'
        group_id = 'test-group'
        topics = ['topic1', 'topic2']
        
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance
        
        consumer_manager = KafkaConsumerManager(
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            topics=topics,
            processor=MagicMock()
        )
        
        # Call the method with the mock in place
        result = consumer_manager._create_consumer()
        
        # Verify the mock was called correctly
        mock_kafka_consumer.assert_called_once()
        assert result == mock_consumer_instance

    def test_process_message(self, mock_processor):
        """Test processing message."""
        consumer_manager = KafkaConsumerManager(
            bootstrap_servers='kafka:9092',
            group_id='test-group',
            topics=['postgres.public.advertiser'],
            processor=mock_processor
        )
        
        topic = 'postgres.public.advertiser'
        message = {
            'id': 1,
            'name': 'Test Advertiser',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        consumer_manager._process_message(topic, message)
        
        mock_processor.process_advertiser.assert_called_once_with(message)


@pytest.mark.integration
class TestStreamETLIntegration:
    """Integration tests for the Stream ETL service.
    
    These tests require actual running Kafka and ClickHouse instances.
    """
    
    @pytest.fixture
    def stream_etl_config(self):
        """Get application configuration from environment."""
        config = AppConfig()
        # Switch to local hosts for testing
        config.clickhouse.host = 'localhost'
        config.clickhouse.port = '9001'
        config.kafka.bootstrap_servers = 'localhost:9092'
        return config
    
    @patch('clickhouse_driver.Client')
    @patch('kafka.KafkaConsumer')
    def test_end_to_end_flow(self, mock_kafka_consumer, mock_clickhouse_client, stream_etl_config):
        """Test end-to-end flow of processing messages."""
        # Setup mocks
        mock_consumer_instance = MagicMock()
        mock_kafka_consumer.return_value = mock_consumer_instance
        
        mock_client_instance = MagicMock()
        mock_clickhouse_client.return_value = mock_client_instance
        
        # Create processor
        processor = DataProcessor(stream_etl_config)
        
        # Create consumer manager
        consumer_manager = KafkaConsumerManager(
            bootstrap_servers=stream_etl_config.kafka.bootstrap_servers,
            group_id='test-group',
            topics=[
                stream_etl_config.kafka.advertiser_topic,
                stream_etl_config.kafka.campaign_topic,
                stream_etl_config.kafka.impressions_topic,
                stream_etl_config.kafka.clicks_topic
            ],
            processor=processor
        )
        
        # Test with sample messages
        advertiser_message = {
            'op': 'c',
            'id': 1,
            'name': 'Test Advertiser',
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        }
        
        consumer_manager._process_message(stream_etl_config.kafka.advertiser_topic, advertiser_message)
        
        # Verify ClickHouse client was called
        mock_client_instance.execute.assert_called()
