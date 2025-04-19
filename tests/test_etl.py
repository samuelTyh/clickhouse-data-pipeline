import sys
import os
import pytest
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.core.config import AppConfig, PostgresConfig, ClickhouseConfig, ETLConfig
from etl.core.db import PostgresConnector, ClickhouseConnector
from etl.core.pipeline import DataExtractor, DataTransformer, DataLoader, ETLPipeline


@pytest.fixture
def postgres_config():
    """Fixture for PostgreSQL configuration."""
    return PostgresConfig(
        host="localhost",
        port="5432",
        database="test_db",
        user="test_user",
        password="test_password"
    )


@pytest.fixture
def clickhouse_config():
    """Fixture for ClickHouse configuration."""
    return ClickhouseConfig(
        host="localhost",
        port="9000",
        database="test_analytics",
        user="test_user",
        password="test_password"
    )


@pytest.fixture
def etl_config():
    """Fixture for ETL configuration."""
    return ETLConfig(
        sync_interval=60,
        schema_path="test_schema.sql",
        views_path="test_views.sql"
    )


@pytest.fixture
def app_config(postgres_config, clickhouse_config, etl_config):
    """Fixture for application configuration."""
    config = AppConfig()
    config.postgres = postgres_config
    config.clickhouse = clickhouse_config
    config.etl = etl_config
    return config


@pytest.fixture
def mock_pg_connection():
    """Fixture for mocked PostgreSQL connection."""
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    return mock_conn


@pytest.fixture
def mock_pg_connector(mock_pg_connection):
    """Fixture for mocked PostgreSQL connector."""
    with patch('psycopg.connect', return_value=mock_pg_connection):
        connector = MagicMock(spec=PostgresConnector)
        connector.connect.return_value = True
        connector.close.return_value = None
        yield connector


@pytest.fixture
def mock_ch_client():
    """Fixture for mocked ClickHouse client."""
    mock_client = MagicMock()
    mock_client.execute.return_value = []
    return mock_client


@pytest.fixture
def mock_ch_connector(mock_ch_client):
    """Fixture for mocked ClickHouse connector."""
    with patch('clickhouse_driver.Client', return_value=mock_ch_client):
        connector = MagicMock(spec=ClickhouseConnector)
        connector.connect.return_value = True
        connector.close.return_value = None
        connector.client = mock_ch_client
        yield connector


class TestPostgresConnector:
    """Tests for PostgreSQL connector."""

    @patch('psycopg.connect')
    def test_connect_success(self, mock_connect, postgres_config):
        """Test successful connection to PostgreSQL."""
        mock_connect.return_value = MagicMock()
        
        connector = PostgresConnector(postgres_config)
        result = connector.connect()
        
        assert result is True
        mock_connect.assert_called_once_with(
            postgres_config.connection_string,
            autocommit=False
        )
    
    @patch('psycopg.connect')
    def test_connect_failure(self, mock_connect, postgres_config):
        """Test failed connection to PostgreSQL."""
        mock_connect.side_effect = Exception("Connection failed")
        
        connector = PostgresConnector(postgres_config)
        result = connector.connect()
        
        assert result is False
    
    @patch('psycopg.connect')
    def test_close(self, mock_connect, postgres_config):
        """Test closing PostgreSQL connection."""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn
        
        connector = PostgresConnector(postgres_config)
        connector.connect()  # This sets connector.conn to the mock connection
        
        connector.close()
        
        mock_conn.close.assert_called_once()
        assert connector.conn is None
    
    @patch('psycopg.connect')
    def test_execute_query(self, mock_connect, postgres_config):
        """Test executing a query on PostgreSQL."""
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [('result1',), ('result2',)]
        mock_cursor.description = True
        
        mock_conn = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        
        mock_connect.return_value = mock_conn
        
        connector = PostgresConnector(postgres_config)
        connector.connect()
        
        results = connector.execute_query("SELECT * FROM test")
        
        assert results == [('result1',), ('result2',)]
        mock_cursor.execute.assert_called_once_with("SELECT * FROM test", ())


class TestClickhouseConnector:
    """Tests for ClickHouse connector."""

    @patch('clickhouse_driver.Client')
    def test_connect_success(self, mock_client, clickhouse_config):
        """Test successful connection to ClickHouse."""
        mock_client.return_value = MagicMock()
        mock_client.return_value.execute.return_value = True
        
        connector = ClickhouseConnector(clickhouse_config)
        result = connector.connect()
        
        assert result is True
        mock_client.assert_called_once_with(
            host=clickhouse_config.host,
            port=clickhouse_config.port,
            user=clickhouse_config.user,
            password=clickhouse_config.password,
            database=clickhouse_config.database
        )
    
    @patch('clickhouse_driver.Client')
    def test_connect_failure(self, mock_client, clickhouse_config):
        """Test failed connection to ClickHouse."""
        mock_client.return_value = MagicMock()
        mock_client.return_value.execute.side_effect = Exception("Connection failed")
        
        connector = ClickhouseConnector(clickhouse_config)
        result = connector.connect()
        
        assert result is False
    
    def test_close(self, clickhouse_config):
        """Test closing ClickHouse connection."""
        connector = ClickhouseConnector(clickhouse_config)
        connector.client = MagicMock()
        
        connector.close()
        
        assert connector.client is None
    
    @patch('clickhouse_driver.Client')
    def test_execute_query(self, mock_client_class, clickhouse_config):
        """Test executing a query on ClickHouse."""
        # Create a mock client instance
        mock_client_instance = MagicMock()
        mock_client_instance.execute.return_value = [('result1',), ('result2',)]
        
        # Make the Client constructor return our mock instance
        mock_client_class.return_value = mock_client_instance
        
        connector = ClickhouseConnector(clickhouse_config)
        connector.connect()
        
        # Reset the mock to clear the call from connect() that does SELECT 1
        mock_client_instance.execute.reset_mock()
        
        results = connector.execute_query("SELECT * FROM test")
        
        assert results == [('result1',), ('result2',)]
        mock_client_instance.execute.assert_called_once_with("SELECT * FROM test", [])


class TestDataExtractor:
    """Tests for DataExtractor."""

    def test_extract_advertisers(self, mock_pg_connector):
        """Test extracting advertisers."""
        expected_data = [(1, 'Advertiser A', datetime.now(), datetime.now())]
        mock_pg_connector.execute_query.return_value = expected_data
        
        extractor = DataExtractor(mock_pg_connector)
        since = datetime.now() - timedelta(days=1)
        
        result = extractor.extract_advertisers(since)
        
        assert result == expected_data
        mock_pg_connector.execute_query.assert_called_once()
    
    def test_extract_campaigns(self, mock_pg_connector):
        """Test extracting campaigns."""
        expected_data = [(1, 'Campaign A', 1.0, 100.0, datetime.now().date(), 
                         datetime.now().date(), 1, datetime.now(), datetime.now())]
        mock_pg_connector.execute_query.return_value = expected_data
        
        extractor = DataExtractor(mock_pg_connector)
        since = datetime.now() - timedelta(days=1)
        
        result = extractor.extract_campaigns(since)
        
        assert result == expected_data
        mock_pg_connector.execute_query.assert_called_once()
    
    def test_extract_impressions(self, mock_pg_connector):
        """Test extracting impressions."""
        expected_data = [(1, 1, datetime.now())]
        mock_pg_connector.execute_query.return_value = expected_data
        
        extractor = DataExtractor(mock_pg_connector)
        since = datetime.now() - timedelta(days=1)
        
        result = extractor.extract_impressions(since)
        
        assert result == expected_data
        mock_pg_connector.execute_query.assert_called_once()
    
    def test_extract_clicks(self, mock_pg_connector):
        """Test extracting clicks."""
        expected_data = [(1, 1, datetime.now())]
        mock_pg_connector.execute_query.return_value = expected_data
        
        extractor = DataExtractor(mock_pg_connector)
        since = datetime.now() - timedelta(days=1)
        
        result = extractor.extract_clicks(since)
        
        assert result == expected_data
        mock_pg_connector.execute_query.assert_called_once()


class TestDataTransformer:
    """Tests for DataTransformer."""

    def test_transform_advertisers(self):
        """Test transforming advertiser data."""
        now = datetime.now()
        input_data = [(1, 'Advertiser A', now, now)]
        
        transformer = DataTransformer()
        result = transformer.transform_advertisers(input_data)
        
        assert result == [(1, 'Advertiser A', now, now)]
    
    def test_transform_campaigns(self):
        """Test transforming campaign data."""
        now = datetime.now()
        today = now.date()
        input_data = [(1, 'Campaign A', '1.5', 100, today, today, 1, now, now)]
        
        transformer = DataTransformer()
        result = transformer.transform_campaigns(input_data)
        
        assert result == [(1, 'Campaign A', 1.5, 100.0, today, today, 1, now, now)]
    
    def test_transform_impressions(self):
        """Test transforming impression data."""
        now = datetime.now()
        input_data = [(1, 1, now)]
        
        transformer = DataTransformer()
        result = transformer.transform_impressions(input_data)
        
        assert len(result) == 1
        assert result[0][0] == 1  # impression_id
        assert result[0][1] == 1  # campaign_id
        assert result[0][2] == now.date()  # event_date
        assert result[0][3] == now  # event_time
        assert result[0][4] == now  # created_at
    
    def test_transform_clicks(self):
        """Test transforming click data."""
        now = datetime.now()
        input_data = [(1, 1, now)]
        
        transformer = DataTransformer()
        result = transformer.transform_clicks(input_data)
        
        assert len(result) == 1
        assert result[0][0] == 1  # click_id
        assert result[0][1] == 1  # campaign_id
        assert result[0][2] == now.date()  # event_date
        assert result[0][3] == now  # event_time
        assert result[0][4] == now  # created_at


class TestDataLoader:
    """Tests for DataLoader."""

    def test_load_advertisers(self, mock_ch_connector):
        """Test loading advertiser data."""
        now = datetime.now()
        data = [(1, 'Advertiser A', now, now)]
        
        loader = DataLoader(mock_ch_connector)
        count = loader.load_advertisers(data)
        
        assert count == 1
        mock_ch_connector.execute_query.assert_called_once()
    
    def test_load_campaigns(self, mock_ch_connector):
        """Test loading campaign data."""
        now = datetime.now()
        today = now.date()
        data = [(1, 'Campaign A', 1.5, 100.0, today, today, 1, now, now)]
        
        loader = DataLoader(mock_ch_connector)
        count = loader.load_campaigns(data)
        
        assert count == 1
        mock_ch_connector.execute_query.assert_called_once()
    
    def test_load_impressions(self, mock_ch_connector):
        """Test loading impression data."""
        now = datetime.now()
        data = [(1, 1, now.date(), now, now)]
        
        loader = DataLoader(mock_ch_connector)
        count = loader.load_impressions(data)
        
        assert count == 1
        mock_ch_connector.execute_query.assert_called_once()
    
    def test_load_clicks(self, mock_ch_connector):
        """Test loading click data."""
        now = datetime.now()
        data = [(1, 1, now.date(), now, now)]
        
        loader = DataLoader(mock_ch_connector)
        count = loader.load_clicks(data)
        
        assert count == 1
        mock_ch_connector.execute_query.assert_called_once()
    
    def test_load_empty_data(self, mock_ch_connector):
        """Test loading empty data."""
        loader = DataLoader(mock_ch_connector)
        
        count = loader.load_advertisers([])
        assert count == 0
        
        count = loader.load_campaigns([])
        assert count == 0
        
        count = loader.load_impressions([])
        assert count == 0
        
        count = loader.load_clicks([])
        assert count == 0


class TestETLPipeline:
    """Tests for ETLPipeline."""

    def setup_method(self):
        """Set up test environment."""
        self.mock_extractor = MagicMock(spec=DataExtractor)
        self.transformer = DataTransformer()
        self.mock_loader = MagicMock(spec=DataLoader)
        
        self.pipeline = ETLPipeline(
            self.mock_extractor,
            self.transformer,
            self.mock_loader
        )

    def test_sync_advertisers(self):
        """Test syncing advertisers."""
        now = datetime.now()
        advertiser_data = [(1, 'Advertiser A', now, now)]
        transformed_data = [(1, 'Advertiser A', now, now)]
        
        self.mock_extractor.extract_advertisers.return_value = advertiser_data
        self.mock_loader.load_advertisers.return_value = 1
        
        count = self.pipeline.sync_advertisers()
        
        assert count == 1
        self.mock_extractor.extract_advertisers.assert_called_once()
        self.mock_loader.load_advertisers.assert_called_once()
        assert self.pipeline.last_sync['advertiser'] == now

    def test_sync_campaigns(self):
        """Test syncing campaigns."""
        now = datetime.now()
        today = now.date()
        campaign_data = [(1, 'Campaign A', 1.5, 100.0, today, today, 1, now, now)]
        
        self.mock_extractor.extract_campaigns.return_value = campaign_data
        self.mock_loader.load_campaigns.return_value = 1
        
        count = self.pipeline.sync_campaigns()
        
        assert count == 1
        self.mock_extractor.extract_campaigns.assert_called_once()
        self.mock_loader.load_campaigns.assert_called_once()
        assert self.pipeline.last_sync['campaign'] == now

    def test_sync_impressions(self):
        """Test syncing impressions."""
        now = datetime.now()
        impression_data = [(1, 1, now)]
        
        self.mock_extractor.extract_impressions.return_value = impression_data
        self.mock_loader.load_impressions.return_value = 1
        
        count = self.pipeline.sync_impressions()
        
        assert count == 1
        self.mock_extractor.extract_impressions.assert_called_once()
        self.mock_loader.load_impressions.assert_called_once()
        assert self.pipeline.last_sync['impressions'] == now

    def test_sync_clicks(self):
        """Test syncing clicks."""
        now = datetime.now()
        click_data = [(1, 1, now)]
        
        self.mock_extractor.extract_clicks.return_value = click_data
        self.mock_loader.load_clicks.return_value = 1
        
        count = self.pipeline.sync_clicks()
        
        assert count == 1
        self.mock_extractor.extract_clicks.assert_called_once()
        self.mock_loader.load_clicks.assert_called_once()
        assert self.pipeline.last_sync['clicks'] == now

    def test_run_sync_cycle_success(self):
        """Test running a complete sync cycle successfully."""
        self.pipeline.sync_advertisers = MagicMock(return_value=1)
        self.pipeline.sync_campaigns = MagicMock(return_value=2)
        self.pipeline.sync_impressions = MagicMock(return_value=10)
        self.pipeline.sync_clicks = MagicMock(return_value=5)
        
        result = self.pipeline.run_sync_cycle()
        
        assert result is True
        self.pipeline.sync_advertisers.assert_called_once()
        self.pipeline.sync_campaigns.assert_called_once()
        self.pipeline.sync_impressions.assert_called_once()
        self.pipeline.sync_clicks.assert_called_once()
        
        assert self.pipeline.sync_stats['advertiser'] == 1
        assert self.pipeline.sync_stats['campaign'] == 2
        assert self.pipeline.sync_stats['impressions'] == 10
        assert self.pipeline.sync_stats['clicks'] == 5

    def test_run_sync_cycle_failure(self):
        """Test running a sync cycle with failure."""
        self.pipeline.sync_advertisers = MagicMock(side_effect=Exception("Test error"))
        
        result = self.pipeline.run_sync_cycle()
        
        assert result is False
        self.pipeline.sync_advertisers.assert_called_once()

    def test_run_sync_cycle_no_updates(self):
        """Test running a sync cycle with no updates."""
        self.pipeline.sync_advertisers = MagicMock(return_value=0)
        self.pipeline.sync_campaigns = MagicMock(return_value=0)
        self.pipeline.sync_impressions = MagicMock(return_value=0)
        self.pipeline.sync_clicks = MagicMock(return_value=0)
        
        result = self.pipeline.run_sync_cycle()
        
        assert result is True
        assert self.pipeline.sync_stats['advertiser'] == 0
        assert self.pipeline.sync_stats['campaign'] == 0
        assert self.pipeline.sync_stats['impressions'] == 0
        assert self.pipeline.sync_stats['clicks'] == 0
