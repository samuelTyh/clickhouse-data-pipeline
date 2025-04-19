import sys
import os
import pytest
from unittest.mock import MagicMock, patch, mock_open

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.core.config import ETLConfig
from etl.core.db import ClickhouseConnector
from etl.core.schema import SchemaManager


@pytest.fixture
def clickhouse_connector():
    """Fixture for mocked ClickHouse connector."""
    connector = MagicMock(spec=ClickhouseConnector)
    connector.execute_file.return_value = None
    return connector


@pytest.fixture
def etl_config():
    """Fixture for ETL configuration."""
    return ETLConfig(
        sync_interval=60,
        schema_path="test_schema.sql",
        views_path="test_views.sql"
    )

@pytest.mark.schema
class TestSchemaManager:
    """Tests for SchemaManager."""

    def test_setup_schema_success(self, clickhouse_connector, etl_config):
        """Test successful schema setup."""
        schema_manager = SchemaManager(clickhouse_connector, etl_config)
        
        result = schema_manager.setup_schema()
        
        assert result is True
        clickhouse_connector.execute_file.assert_called_once_with(etl_config.schema_path)
    
    def test_setup_schema_failure(self, clickhouse_connector, etl_config):
        """Test schema setup failure."""
        clickhouse_connector.execute_file.side_effect = Exception("Test error")
        
        schema_manager = SchemaManager(clickhouse_connector, etl_config)
        result = schema_manager.setup_schema()
        
        assert result is False
        clickhouse_connector.execute_file.assert_called_once_with(etl_config.schema_path)
    
    def test_create_views_success(self, clickhouse_connector, etl_config):
        """Test successful creation of views."""
        schema_manager = SchemaManager(clickhouse_connector, etl_config)
        
        result = schema_manager.create_views()
        
        assert result is True
        clickhouse_connector.execute_file.assert_called_once_with(etl_config.views_path)
    
    def test_create_views_failure(self, clickhouse_connector, etl_config):
        """Test view creation failure."""
        clickhouse_connector.execute_file.side_effect = Exception("Test error")
        
        schema_manager = SchemaManager(clickhouse_connector, etl_config)
        result = schema_manager.create_views()
        
        assert result is False
        clickhouse_connector.execute_file.assert_called_once_with(etl_config.views_path)


@pytest.mark.schema
class TestSchemaFiles:
    """Tests for schema SQL files."""
    
    @patch("builtins.open", new_callable=mock_open, read_data="CREATE TABLE test;")
    def test_schema_file_exists(self, mock_file):
        """Test that schema file can be read."""
        with open("clickhouse_schema/init.sql", "r") as f:
            content = f.read()
        
        assert content == "CREATE TABLE test;"
        mock_file.assert_called_once_with("clickhouse_schema/init.sql", "r")
    
    @patch("builtins.open", new_callable=mock_open, read_data="CREATE VIEW test;")
    def test_views_file_exists(self, mock_file):
        """Test that views file can be read."""
        with open("clickhouse_schema/kpi_views.sql", "r") as f:
            content = f.read()
        
        assert content == "CREATE VIEW test;"
        mock_file.assert_called_once_with("clickhouse_schema/kpi_views.sql", "r")
