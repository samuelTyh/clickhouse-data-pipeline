import sys
import os
import pytest
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from etl.core.config import AppConfig
from etl.core.db import PostgresConnector, ClickhouseConnector
from etl.core.schema import SchemaManager
from etl.core.pipeline import DataExtractor, DataTransformer, DataLoader, ETLPipeline


class TestIntegration:
    """Integration tests for the ETL pipeline.
    
    These tests require actual running PostgreSQL and ClickHouse instances.
    They are marked with 'integration' so they can be skipped in regular test runs.
    
    Run with: pytest -m integration
    """
    
    @pytest.fixture(scope="class")
    def app_config(self):
        """Get application configuration from environment."""
        return AppConfig()
    
    @pytest.fixture(scope="class")
    def pg_connector(self, app_config):
        """Create PostgreSQL connector."""
        connector = PostgresConnector(app_config.postgres)
        if not connector.connect():
            pytest.skip("PostgreSQL database not available")
        yield connector
        connector.close()
    
    @pytest.fixture(scope="class")
    def ch_connector(self, app_config):
        """Create ClickHouse connector."""
        connector = ClickhouseConnector(app_config.clickhouse)
        if not connector.connect():
            pytest.skip("ClickHouse database not available")
        yield connector
        connector.close()
    
    @pytest.fixture(scope="class")
    def schema_manager(self, app_config, ch_connector):
        """Create schema manager."""
        return SchemaManager(ch_connector, app_config.etl)
    
    @pytest.fixture(scope="class")
    def etl_pipeline(self, pg_connector, ch_connector):
        """Create ETL pipeline."""
        extractor = DataExtractor(pg_connector)
        transformer = DataTransformer()
        loader = DataLoader(ch_connector)
        return ETLPipeline(extractor, transformer, loader)
    
    @pytest.mark.integration
    def test_database_connections(self, pg_connector, ch_connector):
        """Test database connections."""
        # PostgreSQL connection test
        result = pg_connector.execute_query("SELECT 1")
        assert result == [(1,)]
        
        # ClickHouse connection test
        result = ch_connector.execute_query("SELECT 1")
        assert result == [(1,)]
    
    @pytest.mark.integration
    def test_schema_setup(self, schema_manager):
        """Test schema setup."""
        result = schema_manager.setup_schema()
        assert result is True
        
        result = schema_manager.create_views()
        assert result is True
    
    @pytest.mark.integration
    def test_full_sync_cycle(self, etl_pipeline):
        """Test a full ETL sync cycle."""
        # Reset sync timestamps to get all data
        for key in etl_pipeline.last_sync:
            etl_pipeline.last_sync[key] = datetime.min
        
        # Run sync cycle
        result = etl_pipeline.run_sync_cycle()
        assert result is True
        
        # Verify counts (these will depend on your actual data)
        print(f"Sync stats: {etl_pipeline.sync_stats}")
        assert isinstance(etl_pipeline.sync_stats['advertiser'], int)
        assert isinstance(etl_pipeline.sync_stats['campaign'], int)
        assert isinstance(etl_pipeline.sync_stats['impressions'], int)
        assert isinstance(etl_pipeline.sync_stats['clicks'], int)
    
    @pytest.mark.integration
    def test_incremental_sync(self, etl_pipeline):
        """Test incremental sync (no new data)."""
        # Run a second sync cycle immediately after the first
        # This should find no new data
        result = etl_pipeline.run_sync_cycle()
        assert result is True
        
        # All sync stats should be 0 if no new data was added
        assert etl_pipeline.sync_stats['advertiser'] == 0
        assert etl_pipeline.sync_stats['campaign'] == 0
        assert etl_pipeline.sync_stats['impressions'] == 0
        assert etl_pipeline.sync_stats['clicks'] == 0


@pytest.mark.integration
def test_etl_service_main():
    """Test the main ETL service with run_once flag.
    
    This test requires the ETL service to be properly configured.
    """
    from etl.main import AdtechETLService
    import unittest.mock
    
    # Mock the initialize and run_sync methods to avoid needing actual DB connections
    with unittest.mock.patch.object(AdtechETLService, 'initialize', return_value=True), \
         unittest.mock.patch.object(AdtechETLService, 'run_sync', return_value=True):
        
        # Create service
        service = AdtechETLService()
        
        # Run once with short timeout
        try:
            service.run_service(run_once=True, interval=1)
            # If we reach here, the service ran successfully
            assert True
        except SystemExit as e:
            if e.code != 0:
                pytest.fail(f"ETL service failed with exit code {e.code}")