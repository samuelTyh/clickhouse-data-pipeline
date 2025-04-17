import logging
from .db import ClickhouseConnector
from .config import ETLConfig

logger = logging.getLogger('adtech-etl.schema')


class SchemaManager:
    """Manages ClickHouse schema creation and updates."""
    
    def __init__(self, db_connector: ClickhouseConnector, config: ETLConfig):
        """Initialize with ClickHouse connector and configuration."""
        self.db = db_connector
        self.config = config
    
    def setup_schema(self) -> bool:
        """Initialize ClickHouse schema if not exists."""
        try:
            self.db.execute_file(self.config.schema_path)
            logger.info("ClickHouse schema initialized successfully")
            return True
        except Exception as e:
            logger.error(f"Error setting up ClickHouse schema: {e}")
            return False
    
    def create_views(self) -> bool:
        """Create materialized views in ClickHouse."""
        try:
            self.db.execute_file(self.config.views_path)
            logger.info("ClickHouse materialized views created successfully")
            return True
        except Exception as e:
            logger.error(f"Error setting up ClickHouse materialized views: {e}")
            return False
