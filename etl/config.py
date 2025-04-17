import os
import sys
import logging
from dataclasses import dataclass


def configure_logging(level: int = logging.INFO) -> None:
    """Configure logging for the application."""
    # Set up root logger
    logger = logging.getLogger('adtech-etl')
    logger.setLevel(level)
    
    # Create console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(level)
    
    # Create formatter
    formatter = logging.Formatter(
        '%(asctime)s - %(filename)s - %(funcName)s - %(levelname)s - %(message)s'
    )
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(console_handler)
    
    # Add file handler for errors
    error_handler = logging.FileHandler('errors.log')
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)
    logger.addHandler(error_handler)


@dataclass
class PostgresConfig:
    """PostgreSQL connection configuration."""
    host: str = os.environ.get('POSTGRES_HOST', 'postgres')
    port: str = os.environ.get('POSTGRES_PORT', '5432')
    database: str = os.environ.get('POSTGRES_DB', 'postgres')
    user: str = os.environ.get('POSTGRES_USER', 'postgres')
    password: str = os.environ.get('POSTGRES_PASSWORD', 'postgres')
    
    @property
    def connection_string(self) -> str:
        """Return connection string for PostgreSQL."""
        return (
            f"host={self.host} port={self.port} "
            f"dbname={self.database} user={self.user} password={self.password}"
        )


@dataclass
class ClickhouseConfig:
    """ClickHouse connection configuration."""
    host: str = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
    port: str = os.environ.get('CLICKHOUSE_PORT', '9000')
    database: str = os.environ.get('CLICKHOUSE_DB', 'analytics')
    user: str = os.environ.get('CLICKHOUSE_USER', 'sysadmin')
    password: str = os.environ.get('CLICKHOUSE_PASSWORD', 'sysadmin')


@dataclass
class ETLConfig:
    """ETL process configuration."""
    sync_interval: int = int(os.environ.get('SYNC_INTERVAL', '300'))
    schema_path: str = os.environ.get('SCHEMA_PATH', 'clickhouse/schema.sql')
    views_path: str = os.environ.get('VIEWS_PATH', 'clickhouse/views.sql')


class AppConfig:
    """Application configuration container."""
    
    def __init__(self):
        self.postgres = PostgresConfig()
        self.clickhouse = ClickhouseConfig()
        self.etl = ETLConfig()
