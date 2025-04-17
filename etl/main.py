import sys
import time
import logging
import argparse
from typing import Optional

from core import (
    AppConfig, PostgresConnector, ClickhouseConnector, SchemaManager, 
    DataExtractor, DataTransformer, DataLoader, ETLPipeline
)
from logging_config import configure_logging


def parse_arguments():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description='AdTech ETL Service')
    
    parser.add_argument(
        '--run-once', 
        action='store_true',
        help='Run sync once and exit'
    )
    
    parser.add_argument(
        '--interval', 
        type=int, 
        help='Override sync interval (seconds)'
    )
    
    parser.add_argument(
        '--log-level',
        choices=['debug', 'info', 'warning', 'error', 'critical'],
        default='info',
        help='Set logging level'
    )
    
    return parser.parse_args()


class AdtechETLService:
    """Main service class that coordinates ETL processes."""
    
    def __init__(self, config: Optional[AppConfig] = None):
        """Initialize the ETL service."""
        self.config = config or AppConfig()
        self.logger = logging.getLogger('adtech-etl.service')
    
    def initialize(self) -> bool:
        """Initialize the service and set up connections."""
        try:
            # Create database connectors
            self.pg_connector = PostgresConnector(self.config.postgres)
            self.ch_connector = ClickhouseConnector(self.config.clickhouse)
            
            # Test connections
            pg_connected = self.pg_connector.connect()
            ch_connected = self.ch_connector.connect()
            
            if not pg_connected or not ch_connected:
                self.logger.error("Failed to connect to databases")
                return False
            
            # Set up schema manager
            self.schema_manager = SchemaManager(self.ch_connector, self.config.etl)
            
            # Initialize schema
            schema_initialized = self.schema_manager.setup_schema()
            if not schema_initialized:
                self.logger.error("Failed to initialize ClickHouse schema")
                return False
            
            # Create ETL pipeline
            extractor = DataExtractor(self.pg_connector)
            transformer = DataTransformer()
            loader = DataLoader(self.ch_connector)
            self.etl_pipeline = ETLPipeline(extractor, transformer, loader)
            
            self.logger.info("AdTech ETL service initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to initialize ETL service: {e}")
            return False
    
    def run_sync(self) -> bool:
        """Run a single ETL sync cycle."""
        try:
            success = self.etl_pipeline.run_sync_cycle()
            
            # Create ClickHouse views if needed
            self.schema_manager.create_views()
            
            return success
        except Exception as e:
            self.logger.error(f"Sync failed with error: {e}")
            return False
        finally:
            # Ensure connections are closed properly
            self.pg_connector.close()
    
    def run_service(self, run_once: bool = False, interval: Optional[int] = None) -> None:
        """Run the ETL service continuously or once."""
        sync_interval = interval or self.config.etl.sync_interval
        
        if not self.initialize():
            self.logger.error("Service initialization failed. Exiting.")
            sys.exit(1)
        
        self.logger.info(f"AdTech ETL service started with sync interval: {sync_interval}s")
        
        try:
            if run_once:
                success = self.run_sync()
                if not success:
                    sys.exit(1)
            else:
                # Continuous operation
                while True:
                    success = self.run_sync()
                    if not success:
                        self.logger.warning(f"Waiting {sync_interval} seconds before retry...")
                    
                    self.logger.info(f"Sleeping for {sync_interval} seconds...")
                    time.sleep(sync_interval)
                    
        except KeyboardInterrupt:
            self.logger.info("ETL service interrupted, shutting down")
        except Exception as e:
            self.logger.critical(f"Unexpected error: {e}")
            sys.exit(1)
        finally:
            # Clean up resources
            if hasattr(self, 'pg_connector'):
                self.pg_connector.close()
            if hasattr(self, 'ch_connector'):
                self.ch_connector.close()


def main():
    """Main entry point."""
    args = parse_arguments()
    
    # Configure logging
    log_levels = {
        'debug': logging.DEBUG,
        'info': logging.INFO,
        'warning': logging.WARNING,
        'error': logging.ERROR,
        'critical': logging.CRITICAL
    }
    configure_logging(log_levels[args.log_level])
    
    logger = logging.getLogger('adtech-etl')
    logger.info("Starting AdTech ETL service")
    
    # Create and run service
    service = AdtechETLService()
    service.run_service(run_once=args.run_once, interval=args.interval)


if __name__ == "__main__":
    main()
