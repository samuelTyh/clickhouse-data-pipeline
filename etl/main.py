import os
import sys
import time
import logging

import psycopg
import clickhouse_driver

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('adtech-etl')

# Connection settings from environment variables
PG_HOST = os.environ.get('POSTGRES_HOST', 'postgres')
PG_PORT = os.environ.get('POSTGRES_PORT', '5432')
PG_DB = os.environ.get('POSTGRES_DB', 'postgres')
PG_USER = os.environ.get('POSTGRES_USER', 'postgres')
PG_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'postgres')

CH_HOST = os.environ.get('CLICKHOUSE_HOST', 'clickhouse')
CH_PORT = os.environ.get('CLICKHOUSE_PORT', '9000')
CH_DB = os.environ.get('CLICKHOUSE_DB', 'analytics')
CH_USER = os.environ.get('CLICKHOUSE_USER', 'sysadmin')
CH_PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', 'sysadmin')

# Schedule settings
SYNC_INTERVAL = int(os.environ.get('SYNC_INTERVAL', '300'))


def setup_clickhouse_schema():
    """Initialize ClickHouse schema if not exists"""
    client = clickhouse_driver.Client(
        host=CH_HOST,
        port=CH_PORT,
        user=CH_USER,
        password=CH_PASSWORD,
        database=CH_DB
    )
    
    # Read schema from file
    try:
        with open('clickhouse_schema.sql', 'r') as f:
            schema_sql = f.read()
        
        # Execute schema statements
        for statement in schema_sql.split(';'):
            if statement.strip():
                client.execute(statement)
        
        logger.info("ClickHouse schema initialized successfully")
    except Exception as e:
        logger.error(f"Error setting up ClickHouse schema: {e}")
        raise


class AdtechETL:
    
    def __init__(self):
        self.pg_conn = None
        self.ch_client = None

    def connect(self):
        """Establish database connections"""
        try:
            self.pg_conn = psycopg.connect(
                f"host={PG_HOST} port={PG_PORT} dbname={PG_DB} user={PG_USER} password={PG_PASSWORD}",
                autocommit=False,
            )
            
            self.ch_client = clickhouse_driver.Client(
                host=CH_HOST,
                port=CH_PORT,
                user=CH_USER,
                password=CH_PASSWORD,
                database=CH_DB
            )
            
            logger.info("Database connections established")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to databases: {e}")
            return False
        
    def run_sync(self):
        """Run a complete ETL cycle"""
        try:
            logger.info("Starting ETL sync cycle")
            
            if not self.connect():
                return False
            
            logger.info("ETL sync cycle completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"ETL sync failed: {e}")
            return False
        finally:
            if self.pg_conn:
                self.pg_conn.close()


def main():
    logger.info("Starting AdTech ETL service")
    setup_clickhouse_schema()
    etl = AdtechETL()

    try:
        while True:
            success = etl.run_sync()
            if not success:
                logger.warning(f"Waiting {SYNC_INTERVAL} seconds before retry...")
            
            logger.info(f"Sleeping for {SYNC_INTERVAL} seconds...")
            time.sleep(SYNC_INTERVAL)
    except KeyboardInterrupt:
        logger.info("ETL service interrupted, shutting down")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
