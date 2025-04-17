import os
import logging

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
    pass

def main():
    setup_clickhouse_schema()


if __name__ == "__main__":
    main()
