import logging
from typing import List, Optional, Tuple, Union, Dict, Any

import psycopg
import clickhouse_driver

from .config import PostgresConfig, ClickhouseConfig

logger = logging.getLogger('adtech-etl.db')


class PostgresConnector:
    """PostgreSQL connection manager."""
    
    def __init__(self, config: PostgresConfig):
        """Initialize with PostgreSQL configuration."""
        self.config = config
        self.conn = None
    
    def connect(self) -> bool:
        """Establish connection to PostgreSQL database."""
        try:
            self.conn = psycopg.connect(
                self.config.connection_string,
                autocommit=False,
            )
            logger.info("Connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            return False
    
    def close(self) -> None:
        """Close connection if open."""
        if self.conn:
            self.conn.close()
            self.conn = None
            logger.info("Closed PostgreSQL connection")
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[Tuple] = None
    ) -> List[Tuple]:
        """Execute a query and return results."""
        if not self.conn:
            if not self.connect():
                raise ConnectionError("Could not establish PostgreSQL connection")
        
        with self.conn.cursor() as cur:
            cur.execute(query, params or ())
            return cur.fetchall() if cur.description else []
    
    def execute_many(
        self, 
        query: str, 
        params_list: List[Tuple]
    ) -> None:
        """Execute a query with multiple parameter sets."""
        if not self.conn:
            if not self.connect():
                raise ConnectionError("Could not establish PostgreSQL connection")
        
        with self.conn.cursor() as cur:
            cur.executemany(query, params_list)
    
    def commit(self) -> None:
        """Commit current transaction."""
        if self.conn:
            self.conn.commit()


class ClickhouseConnector:
    """ClickHouse connection manager."""
    
    def __init__(self, config: ClickhouseConfig):
        """Initialize with ClickHouse configuration."""
        self.config = config
        self.client = None
    
    def connect(self) -> bool:
        """Establish connection to ClickHouse database."""
        try:
            self.client = clickhouse_driver.Client(
                host=self.config.host,
                port=self.config.port,
                user=self.config.user,
                password=self.config.password,
                database=self.config.database
            )
            # Test connection
            self.client.execute("SELECT 1")
            logger.info("Connected to ClickHouse database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            self.client = None
            return False
    
    def close(self) -> None:
        """Close connection if open."""
        # ClickHouse driver handles connection pooling,
        # so we just need to remove the reference
        self.client = None
        logger.info("Closed ClickHouse connection")
    
    def execute_query(
        self, 
        query: str, 
        params: Optional[List[Union[Tuple, Dict[str, Any]]]] = None
    ) -> List:
        """Execute a query and return results."""
        if not self.client:
            if not self.connect():
                raise ConnectionError("Could not establish ClickHouse connection")
        
        return self.client.execute(query, params or [])
    
    def execute_script(self, sql_script: str) -> None:
        """Execute a multi-statement SQL script."""
        if not self.client:
            if not self.connect():
                raise ConnectionError("Could not establish ClickHouse connection")
        
        # Split script by semicolons and execute each statement
        for statement in sql_script.split(';'):
            if statement.strip():
                self.client.execute(statement)
    
    def execute_file(self, file_path: str) -> None:
        """Execute SQL from a file."""
        try:
            with open(file_path, 'r') as f:
                sql_script = f.read()
            self.execute_script(sql_script)
            logger.info(f"Successfully executed SQL from {file_path}")
        except Exception as e:
            logger.error(f"Error executing SQL from {file_path}: {e}")
            raise
