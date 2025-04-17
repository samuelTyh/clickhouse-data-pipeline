import os
import sys
import time
import logging
from datetime import datetime

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
        self.last_sync = {
            'advertiser': datetime.min,
            'campaign': datetime.min,
            'impressions': datetime.min,
            'clicks': datetime.min
        }
    
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
    
    def sync_advertisers(self):
        """Sync advertisers from PostgreSQL to ClickHouse"""
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, updated_at, created_at 
                FROM advertiser
                WHERE updated_at > %s OR created_at > %s
            """, (self.last_sync['advertiser'], self.last_sync['advertiser']))
            
            rows = cur.fetchall()
            if not rows:
                logger.info("No new advertisers to sync")
                return 0
            
            # Transform data
            data = []
            latest_update = self.last_sync['advertiser']
            for adv_id, name, updated_at, created_at in rows:
                data.append((
                    adv_id,
                    name,
                    updated_at or datetime.now(),
                    created_at or datetime.now()
                ))
                if updated_at and updated_at > latest_update:
                    latest_update = updated_at
                if created_at and created_at > latest_update:
                    latest_update = created_at
            
            # Load to ClickHouse
            self.ch_client.execute(
                """
                INSERT INTO analytics.dim_advertiser
                (advertiser_id, name, updated_at, created_at)
                VALUES
                """,
                data
            )
            
            self.last_sync['advertiser'] = latest_update
            logger.info(f"Synced {len(data)} advertisers")
            return len(data)

    def sync_campaigns(self):
        """Sync campaigns from PostgreSQL to ClickHouse"""
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, bid, budget, start_date, end_date, advertiser_id, updated_at, created_at
                FROM campaign
                WHERE updated_at > %s OR created_at > %s
            """, (self.last_sync['campaign'], self.last_sync['campaign']))
            
            rows = cur.fetchall()
            if not rows:
                logger.info("No new campaigns to sync")
                return 0
            
            # Transform data
            data = []
            latest_update = self.last_sync['campaign']
            for row in rows:
                campaign_id, name, bid, budget, start_date, end_date, advertiser_id, updated_at, created_at = row
                data.append((
                    campaign_id,
                    name,
                    float(bid) if bid else 0.0,
                    float(budget) if budget else 0.0,
                    start_date or datetime.now().date(),
                    end_date or datetime.now().date(),
                    advertiser_id,
                    updated_at or datetime.now(),
                    created_at or datetime.now()
                ))
                if updated_at and updated_at > latest_update:
                    latest_update = updated_at
                if created_at and created_at > latest_update:
                    latest_update = created_at
            
            # Load to ClickHouse
            self.ch_client.execute(
                """
                INSERT INTO analytics.dim_campaign
                (campaign_id, name, bid, budget, start_date, end_date, advertiser_id, updated_at, created_at)
                VALUES
                """,
                data
            )
            
            self.last_sync['campaign'] = latest_update
            logger.info(f"Synced {len(data)} campaigns")
            return len(data)

    def sync_impressions(self):
        """Sync impressions from PostgreSQL to ClickHouse"""
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id, campaign_id, created_at
                FROM impressions
                WHERE created_at > %s
            """, (self.last_sync['impressions'],))
            
            rows = cur.fetchall()
            if not rows:
                logger.info("No new impressions to sync")
                return 0
            
            # Transform data
            data = []
            latest_update = self.last_sync['impressions']
            for imp_id, campaign_id, created_at in rows:
                event_time = created_at or datetime.now()
                event_date = event_time.date()
                
                data.append((
                    imp_id,
                    campaign_id,
                    event_date,
                    event_time,
                    created_at or datetime.now()
                ))
                
                if created_at and created_at > latest_update:
                    latest_update = created_at
            
            # Load to ClickHouse
            self.ch_client.execute(
                """
                INSERT INTO analytics.fact_impressions
                (impression_id, campaign_id, event_date, event_time, created_at)
                VALUES
                """,
                data
            )
            
            self.last_sync['impressions'] = latest_update
            logger.info(f"Synced {len(data)} impressions")
            return len(data)

    def sync_clicks(self):
        """Sync clicks from PostgreSQL to ClickHouse"""
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                SELECT id, campaign_id, created_at
                FROM clicks
                WHERE created_at > %s
            """, (self.last_sync['clicks'],))
            
            rows = cur.fetchall()
            if not rows:
                logger.info("No new clicks to sync")
                return 0
            
            # Transform data
            data = []
            latest_update = self.last_sync['clicks']
            for click_id, campaign_id, created_at in rows:
                event_time = created_at or datetime.now()
                event_date = event_time.date()
                
                data.append((
                    click_id,
                    campaign_id,
                    event_date,
                    event_time,
                    created_at or datetime.now()
                ))
                
                if created_at and created_at > latest_update:
                    latest_update = created_at
            
            # Load to ClickHouse
            self.ch_client.execute(
                """
                INSERT INTO analytics.fact_clicks
                (click_id, campaign_id, event_date, event_time, created_at)
                VALUES
                """,
                data
            )
            
            self.last_sync['clicks'] = latest_update
            logger.info(f"Synced {len(data)} clicks")
            return len(data)

    def refresh_materialized_views(self):
        """Refresh materialized views in ClickHouse"""
        try:
            self.ch_client.execute("OPTIMIZE TABLE analytics.mv_daily_metrics FINAL")
            logger.info("Refreshed materialized views")
        except Exception as e:
            logger.error(f"Error refreshing materialized views: {e}")

    def run_sync(self):
        """Run a complete ETL cycle"""
        try:
            logger.info("Starting ETL sync cycle")
            
            if not self.connect():
                return False
            
            # Sync dimension tables
            self.sync_advertisers()
            self.sync_campaigns()
            
            # Then sync fact tables
            self.sync_impressions()
            self.sync_clicks()
            
            # Refresh materialized views
            self.refresh_materialized_views()
            
            logger.info("ETL sync cycle completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"ETL sync failed: {e}")
            return False
        finally:
            if self.pg_conn:
                self.pg_conn.close()


def main():
    """Main entry point"""
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
