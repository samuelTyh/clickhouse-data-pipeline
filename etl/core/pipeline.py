import logging
from datetime import datetime
from typing import List, Tuple

from .db import PostgresConnector, ClickhouseConnector

logger = logging.getLogger('adtech-etl.core')


class DataExtractor:
    """Extracts data from PostgreSQL source database."""
    
    def __init__(self, db: PostgresConnector):
        """Initialize with PostgreSQL connector."""
        self.db = db
    
    def extract_advertisers(self, since: datetime) -> List[Tuple]:
        """Extract advertisers updated since the given timestamp."""
        query = """
            SELECT id, name, updated_at, created_at 
            FROM advertiser
            WHERE updated_at > %s OR created_at > %s
        """
        return self.db.execute_query(query, (since, since))
    
    def extract_campaigns(self, since: datetime) -> List[Tuple]:
        """Extract campaigns updated since the given timestamp."""
        query = """
            SELECT id, name, bid, budget, start_date, end_date, advertiser_id, updated_at, created_at
            FROM campaign
            WHERE updated_at > %s OR created_at > %s
        """
        return self.db.execute_query(query, (since, since))
    
    def extract_impressions(self, since: datetime) -> List[Tuple]:
        """Extract impressions created since the given timestamp."""
        query = """
            SELECT id, campaign_id, created_at
            FROM impressions
            WHERE created_at > %s
        """
        return self.db.execute_query(query, (since,))
    
    def extract_clicks(self, since: datetime) -> List[Tuple]:
        """Extract clicks created since the given timestamp."""
        query = """
            SELECT id, campaign_id, created_at
            FROM clicks
            WHERE created_at > %s
        """
        return self.db.execute_query(query, (since,))


class DataTransformer:
    """Transforms data for loading into ClickHouse."""
    
    @staticmethod
    def transform_advertisers(rows: List[Tuple]) -> List[Tuple]:
        """Transform advertiser data for ClickHouse."""
        transformed = []
        for adv_id, name, updated_at, created_at in rows:
            transformed.append((
                adv_id,
                name,
                updated_at or datetime.now(),
                created_at or datetime.now()
            ))
        return transformed
    
    @staticmethod
    def transform_campaigns(rows: List[Tuple]) -> List[Tuple]:
        """Transform campaign data for ClickHouse."""
        transformed = []
        for row in rows:
            campaign_id, name, bid, budget, start_date, end_date, advertiser_id, updated_at, created_at = row
            transformed.append((
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
        return transformed
    
    @staticmethod
    def transform_impressions(rows: List[Tuple]) -> List[Tuple]:
        """Transform impression data for ClickHouse."""
        transformed = []
        for imp_id, campaign_id, created_at in rows:
            event_time = created_at or datetime.now()
            event_date = event_time.date()
            
            transformed.append((
                imp_id,
                campaign_id,
                event_date,
                event_time,
                created_at or datetime.now()
            ))
        return transformed
    
    @staticmethod
    def transform_clicks(rows: List[Tuple]) -> List[Tuple]:
        """Transform click data for ClickHouse."""
        transformed = []
        for click_id, campaign_id, created_at in rows:
            event_time = created_at or datetime.now()
            event_date = event_time.date()
            
            transformed.append((
                click_id,
                campaign_id,
                event_date,
                event_time,
                created_at or datetime.now()
            ))
        return transformed


class DataLoader:
    """Loads transformed data into ClickHouse."""
    
    def __init__(self, db: ClickhouseConnector):
        """Initialize with ClickHouse connector."""
        self.db = db
    
    def load_advertisers(self, data: List[Tuple]) -> int:
        """Load advertiser data into ClickHouse."""
        if not data:
            return 0
            
        query = """
            INSERT INTO analytics.dim_advertiser
            (advertiser_id, name, updated_at, created_at)
            VALUES
        """
        self.db.execute_query(query, data)
        return len(data)
    
    def load_campaigns(self, data: List[Tuple]) -> int:
        """Load campaign data into ClickHouse."""
        if not data:
            return 0
            
        query = """
            INSERT INTO analytics.dim_campaign
            (campaign_id, name, bid, budget, start_date, end_date, advertiser_id, updated_at, created_at)
            VALUES
        """
        self.db.execute_query(query, data)
        return len(data)
    
    def load_impressions(self, data: List[Tuple]) -> int:
        """Load impression data into ClickHouse."""
        if not data:
            return 0
            
        query = """
            INSERT INTO analytics.fact_impressions
            (impression_id, campaign_id, event_date, event_time, created_at)
            VALUES
        """
        self.db.execute_query(query, data)
        return len(data)
    
    def load_clicks(self, data: List[Tuple]) -> int:
        """Load click data into ClickHouse."""
        if not data:
            return 0
            
        query = """
            INSERT INTO analytics.fact_clicks
            (click_id, campaign_id, event_date, event_time, created_at)
            VALUES
        """
        self.db.execute_query(query, data)
        return len(data)


class ETLPipeline:
    """Main ETL pipeline that orchestrates extract, transform, and load."""
    
    def __init__(
        self, 
        extractor: DataExtractor, 
        transformer: DataTransformer, 
        loader: DataLoader
    ):
        """Initialize with extractor, transformer, and loader components."""
        self.extractor = extractor
        self.transformer = transformer
        self.loader = loader
        self.last_sync = {
            'advertiser': datetime.min,
            'campaign': datetime.min,
            'impressions': datetime.min,
            'clicks': datetime.min
        }
    
    def sync_advertisers(self) -> int:
        """Sync advertisers from PostgreSQL to ClickHouse."""
        try:
            # Extract
            rows = self.extractor.extract_advertisers(self.last_sync['advertiser'])
            if not rows:
                logger.info("No new advertisers to sync")
                return 0
            
            # Transform
            data = self.transformer.transform_advertisers(rows)
            
            # Update last sync timestamp
            latest_update = self.last_sync['advertiser']
            for _, _, updated_at, created_at in rows:
                if updated_at and updated_at > latest_update:
                    latest_update = updated_at
                if created_at and created_at > latest_update:
                    latest_update = created_at
            
            # Load
            count = self.loader.load_advertisers(data)
            
            self.last_sync['advertiser'] = latest_update
            logger.info(f"Synced {count} advertisers")
            return count
            
        except Exception as e:
            logger.error(f"Error syncing advertisers: {e}")
            return 0
    
    def sync_campaigns(self) -> int:
        """Sync campaigns from PostgreSQL to ClickHouse."""
        try:
            # Extract
            rows = self.extractor.extract_campaigns(self.last_sync['campaign'])
            if not rows:
                logger.info("No new campaigns to sync")
                return 0
            
            # Transform
            data = self.transformer.transform_campaigns(rows)
            
            # Update last sync timestamp
            latest_update = self.last_sync['campaign']
            for *_, updated_at, created_at in rows:
                if updated_at and updated_at > latest_update:
                    latest_update = updated_at
                if created_at and created_at > latest_update:
                    latest_update = created_at
            
            # Load
            count = self.loader.load_campaigns(data)
            
            self.last_sync['campaign'] = latest_update
            logger.info(f"Synced {count} campaigns")
            return count
            
        except Exception as e:
            logger.error(f"Error syncing campaigns: {e}")
            return 0
    
    def sync_impressions(self) -> int:
        """Sync impressions from PostgreSQL to ClickHouse."""
        try:
            # Extract
            rows = self.extractor.extract_impressions(self.last_sync['impressions'])
            if not rows:
                logger.info("No new impressions to sync")
                return 0
            
            # Transform
            data = self.transformer.transform_impressions(rows)
            
            # Update last sync timestamp
            latest_update = self.last_sync['impressions']
            for _, _, created_at in rows:
                if created_at and created_at > latest_update:
                    latest_update = created_at
            
            # Load
            count = self.loader.load_impressions(data)
            
            self.last_sync['impressions'] = latest_update
            logger.info(f"Synced {count} impressions")
            return count
            
        except Exception as e:
            logger.error(f"Error syncing impressions: {e}")
            return 0
    
    def sync_clicks(self) -> int:
        """Sync clicks from PostgreSQL to ClickHouse."""
        try:
            # Extract
            rows = self.extractor.extract_clicks(self.last_sync['clicks'])
            if not rows:
                logger.info("No new clicks to sync")
                return 0
            
            # Transform
            data = self.transformer.transform_clicks(rows)
            
            # Update last sync timestamp
            latest_update = self.last_sync['clicks']
            for _, _, created_at in rows:
                if created_at and created_at > latest_update:
                    latest_update = created_at
            
            # Load
            count = self.loader.load_clicks(data)
            
            self.last_sync['clicks'] = latest_update
            logger.info(f"Synced {count} clicks")
            return count
            
        except Exception as e:
            logger.error(f"Error syncing clicks: {e}")
            return 0
    
    def run_sync_cycle(self) -> bool:
        """Run a complete ETL cycle."""
        try:
            logger.info("Starting ETL sync cycle")
            
            # Sync dimension tables first
            self.sync_advertisers()
            self.sync_campaigns()
            
            # Then sync fact tables
            self.sync_impressions() 
            self.sync_clicks()
            
            logger.info("ETL sync cycle completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"ETL sync cycle failed: {e}")
            return False
