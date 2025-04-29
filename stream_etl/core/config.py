import os
from dataclasses import dataclass


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
class KafkaConfig:
    """Kafka configuration."""
    bootstrap_servers: str = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'kafka:9092')
    group_id: str = os.environ.get('KAFKA_GROUP_ID', 'adtech-etl-group')
    auto_offset_reset: str = os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest')
    
    # Topic configurations
    advertiser_topic: str = os.environ.get('KAFKA_TOPICS_ADVERTISER', 'postgres.public.advertiser')
    campaign_topic: str = os.environ.get('KAFKA_TOPICS_CAMPAIGN', 'postgres.public.campaign')
    impressions_topic: str = os.environ.get('KAFKA_TOPICS_IMPRESSIONS', 'postgres.public.impressions')
    clicks_topic: str = os.environ.get('KAFKA_TOPICS_CLICKS', 'postgres.public.clicks')


class AppConfig:
    """Application configuration container."""
    
    def __init__(self):
        self.postgres = PostgresConfig()
        self.clickhouse = ClickhouseConfig()
        self.kafka = KafkaConfig()
