import psycopg
import os
import random
import time
import logging
import argparse
from datetime import date, timedelta, datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)
logger = logging.getLogger('periodic-seeder')

# Environment variables
POSTGRES_HOST = os.environ.get("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT", "6543")
POSTGRES_DB = os.environ.get("POSTGRES_DB", "postgres")
POSTGRES_USER = os.environ.get("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "postgres")
SEEDER_INTERVAL = int(os.environ.get("SEEDER_INTERVAL", "30"))  # Default 30 seconds


def get_connection():
    """Create a PostgreSQL connection."""
    return psycopg.connect(
        f"host={POSTGRES_HOST} port={POSTGRES_PORT} dbname={POSTGRES_DB} "
        f"user={POSTGRES_USER} password={POSTGRES_PASSWORD}",
        autocommit=False,
    )

def get_existing_advertisers(conn):
    """Get a list of existing advertiser IDs."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM advertiser")
        return [row[0] for row in cur.fetchall()]

def get_existing_advertisers_name(conn):
    """Get a list of existing advertiser names."""
    with conn.cursor() as cur:
        cur.execute("SELECT name FROM advertiser")
        return [row[0] for row in cur.fetchall()]


def get_existing_campaigns(conn):
    """Get a list of existing campaign IDs."""
    with conn.cursor() as cur:
        cur.execute("SELECT id FROM campaign")
        return [row[0] for row in cur.fetchall()]


def create_new_advertisers(conn, num_advertisers=2):
    """Create new advertisers only (no updates), ensuring name uniqueness."""
    adv_ids = []
    existing_names = get_existing_advertisers_name(conn)
    
    with conn.cursor() as cur:
        attempts = 0
        created = 0
        
        # Try to create the requested number of advertisers, but limit attempts
        # to avoid infinite loops if we're running out of unique names
        while created < num_advertisers and attempts < num_advertisers * 3:
            attempts += 1
            
            # Create new advertiser name
            adv_name = f"Advertiser {random.choice('ABCDEFGHIJKLMNOPQRSTUVWXYZ')}"
            
            # Check if name already exists
            if adv_name in existing_names:
                logger.info(f"Skipping advertiser name '{adv_name}' - already exists")
                continue
                
            # Insert new advertiser with unique name
            cur.execute(
                """
                INSERT INTO advertiser (name, updated_at)
                VALUES (%s, NOW()) RETURNING id
                """,
                (adv_name,),
            )
            adv_id = cur.fetchone()[0]
            existing_names.append(adv_name)  # Add to local cache to avoid duplicates
            logger.info(f"Created new advertiser: {adv_id} - {adv_name}")
            adv_ids.append(adv_id)
            created += 1


def create_or_update_campaigns(conn, advertiser_ids, num_operations_per_advertiser=1):
    """Create new or update existing campaigns for each advertiser."""
    campaign_ids = []
    start_date = date.today()
    
    with conn.cursor() as cur:
        for adv_id in advertiser_ids:
            existing_campaigns = get_existing_campaigns(conn)
            
            for _ in range(num_operations_per_advertiser):
                # 30% chance to create new, 70% to update existing
                if not existing_campaigns or random.random() < 0.3:
                    # Create new campaign
                    campaign_name = f"Campaign_{adv_id}_{random.randint(1, 99)}"
                    bid = round(random.uniform(0.5, 5.0), 2)
                    budget = round(random.uniform(50, 500), 2)
                    end_date = start_date + timedelta(days=random.randint(7, 90))
                    
                    cur.execute(
                        """
                        INSERT INTO campaign
                            (name, bid, budget, start_date, end_date, advertiser_id, updated_at)
                        VALUES (%s, %s, %s, %s, %s, %s, NOW()) RETURNING id
                        """,
                        (campaign_name, bid, budget, start_date, end_date, adv_id),
                    )
                    campaign_id = cur.fetchone()[0]
                    logger.info(f"Created new campaign: {campaign_id} - {campaign_name} (Advertiser: {adv_id})")
                elif existing_campaigns:
                    # Update existing campaign
                    campaign_id = random.choice(existing_campaigns)
                    bid = round(random.uniform(0.5, 5.0), 2)
                    budget = round(random.uniform(50, 500), 2)
                    
                    cur.execute(
                        """
                        UPDATE campaign
                        SET bid = %s, budget = %s, updated_at = NOW()
                        WHERE id = %s
                        RETURNING id
                        """,
                        (bid, budget, campaign_id),
                    )
                    logger.info(f"Updated campaign: {campaign_id} (Advertiser: {adv_id}, Bid: {bid}, Budget: {budget})")
                
                campaign_ids.append(campaign_id)
    
    return campaign_ids


def create_impressions(conn, campaign_ids, impressions_per_campaign=20):
    """Create impressions for campaigns."""
    impression_count = 0
    
    with conn.cursor() as cur:
        for campaign_id in campaign_ids:
            # Create impressions distributed over recent time
            for _ in range(random.randint(1, impressions_per_campaign)):
                timestamp = datetime.now() - timedelta(
                    minutes=random.randint(0, 60),
                    seconds=random.randint(0, 59),
                )
                cur.execute(
                    """
                    INSERT INTO impressions (campaign_id, created_at)
                    VALUES (%s, %s)
                    """,
                    (campaign_id, timestamp),
                )
                impression_count += 1
    
    logger.info(f"Created {impression_count} new impressions")
    return impression_count


def create_clicks(conn, campaign_ids, click_ratio=0.1):
    """Create clicks for campaigns based on recent impressions."""
    click_count = 0
    
    with conn.cursor() as cur:
        for campaign_id in campaign_ids:
            # Get recent impressions for this campaign (last hour)
            cur.execute(
                """
                SELECT id, created_at 
                FROM impressions 
                WHERE campaign_id = %s AND created_at > NOW() - INTERVAL '1 hour'
                """, 
                (campaign_id,)
            )
            impressions = cur.fetchall()
            
            if not impressions:
                continue
                
            # Create clicks for a percentage of impressions
            for imp_id, imp_time in random.sample(
                impressions, 
                k=min(int(len(impressions) * click_ratio) + 1, len(impressions))
            ):
                # Add a small delay after impression (1-60 seconds)
                click_time = imp_time + timedelta(seconds=random.randint(1, 60))
                cur.execute(
                    """
                    INSERT INTO clicks (campaign_id, created_at)
                    VALUES (%s, %s)
                    """,
                    (campaign_id, click_time),
                )
                click_count += 1
    
    logger.info(f"Created {click_count} new clicks")
    return click_count


def seed_data(conn):
    """Run one round of data seeding."""
    try:
        # Create advertisers (1-3 operations)
        adv_count = random.randint(1, 3)
        create_new_advertisers(conn, adv_count)
        adv_ids = get_existing_advertisers(conn)
        
        # Create or update campaigns (1-2 per advertiser)
        camp_per_adv = random.randint(1, 2)
        campaign_ids = create_or_update_campaigns(conn, adv_ids, camp_per_adv)
        
        # For all campaigns (including newly created), get IDs for impression/click generation
        all_campaign_ids = get_existing_campaigns(conn)
        sample_size = min(5, len(all_campaign_ids))  # Max 5 campaigns per round
        selected_campaigns = random.sample(all_campaign_ids, sample_size) if all_campaign_ids else []
        
        # Create impressions (10-50 per selected campaign)
        imp_per_camp = random.randint(10, 50)
        create_impressions(conn, selected_campaigns, imp_per_camp)
        
        # Create clicks (5-15% click ratio)
        click_ratio = random.uniform(0.05, 0.15)
        create_clicks(conn, selected_campaigns, click_ratio)
        
        # Commit the transaction
        conn.commit()
        logger.info(f"Seeding round completed successfully: {adv_count} advertiser operations, {len(campaign_ids)} campaign operations")
        
    except Exception as e:
        conn.rollback()
        logger.error(f"Error in seeding round: {e}")
        return False
    
    return True


def run_periodic_seeder(interval=None):
    """Run the seeder periodically."""
    interval = interval or SEEDER_INTERVAL
    logger.info(f"Starting periodic seeder with interval: {interval} seconds")
    
    try:
        while True:
            conn = get_connection()
            if not conn:
                logger.error("Could not connect to Postgres. Retrying in 10 seconds...")
                time.sleep(10)
                continue
                
            try:
                start_time = time.time()
                success = seed_data(conn)
                duration = time.time() - start_time
                
                if success:
                    logger.info(f"Seeding completed in {duration:.2f} seconds. Sleeping for {interval} seconds...")
                else:
                    logger.warning(f"Seeding failed. Sleeping for {interval} seconds before retry...")
                    
                conn.close()
                time.sleep(interval)
                
            except Exception as e:
                logger.error(f"Unexpected error in seeding cycle: {e}")
                if conn:
                    conn.close()
                time.sleep(interval)
                
    except KeyboardInterrupt:
        logger.info("Periodic seeder stopped by user")
        if conn:
            conn.close()


def show_stats(conn):
    """Display current database statistics."""
    with conn.cursor() as cur:
        print("=== Database Statistics ===")

        # Count advertisers
        cur.execute("SELECT COUNT(*) FROM advertiser")
        adv_count = cur.fetchone()[0]
        print(f"Advertisers: {adv_count}")

        # Count campaigns
        cur.execute("SELECT COUNT(*) FROM campaign")
        camp_count = cur.fetchone()[0]
        print(f"Campaigns: {camp_count}")

        # Count impressions
        cur.execute("SELECT COUNT(*) FROM impressions")
        imp_count = cur.fetchone()[0]
        print(f"Impressions: {imp_count}")

        # Count clicks
        cur.execute("SELECT COUNT(*) FROM clicks")
        click_count = cur.fetchone()[0]
        print(f"Clicks: {click_count}")

        # Overall CTR
        if imp_count > 0:
            ctr = (click_count / imp_count) * 100
            print(f"Overall CTR: {ctr:.2f}%")

        # Last hour activity
        print("\n=== Last Hour Activity ===")
        cur.execute("""
            SELECT COUNT(*) FROM impressions 
            WHERE created_at > NOW() - INTERVAL '1 hour'
        """)
        recent_imp = cur.fetchone()[0]
        
        cur.execute("""
            SELECT COUNT(*) FROM clicks 
            WHERE created_at > NOW() - INTERVAL '1 hour'
        """)
        recent_clicks = cur.fetchone()[0]
        
        recent_ctr = (recent_clicks / recent_imp * 100) if recent_imp > 0 else 0
        print(f"Recent Impressions: {recent_imp}")
        print(f"Recent Clicks: {recent_clicks}")
        print(f"Recent CTR: {recent_ctr:.2f}%")


def parse_args():
    """Parse command line arguments."""
    parser = argparse.ArgumentParser(description="Periodic Data Seeder for AdTech Platform")
    
    parser.add_argument(
        "--interval", 
        type=int, 
        help="Seeding interval in seconds (default: from env or 60)"
    )
    
    parser.add_argument(
        "--run-once",
        action="store_true",
        help="Run seeder once and exit"
    )
    
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show database statistics and exit"
    )
    
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    
    if args.stats:
        # Show stats and exit
        conn = get_connection()
        if conn:
            show_stats(conn)
            conn.close()
    elif args.run_once:
        # Run once and exit
        conn = get_connection()
        if conn:
            seed_data(conn)
            conn.close()
    else:
        # Run periodically
        run_periodic_seeder(args.interval)
