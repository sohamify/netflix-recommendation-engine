from confluent_kafka import Producer, KafkaException
import json
import time
import random
import uuid
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine
from faker import Faker
import logging

# ====================== LOGGING SETUP ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ====================== CONFIG & CONNECTIONS ======================
# HARDCODED for Docker - no environment variables
POSTGRES_URI = "postgresql://postgres:postgres@postgres:5432/recommender"
KAFKA_BROKERS = "kafka:9092"

logger.info(f"Connecting to Postgres: {POSTGRES_URI}")
logger.info(f"Connecting to Kafka brokers: {KAFKA_BROKERS}")

# ====================== KAFKA PRODUCER SETUP ======================
def create_producer():
    config = {
        'bootstrap.servers': KAFKA_BROKERS,
        'client.id': 'user-event-producer',
        'acks': 'all',
        'retries': 10,
        'retry.backoff.ms': 1000,
        'linger.ms': 5,
        'batch.num.messages': 1000,
        'queue.buffering.max.kbytes': 1024,
        'compression.type': 'snappy',
        'enable.idempotence': True,
        'message.timeout.ms': 30000,
        'socket.keepalive.enable': True,
    }
    
    logger.info(f"Creating Kafka producer with config: {config}")
    producer = Producer(config)
    
    # Test connection
    try:
        metadata = producer.list_topics(timeout=10)
        logger.info(f"✅ Successfully connected to Kafka")
        logger.info(f"Available topics: {list(metadata.topics.keys())}")
    except KafkaException as e:
        logger.error(f"❌ Failed to connect to Kafka: {e}")
        raise
    
    return producer

# Create producer instance
p = create_producer()

# ====================== DATABASE CONNECTION ======================
def load_data():
    """Load data from Postgres with retries"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Connecting to Postgres (attempt {attempt + 1}/{max_retries})...")
            engine = create_engine(POSTGRES_URI, pool_pre_ping=True)
            
            logger.info("Loading data from Postgres...")
            users_df = pd.read_sql("SELECT user_id FROM users LIMIT 100", engine)
            profiles_df = pd.read_sql("SELECT profile_id, user_id, type FROM profiles", engine)
            movies_df = pd.read_sql("SELECT movie_id FROM movie LIMIT 100", engine)
            tvshows_df = pd.read_sql("SELECT tvshow_id FROM tvshow LIMIT 100", engine)
            
            logger.info(f"✅ Loaded {len(users_df)} users")
            logger.info(f"✅ Loaded {len(profiles_df)} profiles")
            logger.info(f"✅ Loaded {len(movies_df)} movies")
            logger.info(f"✅ Loaded {len(tvshows_df)} tv shows")
            
            # Combine all content IDs
            all_content_ids = pd.concat([
                movies_df['movie_id'],
                tvshows_df['tvshow_id']
            ]).tolist()
            
            logger.info(f"✅ Total content items available: {len(all_content_ids)}")
            
            return users_df, profiles_df, all_content_ids, engine
            
        except Exception as e:
            logger.warning(f"Failed to load data from Postgres: {e}")
            if attempt < max_retries - 1:
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
                retry_delay *= 2
            else:
                logger.error("Max retries reached. Exiting.")
                raise

# Load data
users_df, profiles_df, all_content_ids, engine = load_data()

# ====================== EVENT GENERATION ======================
TOPICS = {
    "watch": "engagement_events",
    "rate": "engagement_events",
    "add_to_list": "user_events",
    "search": "user_events",
    "click": "engagement_events"
}

event_counter = 0
success_count = 0
failed_count = 0

def delivery_report(err, msg):
    """Delivery callback for Kafka messages"""
    global event_counter, success_count, failed_count
    event_counter += 1
    
    if err is not None:
        failed_count += 1
        logger.error(f"❌ Delivery failed for event #{event_counter}: {err}")
    else:
        success_count += 1
        if success_count % 100 == 0:
            logger.info(f"📊 Successfully sent {success_count} events (total: {event_counter}, failed: {failed_count})")

def generate_event(fake_instance):
    """Generate a single synthetic event"""
    if not all_content_ids:
        logger.error("No content IDs available!")
        return
    
    profile = profiles_df.sample(1).iloc[0]
    content_id = random.choice(all_content_ids)
    
    event_type = random.choices(
        ["watch", "rate", "add_to_list", "click", "search"],
        weights=[50, 15, 10, 20, 5], k=1
    )[0]
    
    base = {
        "event_id": str(uuid.uuid4()),
        "profile_id": str(profile["profile_id"]),
        "user_id": str(profile["user_id"]),
        "content_id": str(content_id),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "session_id": str(uuid.uuid4()),
        "event_type": event_type
    }
    
    # Add event-specific fields
    if event_type == "watch":
        base.update({
            "progress_percent": round(random.uniform(0.05, 1.0), 3),
            "duration_watched_sec": random.randint(60, 3600),
            "device": random.choice(["web", "mobile", "smart_tv", "tablet"])
        })
    elif event_type == "rate":
        base.update({
            "rating": round(random.uniform(1.0, 10.0), 1),
            "rating_type": random.choice(["implicit", "explicit"])
        })
    elif event_type == "add_to_list":
        base.update({
            "list_type": random.choice(["watchlist", "favorites", "custom"])
        })
    elif event_type == "click":
        base.update({
            "click_type": random.choice(["thumbnail", "title", "genre", "actor"]),
            "page": random.choice(["home", "search", "recommendations", "details"])
        })
    elif event_type == "search":
        base.update({
            "query": " ".join(fake_instance.words(nb=random.randint(1, 4))),
            "search_type": random.choice(["title", "genre", "actor", "director"])
        })
    
    topic = TOPICS[event_type]
    
    try:
        p.produce(
            topic=topic,
            value=json.dumps(base).encode('utf-8'),
            key=str(profile["user_id"]).encode('utf-8'),
            callback=delivery_report,
            timestamp=int(time.time() * 1000)
        )
    except BufferError as e:
        logger.warning(f"Buffer full, waiting: {e}")
        p.poll(1)
        time.sleep(0.1)
        p.produce(
            topic=topic,
            value=json.dumps(base).encode('utf-8'),
            key=str(profile["user_id"]).encode('utf-8'),
            callback=delivery_report
        )
    except Exception as e:
        logger.error(f"Failed to produce message: {e}")
    
    p.poll(0)

if __name__ == "__main__":
    fake = Faker()
    logger.info("🚀 Synthetic User Event Producer Started...")
    logger.info("Press Ctrl+C to stop")
    
    try:
        while True:
            generate_event(fake)
            
            if event_counter % 1000 == 0:
                p.flush(1)
                
            time.sleep(random.expovariate(1.0))
            
    except KeyboardInterrupt:
        logger.info("\n🛑 Stopping producer...")
        
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        
    finally:
        logger.info("Flushing remaining messages...")
        remaining = p.flush(30)
        
        if remaining > 0:
            logger.warning(f"{remaining} messages were not delivered")
        else:
            logger.info("✅ All messages delivered successfully")
        
        logger.info(f"📊 Final Stats: {success_count} successful, {failed_count} failed out of {event_counter} total")
        logger.info("👋 Producer stopped cleanly")