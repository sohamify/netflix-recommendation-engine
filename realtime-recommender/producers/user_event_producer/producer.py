# producers/user_event_producer/producer.py
from confluent_kafka import Producer
import json
import time
import random
import uuid
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import os

# Load static data
engine = create_engine(os.getenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/recommender"))
users_df = pd.read_sql("SELECT user_id FROM users", engine)
profiles_df = pd.read_sql("SELECT profile_id, user_id, type FROM profiles", engine)
movies_df = pd.read_sql("SELECT movie_id FROM movie", engine)
shows_df = pd.read_sql("SELECT tvshow_id FROM tvshow", engine)
episodes_df = pd.read_sql("SELECT tvshow_episode_id, tvshow_id FROM tvshow_episode", engine)

all_content_ids = (
    movies_df["movie_id"].tolist() +
    shows_df["tvshow_id"].tolist() +
    episodes_df["tvshow_episode_id"].tolist()
)

p = Producer({'bootstrap.servers': os.getenv('KAFKA_BROKERS', 'localhost:9092')})

TOPICS = {
    "user.watch": "engagement_events",
    "user.rate": "engagement_events",
    "user.add_to_list": "user_events",
    "user.search": "user_events",
    "user.click": "engagement_events"
}

def delivery_report(err, msg):
    if err:
        print(f"Delivery failed: {err}")
    else:
        pass  # print(f"Sent to {msg.topic()}")

def generate_event():
    profile = profiles_df.sample(1).iloc[0]
    content_id = random.choice(all_content_ids)

    event_type = random.choices(
        ["watch", "rate", "add_to_list", "click", "search"],
        weights=[50, 15, 10, 20, 5], k=1)[0]

    base = {
        "event_id": str(uuid.uuid4()),
        "profile_id": profile["profile_id"],
        "user_id": profile["user_id"],
        "content_id": content_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "session_id": str(uuid.uuid4())
    }

    if event_type == "watch":
        base.update({
            "event_type": "watch",
            "progress_percent": random.uniform(0.05, 1.0),
            "duration_watched_sec": random.randint(60, 3600)
        })
    elif event_type == "rate":
        base.update({
            "event_type": "rate",
            "rating": round(random.uniform(1, 10), 1)
        })
    elif event_type == "add_to_list":
        base["event_type"] = "add_to_list"
    elif event_type == "click":
        base["event_type"] = "click"
    elif event_type == "search":
        base.update({
            "event_type": "search",
            "query": " ".join(fake.words(nb=random.randint(1,4)))
        })

    topic = TOPICS[[k for k in TOPICS if k.split(".")[1] == event_type][0]]
    p.produce(topic, json.dumps(base), callback=delivery_report)
    p.poll(0)

if __name__ == "__main__":
    from faker import Faker
    fake = Faker()
    print("🚀 Synthetic User Event Producer Started...")
    while True:
        generate_event()
        time.sleep(random.expovariate(1.0))  # Avg ~1 event/sec, bursty