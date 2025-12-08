# scripts/seed_data.py
import os
import pandas as pd
import numpy as np
from faker import Faker
from sqlalchemy import create_engine
from minio import Minio
from datetime import datetime, timedelta
import random
import uuid

fake = Faker()

# ====================== CONFIG ======================
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/recommender")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "content"

engine = create_engine(POSTGRES_URI)
minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

# Ensure bucket exists
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

N_USERS = 10_000
N_MOVIES = 5_000
N_TVSHOWS = 1_000
N_EPISODES_PER_SHOW = 50

# ====================== SEED USERS & PROFILES ======================
def seed_users():
    users = []
    profiles = []
    for _ in range(N_USERS):
        user_id = str(uuid.uuid4())
        users.append({
            "user_id": user_id,
            "name": fake.name(),
            "email": fake.email(),
            "password_hash": fake.sha256(),  # fake password
            "country": fake.country_code(),
            "subscription_plan": random.choice(["basic", "standard", "premium"])
        })
        # Each user has 1-3 profiles
        n_profiles = random.randint(1, 3)
        for i in range(n_profiles):
            profiles.append({
                "profile_name": f"Profile {i+1}" if n_profiles > 1 else "Main",
                "type": random.choice(["adult", "kids"]),
                "user_id": user_id
            })
    pd.DataFrame(users).to_sql("users", engine, if_exists="replace", index=False)
    pd.DataFrame(profiles).to_sql("profiles", engine, if_exists="replace", index=False)
    print(f"Seeded {len(users)} users and {len(profiles)} profiles")

# ====================== SEED CONTENT ======================
def seed_content():
    genres = ["Action", "Comedy", "Drama", "Horror", "Sci-Fi", "Romance", "Thriller", "Documentary", "Animation"]
    maturity_ratings = ["G", "PG", "PG-13", "R", "TV-Y", "TV-14", "TV-MA"]

    # Movies
    movies = []
    for _ in range(N_MOVIES):
        movie_id = str(uuid.uuid4())
        movies.append({
            "movie_id": movie_id,
            "title": fake.catch_phrase() + " " + fake.word().capitalize(),
            "description": fake.paragraph(nb_sentences=3),
            "genre": random.choice(genres),
            "maturity_rating": random.choice(maturity_ratings),
            "cast": ", ".join([fake.name() for _ in range(random.randint(3,8))]),
            "creator": fake.name(),
            "released_year": random.randint(1950, 2025),
            "duration_min": random.randint(60, 180),
            "rating": round(random.uniform(1.0, 10.0), 1)
        })
    # TV Shows
    tvshows = []
    episodes = []
    for _ in range(N_TVSHOWS):
        show_id = str(uuid.uuid4())
        tvshows.append({
            "tvshow_id": show_id,
            "title": fake.company() + " " + random.choice(["Chronicles", "Saga", "Files", "Stories", "Adventures"]),
            "description": fake.paragraph(nb_sentences=5),
            "genre": random.choice(genres),
            "maturity_rating": random.choice(maturity_ratings[:-3]),  # Less extreme for shows
            "cast": ", ".join([fake.name() for _ in range(random.randint(5,12))]),
            "creator": fake.name(),
            "number_of_seasons": random.randint(1, 12),
            "rating": round(random.uniform(4.0, 9.8), 1)
        })
        # Episodes per show
        for season in range(1, tvshows[-1]["number_of_seasons"] + 1):
            n_eps = random.randint(6, 24)
            for ep in range(1, n_eps + 1):
                episodes.append({
                    "tvshow_episode_id": str(uuid.uuid4()),
                    "tvshow_id": show_id,
                    "title": f"S{season:02d}E{ep:02d} - {fake.sentence(nb_words=4)}",
                    "description": fake.paragraph(),
                    "season_number": season,
                    "episode_number": ep,
                    "duration_min": random.randint(20, 70)
                })

    # Save to Postgres
    pd.DataFrame(movies).to_sql("movie", engine, if_exists="replace", index=False)
    pd.DataFrame(tvshows).to_sql("tvshow", engine, if_exists="replace", index=False)
    pd.DataFrame(episodes).to_sql("tvshow_episode", engine, if_exists="replace", index=False)

    # Save raw CSVs to MinIO (data lake)
    for name, df in [("movies.csv", pd.DataFrame(movies)),
                     ("tvshows.csv", pd.DataFrame(tvshows)),
                     ("episodes.csv", pd.DataFrame(episodes))]:
        csv_bytes = df.to_csv(index=False).encode()
        minio_client.put_object(BUCKET_NAME, f"raw/content/{name}", 
                                data=io.BytesIO(csv_bytes), length=len(csv_bytes), content_type="text/csv")
    print(f"Seeded {len(movies)} movies, {len(tvshows)} shows, {len(episodes)} episodes")

# ====================== MAIN ======================
if __name__ == "__main__":
    import io
    print("🌱 Starting data seeding...")
    seed_users()
    seed_content()
    print("✅ Initial seeding complete!")
    print("Next: Run synthetic event producers -> producers/user_event_producer/producer.py")