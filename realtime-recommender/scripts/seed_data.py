# scripts/seed_data.py
import os
import pandas as pd
import numpy as np
import json
import uuid
from faker import Faker
from sqlalchemy import create_engine
import boto3
from botocore.client import Config
from datetime import datetime
import random

fake = Faker()

# ====================== CONFIG ======================
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:soham@localhost:5432/recommender")

# Garage (S3-compatible) configuration
GARAGE_ENDPOINT = os.getenv("GARAGE_ENDPOINT", "http://garage:3900")
GARAGE_ACCESS_KEY = "GK48bac80794ed37b1a39e86b7"
GARAGE_SECRET_KEY = "d692bda02d3183010c58070afccd415325f3608c2ec3c087fde645767984d698"
GARAGE_BUCKET = os.getenv("GARAGE_BUCKET", "data-lake")

print("Using POSTGRES_URI:", POSTGRES_URI)
print("Using GARAGE_ENDPOINT:", GARAGE_ENDPOINT)
print("Using GARAGE_BUCKET:", GARAGE_BUCKET)
print("Using GARAGE_ACCESS_KEY:", GARAGE_ACCESS_KEY)
print("Using GARAGE_SECRET_KEY:", GARAGE_SECRET_KEY)

# Path to datasets (two levels up from scripts/)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATASET_DIR = "/tmp/datasets"

print(f"Looking for datasets in: {DATASET_DIR}")

# Postgres connection
engine = create_engine(POSTGRES_URI)

# Garage / S3 client
s3 = boto3.client(
    's3',
    endpoint_url=GARAGE_ENDPOINT,
    aws_access_key_id=GARAGE_ACCESS_KEY,
    aws_secret_access_key=GARAGE_SECRET_KEY,
    region_name='garage',
    config=Config(signature_version='s3v4')
)

# Verify connection to Garage
try:
    response = s3.list_buckets()
    print("Garage connection successful. Found buckets:", [b['Name'] for b in response.get('Buckets', [])])
except Exception as e:
    print(f"❌ Garage connection failed: {e}")
    print("   Check: GARAGE_ENDPOINT, GARAGE_ACCESS_KEY, GARAGE_SECRET_KEY in .env")
    print("   Make sure Garage container is running and bucket 'data-lake' exists")
    exit(1)

N_USERS = 10_000

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
            "password_hash": fake.sha256(),  # Placeholder
            "country": fake.country_code(),
            "subscription_plan": random.choice(["basic", "standard", "premium"]),
        })
        n_profiles = random.randint(1, 3)
        for i in range(n_profiles):
            profile_id = str(uuid.uuid4())
            profiles.append({
                "profile_id": profile_id,
                "profile_name": f"Profile {i+1}" if n_profiles > 1 else "Main",
                "type": random.choice(["adult", "kids"]),
                "user_id": user_id
            })
    pd.DataFrame(users).to_sql("users", engine, if_exists="replace", index=False)
    pd.DataFrame(profiles).to_sql("profiles", engine, if_exists="replace", index=False)
    print(f"Seeded {len(users)} users and {len(profiles)} profiles")

# ====================== SEED MOVIES (TMDB) ======================
movies_path = os.path.join(DATASET_DIR, "tmdb_5000_movies.csv")
credits_path = os.path.join(DATASET_DIR, "tmdb_5000_credits.csv")
tv_path = os.path.join(DATASET_DIR, "imdb_tvshows.csv")

def seed_movies():
    if not os.path.exists(movies_path) or not os.path.exists(credits_path):
        print(f"Missing files: {movies_path} or {credits_path}")
        exit(1)

    movies_df = pd.read_csv(movies_path)
    credits_df = pd.read_csv(credits_path)

    merged = pd.merge(movies_df, credits_df, left_on='id', right_on='movie_id', how='inner')
    merged.rename(columns={"title_x": "title"}, inplace=True)

    def parse_genres(genres_str):
        if pd.isna(genres_str):
            return ""
        try:
            genres_list = json.loads(genres_str)
            return "|".join([g['name'] for g in genres_list])
        except:
            return ""

    def parse_cast(cast_str):
        if pd.isna(cast_str):
            return ""
        try:
            cast_list = json.loads(cast_str)
            return "|".join([c['name'] for c in cast_list[:5]])
        except:
            return ""

    def parse_creator(crew_str):
        if pd.isna(crew_str):
            return ""
        try:
            crew_list = json.loads(crew_str)
            director = next((c['name'] for c in crew_list if c['job'] == 'Director'), None)
            return director or ""
        except:
            return ""

    merged['genres'] = merged['genres'].apply(parse_genres)
    merged['cast'] = merged['cast'].apply(parse_cast)
    merged['creator'] = merged['crew'].apply(parse_creator)

    movies = []
    for _, row in merged.iterrows():
        movie_id = str(uuid.uuid4())
        released_year = pd.to_datetime(row['release_date']).year if pd.notna(row['release_date']) else np.nan
        movies.append({
            "movie_id": movie_id,
            "source_id": row['id'],
            "title": row['title'],
            "description": row['overview'] or "",
            "genre": row['genres'],
            "maturity_rating": "",
            "cast": row['cast'],
            "creator": row['creator'],
            "released_year": released_year,
            "duration_min": row['runtime'],
            "rating": round(row['vote_average'], 1) if pd.notna(row['vote_average']) else None
        })

    pd.DataFrame(movies).to_sql("movie", engine, if_exists="replace", index=False)

    # Upload raw files to Garage
    for path in [movies_path, credits_path]:
        key = f"raw/content/movies/{os.path.basename(path)}"
        with open(path, "rb") as f:
            s3.upload_fileobj(f, GARAGE_BUCKET, key)
        print(f"Uploaded {os.path.basename(path)} to Garage → {key}")

    print(f"Seeded {len(movies)} movies from TMDB")

# ====================== SEED TV SHOWS (IMDB) ======================
def seed_tvshows():
    if not os.path.exists(tv_path):
        print(f"Missing file: {tv_path}")
        exit(1)

    tv_df = pd.read_csv(tv_path)

    tv_df.rename(columns={
        "Title": "title",
        "About": "description",
        "EpisodeDuration(in Minutes)": "runtime",
        "Genres": "genre",
        "Actors": "actors",
        "Rating": "rating",
        "Votes": "votes",
        "Years": "year"
    }, inplace=True)

    tv_df['year'] = tv_df['year'].astype(str).str.extract(r'(\d{4})').astype(float)
    tv_df['runtime'] = pd.to_numeric(tv_df['runtime'], errors='coerce')
    tv_df['rating'] = pd.to_numeric(tv_df['rating'], errors='coerce')

    tvshows = []
    for _, row in tv_df.iterrows():
        tvshow_id = str(uuid.uuid4())
        tvshows.append({
            "tvshow_id": tvshow_id,
            "source_id": f"{row['title']}_{row['year']}",
            "title": row['title'],
            "description": row['description'] or "",
            "genre": row['genre'].replace(", ", "|") if isinstance(row['genre'], str) else "",
            "maturity_rating": "",
            "cast": row['actors'].replace(", ", "|") if isinstance(row['actors'], str) else "",
            "creator": "",
            "number_of_seasons": random.randint(1, 10),
            "rating": round(row['rating'], 1) if pd.notna(row['rating']) else None
        })

    pd.DataFrame(tvshows).to_sql("tvshow", engine, if_exists="replace", index=False)

    # Upload to Garage (only once)
    key = f"raw/content/tv/{os.path.basename(tv_path)}"
    with open(tv_path, "rb") as f:
        s3.upload_fileobj(f, GARAGE_BUCKET, key)
    print(f"Uploaded {os.path.basename(tv_path)} to Garage → {key}")

    print(f"Seeded {len(tvshows)} TV shows from IMDB")

# ====================== MAIN ======================
if __name__ == "__main__":
    print("🌱 Starting real-dataset seeding...")
    seed_users()
    seed_movies()
    seed_tvshows()
    print("✅ Seeding complete! Raw CSVs uploaded to Garage bucket 'data-lake/raw/content/'")
    print("Next: Restart producer → docker compose restart app")