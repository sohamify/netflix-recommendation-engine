# scripts/seed_data.py
import os
import pandas as pd
import numpy as np
import json
import uuid
from faker import Faker
from sqlalchemy import create_engine
from minio import Minio
from datetime import datetime
import random
import io

fake = Faker()

# ====================== CONFIG ======================
POSTGRES_URI = os.getenv("POSTGRES_URI", "postgresql://postgres:postgres@localhost:5432/recommender")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
BUCKET_NAME = "data-lake"
BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
DATASET_DIR = os.path.join(BASE_DIR, "realtime-recommender", "datasets")


engine = create_engine(POSTGRES_URI)
minio_client = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

# Ensure bucket
if not minio_client.bucket_exists(BUCKET_NAME):
    minio_client.make_bucket(BUCKET_NAME)

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
        # 1-3 profiles per user
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
    # Load raw CSVs (assume in data/raw/)
    #movies_df = pd.read_csv("data/raw/tmdb_5000_movies.csv")
    #credits_df = pd.read_csv("data/raw/tmdb_5000_credits.csv")
    movies_df = pd.read_csv(movies_path)
    credits_df = pd.read_csv(credits_path)
    # Merge on id/movie_id
    merged = pd.merge(movies_df, credits_df, left_on='id', right_on='movie_id', how='inner')
    merged.rename(columns={"title_x": "title"}, inplace=True)

    
    # Parse JSON cols
    def parse_genres(genres_str):
        if pd.isna(genres_str):
            return ""
        try:
            genres_list = json.loads(genres_str)
            return "|".join([g['name'] for g in genres_list])  # Pipe-separated
        except:
            return ""
    
    def parse_cast(cast_str):
        if pd.isna(cast_str):
            return ""
        try:
            cast_list = json.loads(cast_str)
            return "|".join([c['name'] for c in cast_list[:5]])  # Top 5 actors
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
    
    # Map to schema
    movies = []
    for _, row in merged.iterrows():
        movie_id = str(uuid.uuid4())  # Our PK
        released_year = pd.to_datetime(row['release_date']).year if pd.notna(row['release_date']) else np.nan
        movies.append({
            "movie_id": movie_id,
            "source_id": row['id'],  # Original TMDB ID
            "title": row['title'],
            "description": row['overview'] or "",
            "genre": row['genres'],
            "maturity_rating": "",  # No direct; infer later if needed
            "cast": row['cast'],
            "creator": row['creator'],
            "released_year": released_year,
            "duration_min": row['runtime'],
            "rating": round(row['vote_average'], 1) if pd.notna(row['vote_average']) else None
        })
    
    pd.DataFrame(movies).to_sql("movie", engine, if_exists="replace", index=False)
    
    # Upload raw to MinIO
    
    # ---- MinIO: upload TMDB movie datasets ----
    movie_files = [
        movies_path,
        credits_path,
    ]

    for path in movie_files:
        with open(path, "rb") as f:
            minio_client.put_object(
                BUCKET_NAME,
                f"raw/content/movies/{os.path.basename(path)}",
                data=f,
                length=os.path.getsize(path),
                content_type="text/csv"
            )

    
    print(f"Seeded {len(movies)} movies from TMDB")

# ====================== SEED TV SHOWS (IMDB) ======================
# def seed_tvshows():
#     #tv_df = pd.read_csv("data/raw/imdb-tv-shows.csv")  # Assume cols: title,year,rating,votes,genre,actors,certificate,language,country,writer,description,runtime
#     tv_df = pd.read_csv(tv_path)
#     # Clean/map
#     YEAR_COL = next(
#         (c for c in tv_df.columns if c.lower() in {"year", "release_year", "start_year"}), 
#         None
#     )

#     if YEAR_COL:
#         tv_df['year'] = pd.to_numeric(tv_df[YEAR_COL], errors='coerce')
#     else:
#         tv_df['year'] = None

#         tv_df['runtime'] = tv_df['runtime'].str.extract(r'(\d+)').astype(float)  # Extract mins from "XX min"
#         tv_df['genre'] = tv_df['genre'].str.split(', ')  # List for multi
#         tv_df['actors'] = tv_df['actors'].str.split(', ')  # Top few
    
#     tvshows = []
#     for _, row in tv_df.iterrows():
#         tvshow_id = str(uuid.uuid4())
#         genres = "|".join(row['genre']) if isinstance(row['genre'], list) else row['genre']
#         cast = "|".join(row['actors'][:5]) if isinstance(row['actors'], list) else row['actors']
#         tvshows.append({
#             "tvshow_id": tvshow_id,
#             "source_id": row.get('title', '') + str(row['year']),  # Composite key
#             "title": row['title'],
#             "description": row['description'] or "",
#             "genre": genres,
#             "maturity_rating": row['certificate'] or "",
#             "cast": cast,
#             "creator": row['writer'] or "",
#             "number_of_seasons": random.randint(1, 10),  # Synthetic; datasets lack it
#             "rating": round(row['rating'], 1) if pd.notna(row['rating']) else None
#         })
    
#     pd.DataFrame(tvshows).to_sql("tvshow", engine, if_exists="replace", index=False)

def seed_tvshows():
    tv_df = pd.read_csv(tv_path)

    # Normalize columns
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

    # Type casting
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

    # Upload raw to MinIO
    with open(tv_path, "rb") as f:
        minio_client.put_object(
            BUCKET_NAME,
            "raw/content/tv/imdb_tvshows.csv",
            data=f,
            length=os.path.getsize(tv_path),
            content_type="text/csv"
        )

    print(f"Seeded {len(tvshows)} TV shows from IMDB")

    # Upload raw
    # ---- MinIO: upload IMDB TV dataset ----
    with open(tv_path, "rb") as f:
        minio_client.put_object(
            BUCKET_NAME,
            "raw/content/tv/imdb_tv-shows.csv",
            data=f,
            length=os.path.getsize(tv_path),
            content_type="text/csv"
        )

    
    print(f"Seeded {len(tvshows)} TV shows from IMDB")

# ====================== MAIN ======================
if __name__ == "__main__":
    print("🌱 Starting real-dataset seeding...")
    seed_users()
    seed_movies()
    seed_tvshows()
    print("✅ Seeding complete! Raw CSVs in MinIO bucket 'data-lake/raw/content/'")
    print("Next: Update producer.py to use real content_ids → docker build & run")