# scripts/generate_item_embeddings.py
import os
import pandas as pd
from sentence_transformers import SentenceTransformer
from pymilvus import MilvusClient, DataType
from sqlalchemy import create_engine
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

POSTGRES_URI = os.getenv("POSTGRES_URI")
MILVUS_URI   = os.getenv("MILVUS_URI", "http://milvus:19530")
COLLECTION   = "items_v1"
DIMENSION    = 384

if not POSTGRES_URI:
    raise ValueError("POSTGRES_URI environment variable is required")

engine = create_engine(POSTGRES_URI)
client = MilvusClient(uri=MILVUS_URI)

def drop_and_create_collection():
    if client.has_collection(COLLECTION):
        client.drop_collection(COLLECTION)
        logger.info(f"Dropped existing collection {COLLECTION}")

    schema = client.create_schema(auto_id=False, enable_dynamic_field=True)
    schema.add_field("id",           DataType.VARCHAR, is_primary=True, max_length=100)
    schema.add_field("vector",       DataType.FLOAT_VECTOR, dim=DIMENSION)
    schema.add_field("title",        DataType.VARCHAR, max_length=512)
    schema.add_field("content_type", DataType.VARCHAR, max_length=20)
    schema.add_field("genre",        DataType.VARCHAR, max_length=1024, nullable=True)

    index_params = client.prepare_index_params()
    index_params.add_index(
        field_name="vector",
        index_type="IVF_FLAT",
        metric_type="COSINE",
        params={"nlist": 128}
    )

    client.create_collection(collection_name=COLLECTION, schema=schema)
    client.create_index(collection_name=COLLECTION, index_params=index_params)
    client.load_collection(COLLECTION)
    logger.info(f"Collection {COLLECTION} created + indexed")

def embed_and_store():
    logger.info("Loading content from Postgres...")
    movies = pd.read_sql("SELECT movie_id AS id, title, description, genre FROM movie", engine)
    tv    = pd.read_sql("SELECT tvshow_id AS id, title, description, genre FROM tvshow", engine)

    movies["content_type"] = "movie"
    tv["content_type"]     = "tvshow"

    df = pd.concat([movies, tv], ignore_index=True)
    df["text"] = (df["title"].fillna("") + " " + df["description"].fillna("")).str.strip()
    df = df[df["text"] != ""].reset_index(drop=True)

    if len(df) == 0:
        logger.error("No valid content found → aborting")
        return

    logger.info(f"Encoding {len(df)} items with all-MiniLM-L6-v2 ...")
    model = SentenceTransformer("all-MiniLM-L6-v2")
    embeddings = model.encode(
        df["text"].tolist(),
        normalize_embeddings=True,
        show_progress_bar=True,
        batch_size=64
    )

    to_insert = []
    for i, row in df.iterrows():
        to_insert.append({
            "id":           str(row["id"]),
            "vector":       embeddings[i].tolist(),
            "title":        row["title"],
            "content_type": row["content_type"],
            "genre":        row.get("genre", "")
        })

    client.insert(collection_name=COLLECTION, data=to_insert)
    logger.info(f"Successfully stored {len(to_insert)} vectors in Milvus")

if __name__ == "__main__":
    drop_and_create_collection()
    embed_and_store()
    logger.info("Item embedding pipeline finished ✓")