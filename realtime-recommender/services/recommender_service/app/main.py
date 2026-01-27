from fastapi import FastAPI
from pydantic import BaseModel
from pymilvus import MilvusClient
from sentence_transformers import SentenceTransformer
import os

app = FastAPI(title="Recommender - Vector Search")

MILVUS_URI = os.getenv("MILVUS_URI", "http://milvus:19530")
client = MilvusClient(uri=MILVUS_URI)
model = SentenceTransformer('all-MiniLM-L6-v2')
COLLECTION = "item_embeddings"

class SearchQuery(BaseModel):
    query: str
    top_k: int = 10

@app.post("/search")
def semantic_search(q: SearchQuery):
    emb = model.encode([q.query], normalize_embeddings=True)[0].tolist()
    results = client.search(
        collection_name=COLLECTION,
        data=[emb],
        limit=q.top_k,
        output_fields=["title", "content_type", "genre"],
        metric_type="COSINE"
    )
    return {"results": results[0]}

@app.get("/health")
def health():
    return {"status": "ok"}