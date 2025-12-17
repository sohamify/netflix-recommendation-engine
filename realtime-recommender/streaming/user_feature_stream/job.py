# streaming/user_feature_stream/job.py
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import json

# ====================== SPARK SESSION ======================
spark = (SparkSession.builder
         .appName("RealtimeUserFeatureStream")
         .config("spark.sql.shuffle.partitions", "20")
         .config("spark.sql.streaming.schemaInference", "true")
         .getOrCreate())

# ====================== SCHEMA (for engagement_events) ======================
engagement_schema = StructType([
    StructField("event_id", StringType()),
    StructField("profile_id", StringType()),
    StructField("user_id", StringType()),
    StructField("content_id", StringType()),
    StructField("event_type", StringType()),
    StructField("timestamp", TimestampType()),
    StructField("progress_percent", DoubleType(), True),
    StructField("duration_watched_sec", LongType(), True),
    StructField("rating", DoubleType(), True)
])

# ====================== KAFKA SOURCE ======================
raw_stream = (spark
              .readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", os.getenv("KAFKA_BROKERS", "localhost:9092"))
              .option("subscribe", "engagement_events")
              .option("startingOffsets", "earliest")  # Change to "latest" in prod
              .load())

events = (raw_stream
          .select(from_json(col("value").cast("string"), engagement_schema).alias("data"))
          .select("data.*")
          .withWatermark("timestamp", "10 minutes"))

# Filter only meaningful events for features
watch_events = events.filter(col("event_type") == "watch") \
                     .filter(col("progress_percent") > 0.5)  # Count as "watched" if >50%

rate_events = events.filter(col("event_type") == "rate")

# ====================== ENRICH WITH CONTENT GENRE ======================
# Static content lookup (broadcast small tables)
movies = spark.read.format("jdbc") \
    .option("url", os.getenv("POSTGRES_URI", "jdbc:postgresql://postgres:5432/recommender")) \
    .option("dbtable", "movie") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load().select("movie_id", "genre")

tvshows = spark.read.format("jdbc") \
    .option("url", os.getenv("POSTGRES_URI", "jdbc:postgresql://postgres:5432/recommender")) \
    .option("dbtable", "tvshow") \
    .option("user", "postgres") \
    .option("password", "postgres") \
    .option("driver", "org.postgresql.Driver") \
    .load().select("tvshow_id", "genre")

content = movies.unionByName(tvshows, allowMissingColumns=True).withColumnRenamed("movie_id", "content_id") \
                .withColumnRenamed("tvshow_id", "content_id")

broadcast_content = broadcast(content)

enriched = watch_events.join(broadcast_content, "content_id", "left")

# Explode multi-genres (pipe-separated)
genre_exploded = enriched.withColumn("genre_single", explode(split(col("genre"), "\|"))) \
                         .filter(col("genre_single") != "")

# ====================== AGGREGATE USER GENRE AFFINITY (last 30 days sliding) ======================
window = Window.partitionBy("profile_id", "genre_single").orderBy("timestamp").rangeBetween(-2592000, 0)  # 30 days in sec

user_genre_affinity = (genre_exploded
                       .withColumn("watch_weight", lit(1.0))
                       .withColumn("genre_count", sum("watch_weight").over(window))
                       .groupBy("profile_id", "genre_single")
                       .agg(max("genre_count").alias("affinity_score")))

# Pivot to wide vector (profile_id -> genre_vector map)
user_features = user_genre_affinity.groupBy("profile_id") \
    .pivot("genre_single") \
    .agg(first("affinity_score")) \
    .na.fill(0)

# Add recent watches (last 10 content_ids)
recent_watches = watch_events.orderBy(desc("timestamp")) \
    .groupBy("profile_id") \
    .agg(collect_list("content_id").alias("recent_watches")) \
    .withColumn("recent_watches", col("recent_watches").cast("string"))

final_features = user_features.join(recent_watches, "profile_id", "left")

# ====================== SINKS ======================
# 1. Redis (online serving)
def foreach_batch_redis(df, batch_id):
    if df.count() > 0:
        import redis
        r = redis.Redis(host=os.getenv("REDIS_HOST", "localhost"), port=6379, db=0)
        for row in df.collect():
            key = f"user:{row.profile_id}"
            data = row.asDict()
            data.pop("profile_id")
            r.hset(key, mapping=data)
            r.expire(key, 86400 * 30)  # 30 days TTL

# 2. MinIO processed zone (Parquet)
minio_path = "s3a://data-lake/processed/user_features/"

query1 = (final_features.writeStream
          .foreachBatch(foreach_batch_redis)
          .outputMode("update")
          .option("checkpointLocation", "/tmp/checkpoints/redis")
          .start())

query2 = (final_features.writeStream
          .format("delta")
          .outputMode("complete")
          .option("checkpointLocation", "/tmp/checkpoints/minio")
          .partitionBy("profile_id")
          .start(minio_path))

query1.awaitTermination()
query2.awaitTermination()