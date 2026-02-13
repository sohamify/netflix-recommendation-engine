# streaming/user_feature_stream/job.py
import os
import json
import traceback
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

print("=" * 80)
print("REALTIME USER FEATURE STREAM → Redis + Local Delta Sink")
print("=" * 80)

spark = (SparkSession.builder
         .appName("RealtimeUserFeatureStream")
         .config("spark.sql.shuffle.partitions", "20")
         .config("spark.sql.streaming.schemaInference", "true")
         .config("spark.sql.mapKeyDedupPolicy", "LAST_WIN")
         .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")  # ← disables strict late-data check
         .getOrCreate())

spark.sparkContext.setLogLevel("WARN")
print(f"[{datetime.now()}] Spark session ready")

# Schema (unchanged)
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

# Kafka + watermark (only once)
print(f"[{datetime.now()}] Reading Kafka 'engagement_events'...")
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", os.getenv("KAFKA_BROKERS", "kafka:9092")) \
    .option("subscribe", "engagement_events") \
    .option("startingOffsets", "earliest") \
    .option("maxOffsetsPerTrigger", 500) \
    .option("failOnDataLoss", "false") \
    .load()

events = raw_stream \
    .select(from_json(col("value").cast("string"), engagement_schema).alias("data")) \
    .select("data.*") \
    .withWatermark("timestamp", "30 minutes") \
    .withColumn("date", to_date(col("timestamp")))

watch_events = events.filter((col("event_type") == "watch") & (col("progress_percent") > 0.5))

# Postgres content (unchanged)
print(f"[{datetime.now()}] Loading content from Postgres...")
postgres_url = "jdbc:postgresql://postgres:5432/recommender"
props = {"user": "postgres", "password": "postgres", "driver": "org.postgresql.Driver"}

movies = spark.read.jdbc(postgres_url, "movie", properties=props).select("movie_id", "genre")
tvshows = spark.read.jdbc(postgres_url, "tvshow", properties=props).select("tvshow_id", "genre")

content = movies.withColumnRenamed("movie_id", "content_id") \
                .union(tvshows.withColumnRenamed("tvshow_id", "content_id"))

print(f"[{datetime.now()}] Loaded {movies.count()} movies + {tvshows.count()} TV shows")
broadcast_content = broadcast(content)

enriched = watch_events.join(broadcast_content, "content_id", "left_outer")

genre_exploded = enriched.withColumn("genre_single", explode(split(col("genre"), "\\|"))) \
                         .filter(col("genre_single") != "")

# Aggregations (unchanged)
genre_affinity = genre_exploded.groupBy("profile_id", "genre_single") \
                               .agg(count("*").alias("watch_count")) \
                               .groupBy("profile_id") \
                               .agg(map_from_entries(collect_list(struct("genre_single", "watch_count"))).alias("genre_affinity"))

recent_watches = watch_events \
    .groupBy("profile_id", window(col("timestamp"), "7 days")) \
    .agg(collect_list(struct("timestamp", "content_id")).alias("items")) \
    .withColumn("sorted_items", array_sort("items", lambda x, y: when(x.timestamp > y.timestamp, -1).otherwise(1))) \
    .withColumn("recent_watches", expr("slice(transform(sorted_items, x -> x.content_id), 1, 10)")) \
    .select("profile_id", "recent_watches")

# Redis writer (unchanged)
def update_redis(df, batch_id, field_name):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    if df.isEmpty():
        print(f"[{ts}] Batch {batch_id} — Empty {field_name}")
        return
    try:
        import redis
        r = redis.Redis(host=os.getenv("REDIS_HOST", "redis"), port=6379, decode_responses=True, socket_timeout=5)
        r.ping()
        rows = df.collect()
        pipe = r.pipeline()
        updated = 0
        for row in rows:
            key = f"user:{row.profile_id}:features"
            value = json.dumps(getattr(row, field_name) or {})
            pipe.hset(key, field_name, value)
            pipe.expire(key, 30 * 86400)
            updated += 1
        pipe.execute()
        print(f"[{ts}] Batch {batch_id} — Updated {updated} users ({field_name})")
    except Exception as e:
        print(f"[{ts}] Batch {batch_id} — Redis error: {str(e)}")
        traceback.print_exc()

# Raw Delta sink
raw_events_path = "/tmp/raw-events-delta/"
raw_query = events.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoints/raw-events") \
    .partitionBy("date") \
    .trigger(processingTime="5 minutes") \
    .start(raw_events_path)

print(f"[{datetime.now()}] Raw events → {raw_events_path}")

# Redis sinks
print(f"[{datetime.now()}] Starting feature update streams...")
q_genre = genre_affinity.writeStream \
    .foreachBatch(lambda df, bid: update_redis(df, bid, "genre_affinity")) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/genre") \
    .trigger(processingTime="15 seconds") \
    .start()

q_recent = recent_watches.writeStream \
    .foreachBatch(lambda df, bid: update_redis(df, bid, "recent_watches")) \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/checkpoints/recent") \
    .trigger(processingTime="30 seconds") \
    .start()

print(f"[{datetime.now()}] All streams running. Waiting for Kafka events...")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print(f"[{datetime.now()}] Shutdown (Ctrl+C)")
except Exception as e:
    print(f"[{datetime.now()}] Fatal error: {str(e)}")
    traceback.print_exc()
finally:
    print(f"[{datetime.now()}] Stopping queries...")
    for q in [q_genre, q_recent, raw_query]:
        try:
            if q.isActive:
                q.stop()
                print(f"[{datetime.now()}] Stopped {q.id}")
        except:
            pass
    spark.stop()
    print(f"[{datetime.now()}] Session closed.")