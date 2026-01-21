from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

print("Testing MinIO connection...")
buckets = client.list_buckets()
for bucket in buckets:
    print(f"Found bucket: {bucket.name}")

if not client.bucket_exists("data-lake"):
    client.make_bucket("data-lake")
    print("Created bucket: data-lake")
else:
    print("Bucket data-lake already exists")
print("✅ MinIO connection SUCCESS!")