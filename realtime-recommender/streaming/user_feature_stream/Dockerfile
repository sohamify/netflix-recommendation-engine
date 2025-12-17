FROM apache/spark-py:3.5.0

WORKDIR /opt/spark/app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY job.py .

# Entry point for spark-submit
CMD ["/opt/spark/bin/spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,io.delta:delta-spark_2.12:3.0.0", "job.py"]