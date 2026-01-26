FROM bitnami/spark:3.5.3

USER root

# Install system dependencies
RUN install_packages wget postgresql-client

# Download JDBC driver for PostgreSQL
RUN wget -P /opt/bitnami/spark/jars/ \
    https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Install Python dependencies
RUN pip install --no-cache-dir \
    redis==5.0.1 \
    delta-spark==3.2.0 \
    pyspark==3.5.3

# Create checkpoint directory
RUN mkdir -p /tmp/checkpoints && chmod -R 777 /tmp/checkpoints

# Set working directory
WORKDIR /app

# Copy the Spark job
COPY streaming/user_feature_stream/job.py /app/job.py

# Switch back to non-root user
USER 1001

# Default command (will be overridden by docker-compose)
CMD ["/bin/bash"]