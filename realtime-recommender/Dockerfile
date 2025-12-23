FROM python:3.12.0

WORKDIR /app

# Install system deps
RUN apt-get update && apt-get install -y gcc

# Copy requirements
COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Default command (producer example)
CMD ["python", "realtime-recommender/producers/user_event_producer/producer.py"]
