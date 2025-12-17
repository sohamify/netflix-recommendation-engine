# producers/user_event_producer/Dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY producer.py .
CMD ["python", "producer.py"]