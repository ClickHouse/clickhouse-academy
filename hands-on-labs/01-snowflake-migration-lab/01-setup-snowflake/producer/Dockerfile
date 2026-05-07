FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer-cached separately from source)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY producer.py .

# Unbuffered output so logs appear immediately in docker logs
ENV PYTHONUNBUFFERED=1

CMD ["python", "-u", "producer.py"]
