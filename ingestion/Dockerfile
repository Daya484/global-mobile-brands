FROM python:3.11-slim

WORKDIR /app

# Ensures logs show immediately in Cloud Run logs
ENV PYTHONUNBUFFERED=True

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy code + config
COPY main.py .
COPY config/ config/

CMD ["python", "main.py"]