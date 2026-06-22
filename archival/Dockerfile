FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=True

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY config/ config/

CMD ["python", "main.py"]