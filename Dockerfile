FROM python:3.13-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .

CMD exec gunicorn --bind :${PORT:-8080} --workers 2 --threads 8 --timeout 120 main:app

