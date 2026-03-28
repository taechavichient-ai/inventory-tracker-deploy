FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code
COPY *.py ./

# Cloud Run uses PORT env variable
ENV PORT=8080
ENV DATA_DIR=/data

# Create data directory
RUN mkdir -p /data/database

EXPOSE 8080

CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 120 upload_server:app
