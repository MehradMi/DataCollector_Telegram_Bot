FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Install system dependencies (cron for uploader scheduling)
RUN apt-get update && apt-get install -y sqlite3 cron && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . .

# Environment variables
ENV PYTHONUNBUFFERED=1

# Default command (overridden in docker-compose)
CMD ["python", "main.py"]