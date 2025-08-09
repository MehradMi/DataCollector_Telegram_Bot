FROM python:3.11-slim

WORKDIR /app

# Install cron
RUN apt-get update && apt-get install -y --no-install-recommends cron && rm -rf /var/lib/apt/lists/*

# Copy requirements and install
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy your bot code
COPY DataCollect0r_Bot ./DataCollect0r_Bot
COPY .env .env

# Create cron job for upload_data.py (every 12 hours)
RUN echo "0 */12 * * * cd /app/DataCollect0r_Bot && /usr/local/bin/python upload_data.py >> /var/log/cron.log 2>&1" > /etc/cron.d/upload-cron \
    && chmod 0644 /etc/cron.d/upload-cron \
    && crontab /etc/cron.d/upload-cron

# Start cron and the bot
CMD cron && python DataCollect0r_Bot/main.py
