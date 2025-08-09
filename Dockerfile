FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends cron sqlite3 && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY DataCollect0r_Bot ./DataCollect0r_Bot

# Create cron job for upload_data.py (every 12 hours)
RUN echo "0 */12 * * * cd /app/DataCollect0r_Bot && /usr/local/bin/python upload_data.py >> /var/log/cron.log 2>&1" > /etc/cron.d/upload-cron \
    && chmod 0644 /etc/cron.d/upload-cron \
    && crontab /etc/cron.d/upload-cron

CMD cron && python DataCollect0r_Bot/main.py
