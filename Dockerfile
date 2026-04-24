FROM mcr.microsoft.com/playwright/python:v1.58.0

WORKDIR /app

# Install scraper + API Python dependencies
COPY requirements.txt /tmp/scraper-requirements.txt
COPY api/requirements.txt /tmp/api-requirements.txt
RUN pip install --no-cache-dir -r /tmp/scraper-requirements.txt -r /tmp/api-requirements.txt

# Copy project files
COPY . .

# api/posts.db → mount a Coolify volume at /app/api for DB persistence
# facebook_auth.json → upload via /auth/upload in the dashboard

EXPOSE 8000

CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
