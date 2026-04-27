FROM mcr.microsoft.com/playwright/python:v1.58.0
WORKDIR /app

# Install dependencies
COPY requirements.txt /tmp/scraper-requirements.txt
COPY api/requirements.txt /tmp/api-requirements.txt
RUN pip install --no-cache-dir -r /tmp/scraper-requirements.txt -r /tmp/api-requirements.txt

# Copy project files
COPY . .

# Keep a pristine image copy of api/ so entrypoint can sync it into the volume
RUN cp -r /app/api /app/api.image

EXPOSE 8000
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
CMD ["/entrypoint.sh"]
