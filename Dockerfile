# Use the official Playwright Python image which has everything pre-installed
FROM mcr.microsoft.com/playwright/python:v1.58.0

WORKDIR /app

# Copy requirements and install python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the project files
COPY . .

# Run the python scraper
CMD ["python", "scraper_playwright.py"]
