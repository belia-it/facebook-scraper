# Tunisian Derija to French Translation Script

This script translates the `post_text` column from Tunisian Derija to French using the Grok API.

## Setup

1. Install dependencies:
```bash
source venv/bin/activate
pip install -r requirements.txt
```

2. Set your Grok API key as an environment variable:
```bash
export GROK_API_KEY="your_api_key_here"
```

Or enter it when prompted by the script.

## Usage

Run the script:
```bash
source venv/bin/activate
python translate_posts.py
```

The script will:
- Read `covoiturage report example.xlsx`
- Translate the `post_text` column to French
- Create a new file `covoiturage_report_translated.xlsx` with the translations
- Add a new column `post_text_french` containing the French translations

## Features

- **Rate limiting**: 1-second delay between API calls to avoid rate limits
- **Error handling**: Retries failed translations up to 3 times
- **Progress saving**: Saves progress every 10 translations
- **Resume capability**: Skips already translated rows
- **Batch processing**: Handles large datasets efficiently

## Output

The translated Excel file will contain all original columns plus:
- `post_text_french`: French translation of the original post_text

## API Requirements

- Grok API key from X.ai
- The script uses the `grok-beta` model
- Maximum 1000 tokens per translation request

## Error Handling

- Empty or null values are skipped
- Translation errors are marked as "TRANSLATION_ERROR"
- Network timeouts are retried with exponential backoff



## manual run
Run this if you just want to trigger it without logging in to the VPS:

bash
ssh houcem@192.168.100.45 "cd /home/houcem/facebook-scraper && ./venv/bin/python scraper_playwright.py"
Method 2: From inside the VPS
If you are already logged into the VPS via SSH, run:

bash
cd /home/houcem/facebook-scraper
./venv/bin/python scraper_playwright.py

📋 Tips:
Logs: If you want to see what's happening while it runs, you can check the log file on the VPS: tail -f /home/houcem/facebook-scraper/cron.log
Virtual Environment: Ensure you use 

./venv/bin/python
 to make sure all dependencies (like Playwright) are available.

 