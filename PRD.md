# Product Requirements Document (PRD): Facebook Group Scraper

## 1. Executive Summary
The **Facebook Group Scraper** is an automated data extraction and enrichment pipeline designed to monitor specific Facebook groups (primarily for carpooling/covoiturage) and sync structured data into a Google Sheet. It specializes in handling informal "Tunisian Derija" text, transforming it into structured information (Departure, Destination, Price, etc.) using AI-powered processing.

## 2. Project Goals
- **Automated Monitoring**: Regularly check Facebook groups for new posts without manual intervention.
- **Robust Extraction**: Use browser interception (GraphQL) rather than fragile DOM scraping to capture raw data.
- **Data Enrichment**: Automatically translate dialectal Arabic (Derija) to English/French and extract key entities.
- **Persistence**: Centralize all captured data in a searchable, shareable Google Sheet.
- **VPS Ready**: Design the system to run on a headless remote server with persistent authentication.

## 3. Tech Stack
- **Language**: Python 3.8+
- **Browser Automation**: Playwright (Chromium)
- **APIs**:
  - **Google Sheets/Drive API**: For data storage.
  - **Groq Cloud API**: For LLM-based translation and entity extraction (Llama-3.1-8B/70B).
  - **Namsor API**: For profile-name-based gender detection.
- **Infrastructure**: Linux VPS, Cron jobs for automation.

## 4. Technical Architecture

### 4.1. Core Components
1. **Scraper (`scraper_playwright.py`)**: The primary ingestion engine.
2. **Authenticator (`start_remote_auth.py` / `login_helper.py`)**: Capture and refresh Facebook session cookies.
3. **Enricher (`translate_posts.py`)**: Post-processing tool that adds translations and structured fields.
4. **Storage (Google Sheets)**: The final destination for all data.

### 4.2. High-Level Workflow
1. **Trigger**: A Cron job executes the Scraper script every ~45 minutes.
2. **Session Loading**: The script loads a pre-captured `facebook_auth.json` (Playwright storage state).
3. **Navigation**: The browser navigates to the target Facebook group, sorted chronologically.
4. **Interception**: As the page scrolls, the script intercepts background GraphQL responses containing post data.
5. **Deduplication**: New posts are compared against existing Google Sheet records (via URL and Text Hash).
6. **Upload**: New "raw" rows are appended to the Google Sheet.
7. **Processing**: The Enricher script (run manually or via Cron) reads new rows, invokes Groq/Namsor APIs, and fills in the structured columns.

## 5. Functional Requirements

### 5.1. Data Ingestion
- **GraphQL Interception**: The scraper must listen for network responses containing "graphql" and parse the JSON blocks.
- **Recursive Extraction**: Logic must traverse complex, deeply nested JSON objects to find `Story` or `FeedUnit` typenames.
- **Time Parsing**: Convert Facebook's relative time strings (e.g., "5 mins", "Hier à 10:00") into standard ISO YYYY-MM-DD HH:MM:SS format, adjusted for local timezones.
- **Deduplication**: Prevent duplicate entries using a two-tier check:
  - Primary Key: `post_url`.
  - Content Key: Hash of (Username + Normalized Post Text).

### 5.2. AI Processing (Translation & Extraction)
- **Derija Translation**: Convert Tunisian Arabic to English using a specifically tuned LLM prompt.
- **Entity Extraction**: Extract `from_city`, `from_area`, `to_city`, `to_area`, `price`, `nr_passengers`, and `preferred_departure_time` from the original text into JSON format.
- **Precise Areas**: The prompt must prioritize identifying specific landmarks or neighborhoods (e.g., "Kiosque Agile", "Sahloul") within a city.

### 5.3. Authentication Support
- **Remote Login Bypass**: On a VPS, headless browsers cannot solve 2FA. System must support **SSH Tunneling**:
  1. Start browser on VPS with `--remote-debugging-port=9222`.
  2. Local dev tunnels to port 9222 via SSH.
  3. Local dev logs in via their own browser; state is saved to VPS.

## 6. Data Schema (Google Sheet)
| Column Name | Description | Source |
| :--- | :--- | :--- |
| `post_url` | Full link to the FB post | Scraper |
| `post_time` / `post_date` | Extracted timestamp | Scraper (Parsed) |
| `profile_name` | Name of the poster | Scraper |
| `gender` | Likely gender | Namsor API |
| `offer_or_demand` | Carpool type (Offer vs Demand) | Groq API |
| `from_city` / `from_area` | Departure details | Groq API |
| `to_city` / `to_area` | Destination details | Groq API |
| `price` | Price mentioned (if any) | Groq API |
| `post_text` | Original raw content | Scraper |
| `post_text_english` | English translation | Groq API |
| `scrape_timestamp` | When the entry was added | Scraper |

## 7. Definition of Done (DoD)
To consider the project "reproduced" or "feature-complete," the following criteria must be met:

1. **Successful Scraping**: Running the script captures at least 90% of visible posts on the first two scroll pages of a target group.
2. **Zero Duplicates**: Multiple runs of the script result in no duplicate rows in the target Google Sheet.
3. **Field Accuracy**:
   - `post_url` must be valid and clickable.
   - `post_date` must match the actual post date within an error margin of 1 hour (timezone handled).
4. **Translation Integrity**: Groq-generated translations must accurately reflect the sentiment and core locations of the original Derija post.
5. **Headless Execution**: The system must run end-to-end on a Linux VPS without a GUI, surviving for at least 7 days without session invalidation (provided Facebook doesn't force a logout).
6. **Error Handling**:
   - Script must not crash if Facebook redirects to a "Continue as..." modal.
   - API failures (Groq/Namsor) must be logged and the row marked as failed without stopping the entire process.
7. **Documentation**: A `README` or `PRD` exists that allows a new developer to set up the environment, obtain API keys, and run the auth capture process.

## 8. Development & Deployment
- **Local Environment**: `.env` file management for API keys and group URLs.
- **Deployment**: `rsync` or `scp` to move Python scripts and `.env` to VPS.
- **Monitoring**: Log capture (`scraper_vps.log`) for debugging issues on the remote server.
- **Visual Diagnostics**: Script-level logic to take screenshots (`vps_check.png`) on every run to monitor UI changes.
