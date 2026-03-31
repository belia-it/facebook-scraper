#!/usr/bin/env python3
"""
Script to translate Tunisian Derija post_text to French and extract structured data using Groq API
Reads directly from Google Sheets like scraper_playwright.py
"""
#source venv/bin/activate && python translate_posts.py

import pandas as pd
import requests
import json
import time
import os
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from typing import Optional, Dict
from dotenv import load_dotenv

class GroqTranslator:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.groq.com/openai/v1/chat/completions"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
    
    def test_connection(self):
        """Test API connection and get available models"""
        try:
            # First try to get available models
            models_url = "https://api.groq.com/openai/v1/models"
            response = requests.get(
                models_url,
                headers=self.headers,
                timeout=10
            )
            
            if response.status_code == 200:
                models_data = response.json()
                available_models = [model.get('id', 'unknown') for model in models_data.get('data', [])]
                print("✅ Available models:", available_models)
                
                # Find a suitable model for translation
                translation_models = ['llama-3.1-70b-versatile', 'llama-3.1-8b-instant', 'mixtral-8x7b-32768', 'gemma-7b-it']
                for model in translation_models:
                    if model in available_models:
                        self.working_model = model
                        print(f"✅ Selected model: {model}")
                        return True
                
                # If no preferred model found, use the first available
                if available_models:
                    self.working_model = available_models[0]
                    print(f"✅ Using first available model: {available_models[0]}")
                    return True
            else:
                print(f"❌ Could not get models: {response.status_code}")
                
        except Exception as e:
            print(f"❌ Models endpoint error: {e}")
        
        print("❌ No working model found")
        return False
    
    def detect_gender_namsor(self, first_name: str, last_name: str, max_retries: int = 3) -> Optional[str]:
        """Detect gender using Namsor API"""
        if not first_name or not last_name:
            return "unknown"
        
        url = f"https://v2.namsor.com/NamSorAPIv2/api2/json/gender/{first_name}/{last_name}"
        headers = {
            "X-API-KEY": "fc5e50d68e615814a8173172401bbb66",
            "Content-Type": "application/json"
        }
        
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, timeout=10)
                
                if response.status_code == 200:
                    result = response.json()
                    # Namsor returns 'male', 'female', or 'unknown' in 'likelyGender' field
                    gender = result.get('likelyGender', 'unknown').lower()
                    return gender
                else:
                    print(f"Namsor API Error (attempt {attempt + 1}): {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        time.sleep(1)
                        
            except Exception as e:
                print(f"Namsor gender detection error (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(1)
        
        return "unknown"
    
    def extract_structured_data(self, text: str, max_retries: int = 3) -> Optional[Dict]:
        """Extract structured data from Tunisian Derija text using Groq API"""
        if not text or pd.isna(text) or text.strip() == "":
            return {}
        
        for attempt in range(max_retries):
            try:
                payload = {
                    "messages": [
                        {
                            "role": "system",
                            "content": """You are a data extraction expert for Tunisian covoiturage posts. Extract the following information from the given text and return as JSON:
                            - offer_or_demand: "offer" if offering rides, "demand" if seeking rides
                            - from_city: departure city name (e.g., "Tunis", "Sousse", "Ariana")
                            - from_area: specific area/neighborhood/landmark in departure city (be very precise - e.g., "centre ville", "Ariana", "Mourouj", "Kiosque Agile", "Sahloul")
                            - to_city: destination city name (e.g., "Sousse", "Monastir", "Bizerte")
                            - to_area: specific area/neighborhood/landmark in destination city (be very precise - e.g., "centre ville", "Sahloul", "Jammel", "Mourouj")
                            - preferred_departure_time: exact time mentioned (e.g., "15h30", "6h matin", "après-midi", "demain matin")
                            - price: price mentioned in DT (just the number, e.g., 15 for "15 dt", 10 for "10 dt")
                            - nr_passengers: exact number of passengers mentioned (just the number)
                            
                            Be extremely precise about areas - look for specific neighborhoods, landmarks, or zones mentioned. Return only valid JSON with these exact keys. If information is not found, use null or empty string. Do not include gender - that will be detected separately."""
                        },
                        {
                            "role": "user",
                            "content": f"Extract structured data from this covoiturage post: {text}"
                        }
                    ],
                    "model": getattr(self, 'working_model', 'llama-3.1-8b-instant'),
                    "max_tokens": 500,
                    "temperature": 0.1
                }
                
                response = requests.post(
                    self.base_url,
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    extracted_text = result["choices"][0]["message"]["content"].strip()
                    
                    # Try to parse JSON
                    try:
                        # Clean up the response to extract JSON
                        if '```json' in extracted_text:
                            extracted_text = extracted_text.split('```json')[1].split('```')[0]
                        elif '```' in extracted_text:
                            extracted_text = extracted_text.split('```')[1].split('```')[0]
                        
                        data = json.loads(extracted_text)
                        return data
                    except json.JSONDecodeError:
                        print(f"JSON parsing error, raw response: {extracted_text}")
                        return None
                    
                else:
                    print(f"Extraction API Error (attempt {attempt + 1}): {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                    
            except Exception as e:
                print(f"Extraction error (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return None
    
    def translate_text(self, text: str, max_retries: int = 3) -> Optional[str]:
        """Translate Tunisian Derija text to English using Groq API"""
        if not text or pd.isna(text) or text.strip() == "":
            return ""
        
        for attempt in range(max_retries):
            try:
                payload = {
                    "messages": [
                        {
                            "role": "system",
                            "content": "You are a professional translator specializing in Tunisian Derija to English translation. Translate the given text accurately while preserving the meaning and context. Only return the translated English text without any additional explanations."
                        },
                        {
                            "role": "user",
                            "content": f"Translate this Tunisian Derija text to English: {text}"
                        }
                    ],
                    "model": getattr(self, 'working_model', 'llama-3.1-8b-instant'),
                    "max_tokens": 1000,
                    "temperature": 0.3
                }
                
                response = requests.post(
                    self.base_url,
                    headers=self.headers,
                    json=payload,
                    timeout=30
                )
                
                if response.status_code == 200:
                    result = response.json()
                    translated_text = result["choices"][0]["message"]["content"].strip()
                    return translated_text
                else:
                    print(f"API Error (attempt {attempt + 1}): {response.status_code} - {response.text}")
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)  # Exponential backoff
                    
            except Exception as e:
                print(f"Translation error (attempt {attempt + 1}): {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
        
        return None

def process_google_sheets(sheet_name: str, credentials_file: str, api_key: str):
    """Translate post_text and extract structured data from Google Sheets"""
    
    # Initialize translator
    translator = GroqTranslator(api_key)
    
    # Test API connection first
    print("Testing Groq API connection...")
    if not translator.test_connection():
        print("❌ Cannot proceed with translation due to API connection issues")
        return
    
    # Connect to Google Sheets (same as scraper_playwright.py)
    print("1. Connecting to Google Sheets...")
    try:
        scope = ["https://spreadsheets.google.com/feeds", 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scope)
        client = gspread.authorize(creds)
        sheet = client.open(sheet_name).worksheet("Feuille 1")
        print("✅ Connected to Sheets.")
    except Exception as e:
        print(f"❌ Sheets Error: {e}")
        return
    
    # Get all data from sheet
    try:
        all_data = sheet.get_all_records()
        df = pd.DataFrame(all_data)
        print(f"Loaded {len(df)} rows from Google Sheets")
    except Exception as e:
        print(f"Error reading Google Sheets: {e}")
        return
    
    # Check if post_text column exists
    if 'post_text' not in df.columns:
        print("Error: 'post_text' column not found in the Google Sheets")
        return
    
    # Ensure all required columns exist
    required_columns = ['post_text_english', 'gender', 'offer_or_demand', 'from_city', 'from_area', 
                      'to_city', 'to_area', 'preferred_departure_time', 'price', 'nr_passengers']
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = ""
    
    # Process each post_text
    total_rows = len(df)
    translated_count = 0
    extracted_count = 0
    error_count = 0
    
    print("Starting translation and data extraction...")
    
    for index, row in df.iterrows():
        post_text = row['post_text']
        
        # Skip if already processed
        if (pd.notna(row['post_text_english']) and row['post_text_english'].strip() != "" and
            pd.notna(row['gender']) and row['gender'].strip() != "" and
            pd.notna(row['offer_or_demand']) and row['offer_or_demand'].strip() != ""):
            print(f"Row {index + 1}/{total_rows}: Already processed")
            continue
        
        print(f"Processing row {index + 1}/{total_rows}...")
        print(f"Original: {post_text[:100]}...")
        
        # Translate to English
        if pd.isna(row['post_text_english']) or row['post_text_english'].strip() == "":
            translated_text = translator.translate_text(post_text)
            if translated_text is not None:
                df.at[index, 'post_text_english'] = translated_text
                translated_count += 1
                print(f"✓ English: {translated_text[:100]}...")
            else:
                df.at[index, 'post_text_english'] = "TRANSLATION_ERROR"
                error_count += 1
                print(f"✗ Translation failed")
                continue
        
        # Extract structured data
        if pd.isna(row['gender']) or row['gender'].strip() == "":
            # First try to detect gender from profile_name using Namsor
            profile_name = str(row.get('profile_name', ''))
            if profile_name and profile_name.strip():
                # Split profile name into first and last name
                name_parts = profile_name.strip().split()
                if len(name_parts) >= 2:
                    first_name = name_parts[0]
                    last_name = name_parts[-1]
                    detected_gender = translator.detect_gender_namsor(first_name, last_name)
                    df.at[index, 'gender'] = detected_gender
                    print(f"✓ Gender detected via Namsor: {detected_gender}")
                else:
                    df.at[index, 'gender'] = "unknown"
                    print(f"✓ Could not split name, gender: unknown")
            else:
                df.at[index, 'gender'] = "unknown"
                print(f"✓ No profile name, gender: unknown")
        
        # Extract other structured data (excluding gender)
        if (pd.isna(row['offer_or_demand']) or row['offer_or_demand'].strip() == "" or
            pd.isna(row['from_city']) or row['from_city'].strip() == ""):
            extracted_data = translator.extract_structured_data(post_text)
            if extracted_data:
                # Update all extracted fields except gender
                for field in ['offer_or_demand', 'from_city', 'from_area', 
                           'to_city', 'to_area', 'preferred_departure_time', 'price', 'nr_passengers']:
                    if field in extracted_data:
                        value = extracted_data[field]
                        # Convert numeric values to strings
                        if value is not None and isinstance(value, (int, float)):
                            value = str(value)
                        elif value is None:
                            value = ""
                        else:
                            value = str(value)
                        df.at[index, field] = value
                
                extracted_count += 1
                print(f"✓ Extracted: {extracted_data}")
            else:
                error_count += 1
                print(f"✗ Data extraction failed")
        
        # Rate limiting - wait between requests
        time.sleep(2)  # Increased delay for both translation and extraction
        
        # Update Google Sheets every 5 processes
        if (index + 1) % 5 == 0:
            try:
                # Update the sheet with processed data
                sheet.update([df.columns.values.tolist()] + df.values.tolist())
                print(f"Progress saved to Google Sheets")
            except Exception as e:
                print(f"Error saving progress to Sheets: {e}")
    
    # Final save to Google Sheets
    try:
        sheet.update([df.columns.values.tolist()] + df.values.tolist())
        print(f"\nProcessing completed!")
        print(f"Total rows: {total_rows}")
        print(f"Successfully translated: {translated_count}")
        print(f"Successfully extracted data: {extracted_count}")
        print(f"Errors: {error_count}")
        print(f"Google Sheets updated with English translations and extracted data")
    except Exception as e:
        print(f"Error saving final data to Sheets: {e}")

def main():
    # Load environment variables
    load_dotenv()
    
    # Configuration (same as scraper_playwright.py)
    SHEET_NAME = os.getenv("SHEET_NAME", "covoiturage report") 
    CREDENTIALS_FILE = os.getenv("CREDENTIALS_FILE", "credentials.json")
    
    # Get API key from environment variable
    api_key = os.getenv("GROQ_API_KEY")
    if not api_key:
        print("Error: GROQ_API_KEY not found in .env file")
        print("Please add GROQ_API_KEY=your_api_key_here to your .env file")
        return
    
    # Check if credentials file exists
    if not os.path.exists(CREDENTIALS_FILE):
        print(f"Error: Credentials file '{CREDENTIALS_FILE}' not found")
        return
    
    # Start processing
    process_google_sheets(SHEET_NAME, CREDENTIALS_FILE, api_key)

if __name__ == "__main__":
    main()
