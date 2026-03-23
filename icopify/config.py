"""
iCopify Scraper Configuration
==============================
Credentials and settings for the iCopify guest posting marketplace scraper.
"""

import os
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "..", ".env"))

# ─── Login Credentials ──────────────────────────────────────────────────────────
EMAIL = os.getenv("ICOPIFY_EMAIL")
PASSWORD = os.getenv("ICOPIFY_PASSWORD")

# ─── URLs ────────────────────────────────────────────────────────────────────────
BASE_URL = "https://icopify.co"
LOGIN_URL = f"{BASE_URL}/login"
MARKETPLACE_URL = f"{BASE_URL}/project/68623/publishers"

# ─── Scraping Settings ───────────────────────────────────────────────────────────
# Delay between page requests (in seconds) to avoid rate-limiting
REQUEST_DELAY = 1.5

# Max retries per page if request fails
MAX_RETRIES = 3

# Timeout for each HTTP request (seconds)
REQUEST_TIMEOUT = 30

# Number of pages to scrape (set to None for all pages)
MAX_PAGES = None

# ─── Output Settings ────────────────────────────────────────────────────────────
DATA_DIR = os.path.join(BASE_DIR, "data")
COOKIES_DIR = os.path.join(BASE_DIR, "cookies")
COOKIE_FILE = os.path.join(COOKIES_DIR, "icopify_session.json")

# Output CSV filename (will be timestamped automatically)
OUTPUT_PREFIX = "icopify_publishers"

# ─── Filters (Optional) ─────────────────────────────────────────────────────────
# Set these to filter results. Set to None to skip filtering.
FILTERS = {
    "DAFrom": None,       # Moz DA minimum (e.g., 20)
    "DATo": None,         # Moz DA maximum (e.g., 100)
    "DRFrom": None,       # Ahrefs DR minimum
    "DRTo": None,         # Ahrefs DR maximum
    "PriceFrom": None,    # Price minimum
    "PriceTo": None,      # Price maximum
    "TrafficFrom": None,  # Monthly traffic minimum
    "TrafficTo": None,    # Monthly traffic maximum
    "categories": None,   # Category filter
    "language": None,     # Website language filter
    "link_type": None,    # "Follow" or "NoFollow"
}

# ─── Database Settings ──────────────────────────────────────────────────────────
DATABASE_URL = os.getenv("DATABASE_URL")
