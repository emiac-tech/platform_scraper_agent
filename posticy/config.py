"""
Posticy Scraper Configuration
=============================
Credentials and settings for the Posticy guest posting marketplace scraper.
"""

import os
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "..", ".env"))

# ─── Login Credentials ──────────────────────────────────────────────────────────
USERNAME = os.getenv("POSTICY_EMAIL")
PASSWORD = os.getenv("POSTICY_PASSWORD")

# ─── URLs ────────────────────────────────────────────────────────────────────────
BASE_URL = "https://marketplace.posticy.com"
LOGIN_URL = f"{BASE_URL}/login"
MARKETPLACE_URL = f"{BASE_URL}/marketer/marketplace"
API_LISTING_URL = f"{BASE_URL}/marketer/marketplace/listing"

# ─── Scraping Settings ───────────────────────────────────────────────────────────
# Delay between API requests (in seconds) to avoid rate-limiting
REQUEST_DELAY = 1.0

# Max retries per page if request fails
MAX_RETRIES = 3

# Timeout for each HTTP request (seconds)
REQUEST_TIMEOUT = 30

# Marketer ID (From observation)
MARKETER_ID = 24555

# Number of items per request (default in Posticy seems to be 10, but we can try 50 or 100)
PAGE_SIZE = 50

# ─── Output Settings ────────────────────────────────────────────────────────────
DATA_DIR = os.path.join(BASE_DIR, "data")
COOKIES_DIR = os.path.join(BASE_DIR, "cookies")
COOKIE_FILE = os.path.join(COOKIES_DIR, "posticy_session.json")

# Output CSV filename (will be timestamped automatically)
OUTPUT_PREFIX = "posticy_publishers"

# Database Settings (PostgreSQL or SQLite)
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{os.path.join(BASE_DIR, 'marketplaces.db')}")

# ─── Filters ────────────────────────────────────────────────────────────────────
# The API accepts many filters. Set these to prioritize latest data or specific metrics.
DEFAULT_PARAMS = {
    "length": PAGE_SIZE,
    "order[0][column]": "8",  # Index 8 is likely "Added" (based on table headers)
    "order[0][dir]": "desc",  # Sort by latest added
}
