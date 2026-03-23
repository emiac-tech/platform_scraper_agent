import os
from dotenv import load_dotenv

load_dotenv()

# Securing Credentials via Environment Variables
EMAIL = os.getenv("PUBLISUITES_EMAIL")
PASSWORD = os.getenv("PUBLISUITES_PASSWORD")

# Marketplace URLs
LOGIN_URL = "https://www.publisuites.com/login/"
BASE_MARKETPLACE_URL = "https://www.publisuites.com/advertisers/websites/"

# Database Settings
DATABASE_URL = os.getenv("DATABASE_URL")

# Scraper Settings
REQUEST_TIMEOUT = 30
MAX_PAGES = 50  # Adjust as needed for production
