import os
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "..", ".env"))

# Credentials
EMAIL = "gargaditya5777@gmail.com"
PASSWORD = "Emiac@1617#"

# URLs — PressScape is a Next.js app with a JSON API
LOGIN_URL = "https://pressscape.com/api/auth/login"
MARKETPLACE_API_URL = "https://pressscape.com/api/marketplace"
AUTH_CHECK_URL = "https://pressscape.com/api/auth/check"

# Settings
REQUEST_TIMEOUT = 30
MAX_PAGES = 100
PAGE_SIZE = 100  # API supports limit param

# File paths
DATA_DIR = os.path.join(BASE_DIR, "data")
COOKIE_FILE = os.path.join(BASE_DIR, "cookies", "pressscape_session.json")

# DB
DATABASE_URL = os.getenv("DATABASE_URL")
