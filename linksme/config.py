import os
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, "..", ".env"))

# Credentials
EMAIL = "gargaditya5777@gmail.com"
PASSWORD = "Emiac@1617#"

# URLs
LOGIN_URL = "https://app.links.me/login_ajax"
CATALOG_URL = "https://app.links.me/project/2615/catalog/guest-posting"

# Settings
REQUEST_TIMEOUT = 30
MAX_PAGES = 50
PAGE_SIZE = 200

# File paths
DATA_DIR = os.path.join(BASE_DIR, "data")
COOKIE_FILE = os.path.join(BASE_DIR, "cookies", "linksme_session.json")

# DB
DATABASE_URL = os.getenv("DATABASE_URL")
