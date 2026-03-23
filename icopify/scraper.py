#!/usr/bin/env python3
"""
iCopify Publisher Marketplace Scraper
======================================
Scrapes guest posting marketplace data from icopify.co.
Handles login, session persistence via cookies, automatic token refresh,
pagination, and exports data to CSV.

Usage:
    python scraper.py                    # Scrape all publishers
    python scraper.py --pages 5          # Scrape first 5 pages
    python scraper.py --latest           # Fetch only new data since last scrape
    python scraper.py --output my_data   # Custom output filename prefix
"""

import os
import re
import sys
import json
import time
import logging
import argparse
from datetime import datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup
import pandas as pd
import json
import re
import os
import sys
import time
import logging
import argparse
from datetime import datetime
from colorama import Fore, Style, init as colorama_init
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils import normalize_domain

# ─── Initialize colorama ────────────────────────────────────────────────────────
colorama_init(autoreset=True)

# ─── Import config ───────────────────────────────────────────────────────────────
from config import (
    EMAIL, PASSWORD, BASE_URL, LOGIN_URL, MARKETPLACE_URL,
    REQUEST_DELAY, MAX_RETRIES, REQUEST_TIMEOUT, MAX_PAGES,
    DATA_DIR, COOKIES_DIR, COOKIE_FILE, OUTPUT_PREFIX, FILTERS,
    DATABASE_URL
)

# ─── DB Declaration ───────────────────────────────────────────────────────────────
Base = declarative_base()

class PublisherListing(Base):
    __tablename__ = 'publishers_v2'
    id = Column(Integer, primary_key=True, autoincrement=True)
    clean_domain = Column(String(255), unique=True, index=True)
    website_url = Column(String(255))
    host_sites = Column(JSON, default=list)
    item_ids = Column(JSON, default=list)
    categories = Column(JSON, default=list)
    prices_raw = Column(JSON, default=list)
    prices_numerical = Column(JSON, default=list)
    
    moz_da = Column(Integer)
    moz_pa = Column(Integer)
    ahrefs_dr = Column(Integer)
    traffic = Column(Integer)
    
    language = Column(String(100))
    country = Column(String(100))
    scraped_at = Column(DateTime, default=datetime.utcnow)

# ─── Logging Setup ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f'{Fore.CYAN}%(asctime)s{Style.RESET_ALL} | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class ICopifyScraper:
    """
    Scraper for the iCopify Publishers Marketplace.
    
    Features:
        - Automatic login with session cookie persistence
        - Token/session refresh mechanism (via remember_me cookie)
        - Paginated scraping with progress tracking
        - Parse all available columns from HTML table
        - Export to timestamped CSV files
        - "Latest only" mode to avoid re-scraping old data
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Referer": BASE_URL,
    }

    def __init__(self, max_pages=None, output_prefix=None):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.max_pages = max_pages or MAX_PAGES
        self.output_prefix = output_prefix or OUTPUT_PREFIX
        self.all_data = []
        self.total_pages = None
        self.total_websites = None
        self.host_site = "icopify.co"

        # Database Setup
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Ensure directories exist
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(COOKIES_DIR, exist_ok=True)

    def upsert_listing(self, item_data):
        """Insert or update a listing in the database."""
        db_session = self.SessionLocal()
        try:
            domain = normalize_domain(item_data.get('website_url', ''))
            if not domain:
                return False

            existing = db_session.query(PublisherListing).filter_by(clean_domain=domain).first()
            
            host = item_data['host_site']
            pid = item_data['item_id']
            p_raw = item_data['price_raw']
            p_num = item_data.get('price_numerical')
            cat = item_data.get('category')
            
            is_new_host_entry = False
            
            if existing:
                hosts = list(existing.host_sites or [])
                ids = list(existing.item_ids or [])
                p_raws = list(existing.prices_raw or [])
                p_nums = list(existing.prices_numerical or [])
                cats = list(existing.categories or [])

                if host in hosts:
                    idx = hosts.index(host)
                    ids[idx] = pid
                    p_raws[idx] = p_raw
                    p_nums[idx] = p_num
                    while len(cats) <= idx:
                        cats.append(None)
                    cats[idx] = cat
                else:
                    hosts.append(host)
                    ids.append(pid)
                    p_raws.append(p_raw)
                    p_nums.append(p_num)
                    cats.append(cat)
                    is_new_host_entry = True

                existing.host_sites = hosts
                existing.item_ids = ids
                existing.prices_raw = p_raws
                existing.prices_numerical = p_nums
                existing.categories = cats

                if item_data.get('moz_da') and (not existing.moz_da or item_data['moz_da'] > existing.moz_da):
                    existing.moz_da = item_data['moz_da']
                if item_data.get('ahrefs_dr') and (not existing.ahrefs_dr or item_data['ahrefs_dr'] > existing.ahrefs_dr):
                    existing.ahrefs_dr = item_data['ahrefs_dr']
                if item_data.get('traffic') and (not existing.traffic or item_data['traffic'] > existing.traffic):
                    existing.traffic = item_data['traffic']

                existing.scraped_at = datetime.utcnow()
            else:
                db_item = {
                    'clean_domain': domain,
                    'website_url': item_data.get('website_url'),
                    'host_sites': [host],
                    'item_ids': [pid],
                    'categories': [cat],
                    'prices_raw': [p_raw],
                    'prices_numerical': [p_num],
                    'moz_da': item_data.get('moz_da'),
                    'ahrefs_dr': item_data.get('ahrefs_dr'),
                    'traffic': item_data.get('traffic'),
                    'language': item_data.get('language'),
                    'country': item_data.get('country'),
                    'scraped_at': datetime.utcnow()
                }
                new_listing = PublisherListing(**db_item)
                db_session.add(new_listing)
                is_new_host_entry = True
            
            db_session.commit()
            return is_new_host_entry
        except SQLAlchemyError as e:
            db_session.rollback()
            logger.error(f"{Fore.RED}✗ Database error: {e}")
            return False
        finally:
            db_session.close()

    # ─── Cookie / Session Management ────────────────────────────────────────────

    def save_cookies(self):
        """Persist session cookies to disk for reuse across runs."""
        cookies_dict = {}
        for cookie in self.session.cookies:
            cookies_dict[cookie.name] = {
                "value": cookie.value,
                "domain": cookie.domain,
                "path": cookie.path,
                "expires": cookie.expires,
            }
        with open(COOKIE_FILE, "w") as f:
            json.dump(cookies_dict, f, indent=2)
        logger.debug("Cookies saved to disk.")

    def load_cookies(self):
        """Load previously saved cookies from disk."""
        if not os.path.exists(COOKIE_FILE):
            return False

        try:
            with open(COOKIE_FILE, "r") as f:
                cookies_dict = json.load(f)

            for name, data in cookies_dict.items():
                self.session.cookies.set(
                    name,
                    data["value"],
                    domain=data.get("domain", ""),
                    path=data.get("path", "/"),
                )
            logger.info(f"{Fore.GREEN}✓ Loaded saved cookies from disk.")
            return True
        except (json.JSONDecodeError, KeyError, Exception) as e:
            logger.warning(f"Failed to load cookies: {e}")
            return False

    def is_session_valid(self):
        """Check if the current session is still authenticated."""
        try:
            resp = self.session.get(
                MARKETPLACE_URL,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=False
            )
            # If redirected to login page, session is invalid
            if resp.status_code in (301, 302):
                location = resp.headers.get("Location", "")
                if "login" in location.lower():
                    return False
            # If page contains login form, session is invalid
            if resp.status_code == 200:
                if "Log in" in resp.text and 'name="email"' in resp.text:
                    return False
                return True
            return False
        except requests.RequestException:
            return False

    def refresh_session(self):
        """
        Attempt to refresh the session using existing cookies.
        If that fails, perform a full login.
        """
        logger.info(f"{Fore.YELLOW}↻ Attempting session refresh...")

        # Try loading saved cookies first
        if self.load_cookies():
            if self.is_session_valid():
                logger.info(f"{Fore.GREEN}✓ Session refreshed successfully via saved cookies!")
                return True
            else:
                logger.info(f"{Fore.YELLOW}⚠ Saved cookies expired. Performing full login...")

        # Full login required
        return self.login()

    def login(self):
        """
        Perform a full login to iCopify.
        
        1. GET the login page to obtain CSRF token
        2. POST credentials with CSRF token
        3. Save session cookies for future use
        """
        logger.info(f"{Fore.CYAN}▶ Logging in to iCopify...")

        try:
            # Step 1: Get login page for CSRF token
            login_page = self.session.get(LOGIN_URL, timeout=REQUEST_TIMEOUT)
            login_page.raise_for_status()

            soup = BeautifulSoup(login_page.text, "lxml")
            csrf_token = None

            # Try 1: Try to find CSRF token in the form by name
            csrf_input = soup.find("input", {"name": "_token"})
            if csrf_input:
                csrf_token = csrf_input.get("value")

            # Try 2: try meta tag
            if not csrf_token:
                meta = soup.find("meta", {"name": "csrf-token"})
                if meta:
                    csrf_token = meta.get("content")
            
            # Try 3: Regex fallback if BeautifulSoup fails for some reason
            if not csrf_token:
                match = re.search(r'name="_token"\s+value="([^"]+)"', login_page.text)
                if match:
                    csrf_token = match.group(1)

            if not csrf_token:
                logger.error(f"{Fore.RED}✗ Could not find CSRF token on login page!")
                # Log a snippet of the page to help debug
                with open("error_page.html", "w") as f:
                    f.write(login_page.text)
                logger.debug(f"Saved error page to error_page.html")
                return False

            logger.debug(f"CSRF token obtained: {csrf_token[:20]}...")

            # Step 2: Submit login form
            login_data = {
                "_token": csrf_token,
                "email": EMAIL,
                "password": PASSWORD,
                "remember": "on",  # Important: enables the remember_me cookie
            }

            resp = self.session.post(
                LOGIN_URL,
                data=login_data,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            # Check if login was successful
            if resp.status_code == 200:
                # Check if we're still on the login page (login failed)
                if "Log in" in resp.text and 'name="email"' in resp.text:
                    # Check for error messages
                    error_soup = BeautifulSoup(resp.text, "lxml")
                    error_msg = error_soup.find("div", class_="alert-danger")
                    if error_msg:
                        logger.error(f"{Fore.RED}✗ Login failed: {error_msg.get_text(strip=True)}")
                    else:
                        logger.error(f"{Fore.RED}✗ Login failed: credentials may be incorrect.")
                    return False

            # Login successful
            logger.info(f"{Fore.GREEN}✓ Login successful!")

            # Step 3: Save cookies
            self.save_cookies()
            return True

        except requests.RequestException as e:
            logger.error(f"{Fore.RED}✗ Login request failed: {e}")
            return False

    # ─── HTML Parsing ────────────────────────────────────────────────────────────

    def parse_marketplace_page(self, html_content):
        """
        Parse a single marketplace page and extract publisher data.
        
        Returns:
            list[dict]: List of publisher data dictionaries.
        """
        soup = BeautifulSoup(html_content, "lxml")
        publishers = []

        # Extract total count and pages (from first page)
        if self.total_websites is None:
            found_text = soup.find(string=re.compile(r"Found.*Websites"))
            if found_text:
                match = re.search(r"Found[:\s]*([\d,]+)", found_text)
                if match:
                    self.total_websites = int(match.group(1).replace(",", ""))
                    logger.info(f"{Fore.BLUE}📊 Total websites in marketplace: {self.total_websites:,}")

            # Find total pages from pagination
            pagination = soup.find("ul", class_="pagination")
            if pagination:
                page_links = pagination.find_all("a")
                page_numbers = []
                for link in page_links:
                    text = link.get_text(strip=True)
                    if text.isdigit():
                        page_numbers.append(int(text))
                if page_numbers:
                    self.total_pages = max(page_numbers)
                    logger.info(f"{Fore.BLUE}📄 Total pages: {self.total_pages:,}")

        # Find all publisher rows in the table
        # The marketplace uses a table layout where each row is a publisher
        table = soup.find("table")
        if not table:
            # Fallback: try to find publisher divs/cards
            logger.warning("No table found on page, trying alternative selectors...")
            return self._parse_card_layout(soup)

        rows = table.find_all("tr")

        for row in rows:
            cells = row.find_all("td")
            if len(cells) < 4:  # Skip header rows or empty rows
                continue

            try:
                publisher = self._extract_row_data(cells, row)
                if publisher and publisher.get("website_url"):
                    publishers.append(publisher)
            except Exception as e:
                logger.debug(f"Error parsing row: {e}")
                continue

        return publishers

    def _extract_row_data(self, cells, row):
        """Extract data from a single table row."""
        data = {}

        # ─── Column 1: Website Info ──────────────────────────────────────────
        col1 = cells[0]
        
        # Website URL
        site_link = col1.find("a", href=True)
        if site_link:
            url_text = site_link.get_text(strip=True)
            data["website_url"] = url_text
            data["item_id"] = site_link.get("href", "") # Use iCopify internal link as unique ID
        
        data["host_site"] = self.host_site

        # Max DoFollow Links (Auxiliary info - not in core DB table but keeping in dict)
        dofollow_text = col1.get_text()
        dofollow_match = re.search(r"Max\s+(\d+)\s+DoFollow", dofollow_text)
        data["max_dofollow_links"] = int(dofollow_match.group(1)) if dofollow_match else None

        # Turnaround Time
        tat_match = re.search(r"Turnaround\s+Time[:\s]*(\d+)\s*(\w+)", dofollow_text)
        data["turnaround_time"] = f"{tat_match.group(1)} {tat_match.group(2)}" if tat_match else None

        # ─── Column 2: Categories ────────────────────────────────────────────
        if len(cells) > 1:
            col2 = cells[1]
            categories = [b.get_text(strip=True) for b in col2.find_all("span") if b.get_text(strip=True)]
            if not categories:
                cat_text = col2.get_text(strip=True)
                categories = [c.strip() for c in cat_text.split(",") if c.strip()]
            data["category"] = ", ".join(categories) if categories else None

        # ─── Column 3: Monthly Traffic ───────────────────────────────────────
        if len(cells) > 2:
            col3 = cells[2]
            traffic_text = col3.get_text(strip=True)
            traffic_match = re.search(r"([\d,]+)", traffic_text.replace("Monthly Traffic", ""))
            data["traffic"] = int(traffic_match.group(1).replace(",", "")) if traffic_match else 0

        # ─── Column 4: Ahrefs DR ────────────────────────────────────────────
        if len(cells) > 3:
            col4 = cells[3]
            dr_text = col4.get_text(strip=True)
            dr_match = re.search(r"(?:DR\s*)?(\d+)", dr_text)
            data["ahrefs_dr"] = int(dr_match.group(1)) if dr_match else None

        # ─── Column 5: Moz DA ───────────────────────────────────────────────
        if len(cells) > 4:
            col5 = cells[4]
            da_text = col5.get_text(strip=True)
            da_match = re.search(r"(?:DA\s*)?(\d+)", da_text)
            data["moz_da"] = int(da_match.group(1)) if da_match else None

        # ─── Column 6: Language ──────────────────────────────────────────────
        if len(cells) > 5:
            col6 = cells[5]
            data["language"] = col6.get_text(strip=True) or None

        # ─── Column 7: Price ────────────────────────────────────────────────
        if len(cells) > 6:
            col7 = cells[6]
            price_text = col7.get_text(strip=True)
            data["price_raw"] = price_text
            price_match = re.search(r"\$\s*([\d,.]+)", price_text)
            data["price_numerical"] = float(price_match.group(1).replace(",", "")) if price_match else None

        # Empty fields not in iCopify table
        data["country"] = None
        data["date_added"] = None
        data["publisher"] = None
        data["scraped_at"] = datetime.utcnow()

        return data

    def _parse_card_layout(self, soup):
        """
        Fallback parser for card-based layouts (if table isn't found).
        """
        publishers = []

        # Find divs/cards containing publisher info
        cards = soup.find_all("div", class_=re.compile(r"card|publisher|site|item", re.I))

        for card in cards:
            try:
                data = {}
                link = card.find("a", href=True)
                if link:
                    data["website_url"] = link.get_text(strip=True)

                # Try to extract other fields from card text
                card_text = card.get_text()

                dr_match = re.search(r"DR\s*(\d+)", card_text)
                if dr_match:
                    data["ahrefs_dr"] = int(dr_match.group(1))

                da_match = re.search(r"DA\s*(\d+)", card_text)
                if da_match:
                    data["moz_da"] = int(da_match.group(1))

                traffic_match = re.search(r"([\d,]+)\s*(?:traffic|visits)", card_text, re.I)
                if traffic_match:
                    data["monthly_traffic"] = int(traffic_match.group(1).replace(",", ""))

                price_match = re.search(r"\$([\d,.]+)", card_text)
                if price_match:
                    data["price_usd"] = float(price_match.group(1).replace(",", ""))

                data["scraped_at"] = datetime.now().isoformat()

                if data.get("website_url"):
                    publishers.append(data)
            except Exception:
                continue

        return publishers

    # ─── URL Building ────────────────────────────────────────────────────────────

    def _build_marketplace_url(self, page=1):
        """Build the marketplace URL with filters and pagination."""
        params = [f"page={page}"]

        # Apply filters from config
        filter_mapping = {
            "DAFrom": "DAFrom",
            "DATo": "DATo",
            "DRFrom": "DRFrom",
            "DRTo": "DRTo",
            "PriceFrom": "PriceFrom",
            "PriceTo": "PriceTo",
            "TrafficFrom": "TrafficFrom",
            "TrafficTo": "TrafficTo",
        }

        for config_key, param_key in filter_mapping.items():
            value = FILTERS.get(config_key)
            if value is not None:
                params.append(f"{param_key}={value}")

        query_string = "&".join(params)
        return f"{MARKETPLACE_URL}?{query_string}"

    # ─── Main Scraping Logic ─────────────────────────────────────────────────────

    def scrape_page(self, page_num):
        """
        Scrape a single page with retry logic.
        
        Args:
            page_num: Page number to scrape.
            
        Returns:
            list[dict]: Parsed publisher data from the page.
        """
        url = self._build_marketplace_url(page_num)

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)

                # Check for redirect to login (session expired)
                if resp.status_code in (301, 302):
                    location = resp.headers.get("Location", "")
                    if "login" in location.lower():
                        logger.warning(f"{Fore.YELLOW}⚠ Session expired! Refreshing...")
                        if not self.refresh_session():
                            logger.error(f"{Fore.RED}✗ Failed to refresh session.")
                            return []
                        continue

                # Check if we're on the login page
                if resp.status_code == 200 and "Log in" in resp.text[:2000] and 'name="email"' in resp.text[:5000]:
                    logger.warning(f"{Fore.YELLOW}⚠ Redirected to login page. Refreshing session...")
                    if not self.refresh_session():
                        logger.error(f"{Fore.RED}✗ Failed to refresh session.")
                        return []
                    continue

                resp.raise_for_status()
                return self.parse_marketplace_page(resp.text)

            except requests.RequestException as e:
                logger.warning(
                    f"{Fore.YELLOW}⚠ Attempt {attempt}/{MAX_RETRIES} failed for page {page_num}: {e}"
                )
                if attempt < MAX_RETRIES:
                    wait_time = REQUEST_DELAY * attempt
                    time.sleep(wait_time)
                else:
                    logger.error(f"{Fore.RED}✗ All retries exhausted for page {page_num}.")
                    return []

        return []

    def scrape_all(self, latest_only=False):
        """
        Scrape all pages from the marketplace.
        
        Args:
            latest_only: If True, stop when encountering previously scraped URLs.
        """
        logger.info(f"\n{Fore.CYAN}{'='*60}")
        logger.info(f"{Fore.CYAN}  iCopify Publishers Marketplace Scraper")
        logger.info(f"{Fore.CYAN}  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{Fore.CYAN}{'='*60}\n")

        # Step 1: Authenticate
        if not self.refresh_session():
            logger.error(f"{Fore.RED}✗ Authentication failed. Cannot proceed.")
            return False

        # Load previously scraped URLs for "latest" mode
        known_urls = set()
        if latest_only:
            known_urls = self._load_known_urls()
            logger.info(f"{Fore.BLUE}📋 Loaded {len(known_urls):,} previously scraped URLs.")

        # Step 2: Scrape first page to get totals
        logger.info(f"{Fore.CYAN}▶ Fetching first page to determine total count...")
        first_page_data = self.scrape_page(1)

        if not first_page_data:
            logger.error(f"{Fore.RED}✗ Failed to scrape first page. Check login/connection.")
            return False

        self.all_data.extend(first_page_data)

        # Determine total pages to scrape
        total = self.total_pages or 1
        if self.max_pages:
            total = min(total, self.max_pages)

        logger.info(f"{Fore.GREEN}✓ Page 1/{total} scraped: {len(first_page_data)} publishers")

        # Step 3: Scrape remaining pages
        stop_scraping = False
        for page_num in range(2, total + 1):
            if stop_scraping:
                break

            # Rate limiting
            time.sleep(REQUEST_DELAY)

            page_data = self.scrape_page(page_num)

            if not page_data:
                logger.warning(f"{Fore.YELLOW}⚠ No data on page {page_num}, skipping...")
                continue

            # "Latest only" mode: stop if we encounter known URLs
            new_data_run = []
            consecutive_exists = 0
            for pub in page_data:
                # Upsert to Database
                is_new = self.upsert_listing(pub)
                
                if is_new:
                    new_data_run.append(pub)
                    consecutive_exists = 0
                else:
                    consecutive_exists += 1
                
                # If we've seen 10 existing items in a row in latest mode, stop
                if latest_only and consecutive_exists >= 10:
                    logger.info(f"{Fore.YELLOW}⚑ Reached 10 consecutive existing items. Stopping.")
                    stop_scraping = True
                    break
            
            self.all_data.extend(new_data_run)

            # Progress bar
            pct = (page_num / total) * 100
            bar_len = 30
            filled = int(bar_len * page_num / total)
            bar = "█" * filled + "░" * (bar_len - filled)
            sys.stdout.write(
                f"\r{Fore.GREEN}  [{bar}] {pct:.1f}% "
                f"| Page {page_num}/{total} "
                f"| Total scraped: {len(self.all_data):,}"
            )
            sys.stdout.flush()

            # Save cookies periodically (every 50 pages)
            if page_num % 50 == 0:
                self.save_cookies()

        print()  # New line after progress bar
        logger.info(f"\n{Fore.GREEN}✓ Scraping complete! Total publishers: {len(self.all_data):,}")

        # Step 4: Save cookies
        self.save_cookies()

        return True

    # ─── Data Export ─────────────────────────────────────────────────────────────

    def export_to_csv(self):
        """Export scraped data to a timestamped CSV file."""
        if not self.all_data:
            logger.warning(f"{Fore.YELLOW}⚠ No data to export.")
            return None

        df = pd.DataFrame(self.all_data)

        # Reorder columns
        column_order = [
            "website_url", "website_link", "categories", "monthly_traffic",
            "ahrefs_dr", "moz_da", "language", "price_usd",
            "max_dofollow_links", "turnaround_time", "publisher_role",
            "buy_post_url", "scraped_at"
        ]
        # Only include columns that exist
        existing_cols = [c for c in column_order if c in df.columns]
        # Add any extra columns not in the order
        extra_cols = [c for c in df.columns if c not in column_order]
        df = df[existing_cols + extra_cols]

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_prefix}_{timestamp}.csv"
        filepath = os.path.join(DATA_DIR, filename)

        df.to_csv(filepath, index=False, encoding="utf-8-sig")

        logger.info(f"\n{Fore.GREEN}✓ Data exported to: {filepath}")
        logger.info(f"{Fore.BLUE}  📊 Rows: {len(df):,}")
        logger.info(f"{Fore.BLUE}  📊 Columns: {', '.join(df.columns.tolist())}")

        # Print summary stats
        if "price_usd" in df.columns:
            logger.info(f"{Fore.BLUE}  💰 Price range: ${df['price_usd'].min():.2f} - ${df['price_usd'].max():.2f}")
        if "moz_da" in df.columns:
            logger.info(f"{Fore.BLUE}  📈 DA range: {df['moz_da'].min()} - {df['moz_da'].max()}")
        if "ahrefs_dr" in df.columns:
            logger.info(f"{Fore.BLUE}  📈 DR range: {df['ahrefs_dr'].min()} - {df['ahrefs_dr'].max()}")

        return filepath

    def _load_known_urls(self):
        """Load URLs from the most recent CSV export for deduplication."""
        known = set()
        try:
            csv_files = sorted(
                Path(DATA_DIR).glob(f"{self.output_prefix}_*.csv"),
                key=lambda x: x.stat().st_mtime,
                reverse=True,
            )
            if csv_files:
                latest_csv = csv_files[0]
                df = pd.read_csv(latest_csv)
                if "website_url" in df.columns:
                    known = set(df["website_url"].dropna().tolist())
                logger.info(f"{Fore.BLUE}📂 Loaded known URLs from: {latest_csv.name}")
        except Exception as e:
            logger.warning(f"Could not load known URLs: {e}")
        return known


# ─── CLI Entry Point ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="iCopify Publishers Marketplace Scraper",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scraper.py                    Scrape all publishers (all pages)
  python scraper.py --pages 10         Scrape first 10 pages only
  python scraper.py --latest           Fetch only new data since last scrape
  python scraper.py --output my_data   Custom output filename prefix
        """
    )
    parser.add_argument(
        "--pages", type=int, default=None,
        help="Maximum number of pages to scrape (default: all)"
    )
    parser.add_argument(
        "--latest", action="store_true",
        help="Only fetch new data since the last scrape"
    )
    parser.add_argument(
        "--output", type=str, default=None,
        help="Output CSV filename prefix"
    )
    parser.add_argument(
        "--login-only", action="store_true",
        help="Only perform login and save cookies (useful for testing auth)"
    )

    args = parser.parse_args()

    # Initialize scraper
    scraper = ICopifyScraper(
        max_pages=args.pages,
        output_prefix=args.output
    )

    if args.login_only:
        if scraper.refresh_session():
            logger.info(f"{Fore.GREEN}✓ Login successful! Cookies saved.")
        else:
            logger.error(f"{Fore.RED}✗ Login failed!")
            sys.exit(1)
        return

    # Run scraper
    start_time = time.time()
    success = scraper.scrape_all(latest_only=args.latest)

    if success:
        filepath = scraper.export_to_csv()
        elapsed = time.time() - start_time
        logger.info(f"\n{Fore.CYAN}⏱  Total time: {elapsed:.1f}s")
        if filepath:
            logger.info(f"{Fore.GREEN}🎉 Done! CSV saved to: {filepath}")
    else:
        logger.error(f"{Fore.RED}✗ Scraping failed!")
        sys.exit(1)


if __name__ == "__main__":
    main()
