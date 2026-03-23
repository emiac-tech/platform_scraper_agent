#!/usr/bin/env python3
"""
Posticy Publisher Marketplace Scraper
======================================
Scrapes guest posting marketplace data from marketplace.posticy.com.
Uses the internal JSON API for high efficiency and reliability.
Handles login, CSRF tokens, session persistence, pagination, and deduplication.

Usage:
    python scraper.py                    # Scrape all publishers
    python scraper.py --items 500        # Scrape first 500 items
    python scraper.py --latest           # Fetch only new data since last scrape
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
import time
import logging
import argparse
from datetime import datetime
from colorama import Fore, Style, init as colorama_init
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

import sys
sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils import normalize_domain

# ─── Initialize colorama ────────────────────────────────────────────────────────
colorama_init(autoreset=True)

# ─── Import config ───────────────────────────────────────────────────────────────
from config import (
    USERNAME, PASSWORD, BASE_URL, LOGIN_URL, MARKETPLACE_URL, API_LISTING_URL,
    REQUEST_DELAY, MAX_RETRIES, REQUEST_TIMEOUT, MARKETER_ID, PAGE_SIZE,
    DATA_DIR, COOKIES_DIR, COOKIE_FILE, OUTPUT_PREFIX, DEFAULT_PARAMS,
    DATABASE_URL
)

# ─── DB Declaration ───────────────────────────────────────────────────────────────
from models import Base, PublisherListing

# ─── Logging Setup ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f'{Fore.CYAN}%(asctime)s{Style.RESET_ALL} | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class PosticyScraper:
    """
    Scraper for the Posticy Publisher Marketplace.
    
    Features:
        - Authenticated API querying
        - Automatic CSRF token extraction/management
        - Incremental scraping using 'date_added'
        - Parallel processing support (in potential future upgrades)
        - Session persistence via persistent cookies
    """

    HEADERS = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Language": "en-US,en;q=0.9",
        "X-Requested-With": "XMLHttpRequest",
        "Connection": "keep-alive",
        "Referer": MARKETPLACE_URL,
    }

    def __init__(self, max_items=None, output_prefix=None):
        self.session = requests.Session()
        self.session.headers.update(self.HEADERS)
        self.max_items = max_items
        self.output_prefix = output_prefix or OUTPUT_PREFIX
        self.all_data = []
        self.csrf_token = None
        self.total_records = None
        self.marketer_id = MARKETER_ID  # Default from config
        self.host_site = "posticy.com"

        # Database Setup
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

        # Ensure directories exist
        os.makedirs(DATA_DIR, exist_ok=True)
        os.makedirs(COOKIES_DIR, exist_ok=True)

    # ─── Session Management ──────────────────────────────────────────────────────

    def save_cookies(self):
        """Persist session cookies for reuse across runs."""
        cookies_dict = {}
        for cookie in self.session.cookies:
            cookies_dict[cookie.name] = {
                "value": cookie.value,
                "domain": cookie.domain,
                "path": cookie.path,
                "expires": cookie.expires,
            }
        # Save CSRF token as well
        if self.csrf_token:
            cookies_dict["_INTERNAL_CSRF_TOKEN"] = {"value": self.csrf_token}
            
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

            # Extract CSRF token if present
            if "_INTERNAL_CSRF_TOKEN" in cookies_dict:
                self.csrf_token = cookies_dict.pop("_INTERNAL_CSRF_TOKEN")["value"]
                self.session.headers.update({"X-CSRF-TOKEN": self.csrf_token})

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
        """Check if current session is authenticated by hitting the marketplace."""
        try:
            resp = self.session.get(
                MARKETPLACE_URL,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=False
            )
            if resp.status_code == 200:
                if "logout" in resp.text.lower() or "marketer" in resp.text.lower():
                    # Extract fresh CSRF and Marketer ID from page
                    self.extract_csrf_from_page(resp.text)
                    self._extract_marketer_id(resp.text)
                    return True
            return False
        except requests.RequestException:
            return False

    def _extract_marketer_id(self, html):
        """Extract marketer_id from script tags in the page."""
        # Look for marketer_id: "24555" or similar
        match = re.search(r'marketer_id:\s*[\'"](\d+)[\'"]', html)
        if match:
            self.marketer_id = int(match.group(1))
            logger.debug(f"Dynamic Marketer ID extracted: {self.marketer_id}")
            return True
        
        # Fallback to config if not found
        self.marketer_id = MARKETER_ID
        return False

    def extract_csrf_from_page(self, html):
        """Find the CSRF token in meta tags."""
        soup = BeautifulSoup(html, "lxml")
        meta = soup.find("meta", {"name": "csrf-token"})
        if meta:
            self.csrf_token = meta.get("content")
            self.session.headers.update({"X-CSRF-TOKEN": self.csrf_token})
            logger.debug(f"Fresh CSRF token extracted: {self.csrf_token[:15]}...")
            return True
        return False

    def refresh_session(self):
        """Refresh using saved cookies or perform full login."""
        logger.info(f"{Fore.YELLOW}↻ Attempting session refresh...")

        if self.load_cookies():
            if self.is_session_valid():
                logger.info(f"{Fore.GREEN}✓ Session refreshed successfully via saved cookies!")
                return True
            else:
                logger.info(f"{Fore.YELLOW}⚠ Saved cookies expired. Performing full login...")

        return self.login()

    def login(self):
        """Perform a full login to Posticy."""
        logger.info(f"{Fore.CYAN}▶ Logging in to Posticy...")

        try:
            # 1. Get login page for initial CSRF
            login_page = self.session.get(LOGIN_URL, timeout=REQUEST_TIMEOUT)
            login_page.raise_for_status()
            self.extract_csrf_from_page(login_page.text)

            if not self.csrf_token:
                logger.error(f"{Fore.RED}✗ Could not find CSRF token for login!")
                return False

            # 2. Submit credentials
            login_data = {
                "_token": self.csrf_token,
                "email": USERNAME,
                "password": PASSWORD,
                "remember": "on",
            }

            resp = self.session.post(
                LOGIN_URL,
                data=login_data,
                timeout=REQUEST_TIMEOUT,
                allow_redirects=True,
            )

            # Verify if login led us to dashboard or marketplace
            if resp.status_code == 200:
                if "logout" in resp.text.lower() or "dashboard" in resp.text.lower():
                    logger.info(f"{Fore.GREEN}✓ Login successful!")
                    self.extract_csrf_from_page(resp.text)
                    self.save_cookies()
                    return True
                else:
                    logger.error(f"{Fore.RED}✗ Login failed: Credentials or CSRF issue.")
                    return False
            
            return False

        except Exception as e:
            logger.error(f"{Fore.RED}✗ Login failed with exception: {e}")
            return False

    # ─── Data Scraping ───────────────────────────────────────────────────────────

    def fetch_batch(self, start=0, length=PAGE_SIZE):
        """Fetch a batch of publisher listings from the API."""
        params = DEFAULT_PARAMS.copy()
        params.update({
            "start": start,
            "length": length,
            "marketer_id": self.marketer_id,
            "draw": int(time.time() * 1000) % 1000000  # DataTables dummy param
        })

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = self.session.get(
                    API_LISTING_URL,
                    params=params,
                    timeout=REQUEST_TIMEOUT
                )

                # Check if session expired mid-run
                if resp.status_code in (401, 419, 302):
                    logger.warning(f"{Fore.YELLOW}⚠ Session expired! Re-authenticating...")
                    if not self.refresh_session():
                        return None
                    continue

                resp.raise_for_status()
                data = resp.json()
                
                if "data" in data:
                    if self.total_records is None:
                        self.total_records = data.get("recordsTotal", 0)
                        logger.info(f"{Fore.BLUE}📊 Total available publishers: {self.total_records:,}")
                    return data["data"]
                    
                return []

            except (requests.RequestException, json.JSONDecodeError) as e:
                logger.warning(f"{Fore.YELLOW}⚠ API fetch failed (attempt {attempt}/{MAX_RETRIES}): {e}")
                if attempt < MAX_RETRIES:
                    time.sleep(REQUEST_DELAY * attempt)
                else:
                    return None
        return None

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
                # Update existing record
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

                # Update best metrics
                if item_data.get('moz_da') and (not existing.moz_da or item_data['moz_da'] > existing.moz_da):
                    existing.moz_da = item_data['moz_da']
                if item_data.get('ahrefs_dr') and (not existing.ahrefs_dr or item_data['ahrefs_dr'] > existing.ahrefs_dr):
                    existing.ahrefs_dr = item_data['ahrefs_dr']
                if item_data.get('traffic') and (not existing.traffic or item_data['traffic'] > existing.traffic):
                    existing.traffic = item_data['traffic']

                existing.updated_at = datetime.utcnow()
            else:
                # Create brand new record
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
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
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

    def _parse_item(self, item):
        """Clean up API item data for persistence."""
        clean_item = {}
        
        # 1. Clean Domain (handle HTML-wrapped domain names)
        domain_raw = item.get("domain", "")
        if "<a" in domain_raw:
            match = re.search(r'>([^<]+)</a>', domain_raw)
            if match:
                clean_item["website_url"] = match.group(1).strip()
            else:
                clean_item["website_url"] = BeautifulSoup(domain_raw, "lxml").get_text(strip=True)
        else:
            clean_item["website_url"] = domain_raw.strip()

        # 2. Map other fields
        clean_item["item_id"] = item.get("id")
        clean_item["host_site"] = self.host_site
        clean_item["category"] = item.get("category")
        
        # Price (remove Euro symbol or currency if needed)
        price_raw = item.get("price", "€ 0")
        clean_item["price_raw"] = price_raw
        price_match = re.search(r'([\d.,]+)', price_raw.replace("\xa0", " "))
        if price_match:
            clean_item["price_numerical"] = float(price_match.group(1).replace(",", "."))
        
        clean_item["moz_da"] = item.get("moz_da")
        clean_item["ahrefs_dr"] = item.get("ahrefs_dr")
        
        # Traffic (map sr_traffic or estimated_visits)
        traffic_raw = item.get("sr_traffic", 0)
        if traffic_raw and str(traffic_raw).isdigit():
            clean_item["traffic"] = int(traffic_raw)
        else:
            clean_item["traffic"] = 0
            
        clean_item["language"] = item.get("language")
        
        # Clean Country (Extract from flag icon HTML)
        country_raw = item.get("country", "")
        if "<div" in country_raw or "<span" in country_raw:
            # Try to find 'title="... "'
            title_match = re.search(r'title="([^"]+)"', country_raw)
            if title_match:
                clean_item["country"] = title_match.group(1)
            else:
                # Fallback to flag-icon-XX
                flag_match = re.search(r'flag-icon-([a-z]{2})', country_raw)
                if flag_match:
                    clean_item["country"] = flag_match.group(1).upper()
                else:
                    clean_item["country"] = BeautifulSoup(country_raw, "lxml").get_text(strip=True)
        else:
            clean_item["country"] = country_raw.strip()

        clean_item["date_added"] = item.get("date_added")  # Format: DD.MM.YY
        clean_item["publisher"] = BeautifulSoup(item.get("publisher", ""), "lxml").get_text(strip=True)
        
        clean_item["scraped_at"] = datetime.utcnow()
        
        return clean_item

    def scrape_all(self, latest_only=False):
        """Main scraping loop."""
        logger.info(f"\n{Fore.CYAN}{'='*60}")
        logger.info(f"{Fore.CYAN}  Posticy Publisher Marketplace Scraper")
        logger.info(f"{Fore.CYAN}  Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info(f"{Fore.CYAN}{'='*60}\n")

        # Step 1: Login
        if not self.refresh_session():
            logger.error(f"{Fore.RED}✗ Authentication failed. Cannot proceed.")
            return False

        # Load known items for deduplication
        known_ids = set()
        if latest_only:
            known_ids = self._load_known_ids()
            logger.info(f"{Fore.BLUE}📋 Loaded {len(known_ids):,} previously scraped items for delta.")

        # Step 2: Main loop
        start = 0
        limit = self.max_items if self.max_items else 1000000
        total_successfully_scraped = 0
        
        while start < limit:
            logger.info(f"{Fore.CYAN}▶ Fetching items {start} to {start + PAGE_SIZE}...")
            batch = self.fetch_batch(start=start, length=min(PAGE_SIZE, limit - start))
            
            if batch is None:
                logger.error(f"{Fore.RED}✗ Serious API error. Aborting flow.")
                break
                
            if not batch:
                logger.info(f"{Fore.GREEN}✓ Reached end of results pool.")
                break

            new_items_count_run = 0
            existing_consecutive = 0
            
            for raw_item in batch:
                item = self._parse_item(raw_item)
                
                if latest_only and item.get("item_id") in known_ids:
                    logger.info(f"{Fore.YELLOW}⚐ Found previously scraped item (ID: {item.get('item_id')}). Delta complete.")
                    start = limit  # Trigger exit from loop
                    break
                
                # Upsert to Database
                is_new = self.upsert_listing(item)
                
                if is_new:
                    self.all_data.append(item)
                    new_items_count_run += 1
                    total_successfully_scraped += 1
                    existing_consecutive = 0
                else:
                    existing_consecutive += 1
                
                # If we are in 'latest' mode and find 10 existing items in a row, we can probably stop
                if latest_only and existing_consecutive >= 10:
                    logger.info(f"{Fore.YELLOW}⚐ Reached 10 consecutive existing items. Delta complete.")
                    start = limit
                    break

            # Progress update
            if self.total_records:
                pct = (total_successfully_scraped / self.total_records) * 100
                logger.info(f"{Fore.GREEN}  + Scraped {new_items_count_run} new items. Progress: {pct:.1f}%")
            else:
                logger.info(f"{Fore.GREEN}  + Scraped {new_items_count_run} new items.")

            if start >= limit: break # Early exit check

            start += PAGE_SIZE
            time.sleep(REQUEST_DELAY)

        logger.info(f"\n{Fore.GREEN}✓ Total items scraped this run: {len(self.all_data):,}")
        return True

    # ─── Export & Delta tracking ────────────────────────────────────────────────

    def export_to_csv(self):
        """Export to a clean CSV."""
        if not self.all_data:
            logger.warning("No data to export.")
            return None

        df = pd.DataFrame(self.all_data)
        
        # Sort by date added (convert DD.MM.YY to datetime temporarily for sorting)
        try:
            df["temp_date"] = pd.to_datetime(df["date_added"], format="%d.%m.%y", errors='coerce')
            df = df.sort_values(by="temp_date", ascending=False).drop(columns=["temp_date"])
        except Exception:
            pass

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.output_prefix}_{timestamp}.csv"
        filepath = os.path.join(DATA_DIR, filename)

        df.to_csv(filepath, index=False, encoding="utf-8-sig")

        logger.info(f"\n{Fore.GREEN}✓ Data exported to: {filepath}")
        return filepath

    def _load_known_ids(self):
        """Get IDs from the latest CSV to avoid duplicate scraping."""
        ids = set()
        try:
            csv_files = sorted(
                Path(DATA_DIR).glob(f"{self.output_prefix}_*.csv"),
                key=lambda x: x.stat().st_mtime,
                reverse=True,
            )
            if csv_files:
                df = pd.read_csv(csv_files[0])
                if "item_id" in df.columns:
                    ids = set(df["item_id"].dropna().tolist())
        except Exception as e:
            logger.warning(f"Could not load historical IDs: {e}")
        return ids


# ─── Execution ──────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Posticy Marketplace Scraper")
    parser.add_argument("--items", type=int, help="Max items to scrape")
    parser.add_argument("--latest", action="store_true", help="Only scrape items not in previous CSV")
    parser.add_argument("--output", help="Prefix for output CSV")
    
    args = parser.parse_args()

    scraper = PosticyScraper(max_items=args.items, output_prefix=args.output)
    
    start_time = time.time()
    if scraper.scrape_all(latest_only=args.latest):
        filepath = scraper.export_to_csv()
        elapsed = time.time() - start_time
        logger.info(f"\n{Fore.CYAN}⏱  Total time: {elapsed:.1f}s")
        if filepath:
            logger.info(f"{Fore.GREEN}🎉 Bot completed successfully.")
    else:
        sys.exit(1)

if __name__ == "__main__":
    main()
