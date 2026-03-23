#!/usr/bin/env python3
import os
import re
import sys
import json
import time
import logging
import argparse
from datetime import datetime

import requests
from colorama import Fore, Style, init as colorama_init
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils import normalize_domain
from models import Base, PublisherListing

# ─── Initialize colorama ────────────────────────────────────────────────────────
colorama_init(autoreset=True)

# ─── Import config ───────────────────────────────────────────────────────────────
from config import (
    EMAIL, PASSWORD, LOGIN_URL, MARKETPLACE_API_URL,
    REQUEST_TIMEOUT, MAX_PAGES, PAGE_SIZE, DATABASE_URL
)

# ─── Logging Setup ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f'{Fore.MAGENTA}%(asctime)s{Style.RESET_ALL} | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)


class PressScapeScraper:
    """
    Scraper for PressScape.com — a Next.js marketplace with a clean JSON API.

    API endpoints discovered:
        POST /api/auth/login     → JSON login (email/password)
        GET  /api/marketplace    → paginated JSON catalog (page, limit)
        GET  /api/auth/check     → session validation
    """

    def __init__(self, max_pages=None):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "application/json",
            "Content-Type": "application/json",
            "Referer": "https://pressscape.com/marketplace",
        })
        self.max_pages = max_pages or MAX_PAGES
        self.host_site = "pressscape.com"
        self.all_data = []
        self.total_listings = 0

        # Database Setup
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def login(self):
        logger.info(f"{Fore.CYAN}▶ Logging in to PressScape.com (JSON API)...")
        try:
            payload = {
                "email": EMAIL,
                "password": PASSWORD
            }
            resp = self.session.post(
                LOGIN_URL,
                json=payload,
                timeout=REQUEST_TIMEOUT
            )

            if resp.status_code == 200:
                data = resp.json()
                if data.get("success"):
                    user = data.get("user", {})
                    logger.info(f"{Fore.GREEN}✓ Login successful! User: {user.get('name', 'N/A')}")
                    return True
                else:
                    logger.error(f"{Fore.RED}✗ Login failed: {data}")
                    return False
            else:
                logger.error(f"{Fore.RED}✗ Login failed. HTTP {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"{Fore.RED}✗ Login error: {e}")
            return False

    def fetch_page(self, page=1):
        """Fetch a page of marketplace listings from the JSON API."""
        params = {
            "page": page,
            "limit": PAGE_SIZE,
        }
        try:
            resp = self.session.get(
                MARKETPLACE_API_URL,
                params=params,
                timeout=REQUEST_TIMEOUT
            )
            if resp.status_code == 200:
                data = resp.json()
                if self.total_listings == 0:
                    pagination = data.get("pagination", {})
                    self.total_listings = pagination.get("total", 0)
                    total_pages = pagination.get("totalPages", 0)
                    logger.info(f"{Fore.BLUE}📊 Total listings: {self.total_listings:,} across {total_pages} pages")
                return data.get("websites", [])
            elif resp.status_code in (401, 403):
                logger.warning(f"{Fore.YELLOW}⚠ Session expired. Re-authenticating...")
                if self.login():
                    return self.fetch_page(page)
                return None
            else:
                logger.error(f"{Fore.RED}✗ API returned HTTP {resp.status_code}")
                return None
        except Exception as e:
            logger.error(f"{Fore.RED}✗ API fetch error: {e}")
            return None

    def parse_item(self, item):
        """Transform an API item into our standard format."""
        domain = item.get("domain", "")
        if not domain:
            return None

        # Price: API returns price in cents (e.g., 20000 = $200.00)
        price_cents = item.get("price_guest_post") or 0
        price_usd = price_cents / 100.0 if price_cents > 0 else 0.0

        return {
            "website_url": domain,
            "item_id": item.get("id", ""),
            "host_site": self.host_site,
            "price_raw": f"${price_usd:.2f} USD" if price_usd > 0 else "N/A",
            "price_numerical": price_usd,
            "moz_da": item.get("domain_authority") or 0,
            "ahrefs_dr": item.get("domain_rating") or 0,
            "traffic": item.get("organic_traffic") or 0,
            "language": item.get("primary_language"),
            "country": item.get("traffic_country_1"),
            "category": item.get("category"),
        }

    def upsert_listing(self, item_data):
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

                for field in ['moz_da', 'ahrefs_dr', 'traffic']:
                    val = item_data.get(field)
                    if val and (not getattr(existing, field) or val > getattr(existing, field)):
                        setattr(existing, field, val)

                existing.updated_at = datetime.utcnow()
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
                    'created_at': datetime.utcnow(),
                    'updated_at': datetime.utcnow()
                }
                db_session.add(PublisherListing(**db_item))
                is_new_host_entry = True

            db_session.commit()
            return is_new_host_entry
        except SQLAlchemyError as e:
            db_session.rollback()
            logger.error(f"{Fore.RED}✗ DB error: {e}")
            return False
        finally:
            db_session.close()

    def scrape(self, latest_only=False):
        if not self.login():
            return

        logger.info(f"{Fore.CYAN}▶ Starting PressScape scrape (JSON API)...")

        for page in range(1, self.max_pages + 1):
            logger.info(f"{Fore.BLUE}📄 Fetching page {page}...")

            items = self.fetch_page(page)
            if items is None or len(items) == 0:
                logger.info(f"{Fore.YELLOW}⚠ No more results at page {page}.")
                break

            new_count = 0
            existing_count = 0
            for raw_item in items:
                parsed = self.parse_item(raw_item)
                if not parsed:
                    continue
                is_new = self.upsert_listing(parsed)
                if is_new:
                    new_count += 1
                    self.all_data.append(parsed)
                else:
                    existing_count += 1

            logger.info(f"{Fore.GREEN}✓ Page {page} done: {new_count} new, {existing_count} existing.")

            if latest_only and existing_count >= 10:
                logger.info(f"{Fore.YELLOW}⚑ Deduplication threshold met. Stopping.")
                break

            time.sleep(1)  # Politeness

        logger.info(f"{Fore.GREEN}🏁 Finished! Scraped {len(self.all_data)} PressScape items total.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest", action="store_true", help="Stop after encountering duplicates")
    parser.add_argument("--pages", type=int, help="Max pages to scrape")
    args = parser.parse_args()

    scraper = PressScapeScraper(max_pages=args.pages)
    scraper.scrape(latest_only=args.latest)
