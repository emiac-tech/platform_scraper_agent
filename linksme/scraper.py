#!/usr/bin/env python3
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
from config import EMAIL, PASSWORD, LOGIN_URL, CATALOG_URL, REQUEST_TIMEOUT, MAX_PAGES, PAGE_SIZE, DATABASE_URL

# ─── Logging Setup ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f'{Fore.YELLOW}%(asctime)s{Style.RESET_ALL} | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class LinksMeScraper:
    def __init__(self, max_pages=None):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        self.max_pages = max_pages or MAX_PAGES
        self.host_site = "links.me"
        self.all_data = []

        # Database Setup
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def login(self):
        logger.info(f"{Fore.CYAN}▶ Logging in to Links.me...")
        try:
            # Get CSRF token from login page
            r1 = self.session.get("https://app.links.me/login", timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(r1.text, "lxml")
            csrf_token_meta = soup.find("meta", {"name": "csrf-token"})
            if not csrf_token_meta:
                logger.error("Could not find CSRF token.")
                return False
            
            csrf_token = csrf_token_meta["content"]
            
            payload = {
                "_token": csrf_token,
                "email": EMAIL,
                "password": PASSWORD,
                "previous_url": "https://app.links.me"
            }
            
            resp = self.session.post(LOGIN_URL, data=payload, timeout=REQUEST_TIMEOUT)
            
            # Check for success
            if resp.status_code == 200:
                logger.info(f"{Fore.GREEN}✓ Login successful (or AJAX accepted)!")
                return True
            else:
                logger.error(f"{Fore.RED}✗ Login failed. Status: {resp.status_code}")
                return False
        except Exception as e:
            logger.error(f"{Fore.RED}✗ Login error: {e}")
            return False

    def upsert_listing(self, item_data):
        db_session = self.SessionLocal()
        try:
            domain = normalize_domain(item_data.get('website_url', ''))
            if not domain: return False

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
                    while len(cats) <= idx: cats.append(None)
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

                # Update metrics if better
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

    def parse_page(self, html):
        soup = BeautifulSoup(html, "lxml")
        rows = soup.find_all("tr")
        results = []
        seen_ids = set()  # Deduplicate: each row has multiple data-domain elements
        
        for row in rows:
            try:
                domain_tag = row.find(attrs={"data-domain": True})
                if not domain_tag:
                    continue
                
                platform_id = domain_tag["data-platform"]
                if platform_id in seen_ids:
                    continue
                seen_ids.add(platform_id)
                
                domain_name = domain_tag["data-domain"]
                platform_id = domain_tag["data-platform"]
                
                # Default values for robustness
                data = {
                    "website_url": domain_name,
                    "item_id": platform_id,
                    "host_site": self.host_site,
                    "price_raw": "N/A",
                    "price_numerical": 0.0,
                    "moz_da": 0,
                    "ahrefs_dr": 0,
                    "traffic": 0,
                    "scraped_at": datetime.utcnow()
                }
                
                # Metrics parsing
                spans = row.find_all("span")
                numeric_spans = [s.get_text(strip=True) for s in spans if re.search(r'^\d+$', s.get_text(strip=True))]
                
                if len(numeric_spans) >= 2:
                    data["moz_da"] = int(numeric_spans[0])
                    data["ahrefs_dr"] = int(numeric_spans[1])
                
                # Traffic
                traffic_text = ""
                for s in spans:
                    t = s.get_text(strip=True)
                    if "K" in t or "M" in t:
                        if re.search(r'[\d.]+[KBM]', t):
                            traffic_text = t
                            break
                
                if traffic_text:
                    try:
                        mult = 1
                        if "K" in traffic_text: mult = 1000
                        elif "M" in traffic_text: mult = 1000000
                        num_match = re.search(r'([\d.]+)', traffic_text)
                        if num_match:
                            data["traffic"] = int(float(num_match.group(1)) * mult)
                    except (ValueError, TypeError):
                        pass

                # Price
                cost_block = row.find(class_=re.compile("js-cost-block"))
                if cost_block:
                    p_text = cost_block.get_text(strip=True)
                    if p_text:
                        data["price_raw"] = p_text
                        # Extract number
                        p_match = re.search(r'([\d,. ]+)', p_text)
                        if p_match:
                            try:
                                p_str = p_match.group(1).replace(' ', '').replace(',', '.')
                                data["price_numerical"] = float(p_str)
                            except ValueError:
                                pass
                
                if data.get("website_url"):
                    results.append(data)
            except Exception as e:
                logger.debug(f"Error parsing row: {e}")
                continue
        return results

    def scrape(self, latest_only=False):
        if not self.login(): return
        
        logger.info(f"{Fore.CYAN}▶ Starting Links.me scrape...")
        
        for page in range(1, self.max_pages + 1):
            logger.info(f"{Fore.BLUE}📄 Scraping page {page}...")
            
            url = f"{CATALOG_URL}?sort=tr-desc&per_page={PAGE_SIZE}&page={page}"
            
            try:
                resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                
                # Check for "catalog" in text to confirm we are logged in
                if "catalog" not in resp.url and "login" in resp.url:
                    logger.error("Session lost! Redirected to login.")
                    break
                    
                page_data = self.parse_page(resp.text)
                if not page_data:
                    logger.info(f"{Fore.YELLOW}⚠ No more results found at page {page}.")
                    break
                
                new_count = 0
                existing_count = 0
                for item in page_data:
                    is_new = self.upsert_listing(item)
                    if is_new:
                        new_count += 1
                        self.all_data.append(item)
                    else:
                        existing_count += 1
                
                logger.info(f"{Fore.GREEN}✓ Page {page} done: {new_count} new, {existing_count} existing.")
                
                if latest_only and existing_count >= 10:
                    logger.info(f"{Fore.YELLOW}⚑ Deduplication threshold met. Stopping.")
                    break
                    
                time.sleep(2)
                
            except Exception as e:
                logger.error(f"{Fore.RED}✗ Error on page {page}: {e}")
                break

        logger.info(f"{Fore.GREEN}🏁 Finished! Scraped {len(self.all_data)} items.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest", action="store_true", help="Stop after encountering duplicates")
    parser.add_argument("--pages", type=int, help="Max pages to scrape")
    args = parser.parse_args()
    
    scraper = LinksMeScraper(max_pages=args.pages)
    scraper.scrape(latest_only=args.latest)
