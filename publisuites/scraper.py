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
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

sys.path.append(os.path.join(os.path.dirname(os.path.abspath(__file__)), '..'))
from utils import normalize_domain

# ─── Initialize colorama ────────────────────────────────────────────────────────
colorama_init(autoreset=True)

# ─── Import config ───────────────────────────────────────────────────────────────
from config import EMAIL, PASSWORD, LOGIN_URL, BASE_MARKETPLACE_URL, REQUEST_TIMEOUT, MAX_PAGES, DATABASE_URL

# ─── DB Declaration ───────────────────────────────────────────────────────────────
from models import Base, PublisherListing

# ─── Logging Setup ───────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format=f'{Fore.MAGENTA}%(asctime)s{Style.RESET_ALL} | %(levelname)s | %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

class PublisuitesScraper:
    def __init__(self, max_pages=None):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
        })
        self.max_pages = max_pages or MAX_PAGES
        self.host_site = "publisuites.com"
        self.all_data = []

        # Database Setup
        self.engine = create_engine(DATABASE_URL)
        Base.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(bind=self.engine)

    def login(self):
        logger.info(f"{Fore.CYAN}▶ Logging in to Publisuites...")
        try:
            r1 = self.session.get(LOGIN_URL, timeout=REQUEST_TIMEOUT)
            soup = BeautifulSoup(r1.text, "lxml")
            
            form = soup.find("form")
            if not form:
                logger.error(f"{Fore.RED}✗ Could not find login form.")
                return False
                
            data = {}
            for input_tag in form.find_all("input"):
                name = input_tag.get("name")
                value = input_tag.get("value", "")
                if name:
                    data[name] = value
            
            data["email"] = EMAIL
            data["password"] = PASSWORD
            
            action = form.get("action", "/login/")
            if not action.startswith("http"):
                action = "https://www.publisuites.com" + action
                
            resp = self.session.post(action, data=data, timeout=REQUEST_TIMEOUT)
            
            if resp.status_code == 200 and "advertisers" in resp.url:
                logger.info(f"{Fore.GREEN}✓ Login successful!")
                return True
            else:
                logger.error(f"{Fore.RED}✗ Login failed. Check credentials or Captcha.")
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
        cards = soup.find_all("div", class_="card-pressmedia-page")
        results = []
        
        for card in cards:
            try:
                data = {
                    "host_site": self.host_site,
                    "scraped_at": datetime.utcnow()
                }
                
                # URL
                url_tag = card.find("a", class_="white w-500 m-b-0 nullref")
                if url_tag:
                    data["website_url"] = url_tag.get_text(strip=True)
                
                # ID
                parent_id = card.parent.get("id", "")
                data["item_id"] = parent_id.replace("website-", "")
                
                # Metrics
                metrics_text = card.get_text()
                
                # DA
                da_box = card.find(string="DA")
                if da_box:
                    da_val = da_box.parent.parent.find_next_sibling("p")
                    if da_val:
                        data["moz_da"] = int(re.sub(r'\D', '', da_val.get_text()))
                
                # DR
                dr_box = card.find(string="DR")
                if dr_box:
                    dr_val = dr_box.parent.parent.find_next_sibling("p")
                    if dr_val:
                        data["ahrefs_dr"] = int(re.sub(r'\D', '', dr_val.get_text()))
                
                # Traffic
                traffic_box = card.find(string=re.compile("Verified organic traffic", re.I))
                if traffic_box:
                    match = re.search(r"traffic:\s*([\d,.]+)", traffic_box.parent.get_text(), re.I)
                    if match:
                        data["traffic"] = int(re.sub(r'\D', '', match.group(1)))
                elif not data.get("traffic"):
                    traffic_box = card.find(string=re.compile("Web traffic/month", re.I))
                    if traffic_box:
                        traffic_val = traffic_box.parent.parent.find_next_sibling("p")
                        if traffic_val:
                            data["traffic"] = int(re.sub(r'\D', '', traffic_val.get_text()))

                # Language / Country
                lang_tag = card.find("i", class_="fa-globe")
                if lang_tag:
                    data["language"] = lang_tag.parent.get_text(strip=True)
                
                country_tag = card.find("img", class_="flag-details-pressmedia")
                if country_tag:
                    data["country"] = country_tag.parent.get_text(strip=True)

                # Categories
                cat_tag = card.find("i", class_="fa-bookmark")
                if cat_tag:
                    data["category"] = cat_tag.parent.get_text(strip=True)

                # Price
                price_box = card.find("div", class_="premium-price-table-box")
                if price_box:
                    p_text = price_box.get_text(strip=True)
                    data["price_raw"] = p_text
                    data["price_numerical"] = float(re.sub(r'[^\d,.]', '', p_text).replace(',', '.'))
                
                if data.get("website_url"):
                    results.append(data)
            except Exception as e:
                logger.debug(f"Error parsing card: {e}")
                continue
        return results

    def scrape(self, latest_only=False):
        if not self.login(): return
        
        logger.info(f"{Fore.CYAN}▶ Starting Publisuites scrape...")
        
        for page in range(1, self.max_pages + 1):
            logger.info(f"{Fore.BLUE}📄 Scraping page {page}...")
            
            post_data = {
                "page": page,
                "order": "discount",
                "order_dir": "desc",
                "typeview": "cards",
                "searchweb": ""
            }
            
            try:
                resp = self.session.post(BASE_MARKETPLACE_URL, data=post_data, timeout=REQUEST_TIMEOUT)
                resp.raise_for_status()
                
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
                    
                time.sleep(2) # Politeness
                
            except Exception as e:
                logger.error(f"{Fore.RED}✗ Error on page {page}: {e}")
                break

        logger.info(f"{Fore.GREEN}🏁 Finished! Scraped {len(self.all_data)} items.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--latest", action="store_true", help="Stop after encountering duplicates")
    parser.add_argument("--pages", type=int, help="Max pages to scrape")
    args = parser.parse_args()
    
    scraper = PublisuitesScraper(max_pages=args.pages)
    scraper.scrape(latest_only=args.latest)
