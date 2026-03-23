#!/usr/bin/env python3
"""
Master Scraper Orchestrator
===========================
Runs iCopify and Posticy scrapers in parallel and ensures data is synced 
to the centralized database. Schedules itself to run every 7 days.

Usage:
    python master_orchestrator.py          # Run now
    python master_orchestrator.py --cron   # Setup cron job (every 7 days)
"""

import os
import sys
import subprocess
import argparse
import logging
import re
from logging.handlers import RotatingFileHandler
from concurrent.futures import ThreadPoolExecutor
from colorama import Fore, Style, init as colorama_init
from dotenv import load_dotenv

from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Text, JSON
from sqlalchemy.ext.declarative import declarative_base

# Load environment variables from .env
load_dotenv()

colorama_init(autoreset=True)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# ─── Logging Setup (Centralized) ────────────────────────────────────────────────
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)
log_file = os.path.join(BASE_DIR, "logs", "system.log")

formatter = logging.Formatter('%(asctime)s | %(levelname)s | %(message)s', '%Y-%m-%d %H:%M:%S')

# Centralized file handler - max 10MB per file, keeps 5 backups
file_handler = RotatingFileHandler(log_file, maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(formatter)

# Stream handler for console viewing
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter(f'{Fore.MAGENTA}%(asctime)s{Style.RESET_ALL} | %(levelname)s | %(message)s', '%H:%M:%S'))

logging.basicConfig(level=logging.INFO, handlers=[file_handler, stream_handler])
logger = logging.getLogger(__name__)

# Shared DB Model for Initialization
Base = declarative_base()
class PublisherListing(Base):
    __tablename__ = 'publishers_v2'
    id = Column(Integer, primary_key=True, autoincrement=True)
    clean_domain = Column(String(255), unique=True, index=True)
    website_url = Column(String(255))
    host_sites = Column(JSON, default=list) # e.g. ["posticy.com", "icopify.co"]
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
    scraped_at = Column(DateTime)

import yaml
import time
import schedule

def load_scrapers():
    """Dynamically load active scrapers from the external yaml configuration."""
    config_path = os.path.join(BASE_DIR, "scrapers.yml")
    if not os.path.exists(config_path):
        logger.warning(f"{Fore.YELLOW}⚠ No scrapers.yml found. Using empty list.")
        return []
    
    with open(config_path, "r") as f:
        data = yaml.safe_load(f)
        scrapers_list = data.get("scrapers", [])
        
    active_scrapers = []
    for s in scrapers_list:
        if s.get("active", True):
            cmd = s.get("command", ["python3", "scraper.py"])
            # Auto-append --latest by default for incremental updates
            if "--latest" not in cmd:
                cmd.append("--latest")
            
            active_scrapers.append({
                "name": s["name"],
                "path": os.path.join(BASE_DIR, s["path"].lstrip("./")),
                "command": cmd
            })
    return active_scrapers

def run_scraper(scraper):
    """Run a single scraper and log its completion status."""
    name = scraper["name"]
    path = scraper["path"]
    cmd = scraper["command"]
    
    logger.info(f"{Fore.CYAN}▶ Starting {name} scraper parallelly [{path}]...")
    
    try:
        process = subprocess.Popen(
            cmd,
            cwd=path,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True
        )
        
        for line in process.stdout:
            raw_line = line.strip()
            clean_line = re.sub(r'\x1b\[[0-9;]*m', '', raw_line)
            logger.info(f"[{name}] {clean_line}")
            
        process.wait()
        
        if process.returncode == 0:
            logger.info(f"{Fore.GREEN}✓ {name} scraper completed successfully.")
        else:
            logger.error(f"{Fore.RED}✗ {name} scraper failed with exit code {process.returncode}.")
            
    except Exception as e:
        logger.error(f"{Fore.RED}✗ Error running {name} scraper: {e}")

def run_all_scrapers(full=False):
    """Execution trigger to loop through the scrape cycle."""
    scrapers = load_scrapers()
    
    if not scrapers:
        logger.error(f"{Fore.RED}✗ No active scrapers configured in scrapers.yml!")
        return
        
    if full:
        for s in scrapers:
            if "--latest" in s["command"]:
                s["command"].remove("--latest")

    logger.info(f"{Fore.MAGENTA}🚀 Starting Parallel Market Scraper Run...")
    with ThreadPoolExecutor(max_workers=len(scrapers)) as executor:
        executor.map(run_scraper, scrapers)
    logger.info(f"{Fore.MAGENTA}🏁 All scrapers finished.")

def run_daemon(days=7):
    """Run the orchestrator continuously as a Docker-friendly daemon."""
    logger.info(f"{Fore.YELLOW}⚙ Starting DAEMON Mode: Orchestrator will run entirely isolated every {days} days.")
    # Run the initial cycle immediately on boot
    run_all_scrapers(full=False)
    
    # Schedule repeating triggers
    schedule.every(days).days.do(run_all_scrapers, full=False)
    
    while True:
        schedule.run_pending()
        time.sleep(60)

def main():
    parser = argparse.ArgumentParser(description="Master Scraper Orchestrator")
    parser.add_argument("--daemon", action="store_true", help="Run continuously in the background using python scheduler (Docker ideal)")
    parser.add_argument("--full", action="store_true", help="Run full scrape (ignore delta)")
    args = parser.parse_args()

    # Initialize Database Schema universally before running any tasks
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        logger.info(f"{Fore.YELLOW}⚙ Initializing database schema...")
        try:
            engine = create_engine(db_url)
            Base.metadata.create_all(engine)
            logger.info(f"{Fore.GREEN}✓ Shared Database verified/initialized.")
        except Exception as e:
            logger.error(f"{Fore.RED}✗ Could not initialize database: {e}")

    if args.daemon:
        run_daemon(days=7)
    else:
        run_all_scrapers(full=args.full)

if __name__ == "__main__":
    main()
