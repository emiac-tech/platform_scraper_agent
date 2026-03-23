# Guest Posting Marketplace Scraper - Architecture & Documentation

## 1. Project Overview
The Guest Posting Marketplace Scraper is an automated, high-level data pipeline designed to continuously scan, deduplicate, and centralize publisher catalogs from multiple internet marketplaces (currently actively supporting `Posticy.com`, `iCopify.co`, `Publisuites.com`, `Links.me`, and `PressScape.com`). 

Because listings often span across several platforms with varying price points, the system executes parallel crawls, normalizes the identity of each publisher, and intelligently updates a unified database structure to allow straightforward queries and analytics.

---

## 2. Theory & Design Strategy

The core architectural principles driving this project allow it to be highly resilient, scalable, and bandwidth-efficient.

### 2.1 Unified Domain Identity
Instead of using internal platform IDs (e.g., Posticy's integer `2455`), the orchestrator applies a `normalize_domain` function to every discovered link. This function strips protocols (`http://`, `https://`), `www.` prefixes, and trailing slugs to yield a pristine domain signature (e.g., `example.com`).

This `clean_domain` serves as the global `UNIQUE` primary key in the centralized network schema.

### 2.2 Array Mapping Strategy
To answer the complex query of "How much does Example.com cost on Platform A versus Platform B?", the system uses corresponding JSON array fields natively mapped within SQLAlchemy:
- `host_sites`: Array of platforms listing the site (e.g., `["posticy.com", "icopify.co", "publisuites.com"]`)
- `prices_numerical`: Array of floating-point prices matching the host array indices (e.g., `[150.0, 160.0]`)

Because these indices strictly map 1-to-1, `prices_numerical[0]` guarantees the price on `host_sites[0]`.

### 2.3 Delta Scraping ("Upsert" Logic)
Scrapers run on a scheduled loop but avoid downloading the entire marketplace every time. They utilize a delta-check methodology. When fetching items (typically sorted by 'Date Added/Latest'), the database performs an 'Upsert' check:
1. Is the `clean_domain` already in the DB?
2. If yes, is the current `host_site` listed in the schema arrays?
    * **Yes**: Simply update the prices.
    * **No**: Append the new host, price, and ID to their respective arrays in the current record.
3. If the scraper encounters consecutive records (e.g., 10 entries) that *already exist* in the database under its specific `host_site` array, it dynamically halts the process. This confirms no new listings remain, avoiding unnecessary HTTP requests.

---

## 3. Database Schema (`publishers_v2`)
The project utilizes `SQLAlchemy` ORM, allowing flexible connectivity to both local `SQLite` operations and production `PostgreSQL` environments via connection strings.

**The `publishers_v2` Schema encompasses:**
*   `id`: `Integer` (Primary Key, Auto-increment)
*   `clean_domain`: `String` (Unique, Indexed identity)
*   `website_url`: `String` (Raw source link)
*   `host_sites`: `JSON Array` (Origin names: `["posticy.com", "icopify.co"]`)
*   `item_ids`: `JSON Array` (Marketplace-specific internal item IDs)
*   `prices_numerical`: `JSON Array` (Corresponding floats: `[25.5, 30.0]`)
*   `prices_raw`: `JSON Array` (Raw extracted string representations)
*   `categories`: `JSON Array`
*   `traffic`, `moz_da`, `moz_pa`, `ahrefs_dr`: `Integers` (SEO Metrics)
*   `scraped_at`: `DateTime` (UTC timestamp)

By structuring the metrics globally but localizing the origins into JSON arrays, the database maintains exactly ONE row per physical domain on the internet.

---

## 4. Execution & Orchestration

### `orchestrator.py`
The master file orchestrating the ecosystem. It reads scraper configurations, establishes a `ThreadPoolExecutor`, and synchronously calls independent shell commands (`python3 scraper.py --latest`) to run multiple targets completely parallelly.

### The Configuration Environment (`.env`)
Both database models and concurrent tasks bind variables from the central `.env` file, loaded via `python-dotenv`.
```env
DATABASE_URL=postgresql://username:password@localhost:5432/my_database
```
When swapping internal databases, the SQL architecture seamlessly ports to the newly defined environment variable string.

### Automation via CRON
The Orchestrator defines a `setup_cron()` method that hooks directly into the server's crontab system, effectively deploying a 7-day cyclical loop (`0 0 */7 * *`). It ensures constant uptime and fresh data pipelines indefinitely.

---

## 5. Centralized Logging & Error Management

Since parallel execution creates chaotic terminal output, all logging logic was cleanly decoupled from traditional output methods.

### 5.1 System.log (`logs/system.log`)
The orchestrator implements Python’s `logging.handlers.RotatingFileHandler`, producing an automatically rotated, clean log file that safely limits itself to 10MB chunks with 5 historic backups. 

1. **Subprocess Pipe reading**: The orchestrator strictly pipes `stdout` buffers originating from the scrapers.
2. **ANSI Color Stripping**: Using RegEx (`r'\x1b\[[0-9;]*m'`), the engine cleanly scrubs terminal color-coding sequences generated by external libraries like `Colorama`, ensuring plain text readability inside `system.log`.

### 5.2 Failure Protocol & Resilience
To ensure zero data loss handling:
- **Bot Failure**: If a specific platform's API breaks or auth fails, the orchestrator logs a `[PlatformName] failed with exit code X` error inside `system.log`. This failure remains entirely containerized, meaning `iCopify` can crash heavily while `Posticy` continues unaffected entirely within its own thread loop. 
- **Retry Mechanism**: Internal bots implement `MAX_RETRIES` configurations for transient HTTP failures. If a single page times out, it recursively delays itself dynamically before marking the site unreadable, pushing minor warning events out to `system.log` natively.
- **Top Level Crash Catch**: If the orchestrator itself fatals during unattended cron operation, the root stderr is dumped directly into an isolated `logs/cron_crash.log` fallback log buffer exactly where defined in the crontab script string.
