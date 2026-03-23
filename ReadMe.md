# 🚀 Marketplace Scraper Orchestrator

A high-performance, automated data pipeline for centralizing publisher marketplace data. It currently monitors and aggregates listings from **Posticy**, **iCopify**, **Publisuites**, **Links.me**, and **PressScape** into a unified PostgreSQL database.

## 🌟 Key Features
- **Multi-Source Aggregation:** Parallel scraping from multiple marketplaces.
- **Unified Domain Identity:** Automatically normalizes URLs to avoid duplicates across platforms.
- **Smart Deltas:** Only scrapes new listings, saving bandwidth and avoiding rate limits.
- **Dockerized Architecture:** Fully containerized with a master orchestrator and a management webhook.
- **Real-time Monitoring:** Access live logs and data exports via a built-in REST API.

---

## 🛠️ Quick Start (Docker)

The easiest way to run the entire system is using Docker Compose.

### 1. Configure Environment
Copy the example environment file and fill in your credentials:
```bash
cp .env-example .env
# Edit .env with your actual Marketplace logins and Database URL
```

### 2. Launch the System
```bash
docker compose up -d --build
```
This will start:
- **Orchestrator:** Runs scrapers in parallel every 7 days (Daemon mode).
- **Webhook:** Provides API access to logs and data.

### 3. Monitoring
You can monitor the progress without entering the container using the management webhook:
- **Live Logs:** `http://localhost:8000/logs?lines=100`
- **CSV Export:** `http://localhost:8000/download-csv?limit=1000`

---

## 📁 Project Structure
- `/icopify`: Scraper logic for icopify.co.
- `/posticy`: Scraper logic for posticy.com.
- `/publisuites`: Scraper logic for publisuites.com.
- `orchestrator.py`: The master thread manager and scheduler.
- `webhook.py`: FastAPI server for log streaming and data handling.
- `scrapers.yml`: Configuration for active bots and intervals.

---

## 🔍 Architecture & Theory
For a deep dive into how domain normalization and the "Upsert" logic works, please refer to the [Architecture Documentation](architecture_documentation.md).

## 🛡️ Security
- No hardcoded credentials. All logins are managed via `.env`.
- Database paths are mounted via Docker volumes to ensure persistence across restarts.
- Logs are automatically rotated to prevent disk exhaustion.
