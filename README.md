# Medical Telegram Warehouse

## Setup

1. Copy `.env.example` to `.env` (create one based on usage).
   Required vars: `TG_API_ID`, `TG_API_HASH`, `TG_SESSION_NAME`, `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`.

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Run Postgres (via Docker):
   ```bash
   docker-compose up -d db
   ```

## Task 1: Scraping

Run the scraper to collect data from Telegram channels:
```bash
python scripts/telegram.py
```
This will populate `data/raw/` with JSONs and images.

## Task 2: Warehouse

1. Load raw data into Postgres:
   ```bash
   python scripts/loader.py
   ```

2. Run dbt transformation:
   ```bash
   cd medical_warehouse
   dbt deps
   dbt build
   ```

## Project Structure
- `src/`: Scraper application
- `medical_warehouse/`: dbt project
- `data/`: Local data lake
- `scripts/`: Utility scripts
