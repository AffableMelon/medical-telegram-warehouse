import os
import subprocess
from dagster import op, Out, Output, String

# --- Helper to run shell commands ---
def run_command(command, cwd=None):
    process = subprocess.Popen(
        command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, cwd=cwd
    )
    stdout, stderr = process.communicate()
    if process.returncode != 0:
        raise Exception(f"Command failed: {command}\nError: {stderr}")
    return stdout

# --- OPS ---

@op(out=Out(String))
def scrape_telegram_data():
    """Runs the Telegram scraper script."""
    # Using mock scraper to avoid API limits/bans during testing
    # Was: result = run_command("python scripts/telegram.py --limit 100")
    result = run_command("python scripts/mock_scraper.py")
    return Output(result)

@op(out=Out(String))
def load_raw_to_postgres(start_after_scrape: String):
    """Loads raw JSON data into PostgreSQL."""
    # Takes dependency from scrape to ensure ordering
    result = run_command("python scripts/loader.py")
    return Output(result)

@op(out=Out(String))
def run_yolo_enrichment(start_after_load: String):
    """Runs YOLO object detection on downloaded images."""
    result = run_command("python src/yolo_detect.py")
    return Output(result)

@op(out=Out(String))
def run_dbt_transformations(start_after_yolo: String):
    """Runs dbt models to transform raw data into marts."""
    # Run dbt from the medical_warehouse directory
    cwd = os.path.join(os.getcwd(), "medical_warehouse")
    result = run_command("dbt run", cwd=cwd)
    # Also run tests?
    # run_command("dbt test", cwd=cwd) 
    return Output(result)
