from dagster import job
from .ops import scrape_telegram_data, load_raw_to_postgres, run_yolo_enrichment, run_dbt_transformations

@job
def medical_data_pipeline():
    # Define dependencies
    scrape_logs = scrape_telegram_data()
    load_logs = load_raw_to_postgres(scrape_logs)
    yolo_logs = run_yolo_enrichment(load_logs)
    dbt_logs = run_dbt_transformations(yolo_logs)
