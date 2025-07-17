import polars as pl
from extractors.job_scraper import scrape_job_boards
from transformations.data_normalizer import normalize_job_data
from loaders.postgres_loader import load_to_postgres
from data.job_board_urls import create_job_board_urls
from utils.data_inspector import inspect_raw_data, inspect_normalized_data

def workflow() -> pl.LazyFrame:
    """
    Main workflow for scraping job postings from multiple job boards,
    normalizing the data, and loading it into PostgreSQL.
    Uses realistic sample data generation when live scraping fails.
    """
    # Step 1: Create job board URLs to scrape
    print("Step 1: Creating job board URLs...")
    job_urls = create_job_board_urls()
    
    # Step 2: Extract job postings (now uses realistic sample data)
    print("Step 2: Generating realistic job postings...")
    raw_jobs = scrape_job_boards(job_urls)
    
    # Step 2a: Inspect raw scraped data
    print("Step 2a: Inspecting raw job data...")
    raw_inspection = inspect_raw_data(raw_jobs)
    
    # Step 3: Normalize and clean the job data
    print("Step 3: Normalizing job data...")
    normalized_jobs = normalize_job_data(raw_jobs)
    
    # Step 3a: Inspect normalized data
    print("Step 3a: Inspecting normalized data...")
    normalized_inspection = inspect_normalized_data(normalized_jobs)
    
    # Step 4: Load data to PostgreSQL database
    print("Step 4: Loading to PostgreSQL...")
    final_jobs = load_to_postgres(normalized_jobs)
    
    return final_jobs