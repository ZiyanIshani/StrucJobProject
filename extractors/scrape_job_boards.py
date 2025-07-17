from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, DateType
from pyspark.sql.functions import to_date, col
from typing import Iterator
import pandas as pd
from utils.decorators import analytics_decorator
import logging

logger = logging.getLogger(__name__)

#@analytics_decorator()
def scrape_job_boards(df: DataFrame) -> DataFrame:
    """
    Scrape job listings from multiple job boards using Structify's scrape_url.
    Uses mapInPandas for lazy execution and proper DAG visualization.
    """
    def scrape_batch(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        try:
            from structify import Structify
            client = Structify()
        except ImportError:
            logger.error("Structify not available. Install with: pip install structify")
            # Return empty results if Structify is not available
            for batch_df in iterator:
                yield pd.DataFrame(columns=[
                    'source_board', 'source_category', 'source_url', 'job_title',
                    'company_name', 'location', 'job_type', 'salary_range',
                    'description', 'requirements', 'posted_date', 'application_url',
                    'remote_option', 'experience_level', 'industry'
                ])
            return
        
        # Define comprehensive job schema
        job_schema = {
            "name": "job_listings",
            "description": "Job listings scraped from job boards",
            "properties": [
                {"name": "job_title", "description": "The title of the job position"},
                {"name": "company_name", "description": "The name of the hiring company"},
                {"name": "location", "description": "The job location (city, state, country)"},
                {"name": "job_type", "description": "Employment type (full-time, part-time, contract, etc.)"},
                {"name": "salary_range", "description": "Salary range or compensation information"},
                {"name": "description", "description": "Full job description and responsibilities"},
                {"name": "requirements", "description": "Required skills, experience, and qualifications"},
                {"name": "posted_date", "description": "When the job was posted (format: YYYY-MM-DD)"},
                {"name": "application_url", "description": "URL to apply for the job"},
                {"name": "remote_option", "description": "Whether remote work is available"},
                {"name": "experience_level", "description": "Required experience level (entry, mid, senior, etc.)"},
                {"name": "industry", "description": "Industry or sector of the company"}
            ]
        }
        
        for batch_df in iterator:
            results = []
            for _, row in batch_df.iterrows():
                board_name = row['board_name']
                url = row['url']
                category = row['category']
                
                try:
                    # Scrape job listings from the job board
                    job_listings_df = client.dataframe.scrape_url(
                        url=url,
                        table_name="job_listings",
                        schema=job_schema
                    )
                    
                    # Add metadata about the source
                    for _, job_row in job_listings_df.iterrows():
                        # Handle date formatting
                        posted_date = job_row.get('posted_date')
                        if posted_date and not isinstance(posted_date, str):
                            posted_date = str(posted_date)
                        
                        results.append({
                            'source_board': board_name,
                            'source_category': category,
                            'source_url': url,
                            'job_title': job_row.get('job_title'),
                            'company_name': job_row.get('company_name'),
                            'location': job_row.get('location'),
                            'job_type': job_row.get('job_type'),
                            'salary_range': job_row.get('salary_range'),
                            'description': job_row.get('description'),
                            'requirements': job_row.get('requirements'),
                            'posted_date': posted_date,
                            'application_url': job_row.get('application_url'),
                            'remote_option': job_row.get('remote_option'),
                            'experience_level': job_row.get('experience_level'),
                            'industry': job_row.get('industry')
                        })
                        
                    logger.info(f"Successfully scraped {len(job_listings_df)} jobs from {board_name}")
                    
                except Exception as e:
                    logger.error(f"Error scraping {board_name}: {str(e)}")
                    continue
            
            yield pd.DataFrame(results)
    
    # Define output schema
    output_schema = StructType([
        StructField("source_board", StringType(), True),
        StructField("source_category", StringType(), True),
        StructField("source_url", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("company_name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("job_type", StringType(), True),
        StructField("salary_range", StringType(), True),
        StructField("description", StringType(), True),
        StructField("requirements", StringType(), True),
        StructField("posted_date", StringType(), True),
        StructField("application_url", StringType(), True),
        StructField("remote_option", StringType(), True),
        StructField("experience_level", StringType(), True),
        StructField("industry", StringType(), True)
    ])
    
    # Apply the scraping function
    scraped_df = df.mapInPandas(scrape_batch, output_schema)
    
    # Convert posted_date from string to date type
    # Handle various date formats that might come from different job boards
    scraped_df_with_date = scraped_df.withColumn(
        "posted_date", 
        to_date(col("posted_date"), "yyyy-MM-dd")
    )
    
    return scraped_df_with_date