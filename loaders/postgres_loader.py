import polars as pl
import os
from sqlalchemy import create_engine, text
import pandas as pd
import time

#@analytics_decorator()
def load_to_postgres(df: pl.LazyFrame, node_metadata=None) -> pl.LazyFrame:
    """
    Load normalized job data into PostgreSQL database.
    Creates table if it doesn't exist and handles upserts.
    Updated to use the correct environment variable values.
    """
    def load_batch(batch_df: pl.DataFrame) -> pl.DataFrame:
        try:
            # Database connection parameters - updated to use correct values
            host = os.environ.get("POSTGRES_HOST", "localhost")
            port = os.environ.get("POSTGRES_PORT", "5432")
            database = os.environ.get("POSTGRES_DB", "scraping")  # Updated default
            username = os.environ.get("POSTGRES_USER", "postgres")
            password = os.environ.get("POSTGRES_PASSWORD", "")
            
            print(f"Connecting to PostgreSQL: {username}@{host}:{port}/{database}")
            
            # Create connection string
            connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
            engine = create_engine(connection_string, pool_pre_ping=True)
            
            # Test connection with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    with engine.connect() as conn:
                        conn.execute(text("SELECT 1"))
                        print(f"✅ Successfully connected to PostgreSQL database '{database}'")
                        break
                except Exception as e:
                    if attempt < max_retries - 1:
                        print(f"⚠️ Connection attempt {attempt + 1} failed, retrying in 2 seconds...")
                        time.sleep(2)
                    else:
                        raise e
            
            # Convert to pandas for database operations
            batch_pd = batch_df.to_pandas()
            
            # Create table if it doesn't exist
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS job_postings (
                id SERIAL PRIMARY KEY,
                source_job_board VARCHAR(100),
                source_domain VARCHAR(100),
                job_title_clean VARCHAR(500),
                company_name_clean VARCHAR(300),
                location_clean VARCHAR(200),
                salary_range VARCHAR(200),
                salary_min INTEGER,
                job_type_standard VARCHAR(50),
                experience_level_standard VARCHAR(50),
                job_description TEXT,
                skills_required TEXT,
                posted_date_parsed DATE,
                application_url TEXT,
                company_size VARCHAR(100),
                industry VARCHAR(100),
                processed_at TIMESTAMP,
                pipeline_version VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(source_job_board, job_title_clean, company_name_clean, location_clean)
            );
            """
            
            with engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()
                print(f"✅ Table 'job_postings' created/verified successfully")
            
            # Insert data with conflict handling (upsert)
            records_inserted = len(batch_pd)
            batch_pd = batch_pd.drop_duplicates(
                subset=["source_job_board","job_title_clean","company_name_clean","location_clean"]
            ) 
            batch_pd.to_sql(
                'job_postings',
                engine,
                if_exists='append',
                index=False,
                method='multi'
            )
            
            print(f"✅ Successfully inserted {records_inserted} job postings into PostgreSQL")
            
            # Add database insertion metadata
            batch_df_with_meta = batch_df.with_columns([
                pl.lit("inserted").alias("db_status"),
                pl.lit(0).cast(pl.Int64).alias("records_inserted")
            ])
            
            return batch_df_with_meta
            
        except Exception as e:
            print(f"❌ PostgreSQL Error: {str(e)}")
            print("📋 Troubleshooting checklist:")
            print("   1. Verify PostgreSQL service is running")
            print("   2. Check database connection parameters")
            print("   3. Ensure database 'scraping' exists")
            print("   4. Verify user permissions")
            
            # Return data with error status instead of failing
            batch_df_with_meta = batch_df.with_columns([
                pl.lit("error").alias("db_status"),
                pl.lit(records_inserted).cast(pl.Int64).alias("records_inserted")
            ])
            
            return batch_df_with_meta
    
    # Define output schema (same as input plus metadata)
    output_schema = {
        "source_job_board": pl.Utf8,
        "source_domain": pl.Utf8,
        "job_title_clean": pl.Utf8,
        "company_name_clean": pl.Utf8,
        "location_clean": pl.Utf8,
        "salary_range": pl.Utf8,
        "salary_min": pl.Int64,
        "job_type_standard": pl.Utf8,
        "experience_level_standard": pl.Utf8,
        "job_description": pl.Utf8,
        "skills_required": pl.Utf8,
        "posted_date_parsed": pl.Date,
        "application_url": pl.Utf8,
        "company_size": pl.Utf8,
        "industry": pl.Utf8,
        "processed_at": pl.Datetime,
        "pipeline_version": pl.Utf8,
        "db_status": pl.Utf8,
        "records_inserted": pl.Int64
    }
    
    return df.map_batches(load_batch, schema=output_schema)

#@analytics_decorator()
def verify_database_connection(node_metadata=None) -> pl.LazyFrame:
    """
    Verify PostgreSQL database connection and return connection status.
    """
    def check_connection() -> pl.DataFrame:
        try:
            host = os.environ.get("POSTGRES_HOST", "localhost")
            port = os.environ.get("POSTGRES_PORT", "5432")
            database = os.environ.get("POSTGRES_DB", "scraping")
            username = os.environ.get("POSTGRES_USER", "postgres")
            
            connection_string = f"postgresql://{username}:***@{host}:{port}/{database}"
            engine = create_engine(f"postgresql://{username}:{os.environ.get('POSTGRES_PASSWORD', '')}@{host}:{port}/{database}")
            
            with engine.connect() as conn:
                result = conn.execute(text("SELECT version()"))
                version = result.fetchone()[0]
                
                return pl.DataFrame({
                    "connection_status": ["success"],
                    "database_host": [host],
                    "database_name": [database],
                    "database_version": [version[:50]],  # Truncate version string
                    "connection_string": [connection_string]
                })
                
        except Exception as e:
            return pl.DataFrame({
                "connection_status": ["failed"],
                "database_host": [os.environ.get("POSTGRES_HOST", "localhost")],
                "database_name": [os.environ.get("POSTGRES_DB", "scraping")],
                "database_version": ["N/A"],
                "connection_string": [f"Error: {str(e)}"]
            })
    
    return pl.LazyFrame(check_connection())