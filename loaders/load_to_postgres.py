import os
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp
import logging
from transformers.deduplicate_jobs import deduplicate_jobs

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def analytics_decorator():
    def decorator(func):
        return func
    return decorator

#@analytics_decorator()
def load_to_postgres(df: DataFrame) -> DataFrame:
    """
    Load cleaned job data to PostgreSQL database, handling upserts by overwriting
    the table with deduplicated data from current run and existing data.
    Skips loading if PostgreSQL credentials are not provided.
    """
    df_with_timestamp = df.withColumn("processed_at", current_timestamp())
    
    # Fixed environment variable usage
    postgres_host = os.getenv("POSTGRES_HOST", "localhost")
    postgres_user = os.getenv("POSTGRES_USER", "ziyanishani")
    postgres_password = os.getenv("POSTGRES_PASSWORD", "Welloqq11$$")
    postgres_db = os.getenv("POSTGRES_DB", "jobs_db")
    postgres_port = os.getenv("POSTGRES_PORT", "5432")
    
    if not all([postgres_host, postgres_user, postgres_password]):
        logger.warning("PostgreSQL credentials not found. Skipping database load. Data processing completed successfully.")
        return df_with_timestamp
    
    postgres_properties = {
        "user": postgres_user,
        "password": postgres_password,
        "driver": "org.postgresql.Driver"
    }
    
    postgres_url = f"jdbc:postgresql://{postgres_host}:{postgres_port}/{postgres_db}"
    
    try:
        spark = df.sparkSession
        
        # 1. Read existing data from PostgreSQL
        existing_df = None
        try:
            existing_df = spark.read \
                .jdbc(url=postgres_url,
                      table="job_listings",
                      properties=postgres_properties)
            logger.info(f"Successfully read {existing_df.count()} existing job listings from PostgreSQL.")
        except Exception as e_read:
            logger.warning(f"Could not read existing data from PostgreSQL: {e_read}. Proceeding as if no existing data.")
            existing_df = spark.createDataFrame([], df_with_timestamp.schema)

        # 2. Union new data with existing data
        combined_df = existing_df.unionByName(df_with_timestamp, allowMissingColumns=True)

        # 3. Deduplicate the combined DataFrame
        final_data_to_load = deduplicate_jobs(combined_df)
        logger.info(f"After combining and deduplicating, {final_data_to_load.count()} unique job listings prepared for load.")

        # 4. Overwrite the table with the fully deduplicated data
        final_data_to_load.write \
            .jdbc(url=postgres_url,
                  table="job_listings",
                  mode="overwrite",
                  properties=postgres_properties)
        
        logger.info(f"Successfully upserted {final_data_to_load.count()} job listings to PostgreSQL via overwrite.")
        return final_data_to_load

    except Exception as e:
        logger.error(f"Error during PostgreSQL upsert: {str(e)}", exc_info=True)
        logger.info("Continuing without full database upsert due to error...")
        return df_with_timestamp