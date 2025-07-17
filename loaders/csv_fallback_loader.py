import polars as pl
import os
from datetime import datetime

#@analytics_decorator()
def save_to_csv_fallback(df: pl.LazyFrame, node_metadata=None) -> pl.LazyFrame:
    """
    Fallback loader that saves data to CSV when PostgreSQL is not available.
    This ensures the pipeline can complete even without database connectivity.
    Fixed to preserve ALL input columns and add metadata columns properly.
    """
    def save_batch(batch_df: pl.DataFrame) -> pl.DataFrame:
        # Create output directory if it doesn't exist
        output_dir = "output"
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{output_dir}/job_postings_{timestamp}.csv"
        
        try:
            # Save to CSV
            batch_df.write_csv(filename)
            print(f"✅ Data saved to CSV: {filename}")
            print(f"   Records saved: {len(batch_df)}")
            
            # Add metadata columns to the existing DataFrame using with_columns
            result_df = batch_df.with_columns([
                pl.lit("csv_saved").alias("db_status"),
                pl.lit(len(batch_df)).cast(pl.Int64).alias("records_inserted"),
                pl.lit(filename).alias("output_file"),
                pl.lit("csv_fallback").alias("connection_status"),
                pl.lit("").alias("error_message")  # Empty error message for success
            ])
            
            return result_df
            
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Error saving to CSV: {error_msg}")
            
            # Add error metadata columns to the existing DataFrame using with_columns
            result_df = batch_df.with_columns([
                pl.lit("csv_error").alias("db_status"),
                pl.lit(0).cast(pl.Int64).alias("records_inserted"),
                pl.lit("").alias("output_file"),
                pl.lit("csv_failed").alias("connection_status"),
                pl.lit(error_msg).alias("error_message")
            ])
            
            return result_df
    
    # Define output schema - must include ALL input columns plus new metadata columns
    output_schema = {
        # Original data columns (preserve all from input)
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
        # Metadata columns added by this function
        "db_status": pl.Utf8,
        "records_inserted": pl.Int64,
        "output_file": pl.Utf8,
        "connection_status": pl.Utf8,
        "error_message": pl.Utf8
    }
    
    return df.map_batches(save_batch, schema=output_schema)