import polars as pl
from datetime import datetime
import re

#@analytics_decorator()
def normalize_job_data(df: pl.LazyFrame, node_metadata=None) -> pl.LazyFrame:
    """
    Normalize and clean job posting data:
    - Standardize job titles and locations
    - Parse salary ranges
    - Fill missing values
    - Standardize job types and experience levels
    """
    return (
        df
        # Clean and standardize job titles
        .with_columns([
            pl.col("job_title").str.to_titlecase().alias("job_title_clean"),
            pl.col("company_name").str.to_titlecase().alias("company_name_clean")
        ])
        
        # Standardize location format
        .with_columns([
            pl.col("location")
            .str.replace_all(r"\s+", " ")  # Remove extra spaces
            .str.to_titlecase()
            .alias("location_clean")
        ])
        
        # Parse and standardize salary information
        .with_columns([
            pl.col("salary_range")
            .str.extract_all(r"\$[\d,]+")
            .list.join(" - ")
            .alias("salary_parsed"),
            
            # Extract minimum salary
            pl.col("salary_range")
            .str.extract(r"\$?([\d,]+)")
            .str.replace_all(",", "")
            .cast(pl.Int64, strict=False)
            .alias("salary_min"),
        ])
        
        # Standardize job types
        .with_columns([
            pl.when(pl.col("job_type").str.contains("(?i)full.?time"))
            .then(pl.lit("Full-time"))
            .when(pl.col("job_type").str.contains("(?i)part.?time"))
            .then(pl.lit("Part-time"))
            .when(pl.col("job_type").str.contains("(?i)contract"))
            .then(pl.lit("Contract"))
            .when(pl.col("job_type").str.contains("(?i)intern"))
            .then(pl.lit("Internship"))
            .otherwise(pl.col("job_type"))
            .alias("job_type_standard")
        ])
        
        # Standardize experience levels
        .with_columns([
            pl.when(pl.col("experience_level").str.contains("(?i)entry|junior|0-2"))
            .then(pl.lit("Entry Level"))
            .when(pl.col("experience_level").str.contains("(?i)mid|intermediate|2-5"))
            .then(pl.lit("Mid Level"))
            .when(pl.col("experience_level").str.contains("(?i)senior|5+|lead"))
            .then(pl.lit("Senior Level"))
            .when(pl.col("experience_level").str.contains("(?i)principal|staff|architect"))
            .then(pl.lit("Principal Level"))
            .otherwise(pl.col("experience_level"))
            .alias("experience_level_standard")
        ])
        
        # Parse posted date
        .with_columns([
            pl.col("posted_date")
            .str.strptime(pl.Date, format="%Y-%m-%d", strict=False)
            .alias("posted_date_parsed")
        ])
        
        # Fill missing values with defaults
        .with_columns([
            pl.col("job_title_clean").fill_null("Unknown Position"),
            pl.col("company_name_clean").fill_null("Unknown Company"),
            pl.col("location_clean").fill_null("Location Not Specified"),
            pl.col("job_type_standard").fill_null("Not Specified"),
            pl.col("experience_level_standard").fill_null("Not Specified"),
            pl.col("salary_range").fill_null("Not Disclosed"),
            pl.col("industry").fill_null("Not Specified"),
            pl.col("company_size").fill_null("Not Specified")
        ])
        
        # Add processing metadata
        .with_columns([
            pl.lit(datetime.now()).alias("processed_at"),
            pl.lit("v1.0").alias("pipeline_version")
        ])
        
        # Select final columns in desired order
        .select([
            "source_job_board",
            "source_domain",
            "job_title_clean",
            "company_name_clean", 
            "location_clean",
            "salary_range",
            "salary_min",
            "job_type_standard",
            "experience_level_standard",
            "job_description",
            "skills_required",
            "posted_date_parsed",
            "application_url",
            "company_size",
            "industry",
            "processed_at",
            "pipeline_version"
        ])
    )