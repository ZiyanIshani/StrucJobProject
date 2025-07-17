import polars as pl

#@analytics_decorator()
def inspect_raw_data(df: pl.LazyFrame, node_metadata=None) -> pl.LazyFrame:
    """
    Inspect and display sample of raw job data for debugging.
    Shows what values are being generated for each job board.
    """
    return (
        df
        .with_columns([
            pl.col("job_title").str.len_chars().alias("title_length"),
            pl.col("job_description").str.len_chars().alias("description_length"),
            pl.when(pl.col("salary_range").str.contains("Not Disclosed|N/A"))
            .then(pl.lit(False))
            .when(pl.col("salary_range").is_not_null())
            .then(pl.lit(True))
            .otherwise(pl.lit(False))
            .alias("has_salary"),
            pl.when(pl.col("job_title").str.len_chars() > 5)
            .then(pl.lit("Success"))
            .otherwise(pl.lit("Failed"))
            .alias("data_quality"),
            pl.col("skills_required").str.split(",").list.len().alias("skill_count")
        ])
        .select([
            "source_job_board",
            "job_title",
            "company_name",
            "location",
            "salary_range",
            "job_type",
            "experience_level",
            "title_length",
            "description_length",
            "skill_count",
            "has_salary",
            "data_quality"
        ])
    )

@analytics_decorator()
def inspect_normalized_data(df: pl.LazyFrame, node_metadata=None) -> pl.LazyFrame:
    """
    Inspect normalized data to show the transformation results.
    """
    return (
        df
        .with_columns([
            pl.when(pl.col("salary_min").is_not_null())
            .then(pl.lit("Parsed"))
            .otherwise(pl.lit("Not Parsed"))
            .alias("salary_status"),
            pl.when(pl.col("job_title_clean").str.len_chars() > 5)
            .then(pl.lit("Valid"))
            .otherwise(pl.lit("Invalid"))
            .alias("data_quality"),
            pl.col("skills_required").str.split(",").list.len().alias("normalized_skill_count")
        ])
        .select([
            "source_job_board",
            "job_title_clean",
            "company_name_clean",
            "location_clean",
            "salary_range",
            "salary_min",
            "job_type_standard",
            "experience_level_standard",
            "normalized_skill_count",
            "salary_status",
            "data_quality",
            "processed_at"
        ])
    )

@analytics_decorator()
def create_job_summary(df: pl.LazyFrame, node_metadata=None) -> pl.LazyFrame:
    """
    Create a summary of job postings by job board and other dimensions.
    """
    return (
        df
        .group_by("source_job_board")
        .agg([
            pl.count().alias("total_jobs"),
            pl.col("company_name_clean").n_unique().alias("unique_companies"),
            pl.col("salary_min").filter(pl.col("salary_min").is_not_null()).mean().round(0).alias("avg_salary"),
            pl.col("job_type_standard").mode().first().alias("most_common_job_type"),
            pl.col("experience_level_standard").mode().first().alias("most_common_experience"),
            pl.col("location_clean").n_unique().alias("unique_locations")
        ])
        .sort("total_jobs", descending=True)
    )

@analytics_decorator()
def analyze_salary_distribution(df: pl.LazyFrame, node_metadata=None) -> pl.LazyFrame:
    """
    Analyze salary distribution across job boards and experience levels.
    """
    return (
        df
        .filter(pl.col("salary_min").is_not_null())
        .group_by(["source_job_board", "experience_level_standard"])
        .agg([
            pl.count().alias("job_count"),
            pl.col("salary_min").min().alias("min_salary"),
            pl.col("salary_min").max().alias("max_salary"),
            pl.col("salary_min").mean().round(0).alias("avg_salary"),
            pl.col("salary_min").median().alias("median_salary")
        ])
        .sort(["source_job_board", "avg_salary"], descending=[False, True])
    )