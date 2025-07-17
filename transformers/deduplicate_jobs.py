from pyspark.sql import DataFrame
from pyspark.sql.functions import col, row_number, desc
from pyspark.sql.window import Window
from utils.decorators import analytics_decorator



#@analytics_decorator()
def deduplicate_jobs(df: DataFrame) -> DataFrame:
    """
    Remove duplicate job listings based on job title, company, and location.
    Keeps the most recent posting when duplicates are found.
    """
    # Define window for deduplication - partition by key fields, order by posted_date
    window_spec = Window.partitionBy(
        col("job_title_cleaned"),
        col("company_name_cleaned"), 
        col("location_standardized")
    ).orderBy(desc("posted_date"))
    
    # Add row number to identify duplicates
    df_with_row_num = df.withColumn("row_num", row_number().over(window_spec))
    
    # Keep only the first row (most recent) for each group
    deduplicated_df = df_with_row_num.filter(col("row_num") == 1).drop("row_num")
    
    return deduplicated_df