from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from typing import Iterator
import pandas as pd
from utils.decorators import analytics_decorator



#@analytics_decorator()
def clean_job_data(df: DataFrame, node_metadata=None) -> DataFrame:
    """
    Clean and standardize job data using AI-powered enhancement.
    Fixes spelling errors and standardizes location formats.
    """
    def clean_batch(iterator: Iterator[pd.DataFrame]) -> Iterator[pd.DataFrame]:
        from structify import Structify
        client = Structify()
        
        for batch_df in iterator:
            # Clean job titles
            title_cleaned_df = client.dataframe.enhance_column(
                df=batch_df,
                column_name="job_title_cleaned",
                column_description="Fix spelling errors and standardize this job title to a common format",
                table_name="job_listings",
                table_description="Job listings with potentially misspelled or non-standard job titles",
                node_metadata=node_metadata
            )
            
            # Clean and standardize locations
            location_cleaned_df = client.dataframe.enhance_column(
                df=title_cleaned_df,
                column_name="location_standardized",
                column_description="Standardize this location to format: City, State, Country (e.g., 'San Francisco, CA, USA')",
                table_name="job_listings",
                table_description="Job listings with various location formats that need standardization",
                node_metadata=node_metadata
            )
            
            # Clean company names
            company_cleaned_df = client.dataframe.enhance_column(
                df=location_cleaned_df,
                column_name="company_name_cleaned",
                column_description="Fix spelling errors and standardize this company name to its official name",
                table_name="job_listings",
                table_description="Job listings with potentially misspelled company names",
                node_metadata=node_metadata
            )
            
            # Standardize job types
            job_type_cleaned_df = client.dataframe.enhance_column(
                df=company_cleaned_df,
                column_name="job_type_standardized",
                column_description="Standardize this job type to one of: Full-time, Part-time, Contract, Temporary, Internship",
                table_name="job_listings",
                table_description="Job listings with various job type formats",
                node_metadata=node_metadata
            )
            
            # Standardize experience levels
            experience_cleaned_df = client.dataframe.enhance_column(
                df=job_type_cleaned_df,
                column_name="experience_level_standardized",
                column_description="Standardize this experience level to one of: Entry Level, Mid Level, Senior Level, Executive",
                table_name="job_listings",
                table_description="Job listings with various experience level formats",
                node_metadata=node_metadata
            )
            
            yield experience_cleaned_df
    
    # Define output schema with cleaned columns
    input_fields = df.schema.fields
    new_fields = [
        StructField("job_title_cleaned", StringType(), True),
        StructField("location_standardized", StringType(), True),
        StructField("company_name_cleaned", StringType(), True),
        StructField("job_type_standardized", StringType(), True),
        StructField("experience_level_standardized", StringType(), True)
    ]
    output_schema = StructType(input_fields + new_fields)
    
    return df.mapInPandas(clean_batch, output_schema)