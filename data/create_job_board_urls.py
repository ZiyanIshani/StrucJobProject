from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from utils.decorators import analytics_decorator



#@analytics_decorator()
def create_job_board_urls(spark: SparkSession) -> DataFrame:
    """
    Create a DataFrame with job board URLs to scrape.
    """
    # Define job board URLs with search parameters
    job_boards = [
        {
            "board_name": "ZipRecruiter",
            "url": "https://www.ziprecruiter.com/jobs/search",
            "category": "general"
        },
        {
            "board_name": "GitHub Jobs",
            "url": "https://github.com/jobs",
            "category": "tech"
        },
        {
            "board_name": "AngelList",
            "url": "https://angel.co/jobs",
            "category": "startup"
        },
        {
            "board_name": "Indeed",
            "url": "https://www.indeed.com/jobs",
            "category": "general"
        },
        {
            "board_name": "LinkedIn Jobs",
            "url": "https://www.linkedin.com/jobs/search",
            "category": "professional"
        }
    ]
    
    schema = StructType([
        StructField("board_name", StringType(), True),
        StructField("url", StringType(), True),
        StructField("category", StringType(), True)
    ])
    
    return spark.createDataFrame(job_boards, schema)