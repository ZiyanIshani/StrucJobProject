import polars as pl

#@analytics_decorator()
def create_job_board_urls(node_metadata=None) -> pl.LazyFrame:
    """
    Create a DataFrame with more specific job board URLs for better scraping results.
    Uses direct job listing pages rather than search results when possible.
    """
    job_board_data = [
        {
            "job_board": "Indeed",
            "search_url": "https://www.indeed.com/jobs?q=software+developer&l=New+York%2C+NY&radius=25",
            "base_domain": "indeed.com"
        },
        {
            "job_board": "LinkedIn",
            "search_url": "https://www.linkedin.com/jobs/search/?keywords=software%20developer&location=New%20York%2C%20NY",
            "base_domain": "linkedin.com"
        },
        {
            "job_board": "Glassdoor",
            "search_url": "https://www.glassdoor.com/Job/new-york-software-developer-jobs-SRCH_IL.0,8_IC1132348_KO9,27.htm",
            "base_domain": "glassdoor.com"
        },
        {
            "job_board": "Monster",
            "search_url": "https://www.monster.com/jobs/search?q=software-developer&where=New-York__2C-NY",
            "base_domain": "monster.com"
        },
        {
            "job_board": "ZipRecruiter",
            "search_url": "https://www.ziprecruiter.com/Jobs/Software-Developer/New-York,NY",
            "base_domain": "ziprecruiter.com"
        },
        {
            "job_board": "SimplyHired",
            "search_url": "https://www.simplyhired.com/search?q=software+developer&l=New+York%2C+NY",
            "base_domain": "simplyhired.com"
        }
    ]
    
    return pl.DataFrame(job_board_data).lazy()