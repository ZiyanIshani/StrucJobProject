from pydantic import BaseModel, HttpUrl
from datetime import datetime, date
from typing import Optional

class JobPosting(BaseModel):
    """
    Pydantic model for job posting data validation.
    """
    source_job_board: str
    source_domain: str
    job_title_clean: str
    company_name_clean: str
    location_clean: str
    salary_range: Optional[str] = None
    salary_min: Optional[int] = None
    job_type_standard: str
    experience_level_standard: str
    job_description: Optional[str] = None
    skills_required: Optional[str] = None
    posted_date_parsed: Optional[date] = None
    application_url: Optional[str] = None
    company_size: Optional[str] = None
    industry: Optional[str] = None
    processed_at: datetime
    pipeline_version: str
    
    class Config:
        str_strip_whitespace = True
        validate_assignment = True