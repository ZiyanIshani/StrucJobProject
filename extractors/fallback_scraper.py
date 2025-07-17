import polars as pl
import pandas as pd

#@analytics_decorator()
def create_sample_job_data(df: pl.LazyFrame, node_metadata=None) -> pl.LazyFrame:
    """
    Create sample job data as a fallback when scraping fails.
    This helps test the rest of the pipeline while debugging scraping issues.
    """
    def generate_sample_batch(batch_df: pl.DataFrame) -> pl.DataFrame:
        sample_jobs = []
        
        for row in batch_df.iter_rows(named=True):
            job_board = row['job_board']
            search_url = row['search_url']
            base_domain = row['base_domain']
            
            # Generate sample jobs for each job board
            sample_positions = [
                {
                    'job_title': 'Senior Software Engineer',
                    'company_name': 'Tech Corp Inc',
                    'location': 'New York, NY',
                    'salary_range': '$120,000 - $150,000',
                    'job_type': 'Full-time',
                    'experience_level': 'Senior',
                    'job_description': 'We are looking for a senior software engineer to join our team...',
                    'skills_required': 'Python, JavaScript, React, Node.js',
                    'posted_date': '2024-01-15',
                    'company_size': 'Medium (100-500 employees)',
                    'industry': 'Technology'
                },
                {
                    'job_title': 'Frontend Developer',
                    'company_name': 'Digital Solutions LLC',
                    'location': 'Remote',
                    'salary_range': '$80,000 - $100,000',
                    'job_type': 'Full-time',
                    'experience_level': 'Mid-level',
                    'job_description': 'Join our frontend team to build amazing user experiences...',
                    'skills_required': 'React, TypeScript, CSS, HTML',
                    'posted_date': '2024-01-14',
                    'company_size': 'Small (10-50 employees)',
                    'industry': 'Software'
                }
            ]
            
            for job in sample_positions:
                job_data = {
                    'source_job_board': job_board,
                    'source_url': search_url,
                    'source_domain': base_domain,
                    **job,
                    'application_url': f"https://{base_domain}/apply/sample-job"
                }
                sample_jobs.append(job_data)
        
        return pl.DataFrame(sample_jobs)
    
    # Define output schema
    output_schema = {
        "source_job_board": pl.Utf8,
        "source_url": pl.Utf8,
        "source_domain": pl.Utf8,
        "job_title": pl.Utf8,
        "company_name": pl.Utf8,
        "location": pl.Utf8,
        "salary_range": pl.Utf8,
        "job_type": pl.Utf8,
        "experience_level": pl.Utf8,
        "job_description": pl.Utf8,
        "skills_required": pl.Utf8,
        "posted_date": pl.Utf8,
        "application_url": pl.Utf8,
        "company_size": pl.Utf8,
        "industry": pl.Utf8
    }
    
    return df.map_batches(generate_sample_batch, schema=output_schema)