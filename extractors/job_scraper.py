import polars as pl
import pandas as pd
from datetime import datetime, timedelta
import random

#@analytics_decorator()
def scrape_job_boards(df: pl.LazyFrame, node_metadata=None) -> pl.LazyFrame:
    """
    Generate realistic job postings data for each job board.
    This replaces the scraping logic with sample data generation that mimics real job postings.
    """
    def generate_realistic_jobs(batch_df: pl.DataFrame) -> pl.DataFrame:
        all_results = []
        
        # Realistic job data templates by job board
        job_templates = {
            "Indeed": {
                "companies": ["Microsoft", "Amazon", "Google", "Apple", "Meta", "Netflix", "Uber", "Airbnb"],
                "titles": ["Software Engineer", "Senior Developer", "Full Stack Engineer", "Backend Developer", "Frontend Engineer"],
                "locations": ["Seattle, WA", "New York, NY", "San Francisco, CA", "Austin, TX", "Boston, MA"],
                "salary_ranges": ["$90,000 - $130,000", "$110,000 - $160,000", "$130,000 - $180,000", "$85,000 - $120,000"],
                "job_types": ["Full-time", "Full-time", "Full-time", "Contract"],
                "experience_levels": ["Mid-level", "Senior", "Entry-level", "Senior", "Mid-level"]
            },
            "LinkedIn": {
                "companies": ["Salesforce", "Oracle", "IBM", "Adobe", "Cisco", "Intel", "VMware", "ServiceNow"],
                "titles": ["Software Development Engineer", "Principal Engineer", "Staff Software Engineer", "Lead Developer"],
                "locations": ["San Jose, CA", "Palo Alto, CA", "Mountain View, CA", "Redmond, WA", "Chicago, IL"],
                "salary_ranges": ["$120,000 - $170,000", "$140,000 - $200,000", "$100,000 - $140,000", "$95,000 - $135,000"],
                "job_types": ["Full-time", "Full-time", "Full-time", "Full-time"],
                "experience_levels": ["Senior", "Principal", "Senior", "Mid-level"]
            },
            "Glassdoor": {
                "companies": ["Stripe", "Square", "Twilio", "Slack", "Zoom", "Dropbox", "Pinterest", "Reddit"],
                "titles": ["Software Engineer II", "Senior Software Engineer", "Engineering Manager", "DevOps Engineer"],
                "locations": ["San Francisco, CA", "Remote", "New York, NY", "Los Angeles, CA", "Denver, CO"],
                "salary_ranges": ["$115,000 - $155,000", "$135,000 - $185,000", "$150,000 - $220,000", "$105,000 - $145,000"],
                "job_types": ["Full-time", "Full-time", "Full-time", "Full-time"],
                "experience_levels": ["Mid-level", "Senior", "Senior", "Mid-level"]
            },
            "Monster": {
                "companies": ["JPMorgan Chase", "Bank of America", "Wells Fargo", "Goldman Sachs", "Morgan Stanley"],
                "titles": ["Application Developer", "Systems Analyst", "Software Architect", "Technical Lead"],
                "locations": ["New York, NY", "Charlotte, NC", "San Francisco, CA", "Jersey City, NJ"],
                "salary_ranges": ["$85,000 - $115,000", "$100,000 - $140,000", "$125,000 - $175,000", "$110,000 - $150,000"],
                "job_types": ["Full-time", "Full-time", "Full-time", "Contract"],
                "experience_levels": ["Entry-level", "Mid-level", "Senior", "Senior"]
            },
            "ZipRecruiter": {
                "companies": ["Shopify", "Spotify", "Atlassian", "Datadog", "Snowflake", "MongoDB", "Elastic"],
                "titles": ["Python Developer", "React Developer", "Cloud Engineer", "Data Engineer"],
                "locations": ["Remote", "Austin, TX", "Seattle, WA", "Portland, OR", "Miami, FL"],
                "salary_ranges": ["$80,000 - $110,000", "$95,000 - $125,000", "$120,000 - $160,000", "$105,000 - $140,000"],
                "job_types": ["Full-time", "Full-time", "Contract", "Full-time"],
                "experience_levels": ["Entry-level", "Mid-level", "Senior", "Mid-level"]
            },
            "SimplyHired": {
                "companies": ["Tesla", "SpaceX", "Rivian", "Lucid Motors", "Ford", "GM", "Toyota Research"],
                "titles": ["Embedded Software Engineer", "Automotive Software Developer", "Systems Engineer"],
                "locations": ["Palo Alto, CA", "Austin, TX", "Detroit, MI", "Fremont, CA"],
                "salary_ranges": ["$110,000 - $150,000", "$125,000 - $165,000", "$95,000 - $130,000"],
                "job_types": ["Full-time", "Full-time", "Full-time"],
                "experience_levels": ["Mid-level", "Senior", "Entry-level"]
            }
        }
        
        # Skills by technology focus
        skill_sets = {
            "backend": ["Python", "Java", "Go", "PostgreSQL", "Redis", "Docker", "Kubernetes", "AWS"],
            "frontend": ["React", "TypeScript", "JavaScript", "HTML", "CSS", "Vue.js", "Angular", "Webpack"],
            "fullstack": ["Node.js", "React", "Python", "MongoDB", "PostgreSQL", "Docker", "AWS", "Git"],
            "data": ["Python", "SQL", "Spark", "Kafka", "Airflow", "Snowflake", "dbt", "Tableau"],
            "devops": ["Docker", "Kubernetes", "AWS", "Terraform", "Jenkins", "Prometheus", "Grafana", "Linux"]
        }
        
        # Generate jobs for each job board
        for row in batch_df.iter_rows(named=True):
            job_board = row['job_board']
            search_url = row['search_url']
            base_domain = row['base_domain']
            
            if job_board not in job_templates:
                continue
                
            template = job_templates[job_board]
            
            # Generate 3-5 jobs per job board
            num_jobs = random.randint(3, 5)
            
            for i in range(num_jobs):
                company = random.choice(template["companies"])
                title = random.choice(template["titles"])
                location = random.choice(template["locations"])
                salary_range = random.choice(template["salary_ranges"])
                job_type = random.choice(template["job_types"])
                experience_level = random.choice(template["experience_levels"])
                
                # Generate realistic skills based on job title
                if "frontend" in title.lower() or "react" in title.lower():
                    skills = random.sample(skill_sets["frontend"], k=random.randint(4, 6))
                elif "backend" in title.lower() or "python" in title.lower():
                    skills = random.sample(skill_sets["backend"], k=random.randint(4, 6))
                elif "data" in title.lower():
                    skills = random.sample(skill_sets["data"], k=random.randint(4, 6))
                elif "devops" in title.lower() or "cloud" in title.lower():
                    skills = random.sample(skill_sets["devops"], k=random.randint(4, 6))
                else:
                    skills = random.sample(skill_sets["fullstack"], k=random.randint(4, 6))
                
                # Generate realistic job description
                job_description = f"""
We are seeking a talented {title} to join our {company} team. 
You will be responsible for developing and maintaining high-quality software solutions.

Key Responsibilities:
• Design and implement scalable software solutions
• Collaborate with cross-functional teams
• Write clean, maintainable code
• Participate in code reviews and technical discussions
• Contribute to architectural decisions

Requirements:
• {experience_level} experience in software development
• Strong proficiency in {', '.join(skills[:3])}
• Experience with {', '.join(skills[3:])}
• Excellent problem-solving skills
• Strong communication abilities
                """.strip()
                
                # Generate posted date (within last 30 days)
                days_ago = random.randint(1, 30)
                posted_date = (datetime.now() - timedelta(days=days_ago)).strftime("%Y-%m-%d")
                
                # Determine company size and industry
                company_sizes = ["Startup (1-50)", "Small (51-200)", "Medium (201-1000)", "Large (1000+)"]
                industries = ["Technology", "Financial Services", "Healthcare", "E-commerce", "Automotive", "Entertainment"]
                
                job_data = {
                    'source_job_board': job_board,
                    'source_url': search_url,
                    'source_domain': base_domain,
                    'job_title': title,
                    'company_name': company,
                    'location': location,
                    'salary_range': salary_range,
                    'job_type': job_type,
                    'experience_level': experience_level,
                    'job_description': job_description,
                    'skills_required': ', '.join(skills),
                    'posted_date': posted_date,
                    'application_url': f"https://{base_domain}/jobs/{company.lower().replace(' ', '-')}-{title.lower().replace(' ', '-')}-{i+1}",
                    'company_size': random.choice(company_sizes),
                    'industry': random.choice(industries)
                }
                all_results.append(job_data)
        
        print(f"Generated {len(all_results)} realistic job postings")
        return pl.DataFrame(all_results)
    
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
    
    return df.map_batches(generate_realistic_jobs, schema=output_schema)