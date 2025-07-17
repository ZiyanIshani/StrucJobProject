from workflow import workflow
def main():
    """
    Main entry point for the job scraping workflow.
    """
    print("🚀 Starting the job scraping workflow...")
    
    # Execute the workflow
    final_jobs = workflow()
    
    # Display the final jobs DataFrame
    print("✅ Workflow completed successfully!")
    print(final_jobs.collect())  # Collect to trigger execution and display results

if __name__ == "__main__":
    main()
    print("👋 Goodbye! The job scraping workflow has finished running.")
