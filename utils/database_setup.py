import os
import subprocess
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

#@analytics_decorator()
def check_postgres_connection(node_metadata=None):
    """
    Check PostgreSQL connection and provide setup guidance.
    """
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    database = os.environ.get("POSTGRES_DB", "jobs_db")
    username = os.environ.get("POSTGRES_USER", "postgres")
    password = os.environ.get("POSTGRES_PASSWORD", "password")
    
    connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database}"
    
    print("🔍 Checking PostgreSQL connection...")
    print(f"   Host: {host}")
    print(f"   Port: {port}")
    print(f"   Database: {database}")
    print(f"   Username: {username}")
    
    try:
        engine = create_engine(connection_string, connect_args={"connect_timeout": 5})
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"✅ PostgreSQL connection successful!")
            print(f"   Version: {version}")
            return True
            
    except OperationalError as e:
        error_msg = str(e)
        print(f"❌ PostgreSQL connection failed: {error_msg}")
        
        if "Connection refused" in error_msg:
            print("\n💡 PostgreSQL Setup Guide:")
            print("   1. Install PostgreSQL:")
            print("      • Ubuntu/Debian: sudo apt-get install postgresql postgresql-contrib")
            print("      • macOS: brew install postgresql")
            print("      • Windows: Download from https://www.postgresql.org/download/")
            print()
            print("   2. Start PostgreSQL service:")
            print("      • Ubuntu/Debian: sudo service postgresql start")
            print("      • macOS: brew services start postgresql")
            print("      • Windows: Start from Services or pgAdmin")
            print()
            print("   3. Create database and user:")
            print("      sudo -u postgres psql")
            print("      CREATE DATABASE jobs_db;")
            print("      CREATE USER your_username WITH PASSWORD 'your_password';")
            print("      GRANT ALL PRIVILEGES ON DATABASE jobs_db TO your_username;")
            print()
            print("   4. Update environment variables with your actual credentials")
            
        elif "authentication failed" in error_msg:
            print("\n💡 Authentication Fix:")
            print("   • Check POSTGRES_USER and POSTGRES_PASSWORD environment variables")
            print("   • Default PostgreSQL user is usually 'postgres'")
            print("   • Reset password: sudo -u postgres psql -c \"ALTER USER postgres PASSWORD 'newpassword';\"")
            
        elif "does not exist" in error_msg:
            print(f"\n💡 Database '{database}' does not exist:")
            print(f"   Create it: sudo -u postgres createdb {database}")
            print(f"   Or connect to PostgreSQL and run: CREATE DATABASE {database};")
        
        return False
        
    except Exception as e:
        print(f"❌ Unexpected error: {str(e)}")
        return False

def setup_local_postgres():
    """
    Helper function to set up local PostgreSQL for development.
    """
    print("🚀 Setting up local PostgreSQL for development...")
    
    # Check if PostgreSQL is installed
    try:
        result = subprocess.run(['psql', '--version'], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ PostgreSQL is installed: {result.stdout.strip()}")
        else:
            print("❌ PostgreSQL is not installed")
            return False
    except FileNotFoundError:
        print("❌ PostgreSQL is not installed or not in PATH")
        return False
    
    # Try to start PostgreSQL service (Linux/Ubuntu)
    try:
        subprocess.run(['sudo', 'service', 'postgresql', 'start'], check=True)
        print("✅ PostgreSQL service started")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print("⚠️ Could not start PostgreSQL service automatically")
        print("   Please start it manually for your system")
    
    return True

if __name__ == "__main__":
    # Run connection check
    if not check_postgres_connection():
        print("\n🔧 Run setup_local_postgres() to get help setting up PostgreSQL")