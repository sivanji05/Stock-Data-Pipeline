
import os
import sys
import secrets
import string
from cryptography.fernet import Fernet

def generate_fernet_key():
    """Generate a Fernet key for Airflow encryption."""
    return Fernet.generate_key().decode()

def generate_secret_key(length=32):
    """Generate a strong secret key."""
    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(secrets.choice(alphabet) for _ in range(length))

def create_env_file():
    """Create or update the .env file with generated keys."""
    env_file = '.env'
    
    print("Stock Data Pipeline Setup")
    print("=" * 50)
    
    # Check if .env file exists
    if os.path.exists(env_file):
        print(f"Found existing {env_file} file")
        response = input("Do you want to regenerate keys? (y/N): ").lower().strip()
        if response not in ['y', 'yes']:
            print("Setup cancelled. Existing .env file preserved.")
            return
    
    print("Generating security keys...")
    fernet_key = generate_fernet_key()
    secret_key = generate_secret_key()
    
    print("Please provide the following information:")
    
    # Get Alpha Vantage API key
    alpha_vantage_key = input("Enter your Alpha Vantage API key (get it from https://www.alphavantage.co/): ").strip()
    if not alpha_vantage_key:
        print("Warning: No API key provided. You'll need to update the .env file manually.")
        alpha_vantage_key = "YOUR_ALPHA_VANTAGE_API_KEY_HERE_REPLACE_ME"
    
    # Create .env content
    env_content = f"""
FERNET_KEY={fernet_key}
AIRFLOW_SECRET_KEY={secret_key}
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
AIRFLOW_UID=50000
_PIP_ADDITIONAL_REQUIREMENTS=psycopg2-binary requests
AIRFLOW_PROJ_DIR=.
ALPHA_VANTAGE_API_KEY={alpha_vantage_key}
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_DB=airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
STOCK_SYMBOLS=IBM,AAPL,GOOGL,MSFT,TSLA
DATA_RETENTION_DAYS=90
MAX_API_RETRIES=3
RETRY_DELAY_SECONDS=5
"""
    
    # Write .env file
    with open(env_file, 'w') as f:
        f.write(env_content)
    
    print(f"{env_file} file created successfully.")
    print("\nNext steps:")
    print("1. Review the generated .env file")
    print("2. Update Alpha Vantage API key if not provided")
    print("3. Run: docker compose up --build -d")
    print("4. Access Airflow UI at http://localhost:8080")
    
def validate_environment():
    """Validate the current environment setup."""
    print("\nValidating environment...")
    
    # Check if .env file exists
    if not os.path.exists('.env'):
        print(".env file not found. Run setup first.")
        return False
    
    # Check required environment variables
    from dotenv import load_dotenv
    load_dotenv()
    
    required_vars = [
        'FERNET_KEY',
        'AIRFLOW_SECRET_KEY', 
        'ALPHA_VANTAGE_API_KEY'
    ]
    
    missing_vars = []
    for var in required_vars:
        value = os.getenv(var)
        if not value or 'REPLACE_ME' in value:
            missing_vars.append(var)
    
    if missing_vars:
        print(f"Missing or invalid environment variables: {', '.join(missing_vars)}")
        return False
    
    print("Environment validation passed.")
    return True

def main():
    """Main setup function."""
    if len(sys.argv) > 1 and sys.argv[1] == 'validate':
        validate_environment()
    else:
        create_env_file()
        
        # Try to validate
        try:
            validate_environment()
        except ImportError:
            print("\npython-dotenv not installed. Install with: pip install python-dotenv")

if __name__ == "__main__":
    main()
