#!/usr/bin/env python3


import os
import sys
import json
import requests
import psycopg2
from datetime import datetime

def check_environment_variables():
    """Check required environment variables."""
    required_vars = [
        'ALPHA_VANTAGE_API_KEY',
        'POSTGRES_HOST',
        'POSTGRES_PORT',
        'POSTGRES_DB', 
        'POSTGRES_USER',
        'POSTGRES_PASSWORD',
        'FERNET_KEY',
        'AIRFLOW_SECRET_KEY'
    ]
    
    status = {'status': 'ok', 'missing_vars': []}
    
    for var in required_vars:
        value = os.getenv(var)
        if not value or 'REPLACE_ME' in value.upper():
            status['missing_vars'].append(var)
    
    if status['missing_vars']:
        status['status'] = 'error'
        status['message'] = f"Missing environment variables: {', '.join(status['missing_vars'])}"
    
    return status

def check_database_connection():
    """Test PostgreSQL connection."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'airflow'),
            user=os.getenv('POSTGRES_USER', 'airflow'),
            password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
            connect_timeout=10
        )
        
        cur = conn.cursor()
        cur.execute('SELECT version();')
        version = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        return {
            'status': 'ok',
            'message': f'Connected to PostgreSQL: {version}'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Database connection failed: {str(e)}'
        }

def check_api_connectivity():
    """Test Alpha Vantage API connectivity."""
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    
    if not api_key or 'REPLACE_ME' in api_key.upper():
        return {
            'status': 'error',
            'message': 'Alpha Vantage API key not configured'
        }
    
    try:
        # Use a lightweight endpoint that doesn't consume quota
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey=demo"
        response = requests.head(url, timeout=10)
        
        if response.status_code == 200:
            return {
                'status': 'ok', 
                'message': 'Alpha Vantage API is accessible'
            }
        else:
            return {
                'status': 'warning',
                'message': f'API returned status code: {response.status_code}'
            }
            
    except Exception as e:
        return {
            'status': 'error',
            'message': f'API connectivity test failed: {str(e)}'
        }

def check_airflow_services():
    """Check Airflow services status."""
    services = {
        'webserver': 'http://localhost:8080/health',
        'scheduler': None  # No direct health endpoint for scheduler
    }
    
    results = {}
    
    # Check webserver
    try:
        response = requests.get(services['webserver'], timeout=5)
        if response.status_code == 200:
            results['webserver'] = {'status': 'ok', 'message': 'Airflow webserver is healthy'}
        else:
            results['webserver'] = {'status': 'error', 'message': f'Webserver returned {response.status_code}'}
    except Exception as e:
        results['webserver'] = {'status': 'error', 'message': f'Cannot connect to webserver: {str(e)}'}
    
    # For scheduler, we'll just note that it should be checked via Docker
    results['scheduler'] = {'status': 'info', 'message': 'Check scheduler via: docker compose logs airflow-scheduler'}
    
    return results

def get_pipeline_stats():
    """Get pipeline data statistics."""
    try:
        conn = psycopg2.connect(
            host=os.getenv('POSTGRES_HOST', 'localhost'),
            port=os.getenv('POSTGRES_PORT', '5432'),
            database=os.getenv('POSTGRES_DB', 'airflow'),
            user=os.getenv('POSTGRES_USER', 'airflow'),
            password=os.getenv('POSTGRES_PASSWORD', 'airflow'),
            connect_timeout=10
        )
        
        cur = conn.cursor()
        
        # Check if stock_quotes table exists
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'stock_quotes'
            );
        """)
        
        table_exists = cur.fetchone()[0]
        
        if not table_exists:
            return {
                'status': 'info',
                'message': 'stock_quotes table does not exist yet (normal on first run)'
            }
        
        # Get basic statistics
        cur.execute('SELECT COUNT(*) FROM stock_quotes;')
        total_records = cur.fetchone()[0]
        
        cur.execute('SELECT COUNT(DISTINCT symbol) FROM stock_quotes;')
        unique_symbols = cur.fetchone()[0]
        
        cur.execute('SELECT MAX(latest_trading_day) FROM stock_quotes;')
        latest_date = cur.fetchone()[0]
        
        cur.close()
        conn.close()
        
        return {
            'status': 'ok',
            'total_records': total_records,
            'unique_symbols': unique_symbols,
            'latest_trading_day': str(latest_date) if latest_date else 'No data',
            'message': f'{total_records} records for {unique_symbols} symbols'
        }
        
    except Exception as e:
        return {
            'status': 'error',
            'message': f'Failed to get pipeline stats: {str(e)}'
        }

def main():
    """Run health check."""
    print("Stock Data Pipeline Health Check")
    print("=" * 50)
    print(f"Timestamp: {datetime.now().isoformat()}")
    print()
    
    health_report = {
        'timestamp': datetime.now().isoformat(),
        'environment': check_environment_variables(),
        'database': check_database_connection(),
        'api': check_api_connectivity(),
        'airflow': check_airflow_services(),
        'pipeline_stats': get_pipeline_stats()
    }
    
    for component, result in health_report.items():
        if component == 'timestamp':
            continue
        print(f"{component.upper()}")
        if component == 'airflow':
            for service, status in result.items():
                print(f"  {service}: {status['message']}")
        else:
            print(f"  {result['message']}")
            if component == 'pipeline_stats' and result['status'] == 'ok':
                print(f"    Total records: {result['total_records']}")
                print(f"    Unique symbols: {result['unique_symbols']}")
                print(f"    Latest data: {result['latest_trading_day']}")
        print()
    
    has_errors = any(
        comp['status'] == 'error' for comp in [
            health_report['environment'],
            health_report['database'], 
            health_report['api']
        ]
    )
    if has_errors:
        print("Overall Status: ISSUES DETECTED")
        print("Please resolve the errors above before running the pipeline.")
        return 1
    else:
        print("Overall Status: HEALTHY")
        print("Pipeline is ready to run.")
        return 0
    if len(sys.argv) > 1 and sys.argv[1] == '--json':
        print(json.dumps(health_report, indent=2))
        return 0

if __name__ == "__main__":
    sys.exit(main())
