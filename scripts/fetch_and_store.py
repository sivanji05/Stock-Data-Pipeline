import os
import requests
import psycopg2
import json
import time
from datetime import datetime, timedelta
from typing import Dict, Optional, Any
from decimal import Decimal, InvalidOperation
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def validate_environment():
    """Validate that all required environment variables are set."""
    required_vars = [
        'ALPHA_VANTAGE_API_KEY',
        'POSTGRES_HOST',
        'POSTGRES_PORT', 
        'POSTGRES_DB',
        'POSTGRES_USER',
        'POSTGRES_PASSWORD'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    logger.info("All required environment variables are set")

def validate_stock_data(data: Dict[str, Any]) -> bool:
    """Validate the structure and content of stock data."""
    try:
        if not data or 'Global Quote' not in data:
            logger.warning("Invalid data structure: missing 'Global Quote'")
            return False
            
        quote = data['Global Quote']
        
        # Check for required fields
        required_fields = [
            '01. symbol', '02. open', '03. high', '04. low', 
            '05. price', '06. volume', '07. latest trading day'
        ]
        
        missing_fields = [field for field in required_fields if not quote.get(field)]
        if missing_fields:
            logger.warning(f"Missing required fields: {', '.join(missing_fields)}")
            return False
            
        # Validate numeric fields
        numeric_fields = ['02. open', '03. high', '04. low', '05. price', '08. previous close', '09. change']
        for field in numeric_fields:
            value = quote.get(field)
            if value and value != 'N/A':
                try:
                    float(value)
                except (ValueError, TypeError):
                    logger.warning(f"Invalid numeric value for {field}: {value}")
                    return False
        
        # Validate volume
        volume = quote.get('06. volume')
        if volume and volume != 'N/A':
            try:
                int(volume)
            except (ValueError, TypeError):
                logger.warning(f"Invalid volume value: {volume}")
                return False
                
        logger.info("Stock data validation successful")
        return True
        
    except Exception as e:
        logger.error(f"Error during data validation: {e}")
        return False

def fetch_stock_data(symbol: str = 'IBM', max_retries: int = 3, retry_delay: int = 5) -> Optional[Dict[str, Any]]:
    """Fetch stock data from Alpha Vantage API with comprehensive error handling and retries."""
    api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
    if not api_key:
        raise ValueError("ALPHA_VANTAGE_API_KEY environment variable not set.")

    url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"
    
    for attempt in range(max_retries):
        try:
            logger.info(f"Fetching data for {symbol} (attempt {attempt + 1}/{max_retries})")
            
            # Set timeout and headers for better reliability
            headers = {
                'User-Agent': 'Stock-Data-Pipeline/1.0',
                'Accept': 'application/json'
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API error messages
            if 'Error Message' in data:
                logger.error(f"API Error for {symbol}: {data['Error Message']}")
                return None
                
            if 'Note' in data:
                logger.warning(f"API Note for {symbol}: {data['Note']}")
                if attempt < max_retries - 1:
                    logger.info(f"Rate limit hit, waiting {retry_delay * 2} seconds...")
                    time.sleep(retry_delay * 2)
                    continue
                return None
            
            # Validate data structure
            if validate_stock_data(data):
                logger.info(f"Successfully fetched and validated data for {symbol}")
                return data
            else:
                logger.warning(f"Data validation failed for {symbol}")
                return None
                
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout fetching data for {symbol} (attempt {attempt + 1})")
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error fetching data for {symbol} (attempt {attempt + 1})")
        except requests.exceptions.HTTPError as e:
            logger.error(f"HTTP error fetching data for {symbol}: {e}")
            if response.status_code == 429:  # Rate limit
                if attempt < max_retries - 1:
                    logger.info(f"Rate limited, waiting {retry_delay * 2} seconds...")
                    time.sleep(retry_delay * 2)
                    continue
            break  # Don't retry on non-recoverable HTTP errors
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON response for {symbol} (attempt {attempt + 1})")
        except Exception as e:
            logger.error(f"Unexpected error fetching data for {symbol}: {e}")
            
        if attempt < max_retries - 1:
            logger.info(f"Retrying in {retry_delay} seconds...")
            time.sleep(retry_delay)
    
    logger.error(f"Failed to fetch data for {symbol} after {max_retries} attempts")
    return None

def safe_decimal_conversion(value: str, default: Optional[Decimal] = None) -> Optional[Decimal]:
    """Safely convert string to Decimal, handling missing or invalid values."""
    if not value or value == 'N/A' or value == '':
        return default
    
    try:
        # Remove any percentage signs or extra characters
        clean_value = value.replace('%', '').replace(',', '').strip()
        return Decimal(clean_value)
    except (InvalidOperation, ValueError, TypeError):
        logger.warning(f"Could not convert '{value}' to Decimal")
        return default

def get_database_connection(max_retries: int = 3):
    """Get database connection with retry logic."""
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                host=os.getenv('POSTGRES_HOST'),
                port=os.getenv('POSTGRES_PORT'),
                database=os.getenv('POSTGRES_DB'),
                user=os.getenv('POSTGRES_USER'),
                password=os.getenv('POSTGRES_PASSWORD'),
                connect_timeout=10
            )
            return conn
        except psycopg2.OperationalError as e:
            logger.warning(f"Database connection attempt {attempt + 1} failed: {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                raise

def store_data_to_postgres(data: Dict[str, Any]) -> bool:
    """Store stock data to PostgreSQL with comprehensive error handling."""
    if not data or 'Global Quote' not in data:
        logger.warning("No data or invalid data structure to store.")
        return False

    quote = data['Global Quote']
    symbol = quote.get('01. symbol')
    
    if not symbol:
        logger.error("No symbol found in data")
        return False

    conn = None
    try:
        conn = get_database_connection()
        cur = conn.cursor()

        # Create table if it doesn't exist with better schema
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stock_quotes (
                id SERIAL PRIMARY KEY,
                symbol VARCHAR(10) NOT NULL,
                open_price NUMERIC(10, 4),
                high_price NUMERIC(10, 4),
                low_price NUMERIC(10, 4),
                price NUMERIC(10, 4),
                volume BIGINT,
                latest_trading_day DATE,
                previous_close NUMERIC(10, 4),
                change_amount NUMERIC(10, 4),
                change_percent VARCHAR(20),
                data_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(symbol, latest_trading_day)
            );
        """)

        # Create index for better query performance
        cur.execute("""
            CREATE INDEX IF NOT EXISTS idx_stock_quotes_symbol_date 
            ON stock_quotes(symbol, latest_trading_day);
        """)

        # Extract and safely convert data
        open_price = safe_decimal_conversion(quote.get('02. open'))
        high_price = safe_decimal_conversion(quote.get('03. high'))
        low_price = safe_decimal_conversion(quote.get('04. low'))
        price = safe_decimal_conversion(quote.get('05. price'))
        previous_close = safe_decimal_conversion(quote.get('08. previous close'))
        change = safe_decimal_conversion(quote.get('09. change'))
        
        # Handle volume
        volume_str = quote.get('06. volume')
        volume = None
        if volume_str and volume_str != 'N/A':
            try:
                volume = int(volume_str)
            except (ValueError, TypeError):
                logger.warning(f"Invalid volume value: {volume_str}")

        latest_trading_day = quote.get('07. latest trading day')
        change_percent = quote.get('10. change percent')

        # Use INSERT ... ON CONFLICT to handle duplicates
        cur.execute("""
            INSERT INTO stock_quotes (
                symbol, open_price, high_price, low_price, price, volume,
                latest_trading_day, previous_close, change_amount, change_percent
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, latest_trading_day) 
            DO UPDATE SET 
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                price = EXCLUDED.price,
                volume = EXCLUDED.volume,
                previous_close = EXCLUDED.previous_close,
                change_amount = EXCLUDED.change_amount,
                change_percent = EXCLUDED.change_percent,
                data_timestamp = CURRENT_TIMESTAMP
        """, (
            symbol, open_price, high_price, low_price, price, volume,
            latest_trading_day, previous_close, change, change_percent
        ))

        conn.commit()
        logger.info(f"Successfully stored/updated data for {symbol} on {latest_trading_day}")
        return True

    except psycopg2.Error as e:
        logger.error(f"PostgreSQL error storing data for {symbol}: {e}")
        if conn:
            conn.rollback()
        return False
    except Exception as e:
        logger.error(f"Unexpected error storing data for {symbol}: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

def cleanup_old_data(days_to_keep: int = 90) -> bool:
    """Clean up old data to maintain database performance."""
    conn = None
    try:
        conn = get_database_connection()
        cur = conn.cursor()
        
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        cur.execute("""
            DELETE FROM stock_quotes 
            WHERE latest_trading_day < %s
        """, (cutoff_date.date(),))
        
        deleted_rows = cur.rowcount
        conn.commit()
        
        logger.info(f"Cleaned up {deleted_rows} old records older than {cutoff_date.date()}")
        return True
        
    except Exception as e:
        logger.error(f"Error during data cleanup: {e}")
        if conn:
            conn.rollback()
        return False
    finally:
        if conn:
            cur.close()
            conn.close()

def health_check() -> Dict[str, Any]:
    """Perform a health check of the system components."""
    health_status = {
        'timestamp': datetime.now().isoformat(),
        'environment': 'ok',
        'database': 'unknown',
        'api': 'unknown'
    }
    
    try:
        # Check environment variables
        validate_environment()
        health_status['environment'] = 'ok'
    except Exception as e:
        health_status['environment'] = f'error: {e}'
    
    try:
        # Check database connection
        conn = get_database_connection()
        conn.close()
        health_status['database'] = 'ok'
    except Exception as e:
        health_status['database'] = f'error: {e}'
    
    try:
        # Test API connection (without using quota)
        api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=IBM&apikey={api_key}"
        response = requests.head(url, timeout=10)
        if response.status_code == 200:
            health_status['api'] = 'ok'
        else:
            health_status['api'] = f'http_error: {response.status_code}'
    except Exception as e:
        health_status['api'] = f'error: {e}'
    
    return health_status

if __name__ == "__main__":
    # For testing purposes
    import argparse
    
    parser = argparse.ArgumentParser(description='Stock Data Fetcher')
    parser.add_argument('--symbol', default='IBM', help='Stock symbol to fetch')
    parser.add_argument('--health-check', action='store_true', help='Run health check')
    
    args = parser.parse_args()
    
    if args.health_check:
        status = health_check()
        print(json.dumps(status, indent=2))
    else:
        validate_environment()
        stock_data = fetch_stock_data(symbol=args.symbol)
        if stock_data:
            success = store_data_to_postgres(stock_data)
            print(f"Data storage {'successful' if success else 'failed'}")
        else:
            print("Failed to fetch stock data")
