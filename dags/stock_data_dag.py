from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging

# Import the functions from our script
import sys
sys.path.append("/opt/airflow/scripts")
from fetch_and_store import fetch_stock_data, store_data_to_postgres, validate_environment

# Default arguments with comprehensive error handling configuration
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),
}

with DAG(
    dag_id='stock_data_pipeline',
    default_args=default_args,
    description='A robust stock data pipeline with comprehensive error handling',
    schedule_interval=timedelta(days=1),  
    catchup=False,
    max_active_runs=1,  
    tags=['stock_data', 'api', 'postgres', 'etl'],
) as dag:

    def _validate_environment():
        """
        Validate that all required environment variables are set before running the pipeline.
        """
        try:
            validate_environment()
            logging.info("Environment validation successful. All required variables are set.")
        except Exception as e:
            logging.error(f"Environment validation failed. Details: {e}")
            raise

    def _fetch_and_store():
        """
        Fetch stock data for a list of companies and store it in PostgreSQL.
        """
        try:
            logging.info("Starting stock data fetch and store process.")

            symbols = ['IBM', 'AAPL', 'GOOGL', 'MSFT']

            success_count = 0
            total_count = len(symbols)

            for symbol in symbols:
                try:
                    logging.info(f"Fetching data for: {symbol}")
                    stock_data = fetch_stock_data(symbol=symbol)

                    if stock_data:
                        store_data_to_postgres(stock_data)
                        success_count += 1
                        logging.info(f"Data for {symbol} stored successfully.")
                    else:
                        logging.warning(f"No data returned for {symbol}. Skipping.")

                except Exception as e:
                    logging.error(f"Error processing {symbol}: {e}. Continuing with next symbol.")
                    continue

            success_rate = success_count / total_count
            if success_rate < 0.5:
                raise ValueError(f"Less than half of the symbols were processed successfully ({success_count}/{total_count}).")

            logging.info(f"Task completed. Successfully processed {success_count}/{total_count} symbols.")

        except Exception as e:
            logging.error(f"Critical error in fetch_and_store task: {e}.")
            raise

    def _cleanup_old_data():
        """
        Clean up old data to maintain database performance and manage storage.
        """
        try:
            from fetch_and_store import cleanup_old_data
            cleanup_old_data(days_to_keep=90) 
            logging.info("Data cleanup completed successfully.")
        except Exception as e:
            logging.warning(f"Data cleanup failed, but continuing: {e}")

    # Task definitions
    validate_env_task = PythonOperator(
        task_id='validate_environment',
        python_callable=_validate_environment,
        doc_md="""
        ## Validate Environment Task
        This task checks that all required environment variables are set before running the pipeline.
        """,
    )

    fetch_and_store_task = PythonOperator(
        task_id='fetch_and_store_stock_data',
        python_callable=_fetch_and_store,
        doc_md="""
        ## Fetch and Store Task
        This task fetches stock data for several companies and stores it in PostgreSQL.
        """,
    )

    cleanup_task = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=_cleanup_old_data,
        trigger_rule=TriggerRule.ALL_DONE,  # Run regardless of upstream task status
        doc_md="""
        ## Cleanup Task
        This task removes old data to maintain database performance and manage storage.
        """,
    )

    # Task dependencies
    validate_env_task >> fetch_and_store_task >> cleanup_task


