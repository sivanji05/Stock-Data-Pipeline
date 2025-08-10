# Dockerized Stock Market Data Pipeline

A robust, scalable, and production-ready data pipeline using Apache Airflow and PostgreSQL to automatically fetch, process, and store stock market data from Alpha Vantage API.

## 🏗️ Architecture Overview

This pipeline implements the following architecture:

```
Alpha Vantage API → Airflow DAG → PostgreSQL Database
       ↓               ↓              ↓
   JSON Data      Data Processing   Persistent Storage
```

### Key Features

- **🔄 Automated Scheduling**: Runs daily (configurable to hourly)
- **🛡️ Comprehensive Error Handling**: Graceful handling of API failures, network issues, and data validation
- **📊 Data Validation**: Thorough validation of incoming data before storage
- **🔒 Security**: Environment-based credential management
- **📈 Scalability**: Support for multiple stock symbols with parallel processing
- **🔧 Resilience**: Retry mechanisms and failure recovery
- **📝 Monitoring**: Detailed logging and health checks
- **🗄️ Data Integrity**: Duplicate handling and data consistency

## 📁 Project Structure

```
stock_data_pipeline/
├── dags/
│   └── stock_data_dag.py          # Airflow DAG with comprehensive error handling
├── scripts/
│   └── fetch_and_store.py         # Enhanced data fetching and storage logic
├── docker-compose.yml             # Complete Docker orchestration
├── .env                           # Environment configuration (template)
├── logs/                          # Airflow logs (auto-created)
├── plugins/                       # Airflow plugins (auto-created)
└── README.md                      # This file
```

## 🛠️ Prerequisites

- **Docker Desktop** (includes Docker Engine and Docker Compose)
- **Alpha Vantage API Key** (free from https://www.alphavantage.co/)
- **At least 4GB RAM** available for Docker containers

## 🚀 Quick Start

### 1. Clone and Navigate

```bash
git clone <your-repo-url>
cd stock_data_pipeline
```

### 2. Configure Environment Variables

Edit the `.env` file and replace placeholder values:

```bash
# Required: Generate Fernet key
python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"

# Copy the output to FERNET_KEY in .env file
# Also set your Alpha Vantage API key and other credentials
```

**Essential Variables to Update:**
- `FERNET_KEY`: Generate using the command above
- `AIRFLOW_SECRET_KEY`: Any strong random string (min 16 characters)
- `ALPHA_VANTAGE_API_KEY`: Your API key from Alpha Vantage

### 3. Build and Launch Pipeline

```bash
# Build and start all services
docker compose up --build -d

# Check status
docker compose ps

# View logs (if needed)
docker compose logs -f airflow-scheduler
```

### 4. Access Airflow UI

1. Open http://localhost:8080 in your browser
2. Login with `admin` / `admin` (configurable in .env)
3. Find the `stock_data_pipeline` DAG
4. Toggle it ON to start automatic execution

## 📊 Pipeline Details

### DAG Configuration

- **Schedule**: Daily at midnight (configurable)
- **Retries**: 3 attempts with 5-minute delays
- **Timeout**: 30 seconds per API call
- **Parallelism**: Processes multiple stocks concurrently

### Data Processing Flow

1. **Environment Validation**: Checks all required environment variables
2. **Multi-Stock Fetching**: Retrieves data for multiple symbols (IBM, AAPL, GOOGL, MSFT)
3. **Data Validation**: Validates JSON structure and data types
4. **Database Storage**: Stores with duplicate handling (ON CONFLICT)
5. **Data Cleanup**: Removes old data (90+ days) for performance

### Error Handling Strategy

- **API Failures**: Retry with exponential backoff
- **Rate Limiting**: Automatic detection and delay
- **Network Issues**: Connection timeout and retry logic
- **Data Validation**: Graceful handling of missing/invalid data
- **Database Errors**: Transaction rollback and error logging
- **Partial Failures**: Continue processing other stocks if one fails

## 🗄️ Database Schema

```sql
CREATE TABLE stock_quotes (
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
```

## 🔧 Configuration Options

### Changing Schedule Frequency

To run hourly instead of daily, modify `dags/stock_data_dag.py`:

```python
schedule_interval=timedelta(hours=1),  # Change from days=1 to hours=1
```

### Adding More Stock Symbols

Modify the `symbols` list in `_fetch_and_store()` function:

```python
symbols = ['IBM', 'AAPL', 'GOOGL', 'MSFT', 'TSLA', 'AMZN', 'NVDA']
```

### Adjusting Data Retention

Change `DATA_RETENTION_DAYS` in `.env` file or modify cleanup task:

```python
cleanup_old_data(days_to_keep=30)  # Keep 30 days instead of 90
```

## 🔍 Monitoring and Debugging

### Health Check

Run a system health check:

```bash
# Execute health check script
docker compose exec airflow-scheduler python /opt/airflow/scripts/fetch_and_store.py --health-check
```

### View Logs

```bash
# All services
docker compose logs

# Specific service
docker compose logs airflow-scheduler
docker compose logs postgres

# Follow logs in real-time
docker compose logs -f airflow-webserver
```

### Database Access

```bash
# Connect to PostgreSQL
docker compose exec postgres psql -U airflow -d airflow

# View stored data
SELECT * FROM stock_quotes ORDER BY latest_trading_day DESC LIMIT 10;
```

### Testing Individual Components

```bash
# Test data fetching for a specific symbol
docker compose exec airflow-scheduler python /opt/airflow/scripts/fetch_and_store.py --symbol AAPL
```

## 🛠️ Troubleshooting

### Common Issues

1. **"FERNET_KEY not set" Error**
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   # Copy output to .env file
   ```

2. **API Rate Limiting**
   - Alpha Vantage free tier: 25 requests/day
   - Consider upgrading or reducing fetch frequency

3. **Database Connection Issues**
   ```bash
   docker compose logs postgres
   docker compose restart postgres
   ```

4. **Airflow DAG Not Appearing**
   ```bash
   docker compose logs airflow-scheduler
   # Check for syntax errors in DAG files
   ```

5. **Permission Issues (Linux/Mac)**
   ```bash
   mkdir -p logs plugins
   sudo chown -R 50000:0 logs plugins
   ```

### Performance Optimization

- **Memory**: Increase Docker memory allocation to 6GB+ for better performance
- **Disk Space**: Monitor disk usage; logs and data can grow over time
- **Network**: Ensure stable internet connection for API calls

## 🧹 Cleanup and Maintenance

### Stop Pipeline

```bash
# Stop all services
docker compose down

# Stop and remove volumes (deletes all data)
docker compose down -v
```

### Data Backup

```bash
# Backup PostgreSQL data
docker compose exec postgres pg_dump -U airflow airflow > backup.sql

# Restore from backup
docker compose exec -T postgres psql -U airflow airflow < backup.sql
```

### Update Pipeline

```bash
# Pull latest changes and rebuild
git pull
docker compose down
docker compose up --build -d
```

## 📈 Production Considerations

### Security Enhancements

- Use Docker secrets for sensitive data
- Implement SSL/TLS for database connections  
- Set up proper firewall rules
- Use strong passwords and rotate credentials regularly

### Scalability Improvements

- Implement horizontal scaling with Celery executor
- Add Redis for task queuing
- Use external PostgreSQL for better performance
- Implement data partitioning for large datasets

### Monitoring & Alerting

- Set up Airflow email notifications
- Integrate with monitoring tools (Prometheus, Grafana)
- Implement custom health check endpoints
- Add business logic monitoring

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:

1. Check the troubleshooting section above
2. Review Airflow logs for error details
3. Consult Alpha Vantage API documentation
4. Open an issue in this repository

---

**Note**: This pipeline is designed for educational and development purposes. For production use, consider additional security, monitoring, and scalability measures.
*   **Custom Airflow image:** Create a custom Dockerfile for Airflow to pre-install Python dependencies like `requests` and `psycopg2-binary`.
*   **Monitoring and alerting:** Integrate with monitoring tools to get alerts on pipeline failures.
*   **Data visualization:** Add a component to visualize the fetched stock data.


