# 🚀 Quick Start Guide - Stock Data Pipeline

## **One-Command Setup**

1. **Open Command Prompt** and navigate to the project:
   ```cmd
   cd "c:\Users\lenovo\Downloads\stock_data_pipeline\stock_data_pipeline"
   ```

2. **Start the entire pipeline** with one command:
   ```cmd
   docker compose up -d --build
   ```

3. **Access Airflow Web UI**: http://localhost:8080
   - Username: `admin`
   - Password: `admin`

## **Common Commands**

### **Start Pipeline**
```cmd
docker compose up -d --build
```

### **Check Status**
```cmd
docker compose ps
```

### **View Logs**
```cmd
# All services
docker compose logs

# Specific service
docker compose logs airflow-scheduler
docker compose logs postgres
```

### **Stop Pipeline**
```cmd
docker compose down
```

### **Stop and Remove All Data**
```cmd
docker compose down -v
```

### **Restart Pipeline**
```cmd
docker compose restart
```

## **Accessing Services**

| Service | URL/Connection | Credentials |
|---------|----------------|-------------|
| Airflow Web UI | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | airflow / airflow |
| Redis | localhost:6379 | (no auth) |

## **Pipeline Features**

✅ **Automated Stock Data Fetching** (IBM, AAPL, GOOGL, MSFT)  
✅ **Comprehensive Error Handling** with retries  
✅ **Data Validation** and cleaning  
✅ **PostgreSQL Storage** with duplicate handling  
✅ **Automatic Data Cleanup** (90+ days)  
✅ **Health Monitoring** and logging  

## **Troubleshooting**

### **Docker Desktop Not Running**
- Start Docker Desktop from Windows Start Menu
- Wait for the whale icon to appear in system tray

### **Permission Issues**
- Run Command Prompt as Administrator if needed

### **Port Already in Use**
- Stop other services using port 8080 or 5432
- Or change ports in docker-compose.yml

### **API Key Issues**
- Update `.env` file with your Alpha Vantage API key
- Get free key from: https://www.alphavantage.co/

## **Next Steps After Starting**

1. **Open Airflow UI**: http://localhost:8080
2. **Find the DAG**: Look for `stock_data_pipeline`
3. **Enable the DAG**: Toggle the switch to "ON"
4. **Monitor**: Click on DAG to view task execution
5. **Check Data**: Connect to PostgreSQL to view stored stock data

## **File Structure**

```
stock_data_pipeline/
├── dags/
│   └── stock_data_dag.py          # Airflow DAG
├── scripts/
│   └── fetch_and_store.py         # Data fetching logic
├── docker-compose.yml             # Docker orchestration
├── .env                           # Environment variables
├── README.md                      # Detailed documentation
└── QUICK_START.md                 # This file
```

---

**That's it! Your stock data pipeline is production-ready! 🎉**
