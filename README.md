# Store Monitoring System

A FastAPI-based system for monitoring restaurant store uptime and downtime during business hours.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the Application

```bash
python app.py
```

The server will start at `http://localhost:8000`

### 3. Generate Reports

**Trigger Report Generation:**

```bash
curl -X POST "http://localhost:8000/trigger_report"
```

Returns: `{"report_id": "uuid-here"}`

**Check Report Status:**

```bash
curl "http://localhost:8000/get_report/{report_id}"
```

Returns: CSV file when complete, or `{"status": "Running"}` if still processing

## API Endpoints

- `GET /` - Root endpoint
- `POST /trigger_report` - Start report generation
- `GET /get_report/{report_id}` - Get report status or download CSV

## Data Sources

The system automatically loads data from:

- `test_data/` folder (if available)
- Falls back to `data/` folder

Required CSV files:

- `store_status.csv` - Store activity data
- `business_hours.csv` - Store operating hours
- `timezones.csv` - Store timezone information

## Report Schema

Generated CSV contains:

```
store_id, uptime_last_hour, uptime_last_day, uptime_last_week, downtime_last_hour, downtime_last_day, downtime_last_week
```

## Architecture

- **FastAPI** - Web framework
- **SQLite** - Database storage
- **Pandas** - Data processing
- **Async** - Report generation
- **Trigger + Poll** - API pattern

## Future Improvements

- Add progress bar for report generation
- Cache business hours data in memory
- Store report history in database
- Email notifications when reports are ready
- Add basic authentication (username/password)
- Support for different report formats (JSON, Excel)
- Add real-time store status updates
