# MLB Tableau Dashboard

A daily automated pipeline that fetches MLB statistics from the official MLB API and loads them into Snowflake for visualization in Tableau.

## Project Description

This project automates the collection of MLB game data, player statistics, and performance metrics on a daily basis. The pipeline extracts data from the MLB Stats API, transforms it into a structured format, and loads it into Snowflake data warehouse for analysis and visualization in Tableau.

**Key Capabilities:**
- Fetches game schedules, boxscores, and team information from MLB API
- Extracts player-level batting and pitching statistics
- Automatically retrieves player handedness (bat side, pitch hand) from player profiles
- Runs on a configurable schedule (default: daily at 6:00 AM)
- Implements retry logic with exponential backoff for reliability
- Logs all pipeline runs with full error tracking
- Supports JWT token authentication with Snowflake

## Requirements

- **Python 3.8+**
- **Snowflake** account with database/schema setup
- **macOS** (for launchd scheduler) or any system for manual runs

**Python Dependencies:**
```
pandas>=1.3.0
requests>=2.26.0
snowflake-connector-python[pandas]>=2.8.0
python-dotenv>=0.19.0
urllib3>=1.26.0
```

## Setup Instructions

### 1. Clone the Repository
```bash
git clone https://github.com/tcarlon94/mlb-tableau-dashboard.git
cd mlb-tableau-dashboard
```

### 2. Create Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment Variables
Create a `.env` file in the root directory with your Snowflake credentials:
```
ACCOUNT=your_account_id
USER=your_username
JWT_TOKEN=your_jwt_token_here
WAREHOUSE=your_warehouse_name
DATABASE=your_database_name
SCHEMA=your_schema_name
ROLE=your_role_name
```

### 5. Set Up Snowflake Schema
Run the SQL scripts in `sql/` folder in order:
```sql
-- 1. Setup
source sql/01_setup.sql

-- 2. Create dimensions
source sql/02_dimensions.sql

-- 3. Create facts
source sql/03_facts.sql

-- 4. Create views
source sql/04_views.sql
```

## Usage

### Manual Run
```bash
python3 src/run_pipeline.py
```

By default, this fetches data for yesterday (configurable in `config.json`).

### Run for Specific Date
Edit `src/run_pipeline.py` and modify the `run_for_date()` call with a specific date string (YYYY-MM-DD format), or use the new CLI backfill mode described below.

### Backfill Missing Dates
Run the pipeline for a range of dates to backfill missing game data:
```bash
python3 src/run_pipeline.py --start-date 2026-03-25 --end-date 2026-06-30
```
If `--end-date` is omitted, the pipeline runs only the `--start-date`.

### Refresh only
If you only want to load dates that are not already in the games table, add `--refresh-only`:
```bash
python3 src/run_pipeline.py --start-date 2026-03-25 --end-date 2026-06-30 --refresh-only
```
This skips dates already present in `RAW.GAMES` and only processes missing dates.

### Configuration
Edit `config.json` to customize:
- `date_offset`: Number of days to look back (-1 = yesterday, 0 = today)
- `max_workers`: Parallel API calls (default: 10)
- `max_retries`: Retry attempts for failed API calls (default: 3)
- `log_level`: DEBUG, INFO, WARNING, ERROR
- Table names for your Snowflake schema

## Daily Scheduling

### macOS Setup (Automated Runs)

1. **Create logs directory:**
   ```bash
   mkdir -p logs
   ```

2. **Install the scheduler:**
   ```bash
   cp com.mlb.tableau.dashboard.plist ~/Library/LaunchAgents/
   launchctl load ~/Library/LaunchAgents/com.mlb.tableau.dashboard.plist
   ```

3. **Verify installation:**
   ```bash
   launchctl list | grep mlb.tableau.dashboard
   ```

### Monitor Daily Runs
```bash
# View pipeline logs
tail -f logs/pipeline.log

# View scheduler logs
cat logs/launchd.out
cat logs/launchd.err
```

### Manage Scheduler
```bash
# Stop scheduler
launchctl unload ~/Library/LaunchAgents/com.mlb.tableau.dashboard.plist

# Restart scheduler
launchctl load ~/Library/LaunchAgents/com.mlb.tableau.dashboard.plist

# Test immediate run
launchctl start com.mlb.tableau.dashboard
```

### Change Schedule Time
Edit `com.mlb.tableau.dashboard.plist` and change the Hour value:
```xml
<key>Hour</key>
<integer>6</integer>  <!-- Change 6 to your desired hour (0-23) -->
```

Then reload:
```bash
launchctl unload ~/Library/LaunchAgents/com.mlb.tableau.dashboard.plist
launchctl load ~/Library/LaunchAgents/com.mlb.tableau.dashboard.plist
```

For full scheduler documentation, see [SCHEDULER_SETUP.md](SCHEDULER_SETUP.md).

## Project Structure

```
mlb-tableau-dashboard/
├── README.md                              # This file
├── requirements.txt                       # Python dependencies
├── config.json                            # Pipeline configuration
├── com.mlb.tableau.dashboard.plist        # macOS scheduler config
├── SCHEDULER_SETUP.md                     # Detailed scheduler setup
├── .env                                   # Environment variables (gitignored)
├── .gitignore                             # Git ignore rules
│
├── sql/                                   # Snowflake SQL scripts
│   ├── 01_setup.sql                       # Initial setup
│   ├── 02_dimensions.sql                  # Dimension tables
│   ├── 03_facts.sql                       # Fact tables
│   └── 04_views.sql                       # Views
│
├── src/                                   # Python source code
│   ├── run_pipeline.py                    # Main pipeline orchestrator
│   ├── extract.py                         # MLB API data extraction
│   ├── transform.py                       # Data transformation logic
│   └── load.py                            # Snowflake data loading
│
├── tableau/                               # Tableau workbooks
│   └── (Tableau files go here)
│
└── logs/                                  # Pipeline execution logs
    └── pipeline.log                       # Daily run logs
```

## Data Flow

1. **Extract** (`extract.py`)
   - Fetches schedule/games for a specific date
   - Retrieves boxscore data for each game
   - Fetches live-feed decisions for each game to capture winning/losing/save pitchers
   - Gets player details including handedness via parallel API calls

2. **Transform** (`transform.py`)
   - Parses JSON responses into structured DataFrames
   - Extracts player stats (batting, pitching)
   - Parses game-level pitcher decisions from live feed data
   - Handles missing data and type conversions

3. **Load** (`load.py`)
   - Connects to Snowflake via JWT authentication
   - Replaces NaN/None values appropriately
   - Inserts data into dimension and fact tables

## Features

✅ **Automated Daily Runs** - Configurable scheduler via launchd  
✅ **Retry Logic** - Automatic retry with exponential backoff  
✅ **Parallel Processing** - Concurrent API calls for speed  
✅ **JWT Authentication** - Secure Snowflake connection  
✅ **Comprehensive Logging** - Full error tracking and debugging  
✅ **Configuration File** - Easy customization without code changes  
✅ **Player Handedness** - Fetches batting/pitching preferences  
✅ **Data Deduplication** - Handles duplicate player records  

## Troubleshooting

**Pipeline fails with API errors?**
- Check `logs/pipeline.log` for details
- Verify MLB API is accessible: `curl https://statsapi.mlb.com/api/v1/teams`
- Retry logic will attempt up to 3 times automatically

**Snowflake connection issues?**
- Verify credentials in `.env`
- Check JWT token hasn't expired
- Confirm network policy allows your IP

**Scheduler not running?**
- Check launchd logs: `cat logs/launchd.err`
- Verify plist syntax: `plutil -lint com.mlb.tableau.dashboard.plist`
- Ensure Python path is correct: `which python3`

**Port or permission issues?**
- Check logs directory exists: `mkdir -p logs`
- Verify read/write permissions on project folder

## License

MIT

## Contact

For issues or questions, create an issue on GitHub.
