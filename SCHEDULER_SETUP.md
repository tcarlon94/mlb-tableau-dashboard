# MLB Tableau Dashboard - Daily Scheduler Setup

## Features Added
- **Config File** (`config.json`) - Centralized configuration for dates, tables, and logging
- **Logging** - All runs logged to `logs/pipeline.log` with timestamps
- **Retry Logic** - Automatic retry with exponential backoff for API failures
- **Dynamic Dates** - Pipeline calculates date based on `date_offset` in config

## Configuration

Edit `config.json` to customize:
- `date_offset`: -1 = yesterday (default), 0 = today, 1 = tomorrow
- `max_workers`: Number of parallel threads (default: 10)
- `max_retries`: Number of retries for API calls (default: 3)
- `log_level`: DEBUG, INFO, WARNING, ERROR (default: INFO)
- Table names for your Snowflake schema

## Running the Pipeline

**Manual run (uses config date_offset):**
```bash
python3 src/run_pipeline.py
```

**Run for specific date:**
```bash
python3 src/run_pipeline.py  # Edit run_pipeline.py to pass date_str argument if needed
```

## macOS Daily Scheduler (launchd)

### Setup (one-time):

1. **Create logs directory:**
   ```bash
   mkdir -p logs
   ```

2. **Install launchd job:**
   ```bash
   cp com.mlb.tableau.dashboard.plist ~/Library/LaunchAgents/
   launchctl load ~/Library/LaunchAgents/com.mlb.tableau.dashboard.plist
   ```

3. **Verify installation:**
   ```bash
   launchctl list | grep mlb.tableau.dashboard
   ```

### Configuration:
- **Schedule time:** Edit `com.mlb.tableau.dashboard.plist`
  - `<integer>6</integer>` = 6:00 AM (adjust Hour for different times)
  - Currently set to run daily at 6:00 AM

### Monitor Daily Runs:
```bash
# View logs
tail -f logs/pipeline.log

# View scheduler logs
cat logs/launchd.out
cat logs/launchd.err
```

### Managing the Job:
```bash
# Stop the scheduled job
launchctl unload ~/Library/LaunchAgents/com.mlb.tableau.dashboard.plist

# Restart the job
launchctl load ~/Library/LaunchAgents/com.mlb.tableau.dashboard.plist

# Force immediate run (for testing)
launchctl start com.mlb.tableau.dashboard
```

## Troubleshooting

**Job not running?**
- Check launchd logs: `cat logs/launchd.err`
- Verify plist syntax: `plutil -lint com.mlb.tableau.dashboard.plist`
- Check that the Python path is correct: `which python3`

**API failures?**
- Retry logic will automatically retry failed requests
- Check `logs/pipeline.log` for details
- Logs include full stack traces with `ERROR` level entries

**Permission denied?**
- Make sure script has execute permissions:
  ```bash
  chmod +x src/run_pipeline.py
  ```
