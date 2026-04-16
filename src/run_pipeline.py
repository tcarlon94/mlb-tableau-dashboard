import pandas as pd
import logging
import json
from datetime import datetime, timedelta
from pathlib import Path

from extract import get_schedule, get_boxscore, get_teams
from transform import (
    parse_teams,
    parse_schedule_to_games,
    parse_boxscore_players,
    parse_boxscore_batting,
    parse_boxscore_pitching,
)
from load import get_connection, insert_dataframe

# Load configuration
config_path = Path(__file__).parent.parent / "config.json"
with open(config_path) as f:
    config = json.load(f)

# Setup logging
log_dir = Path(__file__).parent.parent / config["logging"]["log_file"].split("/")[0]
log_dir.mkdir(exist_ok=True)
log_file = Path(__file__).parent.parent / config["logging"]["log_file"]

logging.basicConfig(
    level=config["logging"]["level"],
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def run_for_date(date_str: str = None):
    """Run pipeline for a specific date or use configuration default."""
    if date_str is None:
        # Calculate date based on config offset
        date_offset = config["pipeline"]["date_offset"]
        target_date = datetime.now() + timedelta(days=date_offset)
        date_str = target_date.strftime("%Y-%m-%d")
    
    logger.info(f"Starting pipeline for date: {date_str}")
    
    try:
        # 1. Load team dimension
        logger.info("Fetching teams...")
        teams_json = get_teams()
        teams_df = parse_teams(teams_json)

        # 2. Load schedule / games
        logger.info(f"Fetching schedule for {date_str}...")
        schedule_json = get_schedule(date_str)
        games_df = parse_schedule_to_games(schedule_json)

        if games_df.empty:
            logger.warning(f"No games found for {date_str}")
            return

        logger.info(f"Found {len(games_df)} games for {date_str}")

        # 3. Build player, batting, pitching DataFrames across all games
        all_players = []
        all_batting = []
        all_pitching = []

        for _, game_row in games_df.iterrows():
            game_id = int(game_row["game_id"])
            season = int(game_row["season"]) if pd.notna(game_row["season"]) else None
            game_date = str(game_row["game_date"])

            logger.info(f"Processing game {game_id}")
            boxscore_json = get_boxscore(game_id)

            players_df = parse_boxscore_players(boxscore_json)
            batting_df = parse_boxscore_batting(boxscore_json, game_id, game_date, season)
            pitching_df = parse_boxscore_pitching(boxscore_json, game_id, game_date, season)

            if not players_df.empty:
                all_players.append(players_df)
            if not batting_df.empty:
                all_batting.append(batting_df)
            if not pitching_df.empty:
                all_pitching.append(pitching_df)

        players_df = pd.concat(all_players, ignore_index=True).drop_duplicates(subset=["player_id"]) if all_players else pd.DataFrame()
        batting_df = pd.concat(all_batting, ignore_index=True) if all_batting else pd.DataFrame()
        pitching_df = pd.concat(all_pitching, ignore_index=True) if all_pitching else pd.DataFrame()

        conn = get_connection()
        try:
            logger.info("Loading data into Snowflake...")
            insert_dataframe(conn, config["database"]["teams_table"], teams_df)
            insert_dataframe(conn, config["database"]["games_table"], games_df)
            insert_dataframe(conn, config["database"]["players_table"], players_df)
            insert_dataframe(conn, config["database"]["batting_table"], batting_df)
            insert_dataframe(conn, config["database"]["pitching_table"], pitching_df)
            logger.info("Pipeline completed successfully!")
        finally:
            conn.close()
            
    except Exception as e:
        logger.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    run_for_date()