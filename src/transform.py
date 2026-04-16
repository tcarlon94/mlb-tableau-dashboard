import pandas as pd
from extract import get_player
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

# Thread-safe cache for player handedness
_player_cache = {}
_cache_lock = threading.Lock()

def get_player_handedness(player_id: int) -> tuple:
    """Fetch handedness for a player, with caching."""
    with _cache_lock:
        if player_id in _player_cache:
            return _player_cache[player_id]
    
    try:
        player_details = get_player(player_id)
        people = player_details.get("people", [])
        if people:
            person_detail = people[0]
            bat_side = person_detail.get("batSide", {}).get("code")
            pitch_hand = person_detail.get("pitchHand", {}).get("code")
            result = (bat_side, pitch_hand)
        else:
            result = (None, None)
    except Exception as e:
        print(f"Warning: Could not fetch handedness for player {player_id}: {e}")
        result = (None, None)
    
    with _cache_lock:
        _player_cache[player_id] = result
    return result

# Parse function to transform raw JSON data to structured DataFrames for teams
def parse_teams(teams_json: dict) -> pd.DataFrame:
    rows = []

    for team in teams_json.get("teams", []):
        rows.append({
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "team_abbrev": team.get("abbreviation"),
            "league_name": team.get("league", {}).get("name"),
            "division_name": team.get("division", {}).get("name"),
            "venue_name": team.get("venue", {}).get("name"),
            "active_flag": team.get("active"),
        })

    return pd.DataFrame(rows)

# Parse function to transform schedule JSON to games DataFrame
def parse_schedule_to_games(schedule_json: dict) -> pd.DataFrame:
    rows = []

    for date_block in schedule_json.get("dates", []):
        game_date = date_block.get("date")

        for game in date_block.get("games", []):
            home_team = game.get("teams", {}).get("home", {})
            away_team = game.get("teams", {}).get("away", {})

            home_score = home_team.get("score")
            away_score = away_team.get("score")

            winning_team_id = None
            losing_team_id = None

            if home_score is not None and away_score is not None:
                if home_score > away_score:
                    winning_team_id = home_team.get("team", {}).get("id")
                    losing_team_id = away_team.get("team", {}).get("id")
                elif away_score > home_score:
                    winning_team_id = away_team.get("team", {}).get("id")
                    losing_team_id = home_team.get("team", {}).get("id")

            rows.append({
                "game_id": game.get("gamePk"),
                "game_date": game_date,
                "season": game.get("season"),
                "game_type": game.get("gameType"),
                "status": game.get("status", {}).get("detailedState"),
                "home_team_id": home_team.get("team", {}).get("id"),
                "away_team_id": away_team.get("team", {}).get("id"),
                "home_score": home_score,
                "away_score": away_score,
                "winning_team_id": winning_team_id,
                "losing_team_id": losing_team_id,
                "venue_name": game.get("venue", {}).get("name"),
                "day_night": game.get("dayNight"),
            })

    return pd.DataFrame(rows)

# Parse function to transform boxscore JSON to players DataFrame
def parse_boxscore_players(boxscore_json: dict) -> pd.DataFrame:
    rows = []
    player_ids_to_fetch = []
    player_id_to_row = {}

    for side in ["home", "away"]:
        team_data = boxscore_json.get("teams", {}).get(side, {})
        team_id = team_data.get("team", {}).get("id")

        for player_data in team_data.get("players", {}).values():
            person = player_data.get("person", {})
            position = player_data.get("position", {})
            full_name = person.get("fullName")
            player_id = person.get("id")
            
            # Extract first and last names from full_name
            first_name = None
            last_name = None
            if full_name:
                name_parts = full_name.split()
                if len(name_parts) >= 2:
                    first_name = name_parts[0]
                    last_name = " ".join(name_parts[1:])
            
            row_data = {
                "player_id": player_id,
                "full_name": full_name,
                "first_name": first_name,
                "last_name": last_name,
                "primary_position": position.get("abbreviation"),
                "bat_side": None,
                "pitch_hand": None,
                "current_team_id": team_id,
                "active_flag": True,
            }
            
            rows.append(row_data)
            player_ids_to_fetch.append(player_id)
            player_id_to_row[player_id] = row_data

    # Fetch handedness in parallel using thread pool
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_player = {executor.submit(get_player_handedness, pid): pid for pid in player_ids_to_fetch}
        for future in as_completed(future_to_player):
            player_id = future_to_player[future]
            try:
                bat_side, pitch_hand = future.result()
                player_id_to_row[player_id]["bat_side"] = bat_side
                player_id_to_row[player_id]["pitch_hand"] = pitch_hand
            except Exception as e:
                print(f"Error processing player {player_id}: {e}")

    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.drop_duplicates(subset=["player_id"])
    return df

# Parse function to transform boxscore JSON to batting stats DataFrame
def parse_boxscore_batting(boxscore_json: dict, game_id: int, game_date: str, season: int) -> pd.DataFrame:
    rows = []

    teams = boxscore_json.get("teams", {})
    home_team_id = teams.get("home", {}).get("team", {}).get("id")
    away_team_id = teams.get("away", {}).get("team", {}).get("id")

    for side in ["home", "away"]:
        team_data = teams.get(side, {})
        team_id = team_data.get("team", {}).get("id")
        opponent_team_id = away_team_id if side == "home" else home_team_id

        for player_data in team_data.get("players", {}).values():
            person = player_data.get("person", {})
            stats = player_data.get("stats", {}).get("batting", {})

            if not stats:
                continue

            rows.append({
                "game_id": game_id,
                "game_date": game_date,
                "season": season,
                "player_id": person.get("id"),
                "team_id": team_id,
                "opponent_team_id": opponent_team_id,
                "position_code": player_data.get("position", {}).get("abbreviation"),
                "ab": _to_int(stats.get("atBats")),
                "r": _to_int(stats.get("runs")),
                "h": _to_int(stats.get("hits")),
                "doubles": _to_int(stats.get("doubles")),
                "triples": _to_int(stats.get("triples")),
                "hr": _to_int(stats.get("homeRuns")),
                "rbi": _to_int(stats.get("rbi")),
                "bb": _to_int(stats.get("baseOnBalls")),
                "so": _to_int(stats.get("strikeOuts")),
                "hbp": _to_int(stats.get("hitByPitch")),
                "sb": _to_int(stats.get("stolenBases")),
                "cs": _to_int(stats.get("caughtStealing")),
                "avg": _to_float(stats.get("avg")),
                "obp": _to_float(stats.get("obp")),
                "slg": _to_float(stats.get("slg")),
                "ops": _to_float(stats.get("ops")),
            })

    return pd.DataFrame(rows)

# Parse function to transform boxscore JSON to pitching stats DataFrame
def parse_boxscore_pitching(boxscore_json: dict, game_id: int, game_date: str, season: int):
    rows = []

    teams = boxscore_json.get("teams", {})
    home_team_id = teams.get("home", {}).get("team", {}).get("id")
    away_team_id = teams.get("away", {}).get("team", {}).get("id")

    for side in ["home", "away"]:
        team_data = teams.get(side, {})
        team_id = team_data.get("team", {}).get("id")
        opponent_team_id = away_team_id if side == "home" else home_team_id

        print(f"\n--- {side.upper()} TEAM {team_id} ---")

        for player_key, player_data in team_data.get("players", {}).items():
            person = player_data.get("person", {})
            stats = player_data.get("stats", {}).get("pitching", {})

            print(
                "player:",
                person.get("fullName"),
                "| pitching stats keys:",
                list(stats.keys()) if stats else "NO PITCHING STATS"
            )

            if not stats:
                continue

            rows.append({
                "game_id": game_id,
                "game_date": game_date,
                "season": season,
                "player_id": person.get("id"),
                "team_id": team_id,
                "opponent_team_id": opponent_team_id,
                "ip": _ip_to_float(stats.get("inningsPitched")),
                "h_allowed": _to_int(stats.get("hits")),
                "r_allowed": _to_int(stats.get("runs")),
                "er": _to_int(stats.get("earnedRuns")),
                "bb": _to_int(stats.get("baseOnBalls")),
                "so": _to_int(stats.get("strikeOuts")),
                "hr_allowed": _to_int(stats.get("homeRuns")),
                "pitches": _to_int(stats.get("numberOfPitches")),
                "strikes": _to_int(stats.get("strikes")),
                "decision": stats.get("note"),
                "era": _to_float(stats.get("era")),
                "whip": _to_float(stats.get("whip")),
            })

    df = pd.DataFrame(rows)
    print("pitching rows created:", len(df))
    return df

# Helper functions for type conversion and innings pitched parsing
def _to_int(value):
    if value in [None, "", ""]:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _to_float(value):
    if value in [None, "", ""]:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ip_to_float(ip_value):
    """
    Converts baseball innings notation:
    5.0 -> 5.0
    5.1 -> 5 + 1/3
    5.2 -> 5 + 2/3
    """
    if ip_value in [None, ""]:
        return None

    try:
        text = str(ip_value)
        if "." not in text:
            return float(text)

        whole, frac = text.split(".")
        whole = int(whole)
        frac = int(frac)

        if frac == 0:
            return float(whole)
        if frac == 1:
            return whole + (1 / 3)
        if frac == 2:
            return whole + (2 / 3)

        return float(text)
    except (TypeError, ValueError):
        return None