import requests
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://statsapi.mlb.com/api/v1"

def _get_session(max_retries=3, backoff_factor=1.5):
    """Create a requests session with retry strategy."""
    session = requests.Session()
    retry_strategy = Retry(
        total=max_retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

#get schedule for a given date
def get_schedule(date_str: str) -> dict:
    url = f"{BASE_URL}/schedule"
    params = {
        "sportId": 1,
        "date": date_str,
    }
    session = _get_session()
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

#get boxscore for a given game id
def get_boxscore(game_pk: int) -> dict:
    url = f"{BASE_URL}/game/{game_pk}/boxscore"
    session = _get_session()
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()

#get teams list
def get_teams() -> dict:
    url = f"{BASE_URL}/teams"
    params = {"sportId": 1}
    session = _get_session()
    response = session.get(url, params=params, timeout=30)
    response.raise_for_status()
    return response.json()

#get player details including handedness
def get_player(player_id: int) -> dict:
    url = f"{BASE_URL}/people/{player_id}"
    session = _get_session()
    response = session.get(url, timeout=30)
    response.raise_for_status()
    return response.json()