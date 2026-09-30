import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.transform import parse_boxscore_pitching


def test_parse_boxscore_pitching_does_not_emit_debug_stdout(capsys):
    boxscore_json = {
        "teams": {
            "home": {
                "team": {"id": 1},
                "players": {
                    "1": {
                        "person": {"id": 101, "fullName": "Test Starter"},
                        "stats": {
                            "pitching": {
                                "inningsPitched": "5.2",
                                "hits": 4,
                                "runs": 2,
                                "earnedRuns": 1,
                                "baseOnBalls": 1,
                                "strikeOuts": 6,
                                "homeRuns": 1,
                                "numberOfPitches": 78,
                                "strikes": 52,
                                "note": "W",
                                "era": 2.5,
                                "whip": 1.1,
                            }
                        },
                    }
                },
            },
            "away": {
                "team": {"id": 2},
                "players": {
                    "2": {
                        "person": {"id": 102, "fullName": "Away Starter"},
                        "stats": {
                            "pitching": {
                                "inningsPitched": "3.0",
                                "hits": 2,
                                "runs": 1,
                                "earnedRuns": 1,
                                "baseOnBalls": 0,
                                "strikeOuts": 4,
                                "homeRuns": 0,
                                "numberOfPitches": 45,
                                "strikes": 31,
                                "note": "L",
                                "era": 3.0,
                                "whip": 1.0,
                            }
                        },
                    }
                },
            },
        }
    }

    df = parse_boxscore_pitching(boxscore_json, 12345, "2026-09-15", 2026)

    captured = capsys.readouterr()
    assert len(df) == 2
    assert "--- HOME TEAM" not in captured.out
    assert "pitching rows created" not in captured.out
