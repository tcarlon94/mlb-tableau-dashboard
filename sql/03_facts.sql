CREATE TABLE IF NOT EXISTS RAW.GAMES (
    GAME_ID NUMBER,
    GAME_DATE DATE,
    SEASON NUMBER,
    GAME_TYPE STRING,
    STATUS STRING,
    HOME_TEAM_ID NUMBER,
    AWAY_TEAM_ID NUMBER,
    HOME_SCORE NUMBER,
    AWAY_SCORE NUMBER,
    WINNING_TEAM_ID NUMBER,
    LOSING_TEAM_ID NUMBER,
    WINNING_PITCHER_ID NUMBER,
    LOSING_PITCHER_ID NUMBER,
    SAVE_PITCHER_ID NUMBER,
    VENUE_NAME STRING,
    DAY_NIGHT STRING,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.PLAYER_GAME_BATTING (
    GAME_ID NUMBER,
    GAME_DATE DATE,
    SEASON NUMBER,
    PLAYER_ID NUMBER,
    TEAM_ID NUMBER,
    OPPONENT_TEAM_ID NUMBER,
    POSITION_CODE STRING,
    AB NUMBER,
    R NUMBER,
    H NUMBER,
    DOUBLES NUMBER,
    TRIPLES NUMBER,
    HR NUMBER,
    RBI NUMBER,
    BB NUMBER,
    SO NUMBER,
    HBP NUMBER,
    SB NUMBER,
    CS NUMBER,
    AVG FLOAT,
    OBP FLOAT,
    SLG FLOAT,
    OPS FLOAT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS RAW.PLAYER_GAME_PITCHING (
    GAME_ID NUMBER,
    GAME_DATE DATE,
    SEASON NUMBER,
    PLAYER_ID NUMBER,
    TEAM_ID NUMBER,
    OPPONENT_TEAM_ID NUMBER,
    IP FLOAT,
    H_ALLOWED NUMBER,
    R_ALLOWED NUMBER,
    ER NUMBER,
    BB NUMBER,
    SO NUMBER,
    HR_ALLOWED NUMBER,
    PITCHES NUMBER,
    STRIKES NUMBER,
    DECISION STRING,
    ERA FLOAT,
    WHIP FLOAT,
    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

--Facts table for games, with deduplication logic to ensure only the latest record for each game is retained. This is important because the raw data may contain multiple records for the same game due to updates or corrections. The deduplication is achieved using a Common Table Expression (CTE) that ranks records by their creation timestamp and selects only the most recent one for each game ID.
CREATE OR REPLACE VIEW ANALYTICS.FACT_GAME AS
WITH deduped AS (
    SELECT *
    FROM RAW.GAMES
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY GAME_ID
        ORDER BY CREATED_AT DESC
    ) = 1
)
SELECT
    GAME_ID,
    GAME_DATE,
    SEASON,
    GAME_TYPE,
    STATUS,
    HOME_TEAM_ID,
    AWAY_TEAM_ID,
    HOME_SCORE,
    AWAY_SCORE,
    HOME_SCORE + AWAY_SCORE AS TOTAL_RUNS,
    WINNING_TEAM_ID,
    LOSING_TEAM_ID,
    WINNING_PITCHER_ID,
    LOSING_PITCHER_ID,
    SAVE_PITCHER_ID,
    VENUE_NAME,
    DAY_NIGHT,
    1 AS GAME_COUNT
FROM deduped;

--Facts table for team games, with deduplication logic to ensure only the latest record for each game is retained. This is important because the raw data may contain multiple records for the same game due to updates or corrections. The deduplication is achieved using a Common Table Expression (CTE) that ranks records by their creation timestamp and selects only the most recent one for each game ID.
CREATE OR REPLACE VIEW ANALYTICS.FACT_TEAM_GAME AS
WITH games AS (
    SELECT *
    FROM RAW.GAMES
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY GAME_ID
        ORDER BY CREATED_AT DESC
    ) = 1
)

SELECT
    GAME_ID,
    GAME_DATE,
    HOME_TEAM_ID AS TEAM_ID,
    AWAY_TEAM_ID AS OPPONENT_TEAM_ID,
    'HOME' AS HOME_AWAY,
    HOME_SCORE AS RUNS_SCORED,
    AWAY_SCORE AS RUNS_ALLOWED,
    HOME_SCORE - AWAY_SCORE AS RUN_DIFFERENTIAL,
    IFF(HOME_SCORE > AWAY_SCORE, 1, 0) AS WIN_FLAG,
    1 AS TEAM_GAME_COUNT
FROM games

UNION ALL

SELECT
    GAME_ID,
    GAME_DATE,
    AWAY_TEAM_ID AS TEAM_ID,
    HOME_TEAM_ID AS OPPONENT_TEAM_ID,
    'AWAY' AS HOME_AWAY,
    AWAY_SCORE AS RUNS_SCORED,
    HOME_SCORE AS RUNS_ALLOWED,
    AWAY_SCORE - HOME_SCORE AS RUN_DIFFERENTIAL,
    IFF(AWAY_SCORE > HOME_SCORE, 1, 0) AS WIN_FLAG,
    1 AS TEAM_GAME_COUNT
FROM games;

-- Facts table for player games, with deduplication logic to ensure only the latest record for each player-game combination is retained. This is important because the raw data may contain multiple records for the same player in the same game due to updates or corrections. The deduplication is achieved using a Common Table Expression (CTE) that ranks records by their creation timestamp and selects only the most recent one for each player-game combination.
CREATE OR REPLACE VIEW ANALYTICS.FACT_PLAYER_GAME_BATTING AS
WITH deduped AS (
    SELECT *
    FROM RAW.PLAYER_GAME_BATTING
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY GAME_ID, PLAYER_ID, TEAM_ID
        ORDER BY CREATED_AT DESC
    ) = 1
)
SELECT
    GAME_ID,
    GAME_DATE,
    SEASON,
    PLAYER_ID,
    TEAM_ID,
    OPPONENT_TEAM_ID,
    POSITION_CODE,
    AB,
    R,
    H,
    DOUBLES,
    TRIPLES,
    HR,
    RBI,
    BB,
    SO,
    HBP,
    SB,
    CS,
    AVG,
    OBP,
    SLG,
    OPS,
    (HR * 4) + (RBI * 2) + H + BB AS PERFORMANCE_SCORE,
    1 AS PLAYER_GAME_COUNT
FROM deduped;

-- Facts table for player games, with deduplication logic to ensure only the latest record for each player-game combination is retained. This is important because the raw data may contain multiple records for the same player in the same game due to updates or corrections. The deduplication is achieved using a Common Table Expression (CTE) that ranks records by their creation timestamp and selects only the most recent one for each player-game combination.
CREATE OR REPLACE VIEW ANALYTICS.FACT_PLAYER_GAME_PITCHING AS
WITH deduped AS (
    SELECT *
    FROM RAW.PLAYER_GAME_PITCHING
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY GAME_ID, PLAYER_ID, TEAM_ID
        ORDER BY CREATED_AT DESC
    ) = 1
)
SELECT
    GAME_ID,
    GAME_DATE,
    SEASON,
    PLAYER_ID,
    TEAM_ID,
    OPPONENT_TEAM_ID,
    IP,
    H_ALLOWED,
    R_ALLOWED,
    ER,
    BB,
    SO,
    HR_ALLOWED,
    PITCHES,
    STRIKES,
    DECISION,
    ERA,
    WHIP,
    (SO * 2) + (IP * 3) - (ER * 2) - BB AS PERFORMANCE_SCORE,
    1 AS PLAYER_GAME_COUNT
FROM deduped;