-- ============================================================================
-- LeetCode 1097: Game Play Analysis V
-- ============================================================================
-- Problem:
-- You have one table:
--
--   Activity(player_id, device_id, event_date, games_played)
--     - (player_id, event_date) is the primary key.
--     - Each row records that a player logged in on event_date using device_id
--       and played games_played games.
--
-- The install date of a player is the first login date of that player.
--
-- Write a query to report for each install date:
--   1. install_dt — the install date
--   2. installs   — number of players who installed (first logged in) on that date
--   3. Day1_retention — fraction of players who logged in again the day AFTER
--                       their install date, rounded to 2 decimal places.
--
-- Return result ordered by install_dt.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE Activity (
    player_id   INT,
    device_id   INT,
    event_date  DATE,
    games_played INT,
    PRIMARY KEY (player_id, event_date)
);

INSERT INTO Activity (player_id, device_id, event_date, games_played) VALUES
(1, 2, '2016-03-01', 5),
(1, 2, '2016-03-02', 6),
(2, 3, '2017-06-25', 1),
(3, 1, '2016-03-01', 0),
(3, 4, '2016-07-03', 5);

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- | player_id | device_id | event_date | games_played |
-- |-----------|-----------|------------|--------------|
-- | 1         | 2         | 2016-03-01 | 5            |
-- | 1         | 2         | 2016-03-02 | 6            |
-- | 2         | 3         | 2017-06-25 | 1            |
-- | 3         | 1         | 2016-03-01 | 0            |
-- | 3         | 4         | 2016-07-03 | 5            |

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================
-- Step 1: Find each player's install date (first login):
-- | player_id | install_dt |
-- |-----------|------------|
-- | 1         | 2016-03-01 |
-- | 2         | 2017-06-25 |
-- | 3         | 2016-03-01 |
--
-- Step 2: Check who logged in the day after install:
-- | player_id | install_dt | next_day (install+1) | logged in? |
-- |-----------|------------|----------------------|------------|
-- | 1         | 2016-03-01 | 2016-03-02           | YES        |
-- | 2         | 2017-06-25 | 2017-06-26           | NO         |
-- | 3         | 2016-03-01 | 2016-03-02           | NO         |
--
-- Step 3: Group by install_dt:
-- 2016-03-01: 2 installs, 1 returned → 1/2 = 0.50
-- 2017-06-25: 1 install, 0 returned  → 0/1 = 0.00

-- ============================================================================
-- EXPECTED OUTPUT
-- ============================================================================
-- | install_dt | installs | Day1_retention |
-- |------------|----------|----------------|
-- | 2016-03-01 | 2        | 0.50           |
-- | 2017-06-25 | 1        | 0.00           |

-- ============================================================================
-- KEY CONCEPTS
-- ============================================================================
-- 1. Install date = MIN(event_date) per player.
--
-- 2. Day-1 retention check:
--    LEFT JOIN Activity on player_id AND event_date = install_dt + 1 day.
--    If match exists → retained. COUNT the non-NULLs / total installs.
--
-- 3. DATEADD(DAY, 1, install_dt) for "next day" in SQL Server.
--
-- 4. Retention formula:
--    COUNT(next_day_login) / COUNT(*) — COUNT ignores NULLs from LEFT JOIN,
--    so it only counts players who actually came back.
--    Use ROUND(..., 2) and CAST to avoid integer division.

-- DROP TABLE Activity;

-- ============================================================================
-- SQL SERVER SOLUTION (Window function — no JOIN)
-- ============================================================================
-- MIN(event_date) OVER (PARTITION BY player_id) attaches install_dt to every row.
-- COUNT(DISTINCT CASE ...) with different conditions counts installs vs retained.
-- DATEDIFF(DAY, install_dt, event_date) = 1 can also be used instead of
-- event_date = DATEADD(DAY, 1, install_dt) — both check "day after install".

WITH with_install AS (
    SELECT *,
        MIN(event_date) OVER (PARTITION BY player_id) AS install_dt
    FROM Activity
)
SELECT
    install_dt,
    COUNT(DISTINCT CASE WHEN event_date = install_dt THEN player_id END) AS installs,
    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN event_date = DATEADD(DAY, 1, install_dt) THEN player_id END) AS DECIMAL(10,2))
        / COUNT(DISTINCT CASE WHEN event_date = install_dt THEN player_id END),
        2
    ) AS Day1_retention
FROM with_install
GROUP BY install_dt
ORDER BY install_dt;

-- ============================================================================
-- SQL SERVER SOLUTION 2 (DATEDIFF variant)
-- ============================================================================
-- Same logic but uses DATEDIFF(DAY, install_dt, event_date) instead of DATEADD.
-- DATEDIFF = 0 means same day (install), DATEDIFF = 1 means next day (retained).

WITH with_install AS (
    SELECT *,
        MIN(event_date) OVER (PARTITION BY player_id) AS install_dt
    FROM Activity
)
SELECT
    install_dt,
    COUNT(DISTINCT CASE WHEN DATEDIFF(DAY, install_dt, event_date) = 0 THEN player_id END) AS installs,
    ROUND(
        CAST(COUNT(DISTINCT CASE WHEN DATEDIFF(DAY, install_dt, event_date) = 1 THEN player_id END) AS DECIMAL(10,2))
        / COUNT(DISTINCT CASE WHEN DATEDIFF(DAY, install_dt, event_date) = 0 THEN player_id END),
        2
    ) AS Day1_retention
FROM with_install
GROUP BY install_dt
ORDER BY install_dt;
