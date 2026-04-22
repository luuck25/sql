-- ============================================================================
-- LeetCode 1225: Report Contiguous Dates
-- ============================================================================
-- Problem:
-- You have two tables:
--
--   Failed(fail_date DATE)    — PRIMARY KEY (fail_date)
--   Succeeded(success_date DATE) — PRIMARY KEY (success_date)
--
-- A system runs one task per day. Each day is either a "failed" or "succeeded"
-- day (never both, and some days may not appear in either table).
--
-- Write a query to generate a report of contiguous periods of the same state
-- (failed or succeeded) during the period ['2019-01-01', '2019-12-31'].
--
-- Return: period_state ('failed'/'succeeded'), start_date, end_date
-- Order by start_date ASC.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE Failed (
    fail_date DATE PRIMARY KEY
);

CREATE TABLE Succeeded (
    success_date DATE PRIMARY KEY
);

INSERT INTO Failed (fail_date) VALUES
('2019-01-04'),
('2019-01-05'),
('2019-01-06');

INSERT INTO Succeeded (success_date) VALUES
('2019-01-01'),
('2019-01-02'),
('2019-01-03'),
('2019-01-07');

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- Failed:          Succeeded:
-- | fail_date  |   | success_date |
-- |------------|   |--------------|
-- | 2019-01-04 |   | 2019-01-01   |
-- | 2019-01-05 |   | 2019-01-02   |
-- | 2019-01-06 |   | 2019-01-03   |
--                   | 2019-01-07   |

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================
-- Step 1: Combine both tables with a state label, filter to 2019:
-- | date       | state     |
-- |------------|-----------|
-- | 2019-01-01 | succeeded |
-- | 2019-01-02 | succeeded |
-- | 2019-01-03 | succeeded |
-- | 2019-01-04 | failed    |
-- | 2019-01-05 | failed    |
-- | 2019-01-06 | failed    |
-- | 2019-01-07 | succeeded |
--
-- Step 2: Assign ROW_NUMBER per state, then date - ROW_NUMBER = group ID:
-- | date       | state     | rn | date - rn (grp)  |
-- |------------|-----------|----|--------------------|
-- | 2019-01-01 | succeeded | 1  | 2018-12-31         |
-- | 2019-01-02 | succeeded | 2  | 2018-12-31         |
-- | 2019-01-03 | succeeded | 3  | 2018-12-31         |  ← group A
-- | 2019-01-04 | failed    | 1  | 2019-01-03         |
-- | 2019-01-05 | failed    | 2  | 2019-01-03         |
-- | 2019-01-06 | failed    | 3  | 2019-01-03         |  ← group B
-- | 2019-01-07 | succeeded | 4  | 2019-01-03         |  ← group C (different state from B)
--
-- Step 3: GROUP BY state + grp → MIN(date), MAX(date)

-- ============================================================================
-- EXPECTED OUTPUT (ordered by start_date ASC)
-- ============================================================================
-- | period_state | start_date | end_date   |
-- |--------------|------------|------------|
-- | succeeded    | 2019-01-01 | 2019-01-03 |
-- | failed       | 2019-01-04 | 2019-01-06 |
-- | succeeded    | 2019-01-07 | 2019-01-07 |

-- ============================================================================
-- KEY CONCEPTS
-- ============================================================================
-- 1. UNION ALL to combine two tables with a state label:
--    SELECT fail_date AS dt, 'failed' UNION ALL SELECT success_date, 'succeeded'
--
-- 2. Gaps-and-islands with dates:
--    ROW_NUMBER() OVER (PARTITION BY state ORDER BY date) → rn
--    date - rn = constant for consecutive dates within the same state.
--    DATEADD(DAY, -rn, date) gives the group identifier.
--
-- 3. Must PARTITION BY state:
--    Without it, succeeded Jan 7 and failed Jan 4-6 could get the same
--    (date - rn) value. Partitioning by state keeps them in separate groups.
--
-- 4. Date arithmetic for group ID:
--    Unlike integer id - ROW_NUMBER, dates need DATEADD:
--    DATEADD(DAY, -ROW_NUMBER(), date_col) → group identifier.

-- DROP TABLE Failed;
-- DROP TABLE Succeeded;

-- ============================================================================
-- SQL SERVER SOLUTION
-- ============================================================================

WITH combined AS (
    SELECT fail_date AS dt, 'failed' AS period_state
    FROM Failed
    WHERE fail_date BETWEEN '2019-01-01' AND '2019-12-31'
    UNION ALL
    SELECT success_date, 'succeeded'
    FROM Succeeded
    WHERE success_date BETWEEN '2019-01-01' AND '2019-12-31'
),
grouped AS (
    SELECT
        dt,
        period_state,
        DATEADD(DAY, -ROW_NUMBER() OVER (PARTITION BY period_state ORDER BY dt), dt) AS grp
    FROM combined
)
SELECT
    period_state,
    MIN(dt) AS start_date,
    MAX(dt) AS end_date
FROM grouped
GROUP BY period_state, grp
ORDER BY MIN(dt);

-- ============================================================================
-- SQL SERVER SOLUTION 2 (Separate CTEs per state)
-- ============================================================================
-- Process each table independently, then UNION the results.

WITH success_data AS (
    SELECT *,
        DATEADD(DAY, -ROW_NUMBER() OVER (ORDER BY success_date), success_date) AS grp
    FROM Succeeded
    WHERE success_date BETWEEN '2019-01-01' AND '2019-12-31'
),
failed_data AS (
    SELECT *,
        DATEADD(DAY, -ROW_NUMBER() OVER (ORDER BY fail_date), fail_date) AS grp
    FROM Failed
    WHERE fail_date BETWEEN '2019-01-01' AND '2019-12-31'
)
SELECT 'succeeded' AS period_state,
    MIN(success_date) AS start_date,
    MAX(success_date) AS end_date
FROM success_data
GROUP BY grp
UNION ALL
SELECT 'failed' AS period_state,
    MIN(fail_date) AS start_date,
    MAX(fail_date) AS end_date
FROM failed_data
GROUP BY grp
ORDER BY start_date;
ORDER BY MIN(dt);
