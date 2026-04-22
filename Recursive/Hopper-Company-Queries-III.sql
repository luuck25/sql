-- ============================================================================
-- LeetCode 1651: Hopper Company Queries III
-- ============================================================================
-- Problem:
-- Using the same three tables:
--
--   Drivers(driver_id, join_date)
--   Rides(ride_id, user_id, requested_at)
--   AcceptedRides(ride_id, driver_id, ride_distance, ride_duration)
--
-- Write a query to report the average ride distance and average ride duration
-- for every 3-MONTH WINDOW in 2020.
--
-- The windows are:
--   Jan–Mar, Feb–Apr, Mar–May, Apr–Jun, May–Jul, Jun–Aug,
--   Jul–Sep, Aug–Oct, Sep–Nov, Oct–Dec
--
-- That's 10 windows total (months 1–10 as the starting month).
--
-- For each window, compute:
--   average_ride_distance = SUM(ride_distance over 3 months) / 3
--   average_ride_duration = SUM(ride_duration over 3 months) / 3
--
-- Round both to 2 decimal places.
-- Only count rides that were ACCEPTED (exist in AcceptedRides).
-- Return the result ordered by the starting month.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA (same tables as Queries I & II)
-- ============================================================================
-- Tables already created in HopperCompanyQueriesI.sql
-- If running standalone, uncomment below:

-- CREATE TABLE Drivers (
--     driver_id  INT PRIMARY KEY,
--     join_date  DATE NOT NULL
-- );
-- CREATE TABLE Rides (
--     ride_id       INT PRIMARY KEY,
--     user_id       INT,
--     requested_at  DATE NOT NULL
-- );
-- CREATE TABLE AcceptedRides (
--     ride_id        INT PRIMARY KEY,
--     driver_id      INT NOT NULL,
--     ride_distance  INT,
--     ride_duration  INT
-- );
--
-- INSERT INTO Drivers (driver_id, join_date) VALUES
-- (1, '2019-05-15'),
-- (2, '2020-02-10'),
-- (3, '2020-06-20'),
-- (4, '2020-06-25');
--
-- INSERT INTO Rides (ride_id, user_id, requested_at) VALUES
-- (1, 101, '2020-01-15'),
-- (2, 102, '2020-02-20'),
-- (3, 103, '2020-03-10'),
-- (4, 104, '2020-06-05');
--
-- INSERT INTO AcceptedRides (ride_id, driver_id, ride_distance, ride_duration) VALUES
-- (1, 1, 10, 30),
-- (3, 2, 20, 45),
-- (4, 3, 15, 25);

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- Accepted rides with details:
-- | ride_id | driver_id | requested_at | month | distance | duration |
-- |---------|-----------|--------------|-------|----------|----------|
-- | 1       | 1         | 2020-01-15   | 1     | 10       | 30       |
-- | 3       | 2         | 2020-03-10   | 3     | 20       | 45       |
-- | 4       | 3         | 2020-06-05   | 6     | 15       | 25       |

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================

-- STEP 1: Sum distance and duration per month
-- | month | total_distance | total_duration |
-- |-------|----------------|----------------|
-- | 1     | 10             | 30             |
-- | 2     | 0              | 0              |
-- | 3     | 20             | 45             |
-- | 4     | 0              | 0              |
-- | 5     | 0              | 0              |
-- | 6     | 15             | 25             |
-- | 7-12  | 0              | 0              |

-- STEP 2: 3-month sliding window (starting month 1–10)
-- | window | months  | sum_distance | sum_duration | avg_distance | avg_duration |
-- |--------|---------|--------------|--------------|--------------|--------------|
-- | 1      | 1,2,3   | 10+0+20=30   | 30+0+45=75   | 10.00        | 25.00        |
-- | 2      | 2,3,4   | 0+20+0=20    | 0+45+0=45    | 6.67         | 15.00        |
-- | 3      | 3,4,5   | 20+0+0=20    | 45+0+0=45    | 6.67         | 15.00        |
-- | 4      | 4,5,6   | 0+0+15=15    | 0+0+25=25    | 5.00         | 8.33         |
-- | 5      | 5,6,7   | 0+15+0=15    | 0+25+0=25    | 5.00         | 8.33         |
-- | 6      | 6,7,8   | 15+0+0=15    | 25+0+0=25    | 5.00         | 8.33         |
-- | 7      | 7,8,9   | 0+0+0=0      | 0+0+0=0      | 0.00         | 0.00         |
-- | 8      | 8,9,10  | 0+0+0=0      | 0+0+0=0      | 0.00         | 0.00         |
-- | 9      | 9,10,11 | 0+0+0=0      | 0+0+0=0      | 0.00         | 0.00         |
-- | 10     | 10,11,12| 0+0+0=0      | 0+0+0=0      | 0.00         | 0.00         |

-- ============================================================================
-- EXPECTED OUTPUT
-- ============================================================================
-- | month | average_ride_distance | average_ride_duration |
-- |-------|-----------------------|-----------------------|
-- | 1     | 10.00                 | 25.00                 |
-- | 2     | 6.67                  | 15.00                 |
-- | 3     | 6.67                  | 15.00                 |
-- | 4     | 5.00                  | 8.33                  |
-- | 5     | 5.00                  | 8.33                  |
-- | 6     | 5.00                  | 8.33                  |
-- | 7     | 0.00                  | 0.00                  |
-- | 8     | 0.00                  | 0.00                  |
-- | 9     | 0.00                  | 0.00                  |
-- | 10    | 0.00                  | 0.00                  |

-- ============================================================================
-- HINTS
-- ============================================================================
-- 1. Generate months 1–12 using recursive CTE
-- 2. Sum distance/duration per month (LEFT JOIN to keep all months)
-- 3. Use a self-join (m.month BETWEEN start AND start+2) for the 3-month window
--    OR use SUM() OVER (ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)
-- 4. Only output months 1–10 (last valid 3-month window starts at month 10)

-- ============================================================================
-- KEY CONCEPT: SQL Logical Execution Order & WHERE vs Window Functions
-- ============================================================================
-- SQL executes in this logical order:
--
--   1. FROM / JOIN   ← combine tables (ON condition applied here)
--   2. WHERE         ← filter rows
--   3. GROUP BY      ← group rows
--   4. HAVING        ← filter groups
--   5. SELECT        ← compute columns, window functions run here
--   6. DISTINCT
--   7. ORDER BY      ← sort results
--   9. LIMIT/TOP   
--
-- WHY THIS MATTERS FOR THIS PROBLEM:
--
-- Window function solution:
--   WHERE month <= 10  →  removes months 11,12 at step 2
--   SUM() OVER(...)    →  runs at step 5, can only see months 1–10
--   Month 10's window needs 10,11,12 but 11,12 are GONE → wrong result!
--   FIX: wrap in subquery so WHERE filters AFTER the window computes.
--
-- Self-join solution:
--   JOIN mt2 ON ...    →  happens at step 1, mt2 has ALL 12 months
--   WHERE mt1.month <= 10 → step 2, only limits OUTPUT rows (mt1)
--   mt2 data is already joined → month 10 still sees 10,11,12 → correct!
--
-- RULE OF THUMB:
--   - WHERE filters BEFORE window functions (they lose data)
--   - WHERE filters AFTER joins (joined data is preserved)
--   - To filter AFTER a window function, use a subquery or CTE

-- ============================================================================
-- SQL SERVER SOLUTION
-- ============================================================================

WITH Months AS (
    -- Generate months 1–12
    SELECT 1 AS month
    UNION ALL
    SELECT month + 1 FROM Months WHERE month < 12
),
AcceptedRide_cal AS (
    -- Join Rides with AcceptedRides to get ride details with month
    SELECT 
        ar.ride_id,
        ar.driver_id,
        ar.ride_distance,
        ar.ride_duration,
        r.requested_at,
        MONTH(r.requested_at) AS req_month
    FROM Rides r
    JOIN AcceptedRides ar ON r.ride_id = ar.ride_id
    WHERE YEAR(r.requested_at) = 2020
),
MonthlyTotals AS (
    -- Aggregate distance and duration per month
    -- LEFT JOIN ensures all 12 months appear (even with 0 rides)
    SELECT 
        m.month,
        ISNULL(SUM(arc.ride_distance), 0) AS ride_distance,
        ISNULL(SUM(arc.ride_duration), 0) AS ride_duration
    FROM Months m
    LEFT JOIN AcceptedRide_cal arc ON arc.req_month = m.month
    GROUP BY m.month
)
-- Compute 3-month sliding window average
-- Window: current row + next 2 rows = 3-month window
-- Wrap in subquery so WHERE filters AFTER window computation
SELECT * FROM (
    SELECT 
        month,
        ROUND(SUM(ride_distance) OVER (ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) / 3.0, 2)  AS average_ride_distance,
        ROUND(SUM(ride_duration) OVER (ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) / 3.0, 2)  AS average_ride_duration
    FROM MonthlyTotals
) result
WHERE month <= 10
ORDER BY month
OPTION (MAXRECURSION 12);

-- ============================================================================
-- SQL SERVER SOLUTION (Alternative — Self Join)
-- ============================================================================
-- Instead of window functions, join MonthlyTotals to itself
-- where the joined month falls within the 3-month window.

WITH Months AS (
    SELECT 1 AS month
    UNION ALL
    SELECT month + 1 FROM Months WHERE month < 12
),
AcceptedRide_cal AS (
    SELECT 
        ar.ride_distance,
        ar.ride_duration,
        MONTH(r.requested_at) AS req_month
    FROM Rides r
    JOIN AcceptedRides ar ON r.ride_id = ar.ride_id
    WHERE YEAR(r.requested_at) = 2020
),
MonthlyTotals AS (
    SELECT 
        m.month,
        ISNULL(SUM(arc.ride_distance), 0) AS ride_distance,
        ISNULL(SUM(arc.ride_duration), 0) AS ride_duration
    FROM Months m
    LEFT JOIN AcceptedRide_cal arc ON arc.req_month = m.month
    GROUP BY m.month
)
-- Self join: for each starting month (1–10), sum the 3 months in its window
SELECT 
    mt1.month,
    CAST(ROUND(SUM(mt2.ride_distance) / 3.0, 2) AS DECIMAL(10,2)) AS average_ride_distance,
    CAST(ROUND(SUM(mt2.ride_duration) / 3.0, 2) AS DECIMAL(10,2)) AS average_ride_duration
FROM MonthlyTotals mt1
JOIN MonthlyTotals mt2 ON mt2.month BETWEEN mt1.month AND mt1.month + 2
WHERE mt1.month <= 10
GROUP BY mt1.month
ORDER BY mt1.month
OPTION (MAXRECURSION 12);

