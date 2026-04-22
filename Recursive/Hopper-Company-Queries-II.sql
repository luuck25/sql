-- ============================================================================
-- LeetCode 1645: Hopper Company Queries II
-- ============================================================================
-- Problem:
-- Using the same three tables from Hopper Company Queries I:
--
--   Drivers(driver_id, join_date)
--   Rides(ride_id, user_id, requested_at)
--   AcceptedRides(ride_id, driver_id, ride_distance, ride_duration)
--
-- Write a query to report, for EACH month of 2020 (1 through 12):
--   working_percentage = (drivers who accepted at least one ride in that month
--                         / active drivers in that month) * 100
--
-- A driver is "active" in month M if they joined on or before the end of month M.
-- A driver is "working" in month M if they accepted at least one ride in month M.
--
-- Round working_percentage to 2 decimal places.
-- If there are no active drivers in a month, the working_percentage is 0.00.
-- Return the result ordered by month.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA (same tables as Queries I)
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
-- Drivers:
-- | driver_id | join_date  |
-- |-----------|------------|
-- | 1         | 2019-05-15 |  (active from month 1)
-- | 2         | 2020-02-10 |  (active from month 2)
-- | 3         | 2020-06-20 |  (active from month 6)
-- | 4         | 2020-06-25 |  (active from month 6)
--
-- AcceptedRides joined with Rides:
-- | ride_id | driver_id | requested_at | month |
-- |---------|-----------|--------------|-------|
-- | 1       | 1         | 2020-01-15   | 1     |
-- | 3       | 2         | 2020-03-10   | 3     |
-- | 4       | 3         | 2020-06-05   | 6     |

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================

-- STEP 1: Active drivers per month (same as Queries I)
-- | Month | Active Drivers | Count |
-- |-------|----------------|-------|
-- | 1     | D1             | 1     |
-- | 2     | D1, D2         | 2     |
-- | 3-5   | D1, D2         | 2     |
-- | 6-12  | D1, D2, D3, D4 | 4     |

-- STEP 2: Working drivers per month (distinct drivers who accepted a ride)
-- | Month | Working Drivers | Count |
-- |-------|-----------------|-------|
-- | 1     | D1              | 1     |
-- | 3     | D2              | 1     |
-- | 6     | D3              | 1     |
-- (all other months: 0)

-- STEP 3: working_percentage = (working / active) * 100
-- | month | active | working | working_percentage |
-- |-------|--------|---------|--------------------|
-- | 1     | 1      | 1       | 100.00             |
-- | 2     | 2      | 0       | 0.00               |
-- | 3     | 2      | 1       | 50.00              |
-- | 4     | 2      | 0       | 0.00               |
-- | 5     | 2      | 0       | 0.00               |
-- | 6     | 4      | 1       | 25.00              |
-- | 7     | 4      | 0       | 0.00               |
-- | 8     | 4      | 0       | 0.00               |
-- | 9     | 4      | 0       | 0.00               |
-- | 10    | 4      | 0       | 0.00               |
-- | 11    | 4      | 0       | 0.00               |
-- | 12    | 4      | 0       | 0.00               |

-- ============================================================================
-- EXPECTED OUTPUT
-- ============================================================================
-- | month | working_percentage |
-- |-------|--------------------|
-- | 1     | 100.00             |
-- | 2     | 0.00               |
-- | 3     | 50.00              |
-- | 4     | 0.00               |
-- | 5     | 0.00               |
-- | 6     | 25.00              |
-- | 7     | 0.00               |
-- | 8     | 0.00               |
-- | 9     | 0.00               |
-- | 10    | 0.00               |
-- | 11    | 0.00               |
-- | 12    | 0.00               |

-- ============================================================================
-- SQL SERVER SOLUTION
-- ============================================================================

WITH Months AS (
    -- Generate months 1–12
    SELECT 1 AS month
    UNION ALL
    SELECT month + 1 FROM Months WHERE month < 12
),
ActiveDrivers AS (
    -- Count drivers active by end of each month
    -- Active = joined in 2020 and join_month <= current month, OR joined before 2020
    SELECT m.month, COUNT(d.driver_id) AS active_drivers
    FROM Months m
    LEFT JOIN Drivers d ON 
        (m.month >= MONTH(d.join_date) AND YEAR(d.join_date) = 2020)
        OR YEAR(d.join_date) < 2020
    GROUP BY m.month
),
WorkingDrivers AS (
    -- Count distinct drivers who accepted at least one ride per month
    SELECT MONTH(r.requested_at) AS month, COUNT(DISTINCT a.driver_id) AS working_drivers
    FROM Rides r
    JOIN AcceptedRides a ON r.ride_id = a.ride_id
    WHERE YEAR(r.requested_at) = 2020
    GROUP BY MONTH(r.requested_at)
)
SELECT
    ad.month,
    -- working_percentage = (working_drivers / active_drivers) * 100
    -- 100.0 forces decimal division (avoids integer truncation)
    -- NULLIF avoids division by zero (returns NULL instead of error)
    -- Outer ISNULL converts NULL to 0.00
    ISNULL(ROUND(wd.working_drivers * 100.0 / NULLIF(ad.active_drivers, 0), 2), 0.00) AS working_percentage
FROM ActiveDrivers ad
LEFT JOIN WorkingDrivers wd ON ad.month = wd.month
ORDER BY ad.month
OPTION (MAXRECURSION 12);
