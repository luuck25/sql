-- ============================================================================
-- LeetCode 1635: Hopper Company Queries I
-- ============================================================================
-- Problem:
-- Hopper is a ride-sharing company. You have three tables:
--
--   Drivers(driver_id, join_date)
--     - Each row is a driver and the date they joined the platform.
--
--   Rides(ride_id, user_id, requested_at)
--     - Each row is a ride request made by a user on a given date.
--
--   AcceptedRides(ride_id, driver_id, ride_distance, ride_duration)
--     - Each row is a ride that was accepted by a driver.
--     - Not all requested rides are accepted.
--
-- Write a query to report, for EACH month of 2020 (1 through 12):
--   1. active_drivers  — number of drivers who had joined by the end of that month
--                        (join_date <= last day of that month)
--   2. accepted_rides  — number of rides requested AND accepted in that month
--
-- Return the result ordered by month.
-- Months with 0 active drivers or 0 accepted rides should still appear (show 0).
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE Drivers (
    driver_id  INT PRIMARY KEY,
    join_date  DATE NOT NULL
);

CREATE TABLE Rides (
    ride_id       INT PRIMARY KEY,
    user_id       INT,
    requested_at  DATE NOT NULL
);

CREATE TABLE AcceptedRides (
    ride_id        INT PRIMARY KEY,
    driver_id      INT NOT NULL,
    ride_distance  INT,
    ride_duration  INT,
    FOREIGN KEY (ride_id) REFERENCES Rides(ride_id),
    FOREIGN KEY (driver_id) REFERENCES Drivers(driver_id)
);

INSERT INTO Drivers (driver_id, join_date) VALUES
(1, '2019-05-15'),   -- joined before 2020, always active
(2, '2020-02-10'),   -- active from Feb 2020
(3, '2020-06-20'),   -- active from Jun 2020
(4, '2020-06-25');   -- active from Jun 2020

INSERT INTO Rides (ride_id, user_id, requested_at) VALUES
(1, 101, '2020-01-15'),
(2, 102, '2020-02-20'),
(3, 103, '2020-03-10'),
(4, 104, '2020-06-05');

INSERT INTO AcceptedRides (ride_id, driver_id, ride_distance, ride_duration) VALUES
(1, 1, 10, 30),      -- accepted in Jan
(3, 2, 20, 45),      -- accepted in Mar
(4, 3, 15, 25);      -- accepted in Jun
-- Note: ride_id 2 was NOT accepted

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- Drivers:
-- | driver_id | join_date  |
-- |-----------|------------|
-- | 1         | 2019-05-15 |
-- | 2         | 2020-02-10 |
-- | 3         | 2020-06-20 |
-- | 4         | 2020-06-25 |
--
-- Rides:
-- | ride_id | requested_at |
-- |---------|--------------|
-- | 1       | 2020-01-15   |
-- | 2       | 2020-02-20   |
-- | 3       | 2020-03-10   |
-- | 4       | 2020-06-05   |
--
-- AcceptedRides:
-- | ride_id | driver_id |
-- |---------|-----------|
-- | 1       | 1         |
-- | 3       | 2         |
-- | 4       | 3         |

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================

-- STEP 1: Generate Months 1-12 (Recursive CTE)
-- ---------------------------------------------
-- Output:
-- | month |
-- |-------|
-- | 1     |
-- | 2     |
-- | ...   |
-- | 12    |
-- Why? We need ALL 12 months, even if no rides/drivers exist for some months.

-- STEP 2: Count Accepted Rides per Month
-- ---------------------------------------------
-- Join Rides with AcceptedRides (only accepted rides count)
-- Filter for year 2020
-- Group by month
-- Output:
-- | month | cnt |
-- |-------|-----|
-- | 1     | 1   |
-- | 3     | 1   |
-- | 6     | 1   |
-- (Months 2, 4, 5, 7-12 have no accepted rides)

-- STEP 3: Count Active Drivers per Month
-- ---------------------------------------------
-- Driver is active in month M if:
--   - They joined BEFORE 2020 (always active), OR
--   - They joined IN 2020 and join_month <= M
--
-- | Month | Active Drivers     | Explanation                    |
-- |-------|--------------------|--------------------------------|
-- | 1     | Driver 1           | Joined 2019 (before 2020)      |
-- | 2     | Driver 1, 2        | Driver 2 joined Feb 2020       |
-- | 3-5   | Driver 1, 2        | Same                           |
-- | 6     | Driver 1, 2, 3, 4  | Drivers 3,4 joined June        |
-- | 7-12  | Driver 1, 2, 3, 4  | All 4 active                   |

-- STEP 4: Final Output
-- ---------------------------------------------
-- | month | active_drivers | accepted_rides |
-- |-------|----------------|----------------|
-- | 1     | 1              | 1              |
-- | 2     | 2              | 0              |
-- | 3     | 2              | 1              |
-- | 4     | 2              | 0              |
-- | 5     | 2              | 0              |
-- | 6     | 4              | 1              |
-- | 7-12  | 4              | 0              |

-- ============================================================================
-- MYSQL SOLUTION
-- ============================================================================

WITH RECURSIVE
    Months AS (
        SELECT 1 AS month
        UNION ALL
        SELECT month + 1
        FROM Months
        WHERE month < 12
    ),
    Ride AS (
        SELECT MONTH(requested_at) AS month, COUNT(1) AS cnt
        FROM Rides AS r
        JOIN AcceptedRides AS a ON r.ride_id = a.ride_id 
            AND YEAR(requested_at) = 2020
        GROUP BY month
    )
SELECT
    m.month,
    COUNT(driver_id) AS active_drivers,
    IFNULL(r.cnt, 0) AS accepted_rides
FROM Months AS m
LEFT JOIN Drivers AS d ON 
    (m.month >= MONTH(d.join_date) AND YEAR(d.join_date) = 2020) 
    OR YEAR(d.join_date) < 2020
LEFT JOIN Ride AS r ON m.month = r.month
GROUP BY month;

-- ============================================================================
-- SQL SERVER SOLUTION
-- ============================================================================
-- Key differences from MySQL:
--   1. No RECURSIVE keyword needed
--   2. IFNULL -> ISNULL
--   3. Add OPTION (MAXRECURSION 12)
--   4. Add r.cnt to GROUP BY (stricter rules)

WITH Months AS (
    SELECT 1 AS month
    UNION ALL
    SELECT month + 1 FROM Months WHERE month < 12
),
Ride AS (
    SELECT MONTH(requested_at) AS month, COUNT(1) AS cnt
    FROM Rides r
    JOIN AcceptedRides a ON r.ride_id = a.ride_id 
        AND YEAR(requested_at) = 2020
    GROUP BY MONTH(requested_at)
)
SELECT
    m.month,
    COUNT(d.driver_id) AS active_drivers,
    ISNULL(r.cnt, 0) AS accepted_rides
FROM Months m
LEFT JOIN Drivers d ON 
    (m.month >= MONTH(d.join_date) AND YEAR(d.join_date) = 2020) 
    OR YEAR(d.join_date) < 2020
LEFT JOIN Ride r ON m.month = r.month
GROUP BY m.month, r.cnt
ORDER BY m.month
OPTION (MAXRECURSION 12);

-- ============================================================================
-- SQL SERVER SOLUTION (Alternative — cleaner with separate CTEs)
-- ============================================================================
-- Pre-aggregate drivers and rides in separate CTEs, then join.
-- No risk of inflated counts, no need for r.cnt in GROUP BY.

WITH Months AS (
    SELECT 1 AS month
    UNION ALL
    SELECT month + 1 FROM Months WHERE month < 12
),
ActiveDrivers AS (
    SELECT m.month, COUNT(d.driver_id) AS active_drivers
    FROM Months m
    LEFT JOIN Drivers d ON 
        (m.month >= MONTH(d.join_date) AND YEAR(d.join_date) = 2020)
        OR YEAR(d.join_date) < 2020
    GROUP BY m.month
),
RideCounts AS (
    SELECT MONTH(requested_at) AS month, COUNT(1) AS accepted_rides
    FROM Rides r
    JOIN AcceptedRides a ON r.ride_id = a.ride_id
    WHERE YEAR(requested_at) = 2020
    GROUP BY MONTH(requested_at)
)
SELECT
    ad.month,
    ad.active_drivers,
    ISNULL(rc.accepted_rides, 0) AS accepted_rides
FROM ActiveDrivers ad
LEFT JOIN RideCounts rc ON ad.month = rc.month
ORDER BY ad.month
OPTION (MAXRECURSION 12);
