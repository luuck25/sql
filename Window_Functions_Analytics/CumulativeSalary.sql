-- ============================================================================
-- LeetCode 579: Find Cumulative Salary of an Employee
-- ============================================================================
-- Problem:
-- You have one table:
--
--   Employee(id, month, salary)
--     - Each row is an employee's salary for a given month.
--     - (id, month) is the primary key.
--
-- Write a query to get the CUMULATIVE SUM of an employee's salary over a
-- 3-MONTH WINDOW, excluding the MOST RECENT month for each employee.
--
-- For each employee, for each month (except their latest month):
--   cumulative_salary = salary of that month
--                     + salary of (month - 1)
--                     + salary of (month - 2)
--
-- If a previous month doesn't exist, treat salary as 0.
-- Return the result ordered by id ASC, then month DESC.
-- Exclude each employee's most recent month.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE Employee (
    id      INT,
    month   INT,
    salary  INT,
    PRIMARY KEY (id, month)
);

INSERT INTO Employee (id, month, salary) VALUES
(1, 1, 20),
(1, 2, 30),
(1, 3, 40),
(1, 4, 60),
(1, 7, 90),
(1, 8, 130),
(2, 1, 20),
(2, 2, 30),
(3, 2, 40),
(3, 3, 60),
(3, 4, 70);

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- Employee:
-- | id | month | salary |
-- |----|-------|--------|
-- | 1  | 1     | 20     |
-- | 1  | 2     | 30     |
-- | 1  | 3     | 40     |
-- | 1  | 4     | 60     |
-- | 1  | 7     | 90     |
-- | 1  | 8     | 130    |
-- | 2  | 1     | 20     |
-- | 2  | 2     | 30     |
-- | 3  | 2     | 40     |
-- | 3  | 3     | 60     |
-- | 3  | 4     | 70     |

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================

-- STEP 1: Identify each employee's most recent month (to exclude)
-- | id | max_month |
-- |----|-----------|
-- | 1  | 8         |
-- | 2  | 2         |
-- | 3  | 4         |

-- STEP 2: For remaining months, compute 3-month cumulative salary
-- NOTE: Months may NOT be consecutive (e.g., employee 1 has months 1-4, then 7-8)
-- So we can't use ROWS BETWEEN — we need to match by actual month values.
--
-- Employee 1 (exclude month 8):
-- | month | salary | month-1 sal | month-2 sal | cumulative |
-- |-------|--------|-------------|-------------|------------|
-- | 7     | 90     | 0 (no m6)   | 0 (no m5)   | 90         |
-- | 4     | 60     | 40 (m3)     | 30 (m2)     | 130        |
-- | 3     | 40     | 30 (m2)     | 20 (m1)     | 90         |
-- | 2     | 30     | 20 (m1)     | 0 (no m0)   | 50         |
-- | 1     | 20     | 0           | 0           | 20         |
--
-- Employee 2 (exclude month 2):
-- | month | salary | month-1 sal | month-2 sal | cumulative |
-- |-------|--------|-------------|-------------|------------|
-- | 1     | 20     | 0           | 0           | 20         |
--
-- Employee 3 (exclude month 4):
-- | month | salary | month-1 sal | month-2 sal | cumulative |
-- |-------|--------|-------------|-------------|------------|
-- | 3     | 60     | 40 (m2)     | 0 (no m1)   | 100        |
-- | 2     | 40     | 0 (no m1)   | 0           | 40         |

-- ============================================================================
-- EXPECTED OUTPUT (ordered by id ASC, month DESC)
-- ============================================================================
-- | id | month | salary (cumulative) |
-- |----|-------|---------------------|
-- | 1  | 7     | 90                  |
-- | 1  | 4     | 130                 |
-- | 1  | 3     | 90                  |
-- | 1  | 2     | 50                  |
-- | 1  | 1     | 20                  |
-- | 2  | 1     | 20                  |
-- | 3  | 3     | 100                 |
-- | 3  | 2     | 40                  |

-- ============================================================================
-- KEY CONCEPTS
-- ============================================================================
-- 1. ROWS vs RANGE window frame:
--    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW — counts physical rows (wrong
--    here because months may have gaps, e.g., month 4 → 7)
--
--    RANGE BETWEEN 2 PRECEDING AND CURRENT ROW — matches by actual values
--    (would work!), but SQL Server does NOT support numeric offsets with RANGE.
--    Only UNBOUNDED PRECEDING/FOLLOWING allowed. PostgreSQL supports it.
--
-- 2. Self-join approach — why SUM(e2.salary)?
--    The join condition: e2.month BETWEEN e1.month - 2 AND e1.month
--    For each row in e1, this finds UP TO 3 matching rows in e2:
--      e2.month = e1.month      (current month)
--      e2.month = e1.month - 1  (previous month, if exists)
--      e2.month = e1.month - 2  (two months ago, if exists)
--
--    Example: e1 row is (id=1, month=4, salary=60)
--      e2 matches: month 2 (sal 30), month 3 (sal 40), month 4 (sal 60)
--      SUM(e2.salary) = 30 + 40 + 60 = 130 ✓
--
--    Example: e1 row is (id=1, month=7, salary=90)
--      e2 matches: only month 7 (sal 90) — months 5,6 don't exist
--      SUM(e2.salary) = 90 ✓
--
--    We SUM e2.salary (not e1) because e2 is the table providing the
--    matching rows within the 3-month window. e1 is the "anchor" row.
--    GROUP BY e1.id, e1.month collapses the multiple e2 matches into one sum.
--
-- 3. LAG approach — why not just LAG(salary)?
--    LAG looks at physical row position, not month values. With gaps
--    (month 4 → 7), LAG(salary,1) would return month 4's salary for
--    month 7 — wrong! So we also LAG the month column and verify
--    consecutiveness with CASE before adding.
--
-- 4. Excluding the most recent month:
--    Use a subquery/CTE to find MAX(month) per employee, then filter it out.

-- ============================================================================
-- SQL SERVER SOLUTION (Self-Join)
-- ============================================================================

SELECT 
    e1.id,
    e1.month,
    SUM(e2.salary) AS salary
FROM Employee e1
JOIN Employee e2 
    ON e1.id = e2.id 
    AND e2.month BETWEEN e1.month - 2 AND e1.month
WHERE e1.month < (
    SELECT MAX(month) 
    FROM Employee e3 
    WHERE e3.id = e1.id
)
GROUP BY e1.id, e1.month
ORDER BY e1.id ASC, e1.month DESC;

-- ============================================================================
-- SQL SERVER SOLUTION (LAG with Month-Gap Check)
-- ============================================================================
-- LAG gives us previous rows' values, but we must also LAG the month column
-- to verify the previous row is actually the consecutive month (no gaps).

WITH withLag AS (
    SELECT  id,
            month,
            salary,
            -- Get previous salaries (default 0 if no previous row)
            LAG(salary, 1, 0) OVER (PARTITION BY id ORDER BY month ASC) AS prev_sal,
            LAG(salary, 2, 0) OVER (PARTITION BY id ORDER BY month ASC) AS prev_prev_sal,
            -- Get previous months to check for gaps
            LAG(month, 1, 0)  OVER (PARTITION BY id ORDER BY month ASC) AS prev_month,
            LAG(month, 2, 0)  OVER (PARTITION BY id ORDER BY month ASC) AS prev_prev_month
    FROM Employee
)
SELECT  id,
        month,
        salary
        + CASE WHEN prev_month = month - 1 THEN prev_sal ELSE 0 END
        + CASE WHEN prev_prev_month = month - 2 THEN prev_prev_sal ELSE 0 END
        AS salary
FROM withLag
WHERE month < (SELECT MAX(month) FROM Employee e WHERE e.id = withLag.id)
ORDER BY id ASC, month DESC;

-- DROP TABLE Employee;
