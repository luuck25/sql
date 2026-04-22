-- ============================================================================
-- LeetCode 601: Human Traffic of Stadium
-- ============================================================================
-- Problem:
-- You have one table:
--
--   Stadium(id, visit_date, people)
--     - id is the primary key and auto-increments (each row has a unique id).
--     - Each row contains the visit date and the number of people who visited
--       the stadium on that day.
--     - No two rows will have the same visit_date.
--
-- Write a query to display records where there are THREE OR MORE consecutive
-- rows (by id) with people >= 100.
--
-- Return the result ordered by visit_date ASC.
--
-- Note: Each day's id is guaranteed to be consecutive, but some days may be
-- missing from the table.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE Stadium (
    id          INT PRIMARY KEY,
    visit_date  DATE,
    people      INT
);

INSERT INTO Stadium (id, visit_date, people) VALUES
(1, '2017-01-01', 10),
(2, '2017-01-02', 109),
(3, '2017-01-03', 150),
(4, '2017-01-04', 99),
(5, '2017-01-05', 145),
(6, '2017-01-06', 1455),
(7, '2017-01-07', 199),
(8, '2017-01-09', 188);

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- Stadium:
-- | id | visit_date | people |
-- |----|------------|--------|
-- | 1  | 2017-01-01 | 10     |
-- | 2  | 2017-01-02 | 109    |
-- | 3  | 2017-01-03 | 150    |
-- | 4  | 2017-01-04 | 99     |
-- | 5  | 2017-01-05 | 145    |
-- | 6  | 2017-01-06 | 1455   |
-- | 7  | 2017-01-07 | 199    |
-- | 8  | 2017-01-09 | 188    |

-- ============================================================================
-- EXPECTED OUTPUT (ordered by visit_date ASC)
-- ============================================================================
-- | id | visit_date | people |
-- |----|------------|--------|
-- | 5  | 2017-01-05 | 145    |
-- | 6  | 2017-01-06 | 1455   |
-- | 7  | 2017-01-07 | 199    |
-- | 8  | 2017-01-09 | 188    |
--
-- Rows 5,6,7,8 form a group of 4 consecutive ids all with people >= 100.
-- Rows 2,3 have people >= 100 but row 4 has 99 (< 100), breaking the streak.

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================

-- STEP 1: Filter rows with people >= 100
-- | id | visit_date | people |
-- |----|------------|--------|
-- | 2  | 2017-01-02 | 109    |
-- | 3  | 2017-01-03 | 150    |
-- | 5  | 2017-01-05 | 145    |
-- | 6  | 2017-01-06 | 1455   |
-- | 7  | 2017-01-07 | 199    |
-- | 8  | 2017-01-09 | 188    |

-- STEP 2: Assign ROW_NUMBER to filtered rows, then compute id - ROW_NUMBER
-- If ids are consecutive, (id - row_number) stays constant → same group.
-- | id | row_num | id - row_num (grp) |
-- |----|---------|---------------------|
-- | 2  | 1       | 1                   |
-- | 3  | 2       | 1                   |  ← group 1 (only 2 rows → too few)
-- | 5  | 3       | 2                   |
-- | 6  | 4       | 2                   |
-- | 7  | 5       | 2                   |
-- | 8  | 6       | 2                   |  ← group 2 (4 rows → qualifies!)

-- STEP 3: Keep only groups with COUNT(*) >= 3

-- ============================================================================
-- KEY CONCEPTS
-- ============================================================================
-- 1. "Consecutive id" grouping trick:
--    Filter to qualifying rows → assign ROW_NUMBER → compute id - ROW_NUMBER.
--    Consecutive ids produce the same (id - rn) value = same group.
--    This is the classic "gaps and islands" technique.
--
-- 2. Why this works:
--    id:  5, 6, 7, 8    (consecutive)
--    rn:  3, 4, 5, 6    (consecutive)
--    id - rn: 2, 2, 2, 2  ← constant! = one island
--
--    id:  2, 3           (consecutive)
--    rn:  1, 2           (consecutive)
--    id - rn: 1, 1       ← different constant = different island
--
-- 3. Alternative: Self-join approach (3 copies of the table)
--    Check every triple (t1, t2, t3) where ids are consecutive and all >= 100.
--    Works but less elegant for "3 or more".

-- ============================================================================
-- SQL SERVER SOLUTION (Gaps and Islands)
-- ============================================================================

WITH filtered AS (
    SELECT *, 
        ROW_NUMBER() OVER (ORDER BY id) AS rn
    FROM Stadium
    WHERE people >= 100
),
grouped AS (
    SELECT *,
        id - rn AS grp
    FROM filtered
)
SELECT id, visit_date, people
FROM grouped
WHERE grp IN (
    SELECT grp 
    FROM grouped 
    GROUP BY grp 
    HAVING COUNT(*) >= 3
)
ORDER BY visit_date ASC;

-- ============================================================================
-- SQL SERVER SOLUTION (Alternative — Self-Join)
-- ============================================================================
-- Find all rows that appear in ANY group of 3 consecutive ids with people >= 100.
-- A row qualifies if it's the 1st, 2nd, or 3rd in some consecutive triple.

SELECT DISTINCT s.*
FROM Stadium s
JOIN Stadium s1 ON s1.people >= 100
JOIN Stadium s2 ON s2.people >= 100
JOIN Stadium s3 ON s3.people >= 100
    AND s2.id = s1.id + 1
    AND s3.id = s1.id + 2
WHERE s.people >= 100
    AND s.id BETWEEN s1.id AND s3.id
ORDER BY s.visit_date ASC;

-- DROP TABLE Stadium;
