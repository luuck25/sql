-- ============================================================================
-- LeetCode 1204: Last Person to Fit in the Bus
-- ============================================================================
-- Problem:
-- You have one table:
--
--   Queue(person_id, person_name, weight, turn)
--     - person_id is the primary key.
--     - turn is a unique integer representing the boarding order.
--     - weight is the person's weight in kilograms.
--
-- There is a queue of people waiting to board a bus. The bus has a weight
-- limit of 1000 kilograms. People board one by one in order of their turn.
--
-- Find the person_name of the LAST person that can board the bus without
-- exceeding the weight limit (i.e., the cumulative weight stays <= 1000).
--
-- It is guaranteed that the first person does not exceed the weight limit.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE Queue (
    person_id   INT PRIMARY KEY,
    person_name VARCHAR(30),
    weight      INT,
    turn        INT UNIQUE
);

INSERT INTO Queue (person_id, person_name, weight, turn) VALUES
(5, 'Alice',   250, 1),
(4, 'Bob',     175, 5),
(3, 'Alex',    350, 2),
(6, 'John',    400, 3),
(1, 'Winston', 500, 6),
(2, 'Marie',   200, 4);

-- ============================================================================
-- SAMPLE DATA REFERENCE (sorted by turn)
-- ============================================================================
-- | person_id | person_name | weight | turn |
-- |-----------|-------------|--------|------|
-- | 5         | Alice       | 250    | 1    |
-- | 3         | Alex        | 350    | 2    |
-- | 6         | John        | 400    | 3    |
-- | 2         | Marie       | 200    | 4    |
-- | 4         | Bob         | 175    | 5    |
-- | 1         | Winston     | 500    | 6    |

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================
-- Compute cumulative weight in turn order:
--
-- | turn | person_name | weight | cumulative_weight |
-- |------|-------------|--------|-------------------|
-- | 1    | Alice       | 250    | 250               |
-- | 2    | Alex        | 350    | 600               |
-- | 3    | John        | 400    | 1000              |  ← exactly 1000, still fits
-- | 4    | Marie       | 200    | 1200              |  ← exceeds 1000
-- | 5    | Bob         | 175    | 1375              |
-- | 6    | Winston     | 500    | 1875              |
--
-- Last person where cumulative_weight <= 1000 is John (turn 3).

-- ============================================================================
-- EXPECTED OUTPUT
-- ============================================================================
-- | person_name |
-- |-------------|
-- | John        |

-- ============================================================================
-- KEY CONCEPTS
-- ============================================================================
-- 1. Running sum via window frame:
--    SUM(weight) OVER (ORDER BY turn) = cumulative weight.
--    ORDER BY inside OVER() gives implicit ROWS BETWEEN UNBOUNDED PRECEDING
--    AND CURRENT ROW → running sum, not total.
--
-- 2. Find last qualifying row:
--    Filter cumulative_weight <= 1000, then ORDER BY turn DESC, TOP 1.
--    Or use subquery: MAX(turn) WHERE cumulative_weight <= 1000.

-- DROP TABLE Queue;

-- ============================================================================
-- SQL SERVER SOLUTION
-- ============================================================================

WITH cumulative AS (
    SELECT
        person_name,
        turn,
        SUM(weight) OVER (ORDER BY turn) AS cumulative_weight
    FROM Queue
)
SELECT TOP 1 person_name
FROM cumulative
WHERE cumulative_weight <= 1000
ORDER BY turn DESC;

-- ============================================================================
-- SQL SERVER SOLUTION 2 (Subquery with MAX)
-- ============================================================================

WITH cumulative AS (
    SELECT
        person_name,
        turn,
        SUM(weight) OVER (ORDER BY turn) AS cumulative_weight
    FROM Queue
)
SELECT person_name
FROM cumulative
WHERE turn = (SELECT MAX(turn) FROM cumulative WHERE cumulative_weight <= 1000);

