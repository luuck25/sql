-- ============================================================================
-- LeetCode 2494: Merge Overlapping Events in the Same Hall
-- ============================================================================
-- Problem:
-- You have one table:
--
--   HallEvents(hall_id, start_day, end_day)
--     - Each row represents an event held in a hall during a date range
--       [start_day, end_day] (inclusive).
--     - (hall_id, start_day) is the primary key.
--
-- Write a query to merge all overlapping events that are held in the SAME hall.
-- Two events overlap if they share at least one day in common.
--
-- Specifically, if event A is [s1, e1] and event B is [s2, e2] in the same hall,
-- they overlap if s2 <= e1 (B starts before A ends). Merge them into
-- [MIN(s1,s2), MAX(e1,e2)].
--
-- If event C then overlaps with the merged A+B, merge again (chain merging).
--
-- Return: hall_id, start_day, end_day for each merged event.
-- Order by hall_id ASC, start_day ASC.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE HallEvents (
    hall_id    INT,
    start_day  DATE,
    end_day    DATE,
    PRIMARY KEY (hall_id, start_day)
);

INSERT INTO HallEvents (hall_id, start_day, end_day) VALUES
(1, '2023-01-13', '2023-01-14'),
(1, '2023-01-14', '2023-01-17'),
(1, '2023-01-18', '2023-01-25'),
(1, '2023-01-26', '2023-01-27'),
(2, '2022-12-09', '2022-12-23'),
(2, '2022-12-13', '2022-12-17'),
(3, '2023-01-01', '2023-01-30'),
(3, '2023-01-05', '2023-01-10'),
(3, '2023-01-15', '2023-01-20'),
(3, '2023-02-01', '2023-02-05');

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- HallEvents:
-- | hall_id | start_day  | end_day    |
-- |---------|------------|------------|
-- | 1       | 2023-01-13 | 2023-01-14 |
-- | 1       | 2023-01-14 | 2023-01-17 |
-- | 1       | 2023-01-18 | 2023-01-25 |
-- | 1       | 2023-01-26 | 2023-01-27 |
-- | 2       | 2022-12-09 | 2022-12-23 |
-- | 2       | 2022-12-13 | 2022-12-17 |
-- | 3       | 2023-01-01 | 2023-01-30 |
-- | 3       | 2023-01-05 | 2023-01-10 |
-- | 3       | 2023-01-15 | 2023-01-20 |
-- | 3       | 2023-02-01 | 2023-02-05 |

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================

-- Hall 1:
-- [Jan 13–14] overlaps with [Jan 14–17] (share Jan 14) → merge to [Jan 13–17]
-- [Jan 13–17] overlaps with [Jan 18–25]? No (17 < 18) → separate
-- [Jan 18–25] and [Jan 26–27]? No (25 < 26) → separate
--
-- Hall 1 result:
--   [2023-01-13, 2023-01-17]  ← merged from 2 events
--   [2023-01-18, 2023-01-25]  ← standalone
--   [2023-01-26, 2023-01-27]  ← standalone

-- Hall 2:
-- [Dec 09–23] overlaps with [Dec 13–17] (13 <= 23) → merge to [Dec 09–23]
--
-- Hall 2 result:
--   [2022-12-09, 2022-12-23]  ← merged from 2 events

-- Hall 3:
-- [Jan 01–30] overlaps with [Jan 05–10] (5 <= 30) → merge to [Jan 01–30]
-- [Jan 01–30] overlaps with [Jan 15–20] (15 <= 30) → still [Jan 01–30]
-- [Jan 01–30] overlaps with [Feb 01–05]? No (Feb 1 > Jan 30) → separate
--
-- Hall 3 result:
--   [2023-01-01, 2023-01-30]  ← merged from 3 events
--   [2023-02-01, 2023-02-05]  ← standalone

-- ============================================================================
-- EXPECTED OUTPUT (ordered by hall_id ASC, start_day ASC)
-- ============================================================================
-- | hall_id | start_day  | end_day    |
-- |---------|------------|------------|
-- | 1       | 2023-01-13 | 2023-01-17 |
-- | 1       | 2023-01-18 | 2023-01-25 |
-- | 1       | 2023-01-26 | 2023-01-27 |
-- | 2       | 2022-12-09 | 2022-12-23 |
-- | 3       | 2023-01-01 | 2023-01-30 |
-- | 3       | 2023-02-01 | 2023-02-05 |

-- ============================================================================
-- KEY CONCEPTS
-- ============================================================================
-- 1. Overlap detection:
--    Two events [s1,e1] and [s2,e2] overlap if s2 <= e1 (when sorted by start).
--    >= not > because inclusive date ranges — shared boundary day = overlap.
--
-- 2. Running MAX(end_day) vs LAG(end_day):
--    LAG only sees the immediately previous row's end_day.
--    Event A [Jan 1–30], B [Jan 5–10], C [Jan 15–20]:
--      LAG(end_day) for C = Jan 10 (B's end) → C looks non-overlapping! WRONG.
--      Running MAX(end_day) for C = Jan 30 (A's end) → C overlaps. CORRECT.
--    Running MAX captures long-spanning events that LAG misses.
--
-- 3. Window frame: ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
--    = all previous rows in the partition, excluding the current row.
--    Excludes current so we compare current start vs previous ends only.
--
-- 4. Group assignment via running SUM:
--    SUM(flag) OVER (ORDER BY ...) = running/cumulative sum, not total.
--    Flag 1 = new group boundary, 0 = overlap (continues current group).
--    Running sum increments only at boundaries → same value = same group.
--
-- 5. Why flag must be 1=new group, 0=overlap (not reversed):
--    If reversed (1=overlap, 0=new group), non-overlapping rows get 0
--    and the running sum doesn't increment → they all get lumped into
--    the same group. The boundary must be the one that increments.
--
-- 6. NULL handling for first row:
--    MAX() over an empty window (no preceding rows) returns NULL.
--    NULL >= start_day evaluates to UNKNOWN (falsy in IIF) → IIF returns 1.
--    So the first event in each hall automatically becomes a new group.
--
-- 7. Overall pattern: Boundary Detection → Group Assignment → Aggregate
--    Step 1: Scan sorted events, flag each as boundary (1) or continuation (0)
--    Step 2: Running SUM of flags → group ID
--    Step 3: GROUP BY group ID, take MIN(start), MAX(end)

-- DROP TABLE HallEvents;

-- ============================================================================
-- SQL SERVER SOLUTION (IIF + Running MAX + Running SUM)
-- ============================================================================
-- CTE 1 (overlap_ind): Detect boundaries using running MAX(end_day) inside IIF.
--   overlap=1 → new group (start_day > all previous end_days, or first event)
--   overlap=0 → overlaps with a previous event (continuation)
-- CTE 2 (T): Running SUM(overlap) → assigns group IDs (indicator).
-- Final: GROUP BY group ID → MIN(start_day), MAX(end_day) per merged interval.

-- CTE step output:
-- | hall_id | start_day  | end_day    | overlap |
-- |---------|------------|------------|---------|
-- | 1       | 2023-01-13 | 2023-01-14 | 1       |  ← first event, MAX over empty = NULL, NULL >= start? → false → 1 (new group)
-- | 1       | 2023-01-14 | 2023-01-17 | 0       |  ← MAX(prev end) = Jan 14 >= Jan 14 → true → 0 (overlap)
-- | 1       | 2023-01-18 | 2023-01-25 | 1       |  ← MAX(prev end) = Jan 17 >= Jan 18 → false → 1 (new group)
-- | 1       | 2023-01-26 | 2023-01-27 | 1       |  ← MAX(prev end) = Jan 25 >= Jan 26 → false → 1 (new group)
-- | 2       | 2022-12-09 | 2022-12-23 | 1       |  ← first event → 1
-- | 2       | 2022-12-13 | 2022-12-17 | 0       |  ← MAX(prev end) = Dec 23 >= Dec 13 → true → 0
-- | 3       | 2023-01-01 | 2023-01-30 | 1       |  ← first event → 1
-- | 3       | 2023-01-05 | 2023-01-10 | 0       |  ← MAX(prev end) = Jan 30 >= Jan 5 → true → 0
-- | 3       | 2023-01-15 | 2023-01-20 | 0       |  ← MAX(prev end) = Jan 30 >= Jan 15 → true → 0
-- | 3       | 2023-02-01 | 2023-02-05 | 1       |  ← MAX(prev end) = Jan 30 >= Feb 1 → false → 1

-- T step output (SUM(overlap) as running group ID):
-- | hall_id | start_day  | end_day    | overlap | indicator |
-- |---------|------------|------------|---------|-----------|
-- | 1       | 2023-01-13 | 2023-01-14 | 1       | 1         |
-- | 1       | 2023-01-14 | 2023-01-17 | 0       | 1         |  ← same group as above
-- | 1       | 2023-01-18 | 2023-01-25 | 1       | 2         |
-- | 1       | 2023-01-26 | 2023-01-27 | 1       | 3         |
-- | 2       | 2022-12-09 | 2022-12-23 | 1       | 1         |
-- | 2       | 2022-12-13 | 2022-12-17 | 0       | 1         |
-- | 3       | 2023-01-01 | 2023-01-30 | 1       | 1         |
-- | 3       | 2023-01-05 | 2023-01-10 | 0       | 1         |
-- | 3       | 2023-01-15 | 2023-01-20 | 0       | 1         |
-- | 3       | 2023-02-01 | 2023-02-05 | 1       | 2         |

-- Final output (GROUP BY hall_id, indicator → MIN/MAX):
-- | hall_id | start_day  | end_day    |
-- |---------|------------|------------|
-- | 1       | 2023-01-13 | 2023-01-17 |  ← indicator 1: merged Jan 13–14 + Jan 14–17
-- | 1       | 2023-01-18 | 2023-01-25 |  ← indicator 2
-- | 1       | 2023-01-26 | 2023-01-27 |  ← indicator 3
-- | 2       | 2022-12-09 | 2022-12-23 |  ← indicator 1: merged Dec 09–23 + Dec 13–17
-- | 3       | 2023-01-01 | 2023-01-30 |  ← indicator 1: merged 3 events
-- | 3       | 2023-02-01 | 2023-02-05 |  ← indicator 2

WITH CTE AS (
    SELECT *, 
        IIF(
            MAX(end_day) OVER (
                PARTITION BY hall_id 
                ORDER BY start_day 
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ) >= start_day, 
            0, 1
        ) AS overlap
    FROM HallEvents
), 
T AS (
    SELECT *, 
        SUM(overlap) OVER (PARTITION BY hall_id ORDER BY start_day) AS indicator
    FROM CTE
)
SELECT hall_id, 
    MIN(start_day) AS start_day, 
    MAX(end_day) AS end_day 
FROM T
GROUP BY hall_id, indicator
ORDER BY hall_id, start_day;