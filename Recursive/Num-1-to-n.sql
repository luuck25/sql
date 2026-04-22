-- Recursive CTE to generate numbers 1 through 10

WITH nums AS (
    -- Base case (anchor member): starts the recursion with n = 1
    SELECT 1 AS n

    UNION ALL 

    -- Recursive member: takes the previous row and adds 1 to n
    -- SQL Server repeatedly executes this part, feeding each new row back in
    SELECT n + 1 
    FROM nums            -- references itself — this is what makes it recursive
    WHERE n < 10         -- termination condition: stop when n reaches 10
)
-- Final query: returns all rows produced by the recursion (1, 2, 3, ... 10)
SELECT * FROM nums;