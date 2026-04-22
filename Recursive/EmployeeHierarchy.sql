-- ============================================
-- Employee Hierarchy using Recursive CTE
-- ============================================

-- Org chart:
--
--         1 Alice (CEO)
--        /           \
--    2 Bob          3 Carol
--    /    \             \
-- 4 Dave  5 Eve       6 Frank
--          |
--        7 Grace

CREATE TABLE Employees (
    EmployeeId   INT PRIMARY KEY,
    Name         VARCHAR(50),
    ManagerId    INT NULL,
    FOREIGN KEY (ManagerId) REFERENCES Employees(EmployeeId)
);

INSERT INTO Employees (EmployeeId, Name, ManagerId) VALUES
(1, 'Alice',  NULL),
(2, 'Bob',    1),
(3, 'Carol',  1),
(4, 'Dave',   2),
(5, 'Eve',    2),
(6, 'Frank',  3),
(7, 'Grace',  5);

-- ============================================
-- Problem 1: Find all employees under a given manager (top-down)
-- ============================================
DECLARE @ManagerId INT = 2;  -- Change to 1 (Alice/all), 2 (Bob), 3 (Carol), etc.

WITH EmployeeHierarchy AS (
    -- Base case: start from the given manager
    SELECT EmployeeId, Name, ManagerId, 1 AS Level
    FROM Employees
    WHERE EmployeeId = @ManagerId

    UNION ALL

    -- Recursive step: go DOWN — find direct reports
    SELECT e.EmployeeId, e.Name, e.ManagerId, eh.Level + 1
    FROM Employees e
    JOIN EmployeeHierarchy eh ON e.ManagerId = eh.EmployeeId
)
SELECT 
    EmployeeId,
    Name,
    ManagerId,
    Level,
    REPLICATE(' ', Level - 1) + Name AS OrgChart
FROM EmployeeHierarchy
ORDER BY Level, Name;

-- ============================================
-- Problem 2: Find all managers above a given employee (bottom-up)
-- ============================================
DECLARE @EmployeeId INT = 7;  -- Change to 7 (Grace), 4 (Dave), 6 (Frank), etc.

WITH ManagerChain AS (
    -- Base case: start from the given employee
    SELECT EmployeeId, Name, ManagerId, 1 AS Level
    FROM Employees
    WHERE EmployeeId = @EmployeeId

    UNION ALL

    -- Recursive step: go UP — find the manager of the current person
    SELECT e.EmployeeId, e.Name, e.ManagerId, mc.Level + 1
    FROM Employees e
    JOIN ManagerChain mc ON e.EmployeeId = mc.ManagerId
)
SELECT 
    EmployeeId,
    Name,
    ManagerId,
    Level,
    REPLICATE(' ', Level - 1) + Name AS OrgChart
FROM ManagerChain
ORDER BY Level;

-- DROP TABLE Employees;
