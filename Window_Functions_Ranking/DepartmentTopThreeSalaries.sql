-- ============================================================================
-- LeetCode 185: Department Top Three Salaries
-- ============================================================================
-- Problem:
-- You have two tables:
--
--   Employee(id, name, salary, departmentId)
--     - Each row is an employee with their salary and department.
--
--   Department(id, name)
--     - Each row is a department.
--
-- A company's executives are interested in seeing who earns the most in each
-- department. A HIGH EARNER in a department is an employee who has a salary
-- in the TOP THREE UNIQUE salaries for that department.
--
-- Write a query to find the employees who are high earners in each department.
-- Return the result in any order.
--
-- Note: If multiple employees have the same salary, they all count as one
-- unique salary. E.g., if top 3 unique salaries are 300, 200, 100, then ALL
-- employees earning 300, 200, or 100 are high earners.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE Department (
    id    INT PRIMARY KEY,
    name  VARCHAR(50)
);

CREATE TABLE Employee (
    id             INT PRIMARY KEY,
    name           VARCHAR(50),
    salary         INT,
    departmentId   INT,
    FOREIGN KEY (departmentId) REFERENCES Department(id)
);

INSERT INTO Department (id, name) VALUES
(1, 'IT'),
(2, 'Sales');

INSERT INTO Employee (id, name, salary, departmentId) VALUES
(1, 'Joe',   85000, 1),
(2, 'Henry', 80000, 2),
(3, 'Sam',   60000, 2),
(4, 'Max',   90000, 1),
(5, 'Janet', 69000, 1),
(6, 'Randy', 85000, 1),   -- same salary as Joe
(7, 'Will',  70000, 1);

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- Department:
-- | id | name  |
-- |----|-------|
-- | 1  | IT    |
-- | 2  | Sales |
--
-- Employee:
-- | id | name  | salary | departmentId |
-- |----|-------|--------|--------------|
-- | 1  | Joe   | 85000  | 1 (IT)       |
-- | 2  | Henry | 80000  | 2 (Sales)    |
-- | 3  | Sam   | 60000  | 2 (Sales)    |
-- | 4  | Max   | 90000  | 1 (IT)       |
-- | 5  | Janet | 69000  | 1 (IT)       |
-- | 6  | Randy | 85000  | 1 (IT)       |
-- | 7  | Will  | 70000  | 1 (IT)       |

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================

-- STEP 1: Rank salaries within each department using DENSE_RANK
-- DENSE_RANK (not RANK or ROW_NUMBER) because:
--   - Ties get the same rank
--   - No gaps in ranking
--
-- IT department unique salaries: 90000, 85000, 70000, 69000
-- | name  | salary | DENSE_RANK |
-- |-------|--------|------------|
-- | Max   | 90000  | 1          |
-- | Joe   | 85000  | 2          |
-- | Randy | 85000  | 2          |  ← same rank (tie)
-- | Will  | 70000  | 3          |
-- | Janet | 69000  | 4          |  ← rank 4, NOT a top-3 earner
--
-- Sales department unique salaries: 80000, 60000
-- | name  | salary | DENSE_RANK |
-- |-------|--------|------------|
-- | Henry | 80000  | 1          |
-- | Sam   | 60000  | 2          |

-- STEP 2: Filter where DENSE_RANK <= 3

-- ============================================================================
-- EXPECTED OUTPUT
-- ============================================================================
-- | Department | Employee | Salary |
-- |------------|----------|--------|
-- | IT         | Max      | 90000  |
-- | IT         | Joe      | 85000  |
-- | IT         | Randy    | 85000  |
-- | IT         | Will     | 70000  |
-- | Sales      | Henry    | 80000  |
-- | Sales      | Sam      | 60000  |
--
-- Note: Janet (69000) is excluded — she has the 4th unique salary in IT.

-- ============================================================================
-- KEY CONCEPT: DENSE_RANK vs RANK vs ROW_NUMBER
-- ============================================================================
-- For IT salaries: 90000, 85000, 85000, 70000, 69000
--
-- | Function     | 90000 | 85000 | 85000 | 70000 | 69000 |
-- |--------------|-------|-------|-------|-------|-------|
-- | ROW_NUMBER   | 1     | 2     | 3     | 4     | 5     |  ← unique numbers, arbitrary tie-break
-- | RANK         | 1     | 2     | 2     | 4     | 5     |  ← ties share rank, gaps after
-- | DENSE_RANK   | 1     | 2     | 2     | 3     | 4     |  ← ties share rank, NO gaps
--
-- DENSE_RANK is correct here because "top 3 unique salaries" means no gaps.
-- With RANK, 70000 would get rank 4 (skipping 3) and be excluded — wrong!

-- ============================================================================
-- SQL SERVER SOLUTION
-- ============================================================================

SELECT 
    d.name AS Department,
    e.name AS Employee,
    e.salary AS Salary
FROM (
    SELECT 
        name,
        salary,
        departmentId,
        DENSE_RANK() OVER (PARTITION BY departmentId ORDER BY salary DESC) AS rnk
    FROM Employee
) e
JOIN Department d ON e.departmentId = d.id
WHERE e.rnk <= 3;

-- ============================================================================
-- SQL SERVER SOLUTION (Alternative — CTE first, then rank)
-- ============================================================================
-- Join Employee and Department first in a CTE, then rank by Department name.

WITH EMP_DEP AS (
    SELECT 
        e.name AS Employee,
        salary,
        d.name AS Department
    FROM Employee e
    JOIN Department d ON e.departmentId = d.id
)
SELECT 
    Department,
    Employee,
    Salary
FROM (        
    SELECT *, 
        DENSE_RANK() OVER (PARTITION BY Department ORDER BY salary DESC) AS RANKED_SALARY
    FROM EMP_DEP
) TEMP 
WHERE RANKED_SALARY <= 3;

-- DROP TABLE Employee;
-- DROP TABLE Department;
