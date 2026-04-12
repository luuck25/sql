
# SQL Interview Patterns - Complete Guide

> A comprehensive guide to mastering SQL for technical interviews, featuring 12 core patterns with syntax, approaches, and curated LeetCode problems.

---

## Table of Contents

1. [Recursive CTEs](#1-recursive-ctes)
2. [Window Functions - Ranking](#2-window-functions---ranking)
3. [Window Functions - Analytics (LAG/LEAD)](#3-window-functions---analytics-laglead)
4. [Window Frame](#4-window-frame)
5. [Pivoting with MIN/MAX](#5-pivoting-with-minmax)
6. [Gaps & Islands](#6-gaps--islands)
7. [Date Functions](#7-date-functions)
8. [CONCAT & GROUP_CONCAT](#8-concat--group_concat)
9. [Self-Joins & JOIN (ON vs WHERE)](#9-self-joins--join-on-vs-where)
10. [Subqueries & CTEs](#10-subqueries--ctes)
11. [Anti-Joins](#11-anti-joins)
12. [Aggregation + GROUP BY + HAVING](#12-aggregation--group-by--having)

---

## SQL Logical Order of Operations

Understanding execution order is crucial for optimization:

```
1. FROM        - Tables and joins
2. WHERE       - Row filtering
3. GROUP BY    - Grouping
4. HAVING      - Group filtering
5. SELECT      - Column selection & expressions
6. DISTINCT    - Duplicate removal
7. ORDER BY    - Sorting
8. LIMIT       - Row limiting
```

---

## 1. Recursive CTEs

### Pattern Info
Recursive CTEs allow a query to reference itself, making them ideal for hierarchical/tree-structured data (org charts, folder structures, generating sequences).

### Approach
1. **Anchor member**: Initial query forming the base result set
2. **Recursive member**: Query that references the CTE itself
3. Combine using `UNION ALL`
4. Define terminating criteria to stop recursion

### Syntax
```sql
WITH RECURSIVE cte AS (
    -- Anchor member
    SELECT col1, col2, ...
    FROM table_name
    WHERE condition
    
    UNION ALL
    
    -- Recursive member
    SELECT col1, col2, ...
    FROM table_name
    JOIN cte ON condition
    WHERE condition -- Terminating Criteria
)
SELECT * FROM cte;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Number of Transactions per Visit** | Hard | [LC 1336](https://leetcode.com/problems/number-of-transactions-per-visit/) |
| 2 | **Total Sales Amount by Year** | Hard | [LC 1384](https://leetcode.com/problems/total-sales-amount-by-year/) |
| 3 | **Hopper Company Queries I** | Hard | [LC 1635](https://leetcode.com/problems/hopper-company-queries-i/) |
| 4 | **Hopper Company Queries II** | Hard | [LC 1645](https://leetcode.com/problems/hopper-company-queries-ii/) |
| 5 | **Hopper Company Queries III** | Hard | [LC 1651](https://leetcode.com/problems/hopper-company-queries-iii/) |
| 6 | **Find the Subtasks that Did Not Execute** | Hard | [LC 1767](https://leetcode.com/problems/find-the-subtasks-that-did-not-execute/) |
| 7 | **The Number of Passengers in Each Bus II** ⭐ | Hard | [LC 2153](https://leetcode.com/problems/the-number-of-passengers-in-each-bus-ii/) |
| 8 | All People Report to Given Manager | Medium | [LC 1270](https://leetcode.com/problems/all-people-report-to-given-manager/) |
| 9 | Find the Missing IDs | Medium | [LC 1613](https://leetcode.com/problems/find-the-missing-ids/) |

---

## 2. Window Functions - Ranking

### Pattern Info
Ranking functions assign ranks to rows within a partition without collapsing rows. Essential for Top-N queries, deduplication, and comparisons.

### Approach
1. Identify the partition (grouping) column
2. Choose ranking function based on tie-handling needs
3. Use in subquery/CTE, then filter by rank

### Syntax
```sql
-- ROW_NUMBER: Unique number for each row (no ties)
ROW_NUMBER() OVER (PARTITION BY col ORDER BY col2 DESC)

-- RANK: Same rank for ties, gaps in sequence
RANK() OVER (PARTITION BY col ORDER BY col2 DESC)

-- DENSE_RANK: Same rank for ties, no gaps
DENSE_RANK() OVER (PARTITION BY col ORDER BY col2 DESC)

-- Common pattern: Top N per group
WITH ranked AS (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY group_col ORDER BY value DESC) AS rn
    FROM table_name
)
SELECT * FROM ranked WHERE rn <= 3;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Department Top 3 Salaries** | Hard | [LC 185](https://leetcode.com/problems/department-top-three-salaries/) |
| 2 | **Market Analysis II** | Hard | [LC 1159](https://leetcode.com/problems/market-analysis-ii/) |
| 3 | **Get the Second Most Recent Activity** | Hard | [LC 1369](https://leetcode.com/problems/get-the-second-most-recent-activity/) |
| 4 | **Find the Quiet Students in All Exams** | Hard | [LC 1412](https://leetcode.com/problems/find-the-quiet-students-in-all-exams/) |
| 5 | **First & Last Call on the Same Day** | Hard | [LC 1972](https://leetcode.com/problems/first-and-last-call-on-the-same-day/) |
| 6 | **Seniors & Juniors to Join Company II** | Hard | [LC 2010](https://leetcode.com/problems/the-number-of-seniors-and-juniors-to-join-the-company-ii/) |
| 7 | **Generate the Invoice** | Hard | [LC 2362](https://leetcode.com/problems/generate-the-invoice/) |
| 8 | **Popularity Percentage** | Hard | [LC 2720](https://leetcode.com/problems/popularity-percentage/) |
| 9 | **Status of Flight Tickets** | Hard | [LC 2793](https://leetcode.com/problems/status-of-flight-tickets/) |
| 10 | **Viewers Turned Streamers** | Hard | [LC 2995](https://leetcode.com/problems/viewers-turned-streamers/) |
| 11 | **Tournament Winners** ⭐ | Hard | [LC 1194](https://leetcode.com/problems/tournament-winners/) |
| 12 | Second Highest Salary | Easy | [LC 176](https://leetcode.com/problems/second-highest-salary/) |
| 13 | Nth Highest Salary | Medium | [LC 177](https://leetcode.com/problems/nth-highest-salary/) |
| 14 | Rank Scores | Medium | [LC 178](https://leetcode.com/problems/rank-scores/) |

---

## 3. Window Functions - Analytics (LAG/LEAD)

### Pattern Info
LAG/LEAD access values from previous/next rows without self-joins. Perfect for calculating differences, growth rates, and detecting changes.

### Approach
1. Use `LAG()` to compare with previous row
2. Use `LEAD()` to compare with next row
3. Handle NULLs with `COALESCE` or default values

### Syntax
```sql
-- LAG: Access previous row value
LAG(column, offset, default) OVER (PARTITION BY col ORDER BY col2)

-- LEAD: Access next row value
LEAD(column, offset, default) OVER (PARTITION BY col ORDER BY col2)

-- FIRST_VALUE / LAST_VALUE
FIRST_VALUE(column) OVER (PARTITION BY col ORDER BY col2)
LAST_VALUE(column) OVER (PARTITION BY col ORDER BY col2 
                         ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)

-- Example: Month-over-month growth
SELECT 
    month,
    revenue,
    revenue - LAG(revenue, 1, 0) OVER (ORDER BY month) AS growth
FROM monthly_sales;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Find Cumulative Salary of an Employee** | Hard | [LC 579](https://leetcode.com/problems/find-cumulative-salary-of-an-employee/) |
| 2 | **Human Traffic of Stadium** | Hard | [LC 601](https://leetcode.com/problems/human-traffic-of-stadium/) |
| 3 | **Hopper Company Queries III** | Hard | [LC 1651](https://leetcode.com/problems/hopper-company-queries-iii/) |
| 4 | Rising Temperature | Easy | [LC 197](https://leetcode.com/problems/rising-temperature/) |
| 5 | Consecutive Numbers | Medium | [LC 180](https://leetcode.com/problems/consecutive-numbers/) |
| 6 | Month-over-Month Revenue Growth | Medium | DataLemur |
| 7 | Stock Price Fluctuation | Medium | DataLemur |

---

## 4. Window Frame

### Pattern Info
Window frames define the subset of rows used for calculations. Essential for running totals, moving averages, and cumulative calculations.

### Approach
1. Define frame boundaries using `ROWS` or `RANGE`
2. Use `UNBOUNDED PRECEDING` for cumulative calculations
3. Use `N PRECEDING/FOLLOWING` for moving windows

### Syntax
```sql
-- Frame specification
ROWS BETWEEN <start> AND <end>
RANGE BETWEEN <start> AND <end>

-- Frame boundaries
UNBOUNDED PRECEDING  -- From partition start
N PRECEDING          -- N rows before current
CURRENT ROW          -- Current row
N FOLLOWING          -- N rows after current
UNBOUNDED FOLLOWING  -- To partition end

-- Running total
SUM(amount) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)

-- 3-day moving average
AVG(value) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)

-- Example: Cumulative sum
SELECT 
    date,
    amount,
    SUM(amount) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING) AS running_total
FROM transactions;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Merge Overlapping Events in Same Hall** ⭐ | Hard | [LC 2494](https://leetcode.com/problems/merge-overlapping-events-in-the-same-hall/) |
| 2 | Last Person to Fit in Bus | Medium | [LC 1204](https://leetcode.com/problems/last-person-to-fit-in-the-bus/) |
| 3 | Running Total of Posts | Easy | DataLemur |
| 4 | Rolling Average Tweets | Medium | DataLemur |
| 5 | Game Play Analysis IV | Medium | [LC 550](https://leetcode.com/problems/game-play-analysis-iv/) |

---

## 5. Pivoting with MIN/MAX

### Pattern Info
Transform rows into columns using conditional aggregation. The MIN/MAX trick works because within each group, only one value matches each condition.

### Approach
1. Group by the row identifier
2. Use `CASE WHEN` inside aggregate functions
3. `MAX/MIN` extracts the single matching value per group

### Syntax
```sql
-- Basic pivot with CASE + MAX
SELECT 
    id,
    MAX(CASE WHEN category = 'A' THEN value END) AS category_a,
    MAX(CASE WHEN category = 'B' THEN value END) AS category_b,
    MAX(CASE WHEN category = 'C' THEN value END) AS category_c
FROM table_name
GROUP BY id;

-- Pivot with ROW_NUMBER for multiple values per category
WITH ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY category ORDER BY value) AS rn
    FROM table_name
)
SELECT 
    rn,
    MAX(CASE WHEN category = 'A' THEN name END) AS America,
    MAX(CASE WHEN category = 'E' THEN name END) AS Europe
FROM ranked
GROUP BY rn;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Students Report by Geography** | Hard | [LC 618](https://leetcode.com/problems/students-report-by-geography/) |
| 2 | **Top Three Wineries** | Hard | [LC 2991](https://leetcode.com/problems/top-three-wineries/) |
| 3 | Reformat Department Table | Easy | [LC 1179](https://leetcode.com/problems/reformat-department-table/) |
| 4 | Capital Gain/Loss | Medium | [LC 1393](https://leetcode.com/problems/capital-gainloss/) |
| 5 | Tree Node | Medium | [LC 608](https://leetcode.com/problems/tree-node/) |

---

## 6. Gaps & Islands

### Pattern Info
Find continuous ranges ("islands") and breaks ("gaps") in sequential data. Common in analyzing streaks, sessions, and consecutive events.

### Approach
1. **Row difference method**: `ROW_NUMBER()` - value creates groups
2. **LAG comparison**: Detect when sequence breaks
3. Group by the calculated island identifier

### Syntax
```sql
-- Method 1: Row difference technique
WITH islands AS (
    SELECT *,
           value - ROW_NUMBER() OVER (ORDER BY value) AS grp
    FROM table_name
)
SELECT MIN(value) AS start, MAX(value) AS end
FROM islands
GROUP BY grp;

-- Method 2: LAG to detect gaps
WITH flagged AS (
    SELECT *,
           CASE WHEN value - LAG(value) OVER (ORDER BY value) > 1 
                THEN 1 ELSE 0 END AS new_island
    FROM table_name
),
grouped AS (
    SELECT *, SUM(new_island) OVER (ORDER BY value) AS island_id
    FROM flagged
)
SELECT island_id, MIN(value), MAX(value)
FROM grouped
GROUP BY island_id;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Human Traffic of Stadium** | Hard | [LC 601](https://leetcode.com/problems/human-traffic-of-stadium/) |
| 2 | **Report Contiguous Dates** | Hard | [LC 1225](https://leetcode.com/problems/report-contiguous-dates/) |
| 3 | **Longest Winning Streak** | Hard | [LC 2173](https://leetcode.com/problems/longest-winning-streak/) |
| 4 | **Consecutive Transactions with Increasing Amounts** | Hard | [LC 2701](https://leetcode.com/problems/consecutive-transactions-with-increasing-amounts/) |
| 5 | **Customers with Max Transactions on Consecutive Days** | Hard | [LC 2752](https://leetcode.com/problems/customers-with-maximum-number-of-transactions-on-consecutive-days/) |
| 6 | Consecutive Numbers | Medium | [LC 180](https://leetcode.com/problems/consecutive-numbers/) |
| 7 | Find the Missing IDs | Medium | [LC 1613](https://leetcode.com/problems/find-the-missing-ids/) |
| 8 | Find Start and End of Continuous Ranges | Medium | [LC 1285](https://leetcode.com/problems/find-the-start-and-end-number-of-continuous-ranges/) |

---

## 7. Date Functions

### Pattern Info
Manipulate and analyze date/time data. Critical for time-series analysis, cohort analysis, and period-based reporting.

### Approach
1. Extract components: `YEAR()`, `MONTH()`, `DAY()`, `WEEKDAY()`
2. Date arithmetic: `DATE_ADD()`, `DATE_SUB()`, `DATEDIFF()`
3. Truncation: `DATE_TRUNC()` (PostgreSQL) or equivalent

### Syntax
```sql
-- Extract components
YEAR(date_col)
MONTH(date_col)
DAY(date_col)
WEEKDAY(date_col)  -- Monday=0, Sunday=6 (MySQL)
WEEK(date_col)

-- Date arithmetic
DATE_ADD(date_col, INTERVAL 7 DAY)
DATE_SUB(date_col, INTERVAL 1 MONTH)
DATEDIFF(date1, date2)  -- date1 - date2 in days

-- PostgreSQL
DATE_TRUNC('month', date_col)
EXTRACT(YEAR FROM date_col)

-- Example: Get records from last 30 days
SELECT * FROM orders
WHERE order_date >= DATE_SUB(CURDATE(), INTERVAL 30 DAY);

-- Example: Group by week
SELECT WEEK(order_date) AS week_num, SUM(amount)
FROM orders
GROUP BY WEEK(order_date);
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Game Play Analysis V** | Hard | [LC 1097](https://leetcode.com/problems/game-play-analysis-v/) |
| 2 | **Sales by Day of the Week** | Hard | [LC 1479](https://leetcode.com/problems/sales-by-day-of-the-week/) |
| 3 | **Friday Purchases I** | Hard | [LC 2993](https://leetcode.com/problems/friday-purchases-i/) |
| 4 | **Friday Purchases II** | Hard | [LC 2994](https://leetcode.com/problems/friday-purchases-ii/) |
| 5 | Rising Temperature | Easy | [LC 197](https://leetcode.com/problems/rising-temperature/) |
| 6 | Monthly Transactions I | Medium | [LC 1193](https://leetcode.com/problems/monthly-transactions-i/) |
| 7 | Active Users | Medium | [LC 1454](https://leetcode.com/problems/active-users/) |

---

## 8. CONCAT & GROUP_CONCAT

### Pattern Info
Merge strings from columns or aggregate strings from multiple rows. Useful for generating formatted output and combining related data.

### Approach
1. `CONCAT()` for column-level string merging
2. `GROUP_CONCAT()` (MySQL) / `STRING_AGG()` (PostgreSQL/SQL Server) for row aggregation
3. Control ordering and separators

### Syntax
```sql
-- CONCAT: Merge columns
CONCAT(str1, str2, ..., strN)
CONCAT_WS(',', str1, str2, str3)  -- With separator

-- GROUP_CONCAT (MySQL): Aggregate rows
GROUP_CONCAT(
    [DISTINCT] expression
    [ORDER BY column]
    [SEPARATOR 'sep']
)

-- STRING_AGG (PostgreSQL/SQL Server)
STRING_AGG(expression, separator ORDER BY column)

-- Example: Combine emails with semicolon
SELECT 
    department,
    GROUP_CONCAT(email ORDER BY name SEPARATOR '; ') AS all_emails
FROM employees
GROUP BY department;

-- Example: Build equation
SELECT CONCAT(
    CASE WHEN coef > 0 THEN '+' ELSE '' END,
    coef, 'X^', power
) AS term
FROM equation;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Build the Equation** | Hard | [LC 2118](https://leetcode.com/problems/build-the-equation/) |
| 2 | **Finding the Topic of Each Post** | Hard | [LC 2199](https://leetcode.com/problems/finding-the-topic-of-each-post/) |
| 3 | **Top Three Wineries** | Hard | [LC 2991](https://leetcode.com/problems/top-three-wineries/) |
| 4 | Group Sold Products By Date | Easy | [LC 1484](https://leetcode.com/problems/group-sold-products-by-the-date/) |

---

## 9. Self-Joins & JOIN (ON vs WHERE)

### Pattern Info
Self-joins compare rows within the same table. Understanding ON vs WHERE in outer joins is critical for correct filtering.

### Approach
1. **Self-join**: Alias the same table twice
2. **ON clause**: Filter during join (preserves outer rows)
3. **WHERE clause**: Filter after join (can eliminate outer rows)

### Syntax
```sql
-- Self-join: Compare rows in same table
SELECT e.name AS employee, m.name AS manager
FROM employees e
JOIN employees m ON e.manager_id = m.id;

-- ON vs WHERE in LEFT JOIN
-- ON: Preserves all left rows, filters right side only
SELECT u.user_id, COUNT(o.order_id)
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id 
                   AND YEAR(o.order_date) = 2019
GROUP BY u.user_id;

-- WHERE: Filters combined result (excludes non-matching left rows)
SELECT u.user_id, COUNT(o.order_id)
FROM users u
LEFT JOIN orders o ON u.user_id = o.user_id
WHERE YEAR(o.order_date) = 2019  -- Excludes users with no 2019 orders!
GROUP BY u.user_id;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Customers with Strictly Increasing Purchases** | Hard | [LC 2474](https://leetcode.com/problems/customers-with-strictly-increasing-purchases/) |
| 2 | **Market Analysis I** | Medium | [LC 1158](https://leetcode.com/problems/market-analysis-i/) |
| 3 | **Market Analysis II** | Hard | [LC 1159](https://leetcode.com/problems/market-analysis-ii/) |
| 4 | Trips and Users | Hard | [LC 262](https://leetcode.com/problems/trips-and-users/) |
| 5 | Employees Earning More Than Managers | Easy | [LC 181](https://leetcode.com/problems/employees-earning-more-than-their-managers/) |
| 6 | Find Duplicate Emails | Easy | [LC 182](https://leetcode.com/problems/duplicate-emails/) |
| 7 | Friend Requests II | Medium | [LC 602](https://leetcode.com/problems/friend-requests-ii-who-has-the-most-friends/) |

---

## 10. Subqueries & CTEs

### Pattern Info
Break complex queries into manageable parts. CTEs improve readability; subqueries can be correlated (row-by-row) or uncorrelated.

### Approach
1. **CTE**: Define reusable named result sets
2. **Correlated subquery**: References outer query (runs per row)
3. **Scalar subquery**: Returns single value
4. **Derived table**: Subquery in FROM clause

### Syntax
```sql
-- CTE (Common Table Expression)
WITH cte_name AS (
    SELECT col1, col2
    FROM table_name
    WHERE condition
)
SELECT * FROM cte_name;

-- Multiple CTEs
WITH 
    cte1 AS (SELECT ...),
    cte2 AS (SELECT ... FROM cte1)
SELECT * FROM cte2;

-- Correlated subquery
SELECT e.name, e.salary,
       (SELECT AVG(salary) FROM employees e2 
        WHERE e2.dept_id = e.dept_id) AS dept_avg
FROM employees e;

-- Subquery in WHERE
SELECT * FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

-- Derived table
SELECT * FROM (
    SELECT dept_id, AVG(salary) AS avg_sal
    FROM employees
    GROUP BY dept_id
) AS dept_stats
WHERE avg_sal > 50000;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Median Employee Salary** | Hard | [LC 569](https://leetcode.com/problems/median-employee-salary/) |
| 2 | **Find Cumulative Salary of an Employee** | Hard | [LC 579](https://leetcode.com/problems/find-cumulative-salary-of-an-employee/) |
| 3 | **Game Play Analysis V** | Hard | [LC 1097](https://leetcode.com/problems/game-play-analysis-v/) |
| 4 | Customers Who Never Order | Easy | [LC 183](https://leetcode.com/problems/customers-who-never-order/) |
| 5 | Department Highest Salary | Medium | [LC 184](https://leetcode.com/problems/department-highest-salary/) |
| 6 | Exchange Seats | Medium | [LC 626](https://leetcode.com/problems/exchange-seats/) |

---

## 11. Anti-Joins

### Pattern Info
Find rows that DON'T have a match in another table. Three approaches: LEFT JOIN + NULL, NOT EXISTS, NOT IN.

### Approach
1. **LEFT JOIN + IS NULL**: Most readable, good performance
2. **NOT EXISTS**: Often fastest, handles NULLs well
3. **NOT IN**: Simplest syntax, but beware of NULLs

### Syntax
```sql
-- Method 1: LEFT JOIN + IS NULL (recommended)
SELECT c.*
FROM customers c
LEFT JOIN orders o ON c.id = o.customer_id
WHERE o.customer_id IS NULL;

-- Method 2: NOT EXISTS
SELECT c.*
FROM customers c
WHERE NOT EXISTS (
    SELECT 1 FROM orders o 
    WHERE o.customer_id = c.id
);

-- Method 3: NOT IN (careful with NULLs!)
SELECT c.*
FROM customers c
WHERE c.id NOT IN (
    SELECT customer_id FROM orders 
    WHERE customer_id IS NOT NULL  -- Important!
);
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Sales by Day of the Week** | Hard | [LC 1479](https://leetcode.com/problems/sales-by-day-of-the-week/) |
| 2 | **Find the Missing IDs** | Medium | [LC 1613](https://leetcode.com/problems/find-the-missing-ids/) |
| 3 | Customers Who Never Order | Easy | [LC 183](https://leetcode.com/problems/customers-who-never-order/) |
| 4 | Students and Examinations | Easy | [LC 1280](https://leetcode.com/problems/students-and-examinations/) |
| 5 | Employees Not in Department | Easy | HackerRank |

---

## 12. Aggregation + GROUP BY + HAVING

### Pattern Info
Core SQL pattern for summarizing data. GROUP BY creates groups; HAVING filters groups (unlike WHERE which filters rows).

### Approach
1. GROUP BY columns that define each group
2. Apply aggregate functions: COUNT, SUM, AVG, MIN, MAX
3. Use HAVING to filter aggregated results
4. Conditional aggregation with CASE inside aggregates

### Syntax
```sql
-- Basic aggregation
SELECT department, COUNT(*) AS emp_count, AVG(salary) AS avg_sal
FROM employees
GROUP BY department
HAVING COUNT(*) > 5;

-- Conditional aggregation
SELECT 
    department,
    COUNT(CASE WHEN status = 'active' THEN 1 END) AS active_count,
    SUM(CASE WHEN gender = 'M' THEN salary ELSE 0 END) AS male_salary
FROM employees
GROUP BY department;

-- Multiple conditions in HAVING
SELECT customer_id, COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(*) >= 3 AND SUM(amount) > 1000;

-- DISTINCT inside aggregate
SELECT department, COUNT(DISTINCT job_title) AS unique_titles
FROM employees
GROUP BY department;
```

### Practice Problems

| # | Problem | Difficulty | Link |
|---|---------|------------|------|
| 1 | **Average Salary: Departments vs Company** | Hard | [LC 615](https://leetcode.com/problems/average-salary-departments-vs-company/) |
| 2 | **Students Report By Geography** | Hard | [LC 618](https://leetcode.com/problems/students-report-by-geography/) |
| 3 | **Market Analysis II** | Hard | [LC 1159](https://leetcode.com/problems/market-analysis-ii/) |
| 4 | Duplicate Emails | Easy | [LC 182](https://leetcode.com/problems/duplicate-emails/) |
| 5 | Classes More Than 5 Students | Easy | [LC 596](https://leetcode.com/problems/classes-more-than-5-students/) |
| 6 | Customers Who Bought All Products | Medium | [LC 1045](https://leetcode.com/problems/customers-who-bought-all-products/) |
| 7 | Immediate Food Delivery II | Medium | [LC 1174](https://leetcode.com/problems/immediate-food-delivery-ii/) |

---

## Bonus: Special Techniques

### Worth Mentioning Problems

| # | Problem | Difficulty | Key Concept | Link |
|---|---------|------------|-------------|------|
| 1 | **Find Median Given Frequency of Numbers** | Hard | Median calculation | [LC 571](https://leetcode.com/problems/find-median-given-frequency-of-numbers/) |
| 2 | **Leetcodify Friends Recommendations** | Hard | Complex joins | [LC 1917](https://leetcode.com/problems/leetcodify-friends-recommendations/) |
| 3 | **Dynamic Pivoting of a Table** | Hard | Stored Procedure | [LC 2252](https://leetcode.com/problems/dynamic-pivoting-of-a-table/) |
| 4 | **Dynamic Unpivoting of a Table** | Hard | Stored Procedure | [LC 2253](https://leetcode.com/problems/dynamic-unpivoting-of-a-table/) |

---

## Study Order Recommendation

| Phase | Patterns | Focus |
|-------|----------|-------|
| **Beginner** | 12 → 10 → 9 | Aggregation, CTEs, Joins |
| **Intermediate** | 2 → 3 → 4 | Window Functions |
| **Advanced** | 6 → 1 → 5 | Gaps & Islands, Recursive, Pivoting |
| **Expert** | 7 → 8 → 11 | Date functions, String aggregation |

---

## Quick Reference: Must-Do Hard Problems

| LC # | Problem | Pattern |
|------|---------|---------|
| 185 | Department Top 3 Salaries | Ranking |
| 262 | Trips and Users | Self-Join |
| 569 | Median Employee Salary | Subquery |
| 579 | Find Cumulative Salary | LAG/LEAD |
| 601 | Human Traffic of Stadium | Gaps & Islands |
| 618 | Students Report by Geography | Pivoting |
| 1194 | Tournament Winners ⭐ | Ranking |
| 1225 | Report Contiguous Dates | Gaps & Islands |
| 1384 | Total Sales Amount by Year | Recursive CTE |
| 2153 | Passengers in Each Bus II ⭐ | Recursive CTE |
| 2494 | Merge Overlapping Events ⭐ | Window Frame |

> ⭐ = Higher difficulty (took longer to solve)

---

## All 44 LeetCode Hard Problems (from Article)

| # | LC # | Problem | Pattern |
|---|------|---------|---------|
| 1 | 185 | Department Top Three Salaries | Ranking |
| 2 | 569 | Median Employee Salary | Subquery |
| 3 | 571 | Find Median Given Frequency of Numbers | Special |
| 4 | 579 | Find Cumulative Salary of an Employee | LAG/LEAD |
| 5 | 601 | Human Traffic of Stadium | Gaps & Islands |
| 6 | 615 | Average Salary: Departments vs Company | Aggregation |
| 7 | 618 | Students Report by Geography | Pivoting |
| 8 | 1097 | Game Play Analysis V | Date Functions |
| 9 | 1159 | Market Analysis II | Ranking |
| 10 | 1194 | Tournament Winners | Ranking |
| 11 | 1225 | Report Contiguous Dates | Gaps & Islands |
| 12 | 1336 | Number of Transactions per Visit | Recursive CTE |
| 13 | 1369 | Get the Second Most Recent Activity | Ranking |
| 14 | 1384 | Total Sales Amount by Year | Recursive CTE |
| 15 | 1412 | Find the Quiet Students in All Exams | Ranking |
| 16 | 1479 | Sales by Day of the Week | Date Functions |
| 17 | 1635 | Hopper Company Queries I | Recursive CTE |
| 18 | 1645 | Hopper Company Queries II | Recursive CTE |
| 19 | 1651 | Hopper Company Queries III | Recursive CTE |
| 20 | 1767 | Find the Subtasks that Did Not Execute | Recursive CTE |
| 21 | 1917 | Leetcodify Friends Recommendations | Special |
| 22 | 1972 | First and Last Call on the Same Day | Ranking |
| 23 | 2010 | Seniors and Juniors to Join Company II | Ranking |
| 24 | 2118 | Build the Equation | CONCAT |
| 25 | 2153 | The Number of Passengers in Each Bus II | Recursive CTE |
| 26 | 2173 | Longest Winning Streak | Gaps & Islands |
| 27 | 2199 | Finding the Topic of Each Post | CONCAT |
| 28 | 2252 | Dynamic Pivoting of a Table | Stored Procedure |
| 29 | 2253 | Dynamic Unpivoting of a Table | Stored Procedure |
| 30 | 2362 | Generate the Invoice | Ranking |
| 31 | 2474 | Customers with Strictly Increasing Purchases | Self-Join |
| 32 | 2494 | Merge Overlapping Events in the Same Hall | Window Frame |
| 33 | 2701 | Consecutive Transactions with Increasing Amounts | Gaps & Islands |
| 34 | 2720 | Popularity Percentage | Ranking |
| 35 | 2752 | Customers with Max Transactions on Consecutive Days | Gaps & Islands |
| 36 | 2793 | Status of Flight Tickets | Ranking |
| 37 | 2991 | Top Three Wineries | Pivoting |
| 38 | 2993 | Friday Purchases I | Date Functions |
| 39 | 2994 | Friday Purchases II | Date Functions |
| 40 | 2995 | Viewers Turned Streamers | Ranking |

---

*Generated for SQL interview preparation. Source: [Medium Article by chinhau](https://medium.com/@chinhaul/what-ive-learned-solving-all-44-sql-leetcode-hard-questions-in-7-days-b31cea776e05). Good luck!*
