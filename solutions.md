# SQL Interview Problems — Solutions Guide

> All solutions tested on **SQL Server** (Azure SQL Edge). Problems organized by pattern.

---

# Recursive CTE Problems

> **Core idea:** A recursive CTE has an anchor member (base case) + a recursive member that references itself with `UNION ALL`. Always needs a termination condition.

## Problems

| # | Problem | LC # | Approach | ⚠️ Special Attention |
|---|---------|------|----------|----------------------|
| 1 | **Generate Numbers 1 to N** | — | Anchor: `SELECT 1`. Recursive: `SELECT n+1 WHERE n < N`. | Basic recursive CTE template. Termination condition in `WHERE`. |
| 2 | **Employee Hierarchy** | — | **Top-down:** Anchor = root manager, recursive step joins on `ManagerId`. **Bottom-up:** Anchor = leaf employee, recursive step joins on `EmployeeId`. | Two directions of traversal. Add a `Level` column to track depth. Self-referencing FK: `FOREIGN KEY (ManagerId) REFERENCES Employees(EmployeeId)`. |
| 3 | **Hopper Company Queries I** | #1635 | Recursive CTE generates months 1–12. LEFT JOIN to get active drivers (joined ≤ end of month) and accepted rides per month. | Active drivers uses `OR` logic: joined in 2020 month M **OR** joined before 2020. Use `ISNULL(..., 0)` for months with no rides. Only one `WITH` keyword in SQL Server — CTEs are comma-separated. CTE named `rides` conflicts with `Rides` table → rename. |
| 4 | **Hopper Company Queries II** | #1645 | Same month-generation CTE. Compute `working_percentage = (working_drivers / active_drivers) × 100`. | **Integer division trap:** `100 / 3 = 33` not `33.33` → use `100.0` for decimal math. **Division by zero:** Use `ISNULL(... / NULLIF(divisor, 0), 0)` pattern. Use `COUNT(DISTINCT driver_id)` for working drivers. |
| 5 | **Hopper Company Queries III** | #1651 | Compute monthly distance/duration for all 12 months, then 3-month rolling average (months 1–3, 2–4, ..., 10–12). Two approaches: **① Window function** with `ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING`. **② Self-join** with `mt2.month BETWEEN mt1.month AND mt1.month + 2`. | **WHERE filters BEFORE window functions but AFTER joins.** Window approach: must wrap in subquery — if you `WHERE month <= 10` directly, months 11–12 are removed before the window computes, so month 10's window is wrong. Self-join approach: `WHERE mt1.month <= 10` only limits output rows; mt2 still has all 12 months in the joined data → correct. |

## When to Use Recursive CTEs
- Generate a sequence of numbers, dates, or months
- Traverse hierarchical/tree data (org charts, BOM, categories)
- Problems requiring "for each month in a range" → recursive CTE to generate the range, then LEFT JOIN actual data

---

# Window Functions — Ranking

> **Core idea:** `DENSE_RANK()`, `RANK()`, `ROW_NUMBER()` over a partition to assign rankings, then filter by rank.

## Problems

| # | Problem | LC # | Approach | ⚠️ Special Attention |
|---|---------|------|----------|----------------------|
| 1 | **Department Top Three Salaries** | #185 | `DENSE_RANK() OVER (PARTITION BY departmentId ORDER BY salary DESC)` then filter `rnk <= 3`. Two approaches: **① Subquery** — rank in subquery, filter in outer. **② CTE** — rank in CTE, join department in outer query. | **DENSE_RANK vs RANK vs ROW_NUMBER:** For "top 3 unique salaries" you MUST use `DENSE_RANK`. `RANK` leaves gaps after ties (70000 gets rank 4, excluded). `ROW_NUMBER` gives unique numbers (arbitrary tie-break, excludes valid ties). |
| 2 | **Market Analysis II** | #1159 | `ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY order_date ASC)` to find each seller's 2nd sold item. LEFT JOIN Users to check if item brand = favorite brand. | **Column alias starting with number:** `2nd_item_fav_brand` → must use brackets `[2nd_item_fav_brand]` in SQL Server. **LEFT JOIN + WHERE trap:** `WHERE` filters after join and removes NULLs (users with < 2 sales). Keep conditions in `ON` clause or use `CASE`/`ISNULL` to preserve NULLs. Use `ROW_NUMBER` (not RANK) — we want exactly the 2nd item, not tied 2nd items. |

## When to Use Ranking Window Functions
- "Top N per group" problems → `DENSE_RANK` or `ROW_NUMBER` + filter
- "Nth item per group" → `ROW_NUMBER` partitioned and ordered, then `WHERE rn = N`
- Need to preserve ties → `DENSE_RANK`; need exactly one per rank → `ROW_NUMBER`

---

# Window Functions — Analytics (LAG / LEAD / SUM OVER)

> **Core idea:** `LAG`/`LEAD` access previous/next rows. `SUM() OVER(...)` computes running/sliding aggregates. Watch out for gaps in data.

## Problems

| # | Problem | LC # | Approach | ⚠️ Special Attention |
|---|---------|------|----------|----------------------|
| 1 | **Cumulative Salary of an Employee** | #579 | 3-month cumulative salary excluding most recent month. **① Self-join:** `e2.month BETWEEN e1.month - 2 AND e1.month` then `SUM(e2.salary)`. **② LAG with month-gap check:** LAG salary AND month columns; only add previous salary if `prev_month = month - 1`. | **Why SUM(e2.salary) not e1?** The self-join produces up to 3 e2 rows per e1 row (current, month-1, month-2). `SUM(e2.salary)` aggregates those matches. e1 is the anchor; e2 provides the window data. GROUP BY e1 collapses e2 matches. |
|   |  |  |  | **ROWS BETWEEN 2 PRECEDING fails** because it counts physical rows, not month values. With gaps (month 4 → 7), it grabs wrong months. |
|   |  |  |  | **RANGE BETWEEN 2 PRECEDING** would conceptually work (matches by value), but **SQL Server does NOT support numeric offsets with RANGE** — only `UNBOUNDED PRECEDING/FOLLOWING`. PostgreSQL supports it. |
|   |  |  |  | **LAG alone fails** with month gaps — `LAG(salary, 1)` grabs the previous *row*, not previous *month*. Must also `LAG(month)` and verify with `CASE WHEN prev_month = month - 1`. |
|   |  |  |  | **Exclude most recent month:** subquery `WHERE month < (SELECT MAX(month) ... WHERE id = ...)`. |

## When to Use Analytics Window Functions
- Running totals / cumulative sums → `SUM() OVER (ORDER BY ...)`
- Comparing current row to previous/next → `LAG()` / `LEAD()`
- Data has gaps → self-join by value range or LAG with value verification
- `ROWS` = physical row count, `RANGE` = value-based (limited in SQL Server)

---

# Other Problems (StrataScratch)

| # | Problem | Source | File |
|---|---------|--------|------|
| 1 | Find the Genre of the Person with Most Oscar Winnings | StrataScratch #10171 | `stratascratch/10171-...sql` |
| 2 | Top Actor Ratings by Genre | StrataScratch #10548 | `stratascratch/10548-...sql` |
| 3 | Five Year Sales Growth Regions | StrataScratch #10550 | `stratascratch/10550-...sql` |
| 4 | Recommendation System | StrataScratch #2081 | `stratascratch/2081-...sql` |
| 5 | Find the Number of Unique Properties | StrataScratch #9642 | `stratascratch/9642-...sql` |

---

# Quick Reference — Common Pitfalls

| Pitfall | Details |
|---------|---------|
| **Integer division** | `100 / 3 = 33` → use `100.0 / 3` for decimal results |
| **Division by zero** | `ISNULL(x / NULLIF(divisor, 0), 0)` |
| **One WITH per query** | SQL Server: only one `WITH`, separate CTEs with commas |
| **CTE name = table name** | CTE named `rides` shadows `Rides` table → rename |
| **Column alias starts with number** | `2nd_item_fav_brand` → `[2nd_item_fav_brand]` |
| **WHERE vs ON in LEFT JOIN** | `WHERE` filters after join (removes NULLs). `ON` filters during join (preserves NULLs). |
| **WHERE vs window functions** | `WHERE` runs before `SELECT` → window functions lose filtered rows. Wrap in subquery to filter after. |
| **RANGE not supported (SQL Server)** | Only `UNBOUNDED PRECEDING/FOLLOWING` with `RANGE`. Numeric offsets like `RANGE BETWEEN 2 PRECEDING` → not supported (use PostgreSQL). |
| **ROWS vs RANGE** | `ROWS` = physical row position. `RANGE` = value-based. With data gaps, `ROWS BETWEEN 2 PRECEDING` grabs wrong rows. |
| **LAG with gaps** | `LAG(salary, 1)` looks at row position, not value. Must also LAG the ordering column and verify consecutiveness. |
| **DENSE_RANK vs RANK** | "Top N unique" → `DENSE_RANK` (no gaps). `RANK` skips ranks after ties. |
