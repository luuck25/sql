# SQL Interview Problems — Solutions Guide

> All solutions tested on **SQL Server** (Azure SQL Edge). Problems organized by pattern.

---

# Recursive CTE Problems

> **Core idea:** A recursive CTE has an anchor member (base case) + a recursive member that references itself with `UNION ALL`. Always needs a termination condition.

---

### 1. Generate Numbers 1 to N

**Approach:**
- Anchor: `SELECT 1 AS n`
- Recursive: `SELECT n + 1 FROM cte WHERE n < N`

**⚠️ Special Attention:**
- Basic recursive CTE template — memorize the shape
- Termination condition goes in `WHERE` of the recursive member

---

### 2. Employee Hierarchy

**Approach:**
- **Top-down:** Anchor = root manager → recursive step joins on `ManagerId`
- **Bottom-up:** Anchor = leaf employee → recursive step joins on `EmployeeId`

**⚠️ Special Attention:**
- Two directions of traversal — know both
- Add a `Level` column to track depth
- Self-referencing FK: `FOREIGN KEY (ManagerId) REFERENCES Employees(EmployeeId)`

---

### 3. Hopper Company Queries I — `LC #1635`

**Approach:**
- Recursive CTE generates months 1–12
- LEFT JOIN to get active drivers (joined ≤ end of month) and accepted rides per month

**⚠️ Special Attention:**
- Active drivers uses `OR` logic: joined in 2020 month M **OR** joined before 2020
- `ISNULL(..., 0)` for months with no rides
- Only one `WITH` keyword in SQL Server — CTEs are comma-separated
- CTE named `rides` conflicts with `Rides` table → rename it

---

### 4. Hopper Company Queries II — `LC #1645`

**Approach:**
- Same month-generation CTE
- `working_percentage = (working_drivers / active_drivers) × 100`

**⚠️ Special Attention:**
- **Integer division trap:** `100 / 3 = 33` not `33.33` → use `100.0`
- **Division by zero:** `ISNULL(x / NULLIF(divisor, 0), 0)`
- Use `COUNT(DISTINCT driver_id)` for working drivers

---

### 5. Hopper Company Queries III — `LC #1651`

**Approach (two solutions):**
- **① Window function:** `ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING`
- **② Self-join:** `mt2.month BETWEEN mt1.month AND mt1.month + 2`

**⚠️ Special Attention:**
- **`WHERE` filters BEFORE window functions but AFTER joins**
- Window approach: if you `WHERE month <= 10` directly, months 11–12 are gone before the window computes → month 10's window is wrong
  - **Fix:** wrap in subquery, filter AFTER window computes
- Self-join approach: `WHERE mt1.month <= 10` only limits output; mt2 still has all 12 months → correct
- **Rule:** `WHERE` removes data for windows. Joins preserve it.

---

### When to Use Recursive CTEs
- Generate a sequence of numbers, dates, or months
- Traverse hierarchical/tree data (org charts, BOM, categories)
- "For each month in a range" → recursive CTE to generate the range, then LEFT JOIN actual data

---

# Window Functions — Ranking

> **Core idea:** `DENSE_RANK()`, `RANK()`, `ROW_NUMBER()` over a partition to assign rankings, then filter by rank.

---

### 1. Department Top Three Salaries — `LC #185`

**Approach (two solutions):**
- **① Subquery:** rank in subquery, filter `rnk <= 3` in outer query
- **② CTE:** rank in CTE, join Department table in outer query

```
DENSE_RANK() OVER (PARTITION BY departmentId ORDER BY salary DESC)
```

**⚠️ Special Attention:**
- Must use **`DENSE_RANK`** for "top 3 unique salaries":
  ```
  Salaries:    90000  85000  85000  70000  69000
  ROW_NUMBER:    1      2      3      4      5    ← arbitrary tie-break, excludes valid ties
  RANK:          1      2      2      4      5    ← gaps after ties, 70000 gets rank 4 (excluded!)
  DENSE_RANK:    1      2      2      3      4    ← no gaps, 70000 gets rank 3 ✓
  ```

---

### 2. Market Analysis II — `LC #1159`

**Approach:**
- `ROW_NUMBER() OVER (PARTITION BY seller_id ORDER BY order_date ASC)` → find 2nd sold item
- LEFT JOIN Users to check if item brand = favorite brand

**⚠️ Special Attention:**
- **Column alias starts with number:** `2nd_item_fav_brand` → must use `[2nd_item_fav_brand]`
- **LEFT JOIN + WHERE trap:** `WHERE` removes NULLs (users with < 2 sales disappear)
  - Keep conditions in `ON` clause or use `CASE`/`ISNULL` to preserve NULLs
- Use `ROW_NUMBER` not `RANK` — we want exactly the 2nd item, not tied 2nd items

---

### When to Use Ranking Window Functions
- "Top N per group" → `DENSE_RANK` or `ROW_NUMBER` + filter
- "Nth item per group" → `ROW_NUMBER`, then `WHERE rn = N`
- Preserve ties → `DENSE_RANK`; exactly one per rank → `ROW_NUMBER`

---

# Window Functions — Analytics (LAG / LEAD / SUM OVER)

> **Core idea:** `LAG`/`LEAD` access previous/next rows. `SUM() OVER(...)` computes running/sliding aggregates. Watch out for gaps in data.

---

### 1. Cumulative Salary of an Employee — `LC #579`

**Approach (two solutions):**
- **① Self-join:**
  ```sql
  JOIN Employee e2 ON e1.id = e2.id
      AND e2.month BETWEEN e1.month - 2 AND e1.month
  -- then SUM(e2.salary), GROUP BY e1.id, e1.month
  ```
- **② LAG with month-gap check:**
  ```sql
  LAG(salary, 1, 0) ... AS prev_sal
  LAG(month,  1, 0) ... AS prev_month
  -- Only add prev_sal if prev_month = month - 1
  ```

**⚠️ Special Attention:**

- **Why `SUM(e2.salary)` not `e1.salary`?**
  - The self-join produces up to 3 e2 rows per e1 row (current, month−1, month−2)
  - `SUM(e2.salary)` aggregates those matches
  - e1 = anchor row, e2 = window data. `GROUP BY e1` collapses e2 matches

- **`ROWS BETWEEN 2 PRECEDING` fails**
  - Counts physical rows, not month values
  - With gaps (month 4 → 7), it grabs the wrong months

- **`RANGE BETWEEN 2 PRECEDING` — not supported in SQL Server**
  - Would conceptually work (matches by value)
  - SQL Server only allows `UNBOUNDED PRECEDING/FOLLOWING` with `RANGE`
  - PostgreSQL supports numeric RANGE offsets

- **`LAG` alone fails with month gaps**
  - `LAG(salary, 1)` grabs the previous *row*, not previous *month*
  - Must also `LAG(month)` and verify: `CASE WHEN prev_month = month - 1 THEN prev_sal ELSE 0 END`

- **Exclude most recent month:**
  - `WHERE month < (SELECT MAX(month) FROM Employee e WHERE e.id = ...)`

---

### When to Use Analytics Window Functions
- Running totals / cumulative sums → `SUM() OVER (ORDER BY ...)`
- Comparing current row to previous/next → `LAG()` / `LEAD()`
- Data has gaps → self-join by value range or LAG with value verification
- `ROWS` = physical row count, `RANGE` = value-based (limited in SQL Server)

---

# Other Problems (StrataScratch)

| # | Problem | Source | File |
|---|---------|--------|------|
| 1 | Find the Genre of the Person with Most Oscar Winnings | #10171 | `stratascratch/10171-...sql` |
| 2 | Top Actor Ratings by Genre | #10548 | `stratascratch/10548-...sql` |
| 3 | Five Year Sales Growth Regions | #10550 | `stratascratch/10550-...sql` |
| 4 | Recommendation System | #2081 | `stratascratch/2081-...sql` |
| 5 | Find the Number of Unique Properties | #9642 | `stratascratch/9642-...sql` |

---

# Quick Reference — Common Pitfalls

| Pitfall | Fix |
|---------|-----|
| Integer division | `100 / 3 = 33` → use `100.0 / 3` |
| Division by zero | `ISNULL(x / NULLIF(divisor, 0), 0)` |
| One `WITH` per query | CTEs comma-separated, not multiple `WITH` |
| CTE name = table name | CTE `rides` shadows `Rides` table → rename |
| Alias starts with number | `[2nd_item_fav_brand]` — use brackets |
| `WHERE` vs `ON` (LEFT JOIN) | `WHERE` removes NULLs. `ON` preserves them. |
| `WHERE` vs window functions | `WHERE` runs before `SELECT` → windows lose data. Wrap in subquery. |
| `RANGE` (SQL Server) | Only `UNBOUNDED` allowed. No numeric offsets like `RANGE BETWEEN 2 PRECEDING`. |
| `ROWS` vs `RANGE` | `ROWS` = row position. `RANGE` = value. Gaps break `ROWS`. |
| `LAG` with gaps | LAG = row position. Also LAG the ordering column and verify. |
| `DENSE_RANK` vs `RANK` | "Top N unique" → `DENSE_RANK`. `RANK` skips after ties. |
