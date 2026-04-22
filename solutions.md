# SQL Interview Problems — Solutions Guide

> All solutions tested on **SQL Server** (Azure SQL Edge). Problems organized by pattern.

---

## Table of Contents

**1. [Recursive CTE Problems](#1-recursive-cte-problems)**
   - 1.1 Generate Numbers 1 to N
   - 1.2 Employee Hierarchy
   - 1.3 Hopper Company Queries I — LC #1635
   - 1.4 Hopper Company Queries II — LC #1645
   - 1.5 Hopper Company Queries III — LC #1651

**2. [Window Functions — Ranking](#2-window-functions--ranking)**
   - 2.1 Department Top Three Salaries — LC #185
   - 2.2 Market Analysis II — LC #1159

**3. [Window Functions — Analytics (LAG / LEAD / SUM OVER)](#3-window-functions--analytics-lag--lead--sum-over)**
   - 3.1 Cumulative Salary of an Employee — LC #579

**4. [Other Problems (StrataScratch)](#4-other-problems-stratascratch)**

**5. [Quick Reference — Common Pitfalls](#5-quick-reference--common-pitfalls)**

**6. [Deep Dive — Key Learnings](#6-deep-dive--key-learnings)**
   - 6.1 WHERE Behavior — Self-Join vs Window Functions
   - 6.2 Filter in ON clause vs WHERE clause

---

# 1. Recursive CTE Problems

> **Core idea:** A recursive CTE has an anchor member (base case) + a recursive member that references itself with `UNION ALL`. Always needs a termination condition.

---

### 1.1 Generate Numbers 1 to N

**Approach:**
- Anchor: `SELECT 1 AS n`
- Recursive: `SELECT n + 1 FROM cte WHERE n < N`

**⚠️ Special Attention:**
- Basic recursive CTE template — memorize the shape
- Termination condition goes in `WHERE` of the recursive member

---

### 1.2 Employee Hierarchy

**Approach:**
- **Top-down:** Anchor = root manager → recursive step joins on `ManagerId`
- **Bottom-up:** Anchor = leaf employee → recursive step joins on `EmployeeId`

**⚠️ Special Attention:**
- Two directions of traversal — know both
- Add a `Level` column to track depth
- Self-referencing FK: `FOREIGN KEY (ManagerId) REFERENCES Employees(EmployeeId)`

---

### 1.3 Hopper Company Queries I — `LC #1635`

**Approach:**
- Recursive CTE generates months 1–12
- LEFT JOIN to get active drivers (joined ≤ end of month) and accepted rides per month

**⚠️ Special Attention:**
- Active drivers uses `OR` logic: joined in 2020 month M **OR** joined before 2020
- `ISNULL(..., 0)` for months with no rides
- Only one `WITH` keyword in SQL Server — CTEs are comma-separated
- CTE named `rides` conflicts with `Rides` table → rename it

---

### 1.4 Hopper Company Queries II — `LC #1645`

**Approach:**
- Same month-generation CTE
- `working_percentage = (working_drivers / active_drivers) × 100`

**⚠️ Special Attention:**
- **Integer division trap:** `100 / 3 = 33` not `33.33` → use `100.0`
- **Division by zero:** `ISNULL(x / NULLIF(divisor, 0), 0)`
- Use `COUNT(DISTINCT driver_id)` for working drivers

---

### 1.5 Hopper Company Queries III — `LC #1651`

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

# 2. Window Functions — Ranking

> **Core idea:** `DENSE_RANK()`, `RANK()`, `ROW_NUMBER()` over a partition to assign rankings, then filter by rank.

---

### 2.1 Department Top Three Salaries — `LC #185`

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

### 2.2 Market Analysis II — `LC #1159`

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

# 3. Window Functions — Analytics (LAG / LEAD / SUM OVER)

> **Core idea:** `LAG`/`LEAD` access previous/next rows. `SUM() OVER(...)` computes running/sliding aggregates. Watch out for gaps in data.

---

### 3.1 Cumulative Salary of an Employee — `LC #579`

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

# 4. Other Problems (StrataScratch)

| # | Problem | Source | File |
|---|---------|--------|------|
| 1 | Find the Genre of the Person with Most Oscar Winnings | #10171 | `stratascratch/10171-...sql` |
| 2 | Top Actor Ratings by Genre | #10548 | `stratascratch/10548-...sql` |
| 3 | Five Year Sales Growth Regions | #10550 | `stratascratch/10550-...sql` |
| 4 | Recommendation System | #2081 | `stratascratch/2081-...sql` |
| 5 | Find the Number of Unique Properties | #9642 | `stratascratch/9642-...sql` |

---

# 5. Quick Reference — Common Pitfalls

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

---

# 6. Deep Dive — Key Learnings

---

## 6.1 WHERE Behavior — Self-Join vs Window Functions

> SQL executes in this logical order:
> 1. `FROM / JOIN` ← pairs created here
> 2. `WHERE` ← filter rows
> 3. `GROUP BY`
> 4. `HAVING`
> 5. `SELECT` ← **window functions run here**
> 6. `ORDER BY`
>
> Key insight: **WHERE runs AFTER joins (step 1→2) but BEFORE window functions (step 2→5)**

**Sample data:** Monthly sales, compute rolling sum of **current + next 2 months**, output only months 1–3.

| month | sales |
|-------|-------|
| 1     | 10    |
| 2     | 20    |
| 3     | 30    |
| 4     | 40    |
| 5     | 50    |

**Expected output** (months 1–3 only):

| month | rolling_sum | Calculation |
|-------|-------------|-------------|
| 1     | 60          | 10 + 20 + 30 |
| 2     | 90          | 20 + 30 + 40 |
| 3     | 120         | 30 + 40 + 50 |

> ⚠️ Month 3 **needs** months 4 and 5 to compute its sum. This is where the approaches differ.

---

### Self-Join: WHERE is SAFE ✅

```sql
SELECT s1.month, SUM(s2.sales) AS rolling_sum
FROM Sales s1
JOIN Sales s2 ON s2.month BETWEEN s1.month AND s1.month + 2
WHERE s1.month <= 3
GROUP BY s1.month
```

**Step 1 (JOIN)** — all pairs created FIRST:

| s1.month | s1.sales | s2.month | s2.sales | Match condition |
|----------|----------|----------|----------|-----------------|
| 1 | 10 | 1 | 10 | 1 BETWEEN 1 AND 3 ✓ |
| 1 | 10 | 2 | 20 | 2 BETWEEN 1 AND 3 ✓ |
| 1 | 10 | 3 | 30 | 3 BETWEEN 1 AND 3 ✓ |
| 2 | 20 | 2 | 20 | 2 BETWEEN 2 AND 4 ✓ |
| 2 | 20 | 3 | 30 | 3 BETWEEN 2 AND 4 ✓ |
| 2 | 20 | 4 | 40 | 4 BETWEEN 2 AND 4 ✓ |
| 3 | 30 | 3 | 30 | 3 BETWEEN 3 AND 5 ✓ |
| 3 | 30 | 4 | 40 | 4 BETWEEN 3 AND 5 ✓ |
| 3 | 30 | 5 | 50 | 5 BETWEEN 3 AND 5 ✓ |
| 4 | 40 | 4 | 40 | 4 BETWEEN 4 AND 6 ✓ |
| 4 | 40 | 5 | 50 | 5 BETWEEN 4 AND 6 ✓ |
| 5 | 50 | 5 | 50 | 5 BETWEEN 5 AND 7 ✓ |

**Step 2 (WHERE s1.month <= 3)** — removes s1.month 4,5 rows only. s2 data already attached:

| s1.month | s2.month | s2.sales | Kept? |
|----------|----------|----------|-------|
| 1 | 1 | 10 | ✅ |
| 1 | 2 | 20 | ✅ |
| 1 | 3 | 30 | ✅ |
| 2 | 2 | 20 | ✅ |
| 2 | 3 | 30 | ✅ |
| 2 | 4 | 40 | ✅ **← month 4 still here via s2!** |
| 3 | 3 | 30 | ✅ |
| 3 | 4 | 40 | ✅ **← month 4 still here via s2!** |
| 3 | 5 | 50 | ✅ **← month 5 still here via s2!** |
| ~~4~~ | ~~4~~ | ~~40~~ | ❌ filtered |
| ~~4~~ | ~~5~~ | ~~50~~ | ❌ filtered |
| ~~5~~ | ~~5~~ | ~~50~~ | ❌ filtered |

**Step 3 (GROUP BY + SUM):**

| s1.month | SUM(s2.sales) | Calculation |
|----------|---------------|-------------|
| 1 | **60** | 10 + 20 + 30 ✅ |
| 2 | **90** | 20 + 30 + 40 ✅ |
| 3 | **120** | 30 + 40 + 50 ✅ |

→ `WHERE` only filters **s1** (output) rows. **s2** (joined data) is preserved — month 3 still sees months 4 and 5.

---

### Window Function: WHERE DESTROYS data ❌

```sql
-- ❌ WRONG
SELECT month,
    SUM(sales) OVER (ORDER BY month ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)
FROM Sales
WHERE month <= 3               -- step 2: months 4, 5 are GONE
```

**Step 2 (WHERE month <= 3)** — months 4, 5 removed from entire dataset:

| month | sales | Survives WHERE? |
|-------|-------|-----------------|
| 1 | 10 | ✅ |
| 2 | 20 | ✅ |
| 3 | 30 | ✅ |
| ~~4~~ | ~~40~~ | ❌ removed |
| ~~5~~ | ~~50~~ | ❌ removed |

**Step 5 (SELECT — window runs)** on the 3 surviving rows:

| month | Window sees | SUM | Expected | Correct? |
|-------|-------------|-----|----------|----------|
| 1 | 1, 2, 3 | **60** | 60 | ✅ |
| 2 | 2, 3 | **50** | 90 | ❌ **missing month 4!** |
| 3 | 3 | **30** | 120 | ❌ **missing months 4, 5!** |

→ WHERE removed months 4, 5 **before** the window could use them.

---

### ✅ FIX: Compute window FIRST in CTE, filter AFTER

```sql
WITH computed AS (
    SELECT month,
        SUM(sales) OVER (ORDER BY month
            ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS rolling_sum
    FROM Sales                 -- no WHERE → all 5 months available for window
)
SELECT * FROM computed
WHERE month <= 3               -- filter AFTER window computed ✓
```

| month | rolling_sum | Correct? |
|-------|-------------|----------|
| 1 | **60** | ✅ |
| 2 | **90** | ✅ |
| 3 | **120** | ✅ |

---

### Summary

| Approach | WHERE timing | Safe to filter directly? |
|----------|-------------|--------------------------|
| **Self-join** | After JOIN (step 1→2) | ✅ Yes — pairs already created. s2 data preserved. |
| **Window function** | Before SELECT (step 2→5) | ❌ No — rows removed before window runs. Wrap in CTE first. |

---

## 6.2 Filter in ON clause vs WHERE clause

Using the same data — but now with a **LEFT JOIN** to a Regions table. We want all months, but only show region info for months ≤ 3.

**Sample data:**

| month | sales |  | month | region |
|-------|-------|--|-------|--------|
| 1 | 10 |  | 1 | East |
| 2 | 20 |  | 2 | West |
| 3 | 30 |  | 3 | East |
| 4 | 40 |  | 4 | North |
| 5 | 50 |  | 5 | South |

---

**Option A: Filter in WHERE** — removes rows ❌

```sql
SELECT s.month, s.sales, r.region
FROM Sales s
LEFT JOIN Regions r ON s.month = r.month
WHERE s.month <= 3
```

**Step 1 (LEFT JOIN)** — all 5 sales rows joined with region:

| s.month | s.sales | r.region |
|---------|---------|----------|
| 1 | 10 | East |
| 2 | 20 | West |
| 3 | 30 | East |
| 4 | 40 | North |
| 5 | 50 | South |

**Step 2 (WHERE month <= 3)** — removes months 4, 5 entirely:

| s.month | s.sales | r.region | Kept? |
|---------|---------|----------|-------|
| 1 | 10 | East | ✅ |
| 2 | 20 | West | ✅ |
| 3 | 30 | East | ✅ |
| ~~4~~ | ~~40~~ | ~~North~~ | ❌ gone |
| ~~5~~ | ~~50~~ | ~~South~~ | ❌ gone |

→ Months 4, 5 are **completely removed** from results. Only 3 rows returned.

---

**Option B: Filter in ON clause** — preserves rows ✅

```sql
SELECT s.month, s.sales, r.region
FROM Sales s
LEFT JOIN Regions r ON s.month = r.month AND s.month <= 3
```

**Step 1 (LEFT JOIN with ON condition)** — join only matches months ≤ 3, but LEFT JOIN keeps all s rows:

| s.month | s.sales | r.region | Why? |
|---------|---------|----------|------|
| 1 | 10 | East | ON matched ✓ |
| 2 | 20 | West | ON matched ✓ |
| 3 | 30 | East | ON matched ✓ |
| 4 | 40 | **NULL** | ON failed (4 > 3), LEFT JOIN keeps row with NULL |
| 5 | 50 | **NULL** | ON failed (5 > 3), LEFT JOIN keeps row with NULL |

→ All 5 rows returned. Months 4, 5 have NULL region but **are not removed**.

**💡 Mental model:** `ON` condition ≈ runtime CTE on the right table. This gives the same result:

```sql
-- Option B equivalent using CTE on the right table:
WITH filtered_regions AS (
    SELECT * FROM Regions WHERE month <= 3
)
SELECT s.month, s.sales, r.region
FROM Sales s
LEFT JOIN filtered_regions r ON s.month = r.month
```

| s.month | s.sales | r.region | Why? |
|---------|---------|----------|------|
| 1 | 10 | East | matched in filtered_regions ✓ |
| 2 | 20 | West | matched ✓ |
| 3 | 30 | East | matched ✓ |
| 4 | 40 | **NULL** | no month 4 in filtered_regions, LEFT JOIN → NULL |
| 5 | 50 | **NULL** | no month 5 in filtered_regions, LEFT JOIN → NULL |

→ **Same result as ON clause!** The CTE physically removes months 4,5 from the right table before joining. The ON clause achieves the same by refusing to match those pairs. Either way, LEFT JOIN preserves all left rows with NULLs.

> **Key takeaway:** Think of extra conditions in `ON` as narrowing what the right table offers to match against — like joining against a filtered version of it. Left rows without a match survive with NULLs (LEFT JOIN). `WHERE` is different — it runs after the join and kills rows entirely.

---

### When does this matter?

The ON vs WHERE difference is critical with **LEFT JOIN**:

```sql
-- ❌ Market Analysis II trap:
FROM Users u
LEFT JOIN ranked_data r ON r.seller_id = u.user_id
WHERE r.item_brand = u.favorite_brand    -- NULLs filtered out! Users with < 2 sales vanish

-- ✅ Fix: move condition to ON
FROM Users u
LEFT JOIN ranked_data r ON r.seller_id = u.user_id
    AND r.item_brand = u.favorite_brand  -- non-matches get NULL, user row preserved
```

> **With INNER JOIN, WHERE and ON behave identically** — both filter. The difference only matters with LEFT/RIGHT/FULL joins.

---

### Complete Summary

| Filter location | Timing | Effect on LEFT JOIN |
|-----------------|--------|---------------------|
| **`ON` clause** | During JOIN (step 1) | Non-matching rows get NULLs but **stay** |
| **`WHERE` clause** | After JOIN (step 2) | Non-matching rows are **removed** |
| **`WHERE` + window fn** | Before SELECT (step 2→5) | Rows gone before window computes — **wrap in CTE** |
