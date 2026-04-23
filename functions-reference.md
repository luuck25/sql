# SQL Server Functions Reference — Beyond Date Functions

> Common SQL Server functions for interviews. Covers NULL handling, strings, aggregates, math, type conversion, and more.
> Date functions are in [date-functions.md](date-functions.md).

---

## Table of Contents

1. [NULL Handling](#1-null-handling)
2. [Conditional Logic](#2-conditional-logic)
3. [String Functions](#3-string-functions)
4. [Aggregate Functions](#4-aggregate-functions)
5. [Window Functions](#5-window-functions)
6. [Math Functions](#6-math-functions)
7. [Type Conversion](#7-type-conversion)
8. [Logical / Existence Operators](#8-logical--existence-operators)
9. [Set Operators](#9-set-operators)
10. [ANSI vs SQL Server Reference](#10-ansi-vs-sql-server-reference)

---

## 1. NULL Handling

### ISNULL — 2-argument NULL replacement *(SQL Server only)*

```sql
ISNULL(expression, replacement)

ISNULL(NULL, 0)        -- 0
ISNULL(NULL, 'N/A')    -- 'N/A'
ISNULL(5, 0)           -- 5 (not NULL, returns as-is)
```

### COALESCE — First non-NULL value *(ANSI SQL)*

```sql
COALESCE(a, b, c, ...)    -- returns first non-NULL

COALESCE(NULL, NULL, 5)          -- 5
COALESCE(NULL, 'hello', 'world') -- 'hello'
```

### ISNULL vs COALESCE

| | `ISNULL(a, b)` | `COALESCE(a, b, c...)` |
|--|----------------|------------------------|
| Standard | SQL Server only | **ANSI SQL** |
| Arguments | Exactly **2** | **2 or more** |
| Return type | Type of **first** argument | Highest precedence type |

> ⚠️ **Type pitfall:**
> ```sql
> DECLARE @x VARCHAR(3) = NULL;
> ISNULL(@x, 'hello world')    -- 'hel'  (truncated! forced to VARCHAR(3))
> COALESCE(@x, 'hello world')  -- 'hello world' (picks wider type)
> ```

> **Rule:** Use `COALESCE` by default (ANSI, multi-arg, safer types). `ISNULL` for simple 2-arg cases.

### NULLIF — Return NULL if equal *(ANSI SQL)*

```sql
NULLIF(a, b)    -- if a = b → NULL, else a

NULLIF(0, 0)     -- NULL
NULLIF(5, 0)     -- 5
NULLIF('a', 'b') -- 'a'
```

**Main use case — prevent divide-by-zero:**
```sql
-- Without NULLIF:
100 / 0                              -- ERROR!

-- With NULLIF:
100 / NULLIF(divisor, 0)             -- returns NULL instead of error

-- With ISNULL + NULLIF combo (default to 0):
ISNULL(100 / NULLIF(divisor, 0), 0)  -- returns 0 if divisor is 0
```

---

## 2. Conditional Logic

### CASE WHEN *(ANSI SQL)*

```sql
-- Simple CASE:
CASE status
    WHEN 'A' THEN 'Active'
    WHEN 'I' THEN 'Inactive'
    ELSE 'Unknown'
END

-- Searched CASE (more flexible):
CASE
    WHEN score >= 90 THEN 'A'
    WHEN score >= 80 THEN 'B'
    WHEN score >= 70 THEN 'C'
    ELSE 'F'
END
```

**Common patterns:**
```sql
-- Pivoting (conditional aggregation):
SUM(CASE WHEN category = 'A' THEN amount ELSE 0 END) AS category_a

-- Flag creation:
CASE WHEN start_day > prev_max_end THEN 1 ELSE 0 END AS new_group

-- NULL-safe comparison:
CASE WHEN col IS NULL THEN 'missing' ELSE col END
```

### IIF *(SQL Server only)*

```sql
IIF(condition, true_value, false_value)

-- Equivalent to:
CASE WHEN condition THEN true_value ELSE false_value END
```

- Only supports **2 branches** (true/false). Use `CASE` for multiple conditions.
- Not ANSI — not available in PostgreSQL/MySQL.

---

## 3. String Functions

### Length & Measurement

| Function | Purpose | Example | Standard |
|----------|---------|---------|----------|
| `LEN(s)` | Length (no trailing spaces) | `LEN('hello ')` → `5` | SQL Server |
| `DATALENGTH(s)` | Byte length (includes trailing) | `DATALENGTH('hello ')` → `6` | SQL Server |

### Extraction

| Function | Purpose | Example | Standard |
|----------|---------|---------|----------|
| `LEFT(s, n)` | First n chars | `LEFT('hello', 3)` → `'hel'` | SQL Server |
| `RIGHT(s, n)` | Last n chars | `RIGHT('hello', 3)` → `'llo'` | SQL Server |
| `SUBSTRING(s, start, len)` | Extract portion (1-based) | `SUBSTRING('hello', 2, 3)` → `'ell'` | **ANSI** |

### Search & Position

| Function | Purpose | Example | Standard |
|----------|---------|---------|----------|
| `CHARINDEX(find, s)` | Find position (1-based, 0=not found) | `CHARINDEX('ll', 'hello')` → `3` | SQL Server |
| `PATINDEX(pattern, s)` | Find pattern position (wildcards) | `PATINDEX('%[0-9]%', 'abc3')` → `4` | SQL Server |

### Modification

| Function | Purpose | Example | Standard |
|----------|---------|---------|----------|
| `REPLACE(s, old, new)` | Replace all occurrences | `REPLACE('hello', 'l', 'r')` → `'herro'` | **ANSI** |
| `STUFF(s, start, len, new)` | Delete + insert at position | `STUFF('hello', 2, 3, 'XY')` → `'hXYo'` | SQL Server |
| `TRIM(s)` | Remove leading/trailing whitespace | `TRIM('  hi  ')` → `'hi'` | **ANSI** |
| `LTRIM(s)` / `RTRIM(s)` | Left/right trim only | `LTRIM('  hi')` → `'hi'` | SQL Server |
| `UPPER(s)` / `LOWER(s)` | Change case | `UPPER('hello')` → `'HELLO'` | **ANSI** |
| `REVERSE(s)` | Reverse string | `REVERSE('hello')` → `'olleh'` | SQL Server |
| `REPLICATE(s, n)` | Repeat string n times | `REPLICATE('ab', 3)` → `'ababab'` | SQL Server |

### Concatenation

| Function | Purpose | Example | Standard |
|----------|---------|---------|----------|
| `CONCAT(a, b, c...)` | Concatenate (**NULL-safe**: NULLs become '') | `CONCAT('a', NULL, 'b')` → `'ab'` | **ANSI** |
| `CONCAT_WS(sep, a, b...)` | Concat with separator (skips NULLs) | `CONCAT_WS('-', 'a', NULL, 'c')` → `'a-c'` | **ANSI** |
| `+` operator | Concatenate (**NULL-unsafe**: NULL + anything = NULL) | `'a' + NULL` → `NULL` | SQL Server |

> ⚠️ Always use `CONCAT` over `+` — the `+` operator propagates NULLs.

### Aggregation into String

```sql
-- STRING_AGG: Combine values into CSV (SQL Server 2017+)
STRING_AGG(name, ', ')                          -- 'Alice, Bob, Charlie'
STRING_AGG(name, ', ') WITHIN GROUP (ORDER BY name)  -- sorted
```

---

## 4. Aggregate Functions

| Function | Purpose | NULL behavior | Standard |
|----------|---------|--------------|----------|
| `COUNT(*)` | Count all rows | **Includes** NULLs | **ANSI** |
| `COUNT(col)` | Count non-NULL values | **Skips** NULLs | **ANSI** |
| `COUNT(DISTINCT col)` | Count unique non-NULL | **Skips** NULLs | **ANSI** |
| `SUM(col)` | Total | Ignores NULLs | **ANSI** |
| `AVG(col)` | Average | **Ignores NULLs** (not zero!) | **ANSI** |
| `MIN(col)` / `MAX(col)` | Min/Max value | Ignores NULLs | **ANSI** |
| `STRING_AGG(col, sep)` | Concat into string | Skips NULLs | SQL Server 2017+ |

### ⚠️ Key Interview Points

```sql
-- COUNT(*) vs COUNT(col):
-- Table: [1, 2, NULL, 4]
COUNT(*)      -- 4 (counts all rows including NULL)
COUNT(col)    -- 3 (skips NULL)

-- AVG ignores NULLs (doesn't treat as 0):
-- Table: [10, 20, NULL]
AVG(col)      -- 15 (sum=30, count=2), NOT 10 (sum=30, count=3)

-- SUM of empty set = NULL, not 0:
ISNULL(SUM(col), 0)  -- safe default
```

---

## 5. Window Functions

### Ranking

| Function | Purpose | Ties behavior | Standard |
|----------|---------|--------------|----------|
| `ROW_NUMBER()` | Unique sequential number | No ties (arbitrary order) | **ANSI** |
| `RANK()` | Rank with gaps | 1, 2, 2, **4** (skips 3) | **ANSI** |
| `DENSE_RANK()` | Rank without gaps | 1, 2, 2, **3** (no skip) | **ANSI** |
| `NTILE(n)` | Divide into n equal buckets | Distributes evenly | **ANSI** |

### Navigation

| Function | Purpose | Standard |
|----------|---------|----------|
| `LAG(col, n, default)` | Value from n rows **before** | **ANSI** |
| `LEAD(col, n, default)` | Value from n rows **after** | **ANSI** |
| `FIRST_VALUE(col)` | First value in window | **ANSI** |
| `LAST_VALUE(col)` | Last value in window | **ANSI** |

> ⚠️ `LAST_VALUE` needs explicit frame: `ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING`. Default frame only goes to current row.

### Windowed Aggregates

```sql
SUM(col) OVER (ORDER BY ...)          -- running sum
SUM(col) OVER (PARTITION BY ...)      -- total per group
COUNT(*) OVER ()                       -- total count (all rows)
MAX(col) OVER (... ROWS BETWEEN ...)  -- running max
```

---

## 6. Math Functions

| Function | Purpose | Example | Standard |
|----------|---------|---------|----------|
| `ROUND(n, decimals)` | Round to decimals | `ROUND(3.456, 2)` → `3.46` | **ANSI** |
| `CEILING(n)` | Round up to integer | `CEILING(3.1)` → `4` | **ANSI** |
| `FLOOR(n)` | Round down to integer | `FLOOR(3.9)` → `3` | **ANSI** |
| `ABS(n)` | Absolute value | `ABS(-5)` → `5` | **ANSI** |
| `POWER(base, exp)` | Exponentiation | `POWER(2, 3)` → `8` | **ANSI** |
| `SQRT(n)` | Square root | `SQRT(16)` → `4` | **ANSI** |
| `SIGN(n)` | Sign (-1, 0, 1) | `SIGN(-5)` → `-1` | **ANSI** |
| `%` (modulo) | Remainder | `7 % 3` → `1` | SQL Server (`MOD` in ANSI) |

---

## 7. Type Conversion

| Function | Purpose | Standard |
|----------|---------|----------|
| `CAST(expr AS type)` | Convert type (no formatting) | **ANSI** |
| `CONVERT(type, expr [, style])` | Convert with style codes | SQL Server |
| `TRY_CAST(expr AS type)` | Returns **NULL** on failure (no error) | SQL Server |
| `TRY_CONVERT(type, expr)` | Returns **NULL** on failure (no error) | SQL Server |

```sql
-- CAST vs TRY_CAST:
CAST('abc' AS INT)       -- ERROR!
TRY_CAST('abc' AS INT)   -- NULL (safe)

-- Common interview pattern — force decimal division:
CAST(1 AS DECIMAL(10,2)) / 2       -- 0.50
1.0 * 1 / 2                         -- 0.50 (alternative)
```

> **Rule:** Use `CAST` by default. Use `TRY_CAST` when input might be invalid. Use `CONVERT` only for style codes.

---

## 8. Logical / Existence Operators

| Operator | Purpose | Example | Standard |
|----------|---------|---------|----------|
| `EXISTS (subquery)` | True if any rows returned | `WHERE EXISTS (SELECT 1 FROM ...)` | **ANSI** |
| `NOT EXISTS` | True if no rows returned | Anti-join pattern | **ANSI** |
| `IN (list)` | Match any value in list | `WHERE id IN (1, 2, 3)` | **ANSI** |
| `IN (subquery)` | Match any value from subquery | `WHERE id IN (SELECT ...)` | **ANSI** |
| `BETWEEN a AND b` | Range check (inclusive both ends) | `WHERE date BETWEEN '2026-01-01' AND '2026-12-31'` | **ANSI** |
| `LIKE` | Pattern matching | `WHERE name LIKE 'A%'` | **ANSI** |
| `IS NULL` / `IS NOT NULL` | NULL check | `WHERE col IS NULL` | **ANSI** |

### EXISTS vs IN

```sql
-- EXISTS — better for correlated subqueries, handles NULLs safely:
WHERE EXISTS (SELECT 1 FROM Orders o WHERE o.customer_id = c.id)

-- IN — simpler for static lists, ⚠️ breaks with NULLs in subquery:
WHERE id IN (1, 2, 3)
WHERE id NOT IN (SELECT ...)  -- ⚠️ returns empty if subquery has NULL!
```

> **Rule:** Prefer `EXISTS` / `NOT EXISTS` over `IN` / `NOT IN` for subqueries. `NOT IN` silently returns no rows if the subquery contains any NULL.

---

## 9. Set Operators

| Operator | Purpose | Duplicates | Standard |
|----------|---------|-----------|----------|
| `UNION` | Combine results, **remove** duplicates | Deduped | **ANSI** |
| `UNION ALL` | Combine results, **keep** all | All rows | **ANSI** |
| `INTERSECT` | Rows in **both** queries | Deduped | **ANSI** |
| `EXCEPT` | Rows in **first** but not second | Deduped | **ANSI** |

```sql
-- UNION vs UNION ALL:
SELECT 1 UNION SELECT 1        -- returns: 1 (one row)
SELECT 1 UNION ALL SELECT 1    -- returns: 1, 1 (two rows)
```

> **Rule:** Use `UNION ALL` unless you specifically need deduplication. It's faster (no sort/distinct step).

### APPLY *(SQL Server only)*

```sql
-- CROSS APPLY = INNER JOIN + table-valued function (drops non-matches)
-- OUTER APPLY = LEFT JOIN + table-valued function (keeps non-matches as NULL)

SELECT c.name, o.latest_order
FROM Customers c
CROSS APPLY (
    SELECT TOP 1 order_date AS latest_order
    FROM Orders o WHERE o.customer_id = c.id
    ORDER BY order_date DESC
) o
```

> PostgreSQL equivalent: `LATERAL JOIN`

---

## 10. ANSI vs SQL Server Reference

### SQL Server Only → ANSI Equivalent

| SQL Server | ANSI / Portable | Notes |
|-----------|----------------|-------|
| `ISNULL(a, b)` | `COALESCE(a, b)` | COALESCE is multi-arg, safer types |
| `IIF(cond, t, f)` | `CASE WHEN ... THEN ... ELSE ... END` | IIF = 2-branch only |
| `LEN(s)` | `LENGTH(s)` / `CHAR_LENGTH(s)` | PostgreSQL/MySQL |
| `CHARINDEX(find, s)` | `POSITION(find IN s)` | ANSI syntax |
| `STUFF(s, pos, len, new)` | `INSERT(s, pos, len, new)` | MySQL |
| `GETDATE()` | `CURRENT_TIMESTAMP` | Identical result |
| `TOP N` | `FETCH FIRST N ROWS ONLY` | ANSI SQL:2008 |
| `STRING_AGG(col, sep)` | `STRING_AGG(col, sep)` (PG) / `GROUP_CONCAT(col)` (MySQL) | Same name in PostgreSQL |
| `CONVERT(type, expr, style)` | `CAST(expr AS type)` | CAST has no style codes |
| `TRY_CAST` / `TRY_CONVERT` | No direct equivalent | Handle errors differently per DB |
| `CROSS APPLY` | `LATERAL JOIN` | PostgreSQL |
| `PATINDEX()` | Regex (`~`, `REGEXP`) | PostgreSQL/MySQL |
| `DATALENGTH()` | `OCTET_LENGTH()` | ANSI |
| `%` (modulo) | `MOD(a, b)` | ANSI |

### ANSI Functions (Work Everywhere)

`COALESCE` · `NULLIF` · `CASE WHEN` · `CAST` · `SUBSTRING` · `TRIM` · `UPPER` · `LOWER` · `CONCAT` · `REPLACE` · `ROW_NUMBER` · `RANK` · `DENSE_RANK` · `LAG` · `LEAD` · `FIRST_VALUE` · `LAST_VALUE` · `COUNT` · `SUM` · `AVG` · `MIN` · `MAX` · `ROUND` · `ABS` · `CEILING` · `FLOOR` · `POWER` · `SQRT` · `EXISTS` · `IN` · `BETWEEN` · `LIKE` · `IS NULL` · `UNION` · `INTERSECT` · `EXCEPT`
