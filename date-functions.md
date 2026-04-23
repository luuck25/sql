# SQL Server Date Functions — Quick Reference

> Comprehensive guide to date/time functions in SQL Server. Covers extraction, arithmetic, truncation, and common patterns.

---

## Table of Contents

1. [Getting Current Date/Time](#1-getting-current-datetime)
2. [Extracting Date Parts — DATEPART vs DATENAME](#2-extracting-date-parts--datepart-vs-datename)
3. [Shorthand Functions — DAY, MONTH, YEAR](#3-shorthand-functions--day-month-year)
4. [Date Arithmetic — DATEADD & DATEDIFF](#4-date-arithmetic--dateadd--datediff)
5. [Date Truncation — DATETRUNC](#5-date-truncation--datetrunc)
6. [Custom Buckets — DATE_BUCKET](#6-custom-buckets--date_bucket)
7. [Type Conversion — CAST vs CONVERT](#7-type-conversion--cast-vs-convert)
8. [Formatting — FORMAT](#8-formatting--format)
9. [Date Construction & Validation](#9-date-construction--validation)
10. [DATEPART Units Reference](#10-datepart-units-reference)
11. [Interview Tips](#11-interview-tips)

---

## 1. Getting Current Date/Time

| Function | Returns | Type | Standard |
|----------|---------|------|----------|
| `GETDATE()` | `2026-04-23 14:30:45.123` | datetime | SQL Server only |
| `CURRENT_TIMESTAMP` | `2026-04-23 14:30:45.123` | datetime | **ANSI SQL** |
| `SYSDATETIME()` | `2026-04-23 14:30:45.1234567` | datetime2 (more precise) | SQL Server only |
| `GETUTCDATE()` | `2026-04-23 18:30:45.123` | datetime (UTC) | SQL Server only |

### GETDATE() vs CURRENT_TIMESTAMP

**Functionally identical.** Both return current datetime. Only difference:

- `CURRENT_TIMESTAMP` — **ANSI SQL standard**, portable across databases, no parentheses
- `GETDATE()` — **SQL Server specific**, needs `()`

Use `CURRENT_TIMESTAMP` for portability. `GETDATE()` if SQL Server only.

### Getting Date Only (no time)

```sql
-- SQL Server 2022+:
SELECT CURRENT_DATE                    -- ANSI, but only SQL Server 2025+

-- SQL Server 2019 and earlier:
SELECT CAST(GETDATE() AS DATE)         -- 2026-04-23
SELECT CONVERT(DATE, GETDATE())        -- 2026-04-23
```

> ⚠️ `CURRENT_DATE` is ANSI SQL but only available in **SQL Server 2025+**. For older versions, use `CAST(GETDATE() AS DATE)`.

---

## 2. Extracting Date Parts — DATEPART vs DATENAME

### The Two Functions

| Function | Returns | Example (April 23) |
|----------|---------|-------------------|
| `DATEPART(part, date)` | **int** | `4` (for MONTH) |
| `DATENAME(part, date)` | **varchar** | `'April'` (for MONTH) |

### When They Actually Differ

Only **MONTH** and **WEEKDAY** produce meaningfully different output:

| Part | `DATEPART()` | `DATENAME()` | Different? |
|------|-------------|-------------|-----------|
| MONTH | `4` (int) | `'April'` (string) | ✅ **Yes — name vs number** |
| WEEKDAY | `4` (int) | `'Wednesday'` (string) | ✅ **Yes — name vs number** |
| YEAR | `2026` (int) | `'2026'` (string) | ❌ Same value, different type |
| DAY | `23` (int) | `'23'` (string) | ❌ Same value, different type |
| HOUR | `14` (int) | `'14'` (string) | ❌ Same value, different type |
| WEEK | `17` (int) | `'17'` (string) | ❌ Same value, different type |
| QUARTER | `2` (int) | `'2'` (string) | ❌ Same value, different type |

For everything except MONTH/WEEKDAY, `DATENAME` is just `CAST(DATEPART(...) AS VARCHAR)`.

### Rule of Thumb

> **Default to `DATEPART()` for everything.** Only use `DATENAME()` when you need the text name — i.e., `'April'` or `'Wednesday'`.

```sql
-- Use DATEPART for math/comparisons:
WHERE DATEPART(MONTH, order_date) = 4

-- Use DATENAME only for display text:
SELECT DATENAME(MONTH, order_date)    -- 'April'
SELECT DATENAME(WEEKDAY, order_date)  -- 'Wednesday'

-- Pivot by day name (SalesByDayOfTheWeek pattern):
SUM(CASE WHEN DATENAME(WEEKDAY, order_date) = 'Monday' THEN qty ELSE 0 END) AS Monday
```

### Why Two Functions Exist

Legacy design — SQL Server split by return type instead of using a single function with a type flag. Redundant for most parts, but both survive for backward compatibility.

---

## 3. Shorthand Functions — DAY, MONTH, YEAR

`DAY()`, `MONTH()`, `YEAR()` are **shortcuts** for `DATEPART()`:

```sql
-- These are identical:
DAY('2026-04-23')              -- 23
DATEPART(DAY, '2026-04-23')   -- 23

MONTH('2026-04-23')            -- 4
DATEPART(MONTH, '2026-04-23') -- 4

YEAR('2026-04-23')             -- 2026
DATEPART(YEAR, '2026-04-23')  -- 2026
```

Less typing, same result. No shorthand exists for HOUR, MINUTE, WEEK, etc. — use `DATEPART()` for those.

> ⚠️ **`DAY()` pitfall:** `DAY(date)` extracts day-of-month (1–31) only. Do NOT use it for date arithmetic in gaps-and-islands — it breaks across month boundaries. Use `DATEADD` instead.
>
> ```sql
> DAY('2026-01-31')  -- 31
> DAY('2026-02-01')  -- 1  ← resets! Not consecutive with 31.
> ```

---

## 4. Date Arithmetic — DATEADD & DATEDIFF

### DATEADD — Add/Subtract Intervals

```sql
DATEADD(part, number, date)

DATEADD(DAY, 7, '2026-04-23')      -- 2026-04-30 (7 days later)
DATEADD(DAY, -1, '2026-04-23')     -- 2026-04-22 (yesterday)
DATEADD(MONTH, 1, '2026-04-23')    -- 2026-05-23 (next month)
DATEADD(HOUR, 3, '2026-04-23 10:00')  -- 2026-04-23 13:00
```

### DATEDIFF — Difference Between Dates

```sql
DATEDIFF(part, start_date, end_date)    -- = end - start

DATEDIFF(DAY, '2026-04-01', '2026-04-23')    -- 22 days
DATEDIFF(MONTH, '2026-01-01', '2026-04-23')  -- 3 months
DATEDIFF(MINUTE, '10:00', '10:45')            -- 45 minutes
```

### ⚠️ DATEDIFF Gotchas

**Argument order matters:** `DATEDIFF(DAY, start, end)` = `end - start`. Swapping gives negative/wrong result.

```sql
DATEDIFF(DAY, '2026-04-01', '2026-04-23')  --  22 ✅
DATEDIFF(DAY, '2026-04-23', '2026-04-01')  -- -22 (swapped!)
```

**Counts boundary crossings, not full intervals:**

```sql
DATEDIFF(YEAR, '2025-12-31', '2026-01-01')  -- 1 (crossed a year boundary)
-- But it's only 1 day apart!

DATEDIFF(MONTH, '2026-01-31', '2026-02-01')  -- 1 (crossed a month boundary)
-- But it's only 1 day apart!
```

### Common Patterns

```sql
-- Day-1 retention check (GamePlayAnalysisV):
DATEDIFF(DAY, install_dt, event_date) = 1    -- "day after install"
-- Or equivalently:
event_date = DATEADD(DAY, 1, install_dt)

-- Gaps-and-islands with dates (ReportContiguousDates):
DATEADD(DAY, -ROW_NUMBER() OVER (...), date_col) AS grp
-- Constant for consecutive dates → group identifier
```

---

## 5. Date Truncation — DATETRUNC

**Truncates a datetime to the specified precision** — everything below it becomes zero/start.

```sql
DATETRUNC(part, date)
```

### Examples

```sql
-- Given: '2026-04-23 14:35:47.123'

DATETRUNC(YEAR,   ...)  -- 2026-01-01 00:00:00  (first day of year)
DATETRUNC(MONTH,  ...)  -- 2026-04-01 00:00:00  (first day of month)
DATETRUNC(DAY,    ...)  -- 2026-04-23 00:00:00  (midnight)
DATETRUNC(HOUR,   ...)  -- 2026-04-23 14:00:00  (top of hour)
DATETRUNC(MINUTE, ...)  -- 2026-04-23 14:35:00
DATETRUNC(WEEK,   ...)  -- 2026-04-20 00:00:00  (Monday of that week)
```

### Availability

| Database | Function | Available Since |
|----------|----------|----------------|
| SQL Server | `DATETRUNC(MONTH, date)` | **2022+ only** |
| PostgreSQL | `DATE_TRUNC('month', date)` | Always (note: underscore, string part) |
| MySQL | No `DATETRUNC` | Use `DATE_FORMAT()` or `LAST_DAY()` |

### Pre-2022 Workaround

```sql
-- Manual DATETRUNC(MONTH, order_date) for older SQL Server:
DATEADD(MONTH, DATEDIFF(MONTH, 0, order_date), 0)
```

**How it works:**
1. `0` in SQL Server = `1900-01-01` (epoch)
2. `DATEDIFF(MONTH, 0, '2026-04-23')` = `1515` months since epoch (day info lost)
3. `DATEADD(MONTH, 1515, 0)` = `2026-04-01` (rebuilds from epoch, lands on 1st)

Strip to month count → rebuild → always lands on 1st of month.

### Common Use Cases

```sql
-- 1. GROUP BY month (most common):
SELECT DATETRUNC(MONTH, order_date) AS month, SUM(revenue)
FROM Orders
GROUP BY DATETRUNC(MONTH, order_date)

-- 2. Filter rows in current month:
WHERE DATETRUNC(MONTH, created_at) = DATETRUNC(MONTH, GETDATE())

-- 3. Strip time from datetime (daily aggregation):
SELECT DATETRUNC(DAY, login_time) AS login_date, COUNT(DISTINCT user_id)
FROM Logins
GROUP BY DATETRUNC(DAY, login_time)

-- 4. Hourly traffic buckets:
SELECT DATETRUNC(HOUR, event_time), COUNT(*)
FROM PageViews
GROUP BY DATETRUNC(HOUR, event_time)

-- 5. JOIN on same month:
FROM DailyMetrics d
JOIN MonthlyTargets t ON DATETRUNC(MONTH, d.date) = t.target_month
```

> **TL;DR:** Anytime you need to bucket datetimes into uniform time periods — for grouping, filtering, or joining — `DATETRUNC` is the cleanest approach.

---

## 6. Custom Buckets — DATE_BUCKET

`DATETRUNC` always makes **1-unit** buckets. `DATE_BUCKET` lets you choose **any size**.

```sql
DATE_BUCKET(part, width, date [, origin])
```

### DATETRUNC vs DATE_BUCKET

| Function | Bucket size | Example |
|----------|------------|--------|
| `DATETRUNC(HOUR, date)` | Fixed **1 hour** | 10:00, 11:00, 12:00 |
| `DATE_BUCKET(HOUR, 3, date)` | Custom **3 hours** | 09:00, 12:00, 15:00 |

### Example — 15-Minute Buckets

```
Event times:        → DATE_BUCKET(MINUTE, 15, time) → Bucket
─────────────────────────────────────────────────────────────
10:03               → 10:00
10:12               → 10:00       ← same bucket
10:18               → 10:15
10:29               → 10:15       ← same bucket
10:31               → 10:30
10:47               → 10:45
```

```sql
SELECT DATE_BUCKET(MINUTE, 15, event_time) AS bucket, COUNT(*)
FROM Events
GROUP BY DATE_BUCKET(MINUTE, 15, event_time)
```

You **can't** do this with `DATETRUNC` — it only does 1-unit buckets, not 15-minute.

### More Examples

```sql
DATE_BUCKET(DAY,   7, date)   -- weekly buckets (every 7 days)
DATE_BUCKET(MONTH, 3, date)   -- quarterly buckets (every 3 months)
DATE_BUCKET(HOUR,  2, date)   -- 2-hour buckets
DATE_BUCKET(DAY,  14, date)   -- bi-weekly buckets
```

> **Availability:** SQL Server 2022+ only.
> **TL;DR:** `DATETRUNC` = fixed 1-unit buckets. `DATE_BUCKET` = custom N-unit buckets.

---

## 7. Type Conversion — CAST vs CONVERT

Both convert data types. `CAST` is ANSI standard, `CONVERT` is SQL Server specific with extra **style codes**.

### Syntax

```sql
CAST(expression AS target_type)              -- ANSI, no formatting
CONVERT(target_type, expression [, style])   -- SQL Server, has style codes
```

### When They're Identical

```sql
CAST(GETDATE() AS DATE)       -- 2026-04-23
CONVERT(DATE, GETDATE())      -- 2026-04-23 (same thing)

CAST(price AS INT)             -- 99
CONVERT(INT, price)            -- 99 (same thing)
```

### What CONVERT Can Do That CAST Cannot

**Date Format Styles (main difference):**
```sql
CONVERT(VARCHAR, GETDATE(), 101)  -- '04/23/2026' (US)
CONVERT(VARCHAR, GETDATE(), 103)  -- '23/04/2026' (UK)
CONVERT(VARCHAR, GETDATE(), 112)  -- '20260423'   (ISO compact)
CONVERT(VARCHAR, GETDATE(), 120)  -- '2026-04-23 14:30:45'
```

**Binary/Hex & XML Styles:**
```sql
CONVERT(VARCHAR, 0x48656C6C6F, 0)  -- 'Hello' (binary to string)
CONVERT(VARCHAR, xml_col, 1)        -- with XML declaration
```

> **Rule:** Use `CAST` by default (ANSI, portable). Switch to `CONVERT` only when you need a **style code**.

### Common Use in Interview Problems

```sql
-- Force decimal division (avoid integer truncation):
CAST(COUNT(*) AS DECIMAL(10,2)) / total
-- DECIMAL(10,2) = 10 total digits, 2 after decimal point
-- Without CAST: 1 / 2 = 0 (integer division)
-- With CAST:    1.00 / 2 = 0.50

-- Alternative:
1.0 * COUNT(*) / total          -- multiply by 1.0 to force decimal
```

---

## 8. Formatting — FORMAT

`FORMAT` uses **.NET format strings** — most flexible but **slowest** option.

```sql
FORMAT(value, format_string)
```

### FORMAT vs CONVERT for Date Formatting

| | `FORMAT` | `CONVERT` |
|--|---------|----------|
| Flexibility | ✅ Any custom pattern | Limited to ~30 style codes |
| Readability | ✅ `'yyyy-MM-dd'` is obvious | ❌ What's style `101`? |
| Performance | ❌ **Slow** (uses .NET CLR) | ✅ Fast |
| Availability | SQL Server 2012+ | Always |

### Common Patterns

```sql
FORMAT(GETDATE(), 'yyyy-MM-dd')            -- '2026-04-23'
FORMAT(GETDATE(), 'dd-MMM-yyyy')           -- '23-Apr-2026'
FORMAT(GETDATE(), 'yyyy/MM/dd HH:mm:ss')   -- '2026/04/23 14:30:45'
FORMAT(GETDATE(), 'hh:mm tt')              -- '02:30 PM'
```

### ⚠️ Case Sensitivity Rules

**Case matters** in FORMAT patterns. Two pairs are critical:

| ⚠️ Confusing Pair | Uppercase | Lowercase |
|---|---|---|
| **M vs m** | `MM` = **Month** (04) | `mm` = **Minutes** (35) |
| **H vs h** | `HH` = **24-hour** (14) | `hh` = **12-hour** (02) |

Everything else (`yyyy`, `dd`, `ss`, `tt`) — case doesn't matter.

```sql
FORMAT(GETDATE(), 'yyyy-MM-dd HH:mm:ss')  -- 2026-04-23 14:35:47 ✅ CORRECT
FORMAT(GETDATE(), 'yyyy-mm-dd hh:mm:ss')  -- 2026-35-23 02:35:47 ❌ WRONG (mm=minutes, hh=12hr)
```

> **Rule:** Always `MM` for month, `mm` for minutes, `HH` for 24-hour, `hh` for 12-hour.

### Performance Warning

> Avoid `FORMAT` in `WHERE`, `JOIN`, or large result sets — it's significantly slower than `CONVERT`. Use it only for final **display** formatting.

---

## 9. Date Construction & Validation

### Building Dates

| Function | Purpose | Example |
|----------|---------|---------|
| `DATEFROMPARTS(y, m, d)` | Build a DATE | `DATEFROMPARTS(2026, 4, 22)` → `2026-04-22` |
| `DATETIMEFROMPARTS(y,m,d,h,mi,s,ms)` | Build datetime | Full precision |
| `TIMEFROMPARTS(h,mi,s,frac,prec)` | Build time | `TIMEFROMPARTS(14,30,0,0,0)` |

### Validation & Helpers

| Function | Purpose | Example |
|----------|---------|---------|
| `ISDATE(expr)` | Valid date? (0/1) | `ISDATE('2026-02-30')` → `0` |
| `EOMONTH(date)` | Last day of month | `EOMONTH('2026-04-22')` → `2026-04-30` |
| `EOMONTH(date, n)` | Last day ± n months | `EOMONTH('2026-04-22', 1)` → `2026-05-31` |

### Formatting

```sql
FORMAT(GETDATE(), 'yyyy-MM-dd')           -- '2026-04-23'
FORMAT(GETDATE(), 'dd/MM/yyyy HH:mm')     -- '23/04/2026 14:30'
CONVERT(VARCHAR, GETDATE(), 101)           -- '04/23/2026' (US format)
CONVERT(VARCHAR, GETDATE(), 103)           -- '23/04/2026' (UK format)
```

---

## 10. DATEPART Units Reference

Used in `DATEPART`, `DATENAME`, `DATEADD`, `DATEDIFF`, `DATETRUNC`:

| Unit | Abbreviations | Example Value |
|------|--------------|---------------|
| `YEAR` | `yy`, `yyyy` | 2026 |
| `QUARTER` | `qq`, `q` | 2 |
| `MONTH` | `mm`, `m` | 4 |
| `DAYOFYEAR` | `dy`, `y` | 113 |
| `DAY` | `dd`, `d` | 23 |
| `WEEK` | `wk`, `ww` | 17 |
| `WEEKDAY` | `dw` | 4 (Wed, Sunday=1 default) |
| `HOUR` | `hh` | 14 |
| `MINUTE` | `mi`, `n` | 35 |
| `SECOND` | `ss`, `s` | 47 |
| `MILLISECOND` | `ms` | 123 |

---

## 11. Interview Tips

- **No `DATE_TRUNC` before SQL Server 2022** — use `DATEADD(MONTH, DATEDIFF(MONTH, 0, date), 0)` workaround
- **No `CURRENT_DATE` before SQL Server 2025** — use `CAST(GETDATE() AS DATE)`
- **`DATEDIFF` counts boundary crossings**, not full intervals: Dec 31 → Jan 1 = 1 year difference (but only 1 day apart)
- **`DATEDIFF` argument order:** `(part, start, end)` = `end - start`. Swapping gives wrong sign
- **`EOMONTH`** for month-end logic — avoids manual 28/29/30/31 handling
- **`DATENAME` only for MONTH/WEEKDAY** — everything else just returns number as string
- **`DAY()` breaks across months** — don't use it in gaps-and-islands. Use `DATEADD(DAY, -rn, date)` instead
- **`CAST` vs `CONVERT`:** Use `CAST` by default (ANSI). `CONVERT` only for style codes
- **`FORMAT`** is flexible but slow (.NET CLR) — use `CONVERT` for performance, `FORMAT` for display only
- **FORMAT case sensitivity:** `MM` = month, `mm` = minutes; `HH` = 24hr, `hh` = 12hr
- **`DATE_BUCKET`** for custom intervals (15-min, 3-month) — SQL Server 2022+ only
- **Force decimal division:** `CAST(x AS DECIMAL(10,2))` or `1.0 * x` to avoid integer truncation
- **ANSI vs SQL Server:** mention portability awareness in interviews

### Function Decision Tree

```
Need current datetime?
  → CURRENT_TIMESTAMP (ANSI) or GETDATE()

Need to extract a part?
  → Need 'April'/'Wednesday'? → DATENAME()
  → Need number?              → DATEPART() (or DAY/MONTH/YEAR shortcuts)

Need to add/subtract time?
  → DATEADD(part, n, date)

Need difference between dates?
  → DATEDIFF(part, start, end)

Need to group by time bucket?
  → Fixed 1-unit?  → DATETRUNC(part, date)
  → Custom N-unit? → DATE_BUCKET(part, N, date)

Need last day of month?
  → EOMONTH(date)

Need type conversion?
  → No formatting needed? → CAST (ANSI)
  → Need style code?      → CONVERT
  → Need custom pattern?  → FORMAT (slow, display only)
```
