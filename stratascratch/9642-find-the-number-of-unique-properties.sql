/*
================================================================================
PROBLEM: Extract Unique Room Types from Comma-Separated Filter Values
================================================================================

Goal: Split comma-separated room types stored in a single column into 
      individual rows and return distinct room types.

Example Input:
    | filter_room_types           |
    |-----------------------------|
    | "Private room, Shared room" |
    | "Entire home, Private room" |

Expected Output:
    | room_type     |
    |---------------|
    | Private room  |
    | Shared room   |
    | Entire home   |

================================================================================
*/


-- ============================================================================
-- SOLUTION 1: SQL Server (T-SQL) using CROSS APPLY + STRING_SPLIT
-- ============================================================================
/*
Explanation:
------------
1. STRING_SPLIT(a.filter_room_types, ',')
   - Built-in SQL Server function (2016+)
   - Splits the comma-separated string into multiple rows
   - Returns a table with a 'value' column. (table generating function)
   
2. CROSS APPLY
   - Similar to INNER JOIN but works with table-valued functions
   - For each row in airbnb_searches, it applies STRING_SPLIT
   - Creates one row per split value
   
3. LTRIM(RTRIM(...)) 
   - Removes leading and trailing whitespace
   - LTRIM = Left Trim, RTRIM = Right Trim
   
4. WHERE clause filters out empty strings

Time Complexity: O(n * m) where n = rows, m = avg items per row
*/

SELECT DISTINCT
       TRIM(value) AS room_type
FROM airbnb_searches
CROSS APPLY STRING_SPLIT(filter_room_types, ',') -- by default column name is value (STRING_SPLIT returns a table with ONE column)
WHERE TRIM(value) <> '';


SELECT DISTINCT
       LTRIM(RTRIM(s.room_type)) AS room_type
FROM airbnb_searches a
CROSS APPLY STRING_SPLIT(a.filter_room_types, ',') AS s(room_type) -- here s is table alias and room_type → column alias replacing default value
WHERE LTRIM(RTRIM(s.room_type)) <> '';


-- ============================================================================
-- SOLUTION 2: Spark SQL / Hive using LATERAL VIEW + explode
-- ============================================================================
/*
Explanation:
------------
1. split(filter_room_types, ',')
   - Splits the string by comma delimiter
   - Returns an ARRAY of strings
   - Example: "a, b, c" → ["a", " b", " c"]
   
2. explode(array)
   - Converts each array element into a separate row
   - One input row with 3 elements → 3 output rows
   
3. LATERAL VIEW
   - Spark/Hive way to join a table with a table-generating function
   - Similar to CROSS APPLY in SQL Server
   - 't' is the table alias, 'room_type' is the column alias
   
4. TRIM(room_type)
   - Removes both leading and trailing whitespace
   - Spark's TRIM = SQL Server's LTRIM + RTRIM combined

Visual Example:
---------------
Original Row:
| filter_room_types             |
| "Private room, Shared room"   |

After explode:
| room_type       |
| "Private room"  |
| " Shared room"  |  ← Note: has leading space

After TRIM:
| room_type      |
| "Private room" |
| "Shared room"  |  ← Space removed

Time Complexity: O(n * m) where n = rows, m = avg items per row
*/
SELECT DISTINCT
       TRIM(room_type) AS room_type
FROM airbnb_searches
LATERAL VIEW explode(split(filter_room_types, ',')) t AS room_type
WHERE TRIM(room_type) <> '';


Spark Scala 

import org.apache.spark.sql.functions._

airbnbSearchesDF
  .select(explode(split(col("filter_room_types"), ",")).alias("room_type"))
  .select(trim(col("room_type")).alias("room_type"))
  .filter(col("room_type") =!= "")
  .distinct()


-- ============================================================================
-- COMPARISON: SQL Server vs Spark/Hive
-- ============================================================================
/*
| Feature          | SQL Server                  | Spark/Hive                    |
|------------------|-----------------------------|------------------------------ |
| Split to Rows.   | STRING_SPLIT()              | explode(split())                       |
| Explode rows     | CROSS APPLY                 | LATERAL VIEW        |
| Trim whitespace  | LTRIM(RTRIM())              | TRIM()                        |
*/


-- ============================================================================
-- 🔥 ADDITIONAL LEARNINGS: LATERAL VIEW Deep Dive
-- ============================================================================

/*
================================================================================
WHY LATERAL VIEW IS REQUIRED IN SPARK SQL
================================================================================

In Spark SQL, explode() by itself creates rows, but those rows don't exist 
early enough in the query to be used by WHERE or JOIN unless you use LATERAL VIEW.
*/

-- ----------------------------------------------------------------------------
-- ❌ PROBLEM 1: explode() alone - works but LIMITED
-- ----------------------------------------------------------------------------

-- This WORKS ✅
SELECT explode(split(filter_room_types, ',')) AS room_type
FROM airbnb_searches;

/*
Output:
    | room_type       |
    |-----------------|
    | (empty)         |
    | Entire home/apt |
    | Private room    |

So far so good...
*/

-- ----------------------------------------------------------------------------
-- ❌ PROBLEM 2: Trying to FILTER on exploded column - FAILS!
-- ----------------------------------------------------------------------------

-- This FAILS ❌
SELECT explode(split(filter_room_types, ',')) AS room_type
FROM airbnb_searches
WHERE room_type <> '';

/*
ERROR: Column 'room_type' cannot be resolved

Why? SQL execution order:
    1. FROM
    2. WHERE    ← room_type doesn't exist yet!
    3. SELECT   ← room_type is created here (too late)

Spark says: "I don't know what room_type is."
*/

-- ----------------------------------------------------------------------------
-- ❌ PROBLEM 3: Same issue with JOIN
-- ----------------------------------------------------------------------------

-- This is NOT VALID ❌
SELECT *
FROM airbnb_searches a
JOIN (
  SELECT explode(split(filter_room_types, ',')) AS room_type
) b
ON b.room_type = 'Private room';

/*
Why it fails:
  - explode() has no row context
  - It must run per input row
*/

-- ----------------------------------------------------------------------------
-- ✅ SOLUTION: LATERAL VIEW fixes this!
-- ----------------------------------------------------------------------------

SELECT room_type
FROM airbnb_searches
LATERAL VIEW explode(split(filter_room_types, ',')) t AS room_type
WHERE room_type <> '';

/*
What LATERAL VIEW does:
-----------------------
It tells Spark: "Run explode() during the FROM phase, not in SELECT."

New execution order:
    1. FROM (with exploded rows)  ← room_type exists NOW
    2. WHERE                      ← can filter on room_type ✅
    3. SELECT                     ← can reference room_type ✅

Now room_type:
  ✅ exists before WHERE
  ✅ can be filtered
  ✅ can be joined
  ✅ can be grouped
*/

-- ----------------------------------------------------------------------------
-- 💡 DataFrame API vs Spark SQL (why DataFrame feels easier)
-- ----------------------------------------------------------------------------
/*
In DataFrame API, this works:

    df
      .select(explode(split(col("filter_room_types"), ",")).alias("room_type"))
      .filter(col("room_type") =!= "")

Why?
  - Each step creates a NEW DataFrame
  - room_type exists before filter is applied
  
Spark SQL does NOT work this way — it follows strict SQL execution phases.
*/


-- ============================================================================
-- 📚 SPARK EXPLODE FUNCTIONS - COMPLETE REFERENCE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1️⃣ explode() — The Standard One
-- ----------------------------------------------------------------------------
/*
Most commonly used. Works on ARRAY and MAP.

Syntax:
    explode(array_col)
    explode(map_col)

Behavior:
    - One input row → multiple output rows
    - DROPS the row if array/map is empty or NULL

Example:
    explode(array('a','b','c'))
    
    Output:
    | col |
    |-----|
    | a   |
    | b   |
    | c   |
*/

-- ----------------------------------------------------------------------------
-- 2️⃣ explode_outer() — NULL-Safe Explode
-- ----------------------------------------------------------------------------
/*
Use when you want to KEEP rows even if NULL/empty.

Syntax:
    explode_outer(array_col)

Behavior:
    - NULL / empty → produces one row with NULL
    - Similar to LEFT JOIN behavior

Example:
    | input | output |
    |-------|--------|
    | NULL  | NULL   |
    | []    | NULL   |
*/

-- ----------------------------------------------------------------------------
-- 3️⃣ posexplode() — Explode WITH Index
-- ----------------------------------------------------------------------------
/*
Adds the position of each element.

Syntax:
    posexplode(array_col)

Output columns: (pos, value)

Example:
    posexplode(array('a','b','c'))
    
    | pos | value |
    |-----|-------|
    | 0   | a     |
    | 1   | b     |
    | 2   | c     |
*/

-- ----------------------------------------------------------------------------
-- 4️⃣ posexplode_outer() — Index + NULL-Safe
-- ----------------------------------------------------------------------------
/*
Combination of posexplode and explode_outer.
Keeps NULL rows AND provides position index.
*/

-- ----------------------------------------------------------------------------
-- 5️⃣ inline() — Explode Array of STRUCTs
-- ----------------------------------------------------------------------------
/*
Use for arrays containing struct objects.

Syntax:
    inline(array<struct<...>>)

Example:
    inline(array(
      struct(1,'a'),
      struct(2,'b')
    ))
    
    Output:
    | col1 | col2 |
    |------|------|
    | 1    | a    |
    | 2    | b    |

👉 NOT the same as explode — it expands multiple columns from struct fields.
*/

-- ----------------------------------------------------------------------------
-- 6️⃣ LATERAL VIEW Variants
-- ----------------------------------------------------------------------------
/*
Spark SQL syntax:

    LATERAL VIEW explode(...) alias AS col
    LATERAL VIEW OUTER explode(...) alias AS col

    | Variant                      | Keeps NULL rows? |
    |------------------------------|------------------|
    | LATERAL VIEW explode         | ❌ No            |
    | LATERAL VIEW OUTER explode   | ✅ Yes           |
*/


-- ============================================================================
-- 📊 FULL SUMMARY TABLE (BOOKMARK THIS!)
-- ============================================================================
/*
| Function          | Works On      | Keeps NULL | Adds Position | Expands Struct |
|-------------------|---------------|------------|---------------|----------------|
| explode           | array, map    | ❌         | ❌            | ❌             |
| explode_outer     | array, map    | ✅         | ❌            | ❌             |
| posexplode        | array         | ❌         | ✅            | ❌             |
| posexplode_outer  | array         | ✅         | ✅            | ❌             |
| inline            | array<struct> | ❌         | ❌            | ✅             |
*/


-- ============================================================================
-- 🎯 WHEN TO USE WHAT (PRACTICAL RULES)
-- ============================================================================
/*
| Use Case                  | Function to Use      |
|---------------------------|----------------------|
| Simple split → rows       | explode              |
| Keep rows even if NULL    | explode_outer        |
| Need element order/index  | posexplode           |
| JSON array of objects     | inline               |
| Left join behavior        | LATERAL VIEW OUTER   |
*/


-- ============================================================================
-- 🔄 SPARK SQL vs DATAFRAME API MAPPING
-- ============================================================================
/*
| Spark SQL           | DataFrame API                |
|---------------------|------------------------------|
| explode(col)        | explode(col)                 |
| posexplode(col)     | posexplode(col)              |
| LATERAL VIEW        | select(explode(...))         |

💡 DataFrame API does NOT need LATERAL VIEW because each step is a new logical plan.
*/
