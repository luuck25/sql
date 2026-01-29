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


-- ============================================================================
-- 🔄 RELATED ARRAY FUNCTIONS IN SPARK
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 1️⃣ collect_list() / collect_set() — REVERSE of explode
-- ----------------------------------------------------------------------------
/*
Aggregates rows back INTO an array (opposite of explode).

    - collect_list(): Keeps duplicates
    - collect_set():  Removes duplicates

Example:
    SELECT user_id, collect_list(room_type) AS all_rooms
    FROM searches
    GROUP BY user_id;

    -- Input rows:
    | user_id | room_type    |
    |---------|--------------|
    | 1       | Private      |
    | 1       | Private      |
    | 1       | Entire home  |

    -- Output with collect_list:
    | user_id | all_rooms                      |
    |---------|--------------------------------|
    | 1       | [Private, Private, Entire home]|

    -- Output with collect_set:
    | user_id | all_rooms              |
    |---------|------------------------|
    | 1       | [Private, Entire home] |

Use case: After exploding and filtering, re-aggregate into array
*/

-- Spark SQL
SELECT user_id, collect_list(room_type) AS all_rooms
FROM searches
GROUP BY user_id;

-- PySpark
-- df.groupBy("user_id").agg(F.collect_list("room_type").alias("all_rooms"))


-- ----------------------------------------------------------------------------
-- 2️⃣ map_keys() / map_values() — For MAP columns
-- ----------------------------------------------------------------------------
/*
Extract keys or values from a MAP column.

Syntax:
    map_keys(map_col)   -- returns array of keys
    map_values(map_col) -- returns array of values

Example:
    -- If you have: {"a": 1, "b": 2, "c": 3}
    
    SELECT map_keys(my_map) AS keys,
           map_values(my_map) AS values
    FROM table;

    -- Output:
    | keys        | values    |
    |-------------|-----------|
    | [a, b, c]   | [1, 2, 3] |

    -- Then explode if needed:
    SELECT explode(map_keys(my_map)) AS key FROM table;

Use case: When you need only keys or only values from a MAP
*/

-- Spark SQL
SELECT explode(map_keys(properties)) AS property_name
FROM listings;

-- PySpark
-- df.select(F.explode(F.map_keys("properties")).alias("property_name"))


-- ----------------------------------------------------------------------------
-- 3️⃣ Multiple LATERAL VIEWs — Chaining explodes
-- ----------------------------------------------------------------------------
/*
You can chain multiple LATERAL VIEWs for nested arrays.

Example:
    -- orders table has: items (array), each item has tags (array)
    
    SELECT order_id, item, tag
    FROM orders
    LATERAL VIEW explode(items) t1 AS item
    LATERAL VIEW explode(item.tags) t2 AS tag;

    -- Input:
    | order_id | items                                    |
    |----------|------------------------------------------|
    | 1        | [{name: "A", tags: ["x","y"]}, {name: "B", tags: ["z"]}] |

    -- Output (cartesian product of nested arrays):
    | order_id | item        | tag |
    |----------|-------------|-----|
    | 1        | {name: "A"} | x   |
    | 1        | {name: "A"} | y   |
    | 1        | {name: "B"} | z   |

Use case: Deeply nested JSON/arrays
*/

-- Spark SQL
SELECT order_id, item.name, tag
FROM orders
LATERAL VIEW explode(items) t1 AS item
LATERAL VIEW explode(item.tags) t2 AS tag;

-- PySpark
-- df.select("order_id", F.explode("items").alias("item")) \
--   .select("order_id", "item.name", F.explode("item.tags").alias("tag"))


-- ----------------------------------------------------------------------------
-- 4️⃣ arrays_zip() — Combine arrays element-by-element
-- ----------------------------------------------------------------------------
/*
Zips multiple arrays together by position (like Python's zip()).

Syntax:
    arrays_zip(array1, array2, ...)

Example:
    -- If you have two parallel arrays:
    -- names = ['Alice', 'Bob', 'Charlie']
    -- ages  = [25, 30, 35]

    SELECT arrays_zip(names, ages) AS zipped;

    -- Output:
    | zipped                                           |
    |--------------------------------------------------|
    | [{names: Alice, ages: 25}, {names: Bob, ages: 30}, {names: Charlie, ages: 35}] |

    -- Then explode to get rows:
    SELECT explode(arrays_zip(names, ages)) AS person;

    -- Output:
    | person                  |
    |-------------------------|
    | {names: Alice, ages: 25}|
    | {names: Bob, ages: 30}  |
    | {names: Charlie, ages: 35}|

Use case: When arrays are related by position (parallel columns)

⚠️ Requires Spark 2.4+
*/

-- Spark SQL
SELECT inline(arrays_zip(names, ages)) AS (name, age)
FROM users;

-- PySpark
-- df.select(F.explode(F.arrays_zip("names", "ages")).alias("zipped")) \
--   .select("zipped.names", "zipped.ages")


-- ----------------------------------------------------------------------------
-- 5️⃣ flatten() — Flatten nested arrays
-- ----------------------------------------------------------------------------
/*
Converts array of arrays into a single flat array.

Syntax:
    flatten(array_of_arrays)

Example:
    -- Input: [[1, 2], [3, 4], [5]]
    
    SELECT flatten(nested_array) AS flat;

    -- Output: [1, 2, 3, 4, 5]

Use case: When you have array<array<T>> and want array<T>

⚠️ Requires Spark 2.4+
*/

-- Spark SQL
SELECT flatten(array(array(1, 2), array(3, 4), array(5))) AS flat_array;

-- PySpark
-- df.select(F.flatten("nested_array").alias("flat_array"))


-- ----------------------------------------------------------------------------
-- 6️⃣ transform() — Apply function WITHOUT exploding
-- ----------------------------------------------------------------------------
/*
Applies a lambda function to each array element, keeping it as an array.
Does NOT create new rows like explode.

Syntax:
    transform(array, x -> expression)

Example:
    -- Add 10% tax to all prices
    -- Input: prices = [100, 200, 300]

    SELECT transform(prices, x -> x * 1.1) AS prices_with_tax;

    -- Output: [110.0, 220.0, 330.0]

More examples:
    -- Uppercase all strings
    transform(names, x -> upper(x))
    
    -- Extract field from array of structs
    transform(items, x -> x.price)

Use case: Modify array elements in-place WITHOUT creating new rows

⚠️ Requires Spark 2.4+
*/

-- Spark SQL
SELECT transform(prices, x -> x * 1.1) AS prices_with_tax
FROM products;

-- Convert array of structs to array of just one field
SELECT transform(items, x -> x.name) AS item_names
FROM orders;

-- PySpark
-- df.select(F.transform("prices", lambda x: x * 1.1).alias("prices_with_tax"))


-- ============================================================================
-- 📊 COMPLETE ARRAY FUNCTIONS SUMMARY
-- ============================================================================
/*
| Function        | Direction      | Description                              | Spark Ver |
|-----------------|----------------|------------------------------------------|-----------|
| explode         | Array → Rows   | One row per element                      | All       |
| collect_list    | Rows → Array   | Aggregate rows into array (with dups)   | All       |
| collect_set     | Rows → Array   | Aggregate rows into array (no dups)     | All       |
| map_keys        | Map → Array    | Extract keys from map                    | All       |
| map_values      | Map → Array    | Extract values from map                  | All       |
| arrays_zip      | Arrays → Array | Combine arrays by position               | 2.4+      |
| flatten         | Array → Array  | Flatten nested arrays                    | 2.4+      |
| transform       | Array → Array  | Apply function to each element           | 2.4+      |
| inline          | Array → Rows   | Explode array of structs                 | 2.0+      |
*/


-- ============================================================================
-- 🔢 SPARK ARRAY FUNCTIONS - COMPLETE REFERENCE
-- ============================================================================

-- ----------------------------------------------------------------------------
-- 🔍 ARRAY ELEMENT ACCESS & SEARCH
-- ----------------------------------------------------------------------------
/*
| Function                      | Description                    | Example                          |
|-------------------------------|--------------------------------|----------------------------------|
| element_at(array, index)      | Get element at index (1-based) | element_at([10,20,30], 2) → 20   |
| array_contains(array, val)    | Check if array contains value  | array_contains([1,2,3], 2) → true|
| array_position(array, val)    | Find position of value (1-based)| array_position([a,b,c], 'b') → 2|
| size(array)                   | Get array length               | size([1,2,3]) → 3                |
*/

-- Examples:
SELECT element_at(array(10, 20, 30), 2) AS second_element;  -- 20
SELECT array_contains(array(1, 2, 3), 2) AS has_two;        -- true
SELECT array_position(array('a', 'b', 'c'), 'b') AS pos;    -- 2
SELECT size(array(1, 2, 3, 4, 5)) AS arr_length;            -- 5


-- ----------------------------------------------------------------------------
-- ➕ ARRAY MODIFICATION
-- ----------------------------------------------------------------------------
/*
| Function                        | Description              | Example                              |
|---------------------------------|--------------------------|--------------------------------------|
| array_append(array, val)        | Add element to end       | array_append([1,2], 3) → [1,2,3]     |
| array_prepend(array, val)       | Add element to start     | array_prepend([2,3], 1) → [1,2,3]    |
| array_insert(array, pos, val)   | Insert at position       | array_insert([1,3], 2, 2) → [1,2,3]  |
| array_remove(array, val)        | Remove all occurrences   | array_remove([1,2,2,3], 2) → [1,3]   |
| array_compact(array)            | Remove NULLs             | array_compact([1,NULL,2]) → [1,2]    |

⚠️ array_append, array_prepend, array_insert require Spark 3.4+
*/

-- Examples:
SELECT array_remove(array(1, 2, 2, 3), 2) AS removed;       -- [1, 3]
SELECT array_compact(array(1, NULL, 2, NULL, 3)) AS no_nulls; -- [1, 2, 3]


-- ----------------------------------------------------------------------------
-- 🔀 ARRAY MANIPULATION
-- ----------------------------------------------------------------------------
/*
| Function                | Description        | Example                            |
|-------------------------|--------------------|------------------------------------|
| array_distinct(array)   | Remove duplicates  | array_distinct([1,1,2]) → [1,2]    |
| array_sort(array)       | Sort ascending     | array_sort([3,1,2]) → [1,2,3]      |
| array_reverse(array)    | Reverse order      | array_reverse([1,2,3]) → [3,2,1]   |
| shuffle(array)          | Randomize order    | shuffle([1,2,3]) → random order    |
| slice(array, start, len)| Get subarray       | slice([1,2,3,4], 2, 2) → [2,3]     |
*/

-- Examples:
SELECT array_distinct(array(1, 1, 2, 2, 3)) AS unique_vals;   -- [1, 2, 3]
SELECT array_sort(array(3, 1, 4, 1, 5)) AS sorted;            -- [1, 1, 3, 4, 5]
SELECT array_reverse(array(1, 2, 3)) AS reversed;             -- [3, 2, 1]
SELECT slice(array(1, 2, 3, 4, 5), 2, 3) AS sliced;           -- [2, 3, 4]


-- ----------------------------------------------------------------------------
-- 🔗 ARRAY COMBINATION
-- ----------------------------------------------------------------------------
/*
| Function                    | Description              | Example                                |
|-----------------------------|--------------------------|----------------------------------------|
| array_union(arr1, arr2)     | Union (no dups)          | array_union([1,2], [2,3]) → [1,2,3]    |
| array_intersect(arr1, arr2) | Common elements          | array_intersect([1,2,3], [2,3,4]) → [2,3]|
| array_except(arr1, arr2)    | Difference (arr1 - arr2) | array_except([1,2,3], [2]) → [1,3]     |
| concat(arr1, arr2, ...)     | Concatenate arrays       | concat([1,2], [3,4]) → [1,2,3,4]       |
| array_repeat(val, count)    | Repeat value N times     | array_repeat('a', 3) → [a,a,a]         |
*/

-- Examples:
SELECT array_union(array(1, 2, 3), array(3, 4, 5)) AS union_arr;      -- [1, 2, 3, 4, 5]
SELECT array_intersect(array(1, 2, 3), array(2, 3, 4)) AS common;     -- [2, 3]
SELECT array_except(array(1, 2, 3, 4), array(2, 4)) AS difference;    -- [1, 3]
SELECT concat(array(1, 2), array(3, 4), array(5)) AS concatenated;    -- [1, 2, 3, 4, 5]
SELECT array_repeat('hello', 3) AS repeated;                          -- [hello, hello, hello]


-- ----------------------------------------------------------------------------
-- 🔄 HIGHER-ORDER FUNCTIONS (Spark 2.4+)
-- ----------------------------------------------------------------------------
/*
These are powerful functions that take lambda expressions.

| Function                              | Description              | Example                              |
|---------------------------------------|--------------------------|--------------------------------------|
| transform(arr, x -> expr)             | Apply to each element    | transform([1,2,3], x -> x*2) → [2,4,6]|
| filter(arr, x -> condition)           | Filter elements          | filter([1,2,3,4], x -> x > 2) → [3,4]|
| aggregate(arr, start, merge, finish)  | Reduce/fold array        | aggregate([1,2,3], 0, (a,x) -> a+x) → 6|
| exists(arr, x -> condition)           | Any element matches?     | exists([1,2,3], x -> x > 2) → true   |
| forall(arr, x -> condition)           | All elements match?      | forall([1,2,3], x -> x > 0) → true   |
| zip_with(arr1, arr2, (a,b) -> expr)   | Zip with function        | zip_with([1,2], [10,20], (a,b) -> a+b) → [11,22]|
*/

-- Examples:

-- transform: Double each element
SELECT transform(array(1, 2, 3, 4), x -> x * 2) AS doubled;
-- Output: [2, 4, 6, 8]

-- filter: Keep only positive numbers
SELECT filter(array(-2, -1, 0, 1, 2), x -> x > 0) AS positives;
-- Output: [1, 2]

-- aggregate: Sum all elements
SELECT aggregate(array(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x) AS total;
-- Output: 15

-- aggregate: Find max (with finish function)
SELECT aggregate(array(3, 1, 4, 1, 5), 0, (acc, x) -> CASE WHEN x > acc THEN x ELSE acc END) AS max_val;
-- Output: 5

-- exists: Check if any element > 10
SELECT exists(array(1, 5, 15, 3), x -> x > 10) AS has_big_number;
-- Output: true

-- forall: Check if all elements are positive
SELECT forall(array(1, 2, 3, 4), x -> x > 0) AS all_positive;
-- Output: true

-- zip_with: Add corresponding elements
SELECT zip_with(array(1, 2, 3), array(10, 20, 30), (a, b) -> a + b) AS sums;
-- Output: [11, 22, 33]


-- ----------------------------------------------------------------------------
-- 📊 ARRAY AGGREGATION
-- ----------------------------------------------------------------------------
/*
| Function          | Description      | Example                    | Spark Ver |
|-------------------|------------------|----------------------------|-----------|
| array_min(array)  | Minimum value    | array_min([3,1,2]) → 1     | 2.4+      |
| array_max(array)  | Maximum value    | array_max([3,1,2]) → 3     | 2.4+      |
| array_sum(array)  | Sum of elements  | array_sum([1,2,3]) → 6     | 3.4+      |
| array_avg(array)  | Average          | array_avg([1,2,3]) → 2.0   | 3.4+      |
*/

-- Examples:
SELECT array_min(array(5, 2, 8, 1, 9)) AS min_val;  -- 1
SELECT array_max(array(5, 2, 8, 1, 9)) AS max_val;  -- 9

-- For older Spark versions, use aggregate for sum/avg:
SELECT aggregate(array(1, 2, 3, 4, 5), 0, (acc, x) -> acc + x) AS sum_val;  -- 15


-- ----------------------------------------------------------------------------
-- 🔧 ARRAY CREATION
-- ----------------------------------------------------------------------------
/*
| Function                    | Description          | Example                        |
|-----------------------------|----------------------|--------------------------------|
| array(val1, val2, ...)      | Create array         | array(1, 2, 3) → [1,2,3]       |
| sequence(start, end, step)  | Generate sequence    | sequence(1, 10, 2) → [1,3,5,7,9]|
| array_repeat(val, n)        | Repeat value N times | array_repeat(0, 5) → [0,0,0,0,0]|
*/

-- Examples:
SELECT array(1, 2, 3, 4, 5) AS manual_array;
SELECT sequence(1, 10, 2) AS odd_numbers;           -- [1, 3, 5, 7, 9]
SELECT sequence(0, 100, 10) AS tens;                -- [0, 10, 20, 30, ..., 100]
SELECT array_repeat('NA', 5) AS placeholders;       -- [NA, NA, NA, NA, NA]


-- ============================================================================
-- 📋 COMPLETE ARRAY FUNCTIONS QUICK REFERENCE
-- ============================================================================
/*
ACCESS & SEARCH:
    element_at, array_contains, array_position, size

MODIFICATION:
    array_append, array_prepend, array_insert, array_remove, array_compact

MANIPULATION:
    array_distinct, array_sort, array_reverse, shuffle, slice

COMBINATION:
    array_union, array_intersect, array_except, concat, flatten, arrays_zip

HIGHER-ORDER (Lambda):
    transform, filter, aggregate, exists, forall, zip_with

AGGREGATION:
    array_min, array_max, array_sum, array_avg

CREATION:
    array, sequence, array_repeat

EXPLODE (Rows):
    explode, explode_outer, posexplode, posexplode_outer, inline

COLLECT (Aggregate to Array):
    collect_list, collect_set
*/


-- ============================================================================
-- 🔄 PYSPARK & SPARK SCALA DATAFRAME API EQUIVALENTS
-- ============================================================================
/*
All functions available via:
    PySpark: from pyspark.sql import functions as F
    Scala:   import org.apache.spark.sql.functions._

--------------------------------------------------------------------------------
SPARK SQL vs PYSPARK vs SCALA
--------------------------------------------------------------------------------

| Spark SQL                | PySpark DataFrame API                      | Scala DataFrame API                        |
|--------------------------|--------------------------------------------|--------------------------------------------|
| array(1, 2, 3)           | F.array(F.lit(1), F.lit(2), F.lit(3))      | array(lit(1), lit(2), lit(3))              |
| element_at(arr, 2)       | F.element_at("arr", 2)                     | element_at(col("arr"), 2)                  |
| array_contains(arr, v)   | F.array_contains("arr", v)                 | array_contains(col("arr"), v)              |
| size(arr)                | F.size("arr")                              | size(col("arr"))                           |
| array_distinct(arr)      | F.array_distinct("arr")                    | array_distinct(col("arr"))                 |
| array_sort(arr)          | F.array_sort("arr")                        | array_sort(col("arr"))                     |
| array_union(a, b)        | F.array_union("a", "b")                    | array_union(col("a"), col("b"))            |
| array_intersect(a, b)    | F.array_intersect("a", "b")                | array_intersect(col("a"), col("b"))        |
| array_except(a, b)       | F.array_except("a", "b")                   | array_except(col("a"), col("b"))           |
| flatten(arr)             | F.flatten("arr")                           | flatten(col("arr"))                        |
| explode(arr)             | F.explode("arr")                           | explode(col("arr"))                        |
| posexplode(arr)          | F.posexplode("arr")                        | posexplode(col("arr"))                     |
| collect_list(col)        | F.collect_list("col")                      | collect_list(col("col"))                   |
| collect_set(col)         | F.collect_set("col")                       | collect_set(col("col"))                    |
| sequence(1, 10)          | F.sequence(F.lit(1), F.lit(10))            | sequence(lit(1), lit(10))                  |
| array_min(arr)           | F.array_min("arr")                         | array_min(col("arr"))                      |
| array_max(arr)           | F.array_max("arr")                         | array_max(col("arr"))                      |

--------------------------------------------------------------------------------
HIGHER-ORDER FUNCTIONS
--------------------------------------------------------------------------------

| Spark SQL                    | PySpark                                    | Scala                                      |
|------------------------------|--------------------------------------------|--------------------------------------------|
| transform(arr, x -> x*2)     | F.transform("arr", lambda x: x * 2)        | transform(col("arr"), x => x * 2)          |
| filter(arr, x -> x > 0)      | F.filter("arr", lambda x: x > 0)           | filter(col("arr"), x => x > 0)             |
| aggregate(arr, 0, (a,x)->a+x)| F.aggregate("arr", F.lit(0), lambda a,x: a+x)| aggregate(col("arr"), lit(0), (a,x) => a+x)|
| exists(arr, x -> x > 5)      | F.exists("arr", lambda x: x > 5)           | exists(col("arr"), x => x > 5)             |
| forall(arr, x -> x > 0)      | F.forall("arr", lambda x: x > 0)           | forall(col("arr"), x => x > 0)             |
| zip_with(a, b, (x,y)->x+y)   | F.zip_with("a", "b", lambda x,y: x+y)      | zip_with(col("a"), col("b"), (x,y) => x+y) |

--------------------------------------------------------------------------------
COMPLETE EXAMPLES
--------------------------------------------------------------------------------
*/

-- Spark SQL:
SELECT transform(prices, x -> x * 1.1) AS prices_with_tax FROM products;


PySpark:
--------
from pyspark.sql import functions as F

df.select(
    F.transform("prices", lambda x: x * 1.1).alias("prices_with_tax")
)

df.select(F.explode("items").alias("item"))

df.groupBy("user_id").agg(
    F.collect_list("room_type").alias("all_rooms")
)

df.select(
    F.filter("numbers", lambda x: x > 0).alias("positives")
)


Scala:
------
import org.apache.spark.sql.functions._

df.select(
    transform(col("prices"), x => x * 1.1).alias("prices_with_tax")
)

df.select(explode(col("items")).alias("item"))

df.groupBy("user_id").agg(
    collect_list(col("room_type")).alias("all_rooms")
)

df.select(
    filter(col("numbers"), x => x > 0).alias("positives")
)


--------------------------------------------------------------------------------
KEY DIFFERENCES: PySpark vs Scala
--------------------------------------------------------------------------------

| Aspect              | PySpark                          | Scala                              |
|---------------------|----------------------------------|------------------------------------|
| Import              | from pyspark.sql import functions as F | import org.apache.spark.sql.functions._ |
| Prefix              | F.function_name()                | function_name() (no prefix)        |
| Column reference    | F.col("name") or "name"          | col("name") or $"name"             |
| Lambda syntax       | lambda x: x * 2                  | x => x * 2                         |
| Literal values      | F.lit(value)                     | lit(value)                         |
| String to column    | Implicit in many functions       | Must use col() or $                |

Example - Same operation:
    PySpark: df.select(F.col("name"), F.upper(F.col("city")))
    Scala:   df.select(col("name"), upper(col("city")))
    Scala:   df.select($"name", upper($"city"))  // using $ shorthand



