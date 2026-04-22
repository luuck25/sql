-- ============================================================================
-- LeetCode 1479: Sales by Day of the Week
-- ============================================================================
-- Problem:
-- You have two tables:
--
--   Orders(order_id, customer_id, order_date, item_id, quantity)
--     - (order_id, item_id) is the primary key.
--
--   Items(item_id, item_name, item_category)
--     - item_id is the primary key.
--
-- Write a query to report how many units of each item category were ordered
-- on each day of the week (Monday through Sunday).
--
-- Return: category, Monday, Tuesday, Wednesday, Thursday, Friday, Saturday, Sunday
-- Order by category ASC.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE Items (
    item_id       INT PRIMARY KEY,
    item_name     VARCHAR(30),
    item_category VARCHAR(30)
);

CREATE TABLE Orders (
    order_id    INT,
    customer_id INT,
    order_date  DATE,
    item_id     INT,
    quantity    INT,
    PRIMARY KEY (order_id, item_id)
);

INSERT INTO Items (item_id, item_name, item_category) VALUES
(1, 'LC Alg. Book', 'Book'),
(2, 'LC DB. Book', 'Book'),
(3, 'LC SmartPhone', 'Phone'),
(4, 'LC Phone 2020', 'Phone'),
(5, 'LC SmartGlass', 'Glasses'),
(6, 'LC T-Shirt', 'T-Shirt');

INSERT INTO Orders (order_id, customer_id, order_date, item_id, quantity) VALUES
(1, 1, '2020-06-01', 1, 10),
(2, 1, '2020-06-08', 2, 10),
(3, 2, '2020-06-02', 1, 5),
(4, 3, '2020-06-03', 3, 5),
(5, 4, '2020-06-04', 4, 1),
(6, 4, '2020-06-05', 5, 5),
(7, 5, '2020-06-05', 1, 10),
(8, 5, '2020-06-14', 4, 5),
(9, 5, '2020-06-21', 3, 5);

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- Items:
-- | item_id | item_name      | item_category |
-- |---------|----------------|---------------|
-- | 1       | LC Alg. Book   | Book          |
-- | 2       | LC DB. Book    | Book          |
-- | 3       | LC SmartPhone  | Phone         |
-- | 4       | LC Phone 2020  | Phone         |
-- | 5       | LC SmartGlass  | Glasses       |
-- | 6       | LC T-Shirt     | T-Shirt       |
--
-- Orders:
-- | order_id | customer_id | order_date | item_id | quantity |
-- |----------|-------------|------------|---------|----------|
-- | 1        | 1           | 2020-06-01 | 1       | 10       |  ← Monday
-- | 2        | 1           | 2020-06-08 | 2       | 10       |  ← Monday
-- | 3        | 2           | 2020-06-02 | 1       | 5        |  ← Tuesday
-- | 4        | 3           | 2020-06-03 | 3       | 5        |  ← Wednesday
-- | 5        | 4           | 2020-06-04 | 4       | 1        |  ← Thursday
-- | 6        | 4           | 2020-06-05 | 5       | 5        |  ← Friday
-- | 7        | 5           | 2020-06-05 | 1       | 10       |  ← Friday
-- | 8        | 5           | 2020-06-14 | 4       | 5        |  ← Sunday
-- | 9        | 5           | 2020-06-21 | 3       | 5        |  ← Sunday

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================
-- Step 1: JOIN Orders with Items to get category per order.
-- Step 2: Use DATENAME(WEEKDAY, order_date) to get the day name.
-- Step 3: Pivot using SUM(CASE WHEN day = 'Monday' THEN quantity ELSE 0 END).
-- Step 4: Must include ALL categories (even with no orders) → use Items as base.
--         LEFT JOIN ensures categories with 0 orders still appear.

-- ============================================================================
-- EXPECTED OUTPUT (ordered by category)
-- ============================================================================
-- | category | Monday | Tuesday | Wednesday | Thursday | Friday | Saturday | Sunday |
-- |----------|--------|---------|-----------|----------|--------|----------|--------|
-- | Book     | 20     | 5       | 0         | 0        | 10     | 0        | 0      |
-- | Glasses  | 0      | 0       | 0         | 0        | 5      | 0        | 0      |
-- | Phone    | 0      | 0       | 5         | 1        | 0      | 0        | 10     |
-- | T-Shirt  | 0      | 0       | 0         | 0        | 0      | 0        | 0      |

-- ============================================================================
-- KEY CONCEPTS
-- ============================================================================
-- 1. DATENAME(WEEKDAY, date) returns day name as string ('Monday', etc.).
--    Alternative: DATEPART(WEEKDAY, date) returns int (1=Sunday by default).
--
-- 2. Conditional aggregation for pivoting:
--    SUM(CASE WHEN day = 'Monday' THEN quantity ELSE 0 END) AS Monday
--    One CASE per column — this is the standard row-to-column pivot technique.
--
-- 3. LEFT JOIN from Items to Orders:
--    Categories with no orders (T-Shirt) must still appear with all zeros.
--    LEFT JOIN ensures every category row survives.
--
-- 4. ISNULL / COALESCE not needed here because ELSE 0 in CASE handles NULLs.
--    SUM of zeros = 0, not NULL.

-- DROP TABLE Orders;
-- DROP TABLE Items;

-- ============================================================================
-- SQL SERVER SOLUTION
-- ============================================================================

SELECT
    i.item_category AS category,
    SUM(CASE WHEN DATENAME(WEEKDAY, o.order_date) = 'Monday'    THEN o.quantity ELSE 0 END) AS Monday,
    SUM(CASE WHEN DATENAME(WEEKDAY, o.order_date) = 'Tuesday'   THEN o.quantity ELSE 0 END) AS Tuesday,
    SUM(CASE WHEN DATENAME(WEEKDAY, o.order_date) = 'Wednesday' THEN o.quantity ELSE 0 END) AS Wednesday,
    SUM(CASE WHEN DATENAME(WEEKDAY, o.order_date) = 'Thursday'  THEN o.quantity ELSE 0 END) AS Thursday,
    SUM(CASE WHEN DATENAME(WEEKDAY, o.order_date) = 'Friday'    THEN o.quantity ELSE 0 END) AS Friday,
    SUM(CASE WHEN DATENAME(WEEKDAY, o.order_date) = 'Saturday'  THEN o.quantity ELSE 0 END) AS Saturday,
    SUM(CASE WHEN DATENAME(WEEKDAY, o.order_date) = 'Sunday'    THEN o.quantity ELSE 0 END) AS Sunday
FROM Items i
LEFT JOIN Orders o ON i.item_id = o.item_id
GROUP BY i.item_category
ORDER BY i.item_category;
