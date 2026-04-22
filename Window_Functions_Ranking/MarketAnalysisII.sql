-- ============================================================================
-- LeetCode 1159: Market Analysis II
-- ============================================================================
-- Problem:
-- You have three tables:
--
--   Users(user_id, join_date, favorite_brand)
--     - Each row is a user with the date they joined and their favorite brand.
--
--   Orders(order_id, order_date, item_id, buyer_id, seller_id)
--     - Each row is an order. buyer_id and seller_id are user_ids.
--
--   Items(item_id, item_brand)
--     - Each row is an item with its brand.
--
-- Write a query to find for each user whether the brand of the SECOND item
-- they sold (by order_date) is their favorite brand.
--
-- If a user sold less than 2 items, report "no" for that user.
-- If the brand of their 2nd sold item matches their favorite_brand → "yes"
-- Otherwise → "no"
--
-- Return: seller_id, 2nd_item_fav_brand ("yes" or "no")
-- Order by seller_id.
-- ============================================================================

-- ============================================================================
-- DDL & SAMPLE DATA
-- ============================================================================

CREATE TABLE Users (
    user_id        INT PRIMARY KEY,
    join_date      DATE NOT NULL,
    favorite_brand VARCHAR(50)
);

CREATE TABLE Items (
    item_id    INT PRIMARY KEY,
    item_brand VARCHAR(50)
);

CREATE TABLE Orders (
    order_id    INT PRIMARY KEY,
    order_date  DATE NOT NULL,
    item_id     INT,
    buyer_id    INT,
    seller_id   INT,
    FOREIGN KEY (item_id) REFERENCES Items(item_id),
    FOREIGN KEY (buyer_id) REFERENCES Users(user_id),
    FOREIGN KEY (seller_id) REFERENCES Users(user_id)
);

INSERT INTO Users (user_id, join_date, favorite_brand) VALUES
(1, '2019-01-01', 'Lenovo'),
(2, '2019-02-09', 'Samsung'),
(3, '2019-01-19', 'LG'),
(4, '2019-05-21', 'HP');

INSERT INTO Items (item_id, item_brand) VALUES
(1, 'Samsung'),
(2, 'Lenovo'),
(3, 'LG'),
(4, 'HP');

INSERT INTO Orders (order_id, order_date, item_id, buyer_id, seller_id) VALUES
(1, '2019-08-01', 4, 1, 2),   -- seller 2 sells HP
(2, '2019-08-02', 2, 1, 3),   -- seller 3 sells Lenovo
(3, '2019-08-03', 3, 2, 3),   -- seller 3 sells LG (2nd item)
(4, '2019-08-04', 1, 4, 2),   -- seller 2 sells Samsung (2nd item)
(5, '2019-08-04', 1, 3, 4);   -- seller 4 sells Samsung

-- ============================================================================
-- SAMPLE DATA REFERENCE
-- ============================================================================
-- Users:
-- | user_id | join_date  | favorite_brand |
-- |---------|------------|----------------|
-- | 1       | 2019-01-01 | Lenovo         |
-- | 2       | 2019-02-09 | Samsung        |
-- | 3       | 2019-01-19 | LG             |
-- | 4       | 2019-05-21 | HP             |
--
-- Items:
-- | item_id | item_brand |
-- |---------|------------|
-- | 1       | Samsung    |
-- | 2       | Lenovo     |
-- | 3       | LG         |
-- | 4       | HP         |
--
-- Orders (sorted by seller and date):
-- | order_id | order_date | item_id | seller_id | item_brand |
-- |----------|------------|---------|-----------|------------|
-- | 1        | 2019-08-01 | 4       | 2         | HP         |  ← seller 2, 1st sale
-- | 4        | 2019-08-04 | 1       | 2         | Samsung    |  ← seller 2, 2nd sale
-- | 2        | 2019-08-02 | 2       | 3         | Lenovo     |  ← seller 3, 1st sale
-- | 3        | 2019-08-03 | 3       | 3         | LG         |  ← seller 3, 2nd sale
-- | 5        | 2019-08-04 | 1       | 4         | Samsung    |  ← seller 4, 1st sale

-- ============================================================================
-- STEP-BY-STEP EXPLANATION
-- ============================================================================

-- STEP 1: Rank each seller's orders by date using ROW_NUMBER
-- | seller_id | order_date | item_brand | ROW_NUMBER |
-- |-----------|------------|------------|------------|
-- | 2         | 2019-08-01 | HP         | 1          |
-- | 2         | 2019-08-04 | Samsung    | 2          |  ← 2nd item
-- | 3         | 2019-08-02 | Lenovo     | 1          |
-- | 3         | 2019-08-03 | LG         | 2          |  ← 2nd item
-- | 4         | 2019-08-04 | Samsung    | 1          |

-- STEP 2: Filter for ROW_NUMBER = 2 (2nd sold item)
-- | seller_id | item_brand |
-- |-----------|------------|
-- | 2         | Samsung    |
-- | 3         | LG         |
-- (Sellers 1 and 4 have no 2nd item)

-- STEP 3: Compare with favorite_brand
-- | seller_id | favorite_brand | 2nd_item_brand | match? |
-- |-----------|----------------|----------------|--------|
-- | 1         | Lenovo         | NULL (no 2nd)  | no     |
-- | 2         | Samsung        | Samsung        | yes    |
-- | 3         | LG             | LG             | yes    |
-- | 4         | HP             | NULL (no 2nd)  | no     |

-- ============================================================================
-- EXPECTED OUTPUT
-- ============================================================================
-- | seller_id | 2nd_item_fav_brand |
-- |-----------|--------------------|
-- | 1         | no                 |
-- | 2         | yes                |
-- | 3         | yes                |
-- | 4         | no                 |

-- ============================================================================
-- SQL SERVER SOLUTION
-- ============================================================================

WITH ranked_data AS (
    SELECT seller_id, item_brand
    FROM (
        SELECT 
            o.seller_id, 
            i.item_brand, 
            ROW_NUMBER() OVER (PARTITION BY o.seller_id ORDER BY o.order_date ASC) AS seller_rank
        FROM Orders o 
        JOIN Items i ON o.item_id = i.item_id
    ) TEMP 
    WHERE seller_rank = 2
)
SELECT 
    u.user_id AS seller_id,
    CASE 
        WHEN u.favorite_brand = r.item_brand THEN 'yes'
        ELSE 'no'
    END AS [2nd_item_fav_brand]
FROM Users u
LEFT JOIN ranked_data r ON r.seller_id = u.user_id
ORDER BY u.user_id;

-- ============================================================================
-- ALTERNATIVE: ISNULL approach
-- ============================================================================
-- Instead of CASE, use ISNULL to handle NULLs from the LEFT JOIN:

-- SELECT 
--     u.user_id AS seller_id,
--     ISNULL(
--         CASE WHEN u.favorite_brand = r.item_brand THEN 'yes' END,
--         'no'
--     ) AS [2nd_item_fav_brand]
-- FROM Users u
-- LEFT JOIN ranked_data r ON r.seller_id = u.user_id
-- ORDER BY u.user_id;

-- ============================================================================
-- ALTERNATIVE: Brand comparison in ON clause
-- ============================================================================
-- Move the brand check into the JOIN condition. If brands don't match (or no
-- 2nd item exists), the LEFT JOIN sets r columns to NULL → just check IS NOT NULL.

-- SELECT 
--     u.user_id AS seller_id,
--     CASE 
--         WHEN r.item_brand IS NOT NULL THEN 'yes'
--         ELSE 'no'
--     END AS [2nd_item_fav_brand]
-- FROM Users u
-- LEFT JOIN ranked_data r 
--     ON r.seller_id = u.user_id
--     AND u.favorite_brand = r.item_brand
-- ORDER BY u.user_id;

-- ============================================================================
-- KEY CONCEPT: LEFT JOIN + WHERE trap
-- ============================================================================
-- ❌ WRONG: WHERE u.favorite_brand = r.item_brand
--    → NULLs from LEFT JOIN are filtered out (sellers with < 2 sales disappear)
--    → 'Lenovo' = NULL evaluates to UNKNOWN (falsy) → row dropped
--
-- ✅ FIX 1: Use CASE in SELECT (main solution above)
-- ✅ FIX 2: Use ISNULL wrapper
-- ✅ FIX 3: Move comparison into ON clause, check IS NOT NULL in SELECT

-- DROP TABLE Orders;
-- DROP TABLE Items;
-- DROP TABLE Users;
