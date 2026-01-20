-- =============================================================================
-- QUERY: Find regions with 5+ consecutive years of sales growth
-- TECHNIQUE: "Gaps and Islands" pattern using ROW_NUMBER difference
-- =============================================================================

-- STEP 1: Get current year sales alongside previous year sales for comparison
WITH consecutive_sales AS (
    SELECT
        region,
        year,
        sales AS cur_sales,
        -- LAG fetches the sales value from the previous row (previous year)
        -- within each region, ordered by year
        LAG(sales, 1) OVER (
            PARTITION BY region
            ORDER BY year
        ) AS prev_sales
    FROM sales
),

-- STEP 2: Identify which years had growth compared to previous year
growth_sales AS (
    SELECT
        region,
        year,
        cur_sales,
        prev_sales,
        -- Flag: 1 = growth year (sales increased), 0 = no growth
        CASE
            WHEN cur_sales > prev_sales THEN 1
            ELSE 0
        END AS is_growth
    FROM consecutive_sales
    -- Exclude first year of each region (no previous year to compare)
    WHERE prev_sales IS NOT NULL
),

-- STEP 3: Assign row numbers ONLY to growth years
-- This is the KEY to the "gaps and islands" technique
growth_groups AS (
    SELECT
        region,
        year,
        -- Sequential number for growth years only (non-growth years are excluded)
        ROW_NUMBER() OVER (
            PARTITION BY region
            ORDER BY year
        ) AS rn
    FROM growth_sales
    WHERE is_growth = 1  -- Only keep years with positive growth
)

-- STEP 4: Group consecutive growth years using (year - row_number) trick
-- 
-- HOW IT WORKS:
-- ┌────────┬──────┬────┬────────────┐
-- │ region │ year │ rn │ year - rn  │  <- Same value = consecutive!
-- ├────────┼──────┼────┼────────────┤
-- │ East   │ 2018 │ 1  │ 2017       │  ← Streak 1 starts
-- │ East   │ 2019 │ 2  │ 2017       │  ← Same group (2017)
-- │ East   │ 2020 │ 3  │ 2017       │  ← Same group (2017)
-- │ East   │ 2023 │ 4  │ 2019       │  ← NEW streak (gap in 2021-2022)
-- │ East   │ 2024 │ 5  │ 2019       │  ← Same group (2019)
-- └────────┴──────┴────┴────────────┘
--
-- When years are consecutive: year increments by 1, rn increments by 1
-- So (year - rn) stays CONSTANT for consecutive years!
-- When there's a gap: year jumps but rn only +1, so (year - rn) changes

SELECT
    region,
    MIN(year) AS start_year,       -- First year of the growth streak
    COUNT(*) AS streak_length      -- How many consecutive growth years
FROM growth_groups
GROUP BY
    region,
    year - rn                      -- Magic formula: groups consecutive years together
HAVING COUNT(*) >= 5               -- Only show streaks of 5+ years
ORDER BY region, start_year;

--------------------------------------

Raw Data (East region):
┌──────┬───────┬─────────┬───────────┐
│ Year │ Sales │ Growth? │ In Result │
├──────┼───────┼─────────┼───────────┤
│ 2015 │ 100   │ -       │ No        │  (no previous year)
│ 2016 │ 90    │ No ↓    │ No        │
│ 2017 │ 110   │ Yes ↑   │ Yes       │  ← Streak starts
│ 2018 │ 120   │ Yes ↑   │ Yes       │
│ 2019 │ 130   │ Yes ↑   │ Yes       │
│ 2020 │ 140   │ Yes ↑   │ Yes       │
│ 2021 │ 150   │ Yes ↑   │ Yes       │  ← 5 consecutive! ✓
│ 2022 │ 145   │ No ↓    │ No        │  ← Streak breaks
│ 2023 │ 160   │ Yes ↑   │ Yes       │  ← New streak (only 2 years)
│ 2024 │ 170   │ Yes ↑   │ Yes       │
└──────┴───────┴─────────┴───────────┘

Output:
┌────────┬────────────┬───────────────┐
│ region │ start_year │ streak_length │
├────────┼────────────┼───────────────┤
│ East   │ 2017       │ 5             │
└────────┴────────────┴───────────────┘
--------------------
KEY CONCEPT DOUBT
--------------------
Usually we say group by need same column what are in select, how it is wokring in above final query

The Rule You Know
"Every non-aggregated column in SELECT must appear in GROUP BY"

This is a one-way rule:

SELECT → GROUP BY: Non-aggregated SELECT columns MUST be in GROUP BY ✅
GROUP BY → SELECT: GROUP BY columns do NOT need to be in SELECT ✅

SELECT
    region,                    -- ← Non-aggregated, must be in GROUP BY ✅
    MIN(year) AS start_year,   -- ← Aggregated (MIN), doesn't need GROUP BY ✅
    COUNT(*) AS streak_length  -- ← Aggregated (COUNT), doesn't need GROUP BY ✅
FROM growth_groups
GROUP BY
    region,                    -- ← Used in SELECT ✅
    year - rn                  -- ← NOT in SELECT, but that's OK! ✅
