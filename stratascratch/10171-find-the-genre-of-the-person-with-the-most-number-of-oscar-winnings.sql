-- =============================================================================
-- PURPOSE: Find the top genre(s) of the actor(s) with the most Oscar wins
-- =============================================================================

-- CTE 1: Count how many Oscar wins each nominee has
-- -----------------------------------------------------------------------------
WITH winner_count AS (
    SELECT
        nominee,                      -- The person's name (actor/actress)
        COUNT(*) AS winner_counts     -- Total number of Oscar wins
    FROM oscar_nominees
    WHERE winner = TRUE               -- Only count rows where they actually WON
    GROUP BY nominee                  -- One row per person with their win count
),

-- CTE 2: Rank all winners by their win count (highest first)
-- -----------------------------------------------------------------------------
ranked_winners AS (
    SELECT
        ni.name,                      -- Actor/actress name from nominee_information
        ni.top_genre,                 -- Their primary/best-known genre
        wc.winner_counts,             -- Number of Oscar wins (from CTE above)
        
        -- DENSE_RANK: Assigns rank based on win count
        -- - Ties get the same rank (no gaps in ranking)
        -- - ORDER BY winner_counts DESC: Most wins = rank 1
        -- - Tiebreaker: alphabetical by name (ASC)
        DENSE_RANK() OVER (
            ORDER BY wc.winner_counts DESC, ni.name ASC
        ) AS rnk
    FROM nominee_information ni
    JOIN winner_count wc              -- INNER JOIN: Only people who have won
        ON ni.name = wc.nominee       -- Match on name
)

-- FINAL SELECT: Get the top genre of the person(s) with most wins
-- -----------------------------------------------------------------------------
SELECT top_genre
FROM ranked_winners
WHERE rnk = 1;                        -- Only the top-ranked winner(s)
