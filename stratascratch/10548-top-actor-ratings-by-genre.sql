Top Actor Ratings by Genre

Find the top actors based on their average movie rating within the genre they appear in most frequently.
•  For each actor, determine their most frequent genre (i.e., the one they’ve appeared in the most).
•   If there is a tie in genre count, select the genre where the actor has the highest average rating.
•   If there is still a tie in both count and rating, include all tied genres for that actor.


Rank all resulting actor + genre pairs in descending order by their average movie rating.
•  Return all pairs that fall within the top 3 ranks (not simply the top 3 rows), including ties.
•  Do not skip rank numbers — for example, if two actors are tied at rank 1, the next rank is 2 (not 3).


-- =============================================================================
-- QUERY: Find Top 3 Actors ranked by their best genre's average rating
-- GOAL: For each actor, find their "signature genre" (most appearances), 
--       then rank actors by how well they perform in their signature genre
-- =============================================================================

-- STEP 1: Count movies per actor-genre combination and calculate average rating
-- This gives us: "How many movies did each actor do in each genre, and how good were they?"
with genre_count as (
    select 
        actor_name, 
        genre, 
        count(*) as no_per_genre,           -- Number of movies in this genre
        avg(movie_rating) as avg_rating     -- Average rating in this genre
    from top_actors_rating
    group by actor_name, genre
),

-- STEP 2: Find each actor's "signature genre" (the genre they appear in most)
-- If tied on count, use average rating as tiebreaker
-- 
-- Example:
-- ┌─────────────┬──────────┬──────────────┬────────────┬──────────────────┐
-- │ actor_name  │ genre    │ no_per_genre │ avg_rating │ actor_genre_rank │
-- ├─────────────┼──────────┼──────────────┼────────────┼──────────────────┤
-- │ Tom Hanks   │ Drama    │ 15           │ 8.2        │ 1                │ ← Best genre
-- │ Tom Hanks   │ Comedy   │ 10           │ 7.5        │ 2                │
-- │ Tom Hanks   │ Action   │ 5            │ 6.8        │ 3                │
-- └─────────────┴──────────┴──────────────┴────────────┴──────────────────┘
genre_tied as (
    select 
        *, 
        dense_rank() over(
            partition by actor_name                           -- For each actor separately
            order by no_per_genre desc, avg_rating desc       -- Most movies first, then best rated
        ) as actor_genre_rank
    from genre_count
),

-- STEP 3: Keep only each actor's #1 genre, then rank ALL actors by that genre's avg rating
-- This answers: "Among all actors' best genres, who has the highest average rating?"
--
-- Example:
-- ┌─────────────────┬──────────┬────────────┬────────────┐
-- │ actor_name      │ genre    │ avg_rating │ actor_rank │
-- ├─────────────────┼──────────┼────────────┼────────────┤
-- │ Daniel Day-Lewis│ Drama    │ 8.9        │ 1          │ ← Best actor in their signature genre
-- │ Meryl Streep    │ Drama    │ 8.7        │ 2          │
-- │ Tom Hanks       │ Drama    │ 8.2        │ 3          │
-- │ Jim Carrey      │ Comedy   │ 7.8        │ 4          │
-- └─────────────────┴──────────┴────────────┴────────────┘
final_tab as (
    select 
        actor_name, 
        genre, 
        avg_rating, 
        dense_rank() over(
            order by avg_rating desc      -- Rank all actors globally by their best genre's rating
        ) as actor_rank
    from genre_tied
    where actor_genre_rank = 1            -- Only keep each actor's signature (top) genre
    order by avg_rating desc
)

-- STEP 4: Return only the Top 3 actors
-- These are the actors who perform best in their most-frequently-acted genre
select *
from final_tab
where actor_rank <= 3;

----LEARNINGS----


1. How order on 2 columns work

ORDER BY no_per_genre DESC, avg_rating DESC

  Step-by-step sorting

  Step 1️⃣ Group by no_per_genre
no_per_genre = 2 → drama, sci-fi
no_per_genre = 1 → comedy, thriller, action, romance

Step 2️⃣ Sort inside each group by avg_rating DESC
no_per_genre = 2:
  drama (9.0)
  sci-fi (8.95)

no_per_genre = 1:
  comedy (9.0)
  thriller (9.0)
  action (9.0)
  romance (8.9)

Final order:
| genre    | no_per_genre | avg_rating |
| -------- | ------------ | ---------- |
| drama    | 2            | 9.0        |
| sci-fi   | 2            | 8.95       |
| comedy   | 1            | 9.0        |
| thriller | 1            | 9.0        |
| action   | 1            | 9.0        |
| romance  | 1            | 8.9        |



------------------

2. WHEN TO USE GROUP BY vs ANALYTICAL FUNCTIONS

--------------------------------------------------------
  TRICK -

  When you move from GROUP BY to analytic functions,
the grouping columns usually become PARTITION BY columns.
 ------------------------------------------------------- 
  
  GROUP BY

👉 Reduces rows

Produces one row per group

Collapses detail

Loses row-level identity

Analytic (Window) Functions

👉 Preserve rows

Keep every original row

Add metrics alongside detail

Enable comparisons, ranking, running totals
