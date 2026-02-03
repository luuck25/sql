

WITH user_friends_pages AS (
    SELECT
        uf.user_id,
        up.page_id
    FROM users_friends uf
    JOIN users_pages up
        ON uf.friend_id = up.user_id
)

SELECT DISTINCT
    ufp.user_id,
    ufp.page_id
FROM user_friends_pages ufp
WHERE NOT EXISTS (
    SELECT 1
    FROM users_pages up
    WHERE up.user_id = ufp.user_id
      AND up.page_id = ufp.page_id
)
ORDER BY ufp.user_id, ufp.page_id;
