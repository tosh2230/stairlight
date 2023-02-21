-- This is c
WITH c AS (
    SELECT
        test_id,
        col_c
    FROM
        PROJECT_C.DATASET_C.TABLE_C
    WHERE
        0 = 0
),
-- This is d
d AS (
    SELECT
        test_id,
        col_d
    FROM
        PROJECT_d.DATASET_d.TABLE_d
    WHERE
        0 = 0
)

SELECT
    *
FROM
    PROJECT_B.DATASET_B.TABLE_B AS b
    INNER JOIN c
        ON b.test_id = c.test_id
    INNER JOIN d
        ON b.test_id = d.test_id
WHERE
    1 = 1
