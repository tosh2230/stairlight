WITH c AS (
    SELECT
        test_id,
        col_c
    FROM
        {{ params.sub_table_01 }}
    WHERE
        0 = 0
),
d AS (
    SELECT
        test_id,
        col_d
    FROM
        {{ params.sub_table_02 }}
    WHERE
        0 = 0
)

SELECT
    *
FROM
    {{ params.main_table }} AS b
    INNER JOIN c
        ON b.test_id = c.test_id
    INNER JOIN d
        ON b.test_id = d.test_id
WHERE
    1 = 1
