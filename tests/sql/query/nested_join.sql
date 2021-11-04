SELECT
    *
FROM
    PROJECT_B.DATASET_B.TABLE_B AS b
    INNER JOIN (
        SELECT
            test_id,
            col_c
        FROM
            PROJECT_C.DATASET_C.TABLE_C
        WHERE
            0 = 0
    ) AS c
        ON b.test_id = c.test_id
    INNER JOIN (
        SELECT
            d.test_id,
            d.col_d
        FROM
            PROJECT_d.DATASET_d.TABLE_d d
            LEFT OUTER JOIN PROJECT_e.DATASET_e.TABLE_e
            AS e
                ON d.test_id = e.test_id
        WHERE
            0 = 0
    )
        ON b.test_id = d.test_id
WHERE
    1 = 1
