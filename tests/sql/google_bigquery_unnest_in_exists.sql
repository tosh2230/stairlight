SELECT
    TABLE_f.id,
    TABLE_f.test_column,
FROM
    PROJECT_d.DATASET_e.TABLE_f
WHERE
    EXISTS (
        SELECT
            *
        FROM
            UNNEST(struct_column.array_column) AS a
        WHERE
            a.id = 1
    )
