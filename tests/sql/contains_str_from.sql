WITH cte AS (
    SELECT
        *
    FROM
        test.cte
)

SELECT
    *
FROM
    test.main
LEFT JOIN
    test.sub ON
        sub.id = main.id
        AND main.date = sub.date
LEFT JOIN
    cte ON
        cte.id = main.id
        AND main.date
        BETWEEN cte.date_from AND cte.date_to
WHERE
    sub.name = cte.name
;
