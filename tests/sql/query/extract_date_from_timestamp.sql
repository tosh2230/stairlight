WITH today AS (
    SELECT CURRENT_TIMESTAMP() AS _date
),
last_week AS (
    SELECT TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY) AS _date
)

SELECT
    DATE_DIFF(
        EXTRACT(DATE FROM today._date),
        EXTRACT(DATE FROM last_week._date),
        DAY
    ) AS timediff,
FROM
    today, last_week
;
