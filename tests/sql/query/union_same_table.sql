SELECT
    ride_id,
    'max' as aggregation,
    MAX(timestamp) as time
FROM
    test_project.beam_streaming.taxirides_realtime
GROUP BY
    ride_id
UNION ALL
SELECT
    ride_id,
    'min' as aggregation,
    MIN(timestamp) as time
FROM
    test_project.beam_streaming.taxirides_realtime
GROUP BY
    ride_id
ORDER BY
    ride_id, aggregation
