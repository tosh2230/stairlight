SELECT
    queries.id,
    queries.name,
    queries.query
FROM
    queries
    INNER JOIN data_sources
        ON queries.data_source_id = data_sources.id
