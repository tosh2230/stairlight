WITH

test_A AS (
	SELECT *
	FROM
		project.dataset.table_test_A
),


test_B AS (
	SELECT *
	FROM
		project.dataset.table_test_B AS test_B
	INNER JOIN test_A ON test_B.id = test_A.id
)

SELECT
    *
FROM project.dataset.table_test_C AS test_C
	LEFT JOIN test_B ON test_C.id = test_B.id
