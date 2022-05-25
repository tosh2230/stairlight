WITH

test_A AS (
	SELECT *
	FROM
		project.dataset.table_test_A -- table_test_B
),

test_B AS (
	SELECT *
	FROM
		project.dataset.table_test_B AS test_B
    INNER JOIN test_A ON test_B.id = test_A.id
),

test_C AS (
	SELECT *
	FROM
		project.dataset.table_test_C AS test_C
    LEFT JOIN test_B ON test_C.id = test_B.id
)

SELECT
    *
--project.dataset.table_test_D
FROM project.dataset.table_test_D AS test_D
	INNER JOIN test_C ON test_D.id = test_C.id
