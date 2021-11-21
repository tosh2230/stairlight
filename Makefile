lint:
	poetry run flake8 ./src ./tests
	poetry run isort --check --diff ./src ./tests
	poetry run black --check ./src ./tests
format:
	poetry run isort ./src ./tests
	poetry run black ./src ./tests
setup-gcs:
	gsutil cp -r ./tests/sql/gcs/* gs://stairlight/sql/
