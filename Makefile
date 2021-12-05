lint:
	poetry run flake8 ./src ./tests
	poetry run isort --check --diff ./src ./tests
	poetry run black --check ./src ./tests
format:
	poetry run isort ./src ./tests
	poetry run black ./src ./tests
test:
	@poetry run pytest -v --cov=src
setup-gcs:
	@poetry run python ./scripts/setup_test.py
