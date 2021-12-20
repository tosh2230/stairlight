lint:
	poetry run flake8 ./src ./tests
	poetry run isort --check --diff ./src ./tests
	poetry run black --check ./src ./tests
format:
	poetry run isort ./src ./tests
	poetry run black ./src ./tests
exec:
	@poetry install
	@poetry run python -m stairlight -c config
test:
	@poetry install
	@poetry run pytest -v --cov=src
test-report:
	@rm -r ./htmlcov
	@poetry install
	@poetry run pytest -v --cov=src --cov-report=html
setup-gcs:
	@poetry run python ./scripts/setup_test.py
