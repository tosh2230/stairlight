lint:
	poetry run flake8 ./src ./tests
	poetry run isort --check --diff ./src ./tests
	poetry run black --check ./src ./tests
format:
	poetry run isort ./src ./tests
	poetry run black ./src ./tests
exec:
	poetry run python -B ./src/main.py -c config
test:
	@poetry run pytest -v --cov=src
test-report:
	@rm -r ./htmlcov
	@poetry run pytest -v --cov=src --cov-report=html
setup-gcs:
	@poetry run python ./scripts/setup_test.py
