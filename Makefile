VERSION := $(shell grep -E '^version = *.' pyproject.toml | sed -e 's/version = //g')
EXTRAS = gcs,redash,dbt-bigquery,s3

.PHONY: lint type-check format install exec check test install-test test-report setup-test

lint:
	poetry run flake8 src tests
	poetry run isort --check --diff src tests
	poetry run black --check src tests
type-check:
	poetry run mypy src tests
format:
	poetry run isort src tests
	poetry run black src tests
install:
	@poetry build
	@poetry run pip install "dist/stairlight-$(VERSION).tar.gz[$(EXTRAS)]"
exec:
	@make install
	@poetry run python -m stairlight -c tests/config
check:
	@make install
	@poetry run python -m stairlight check -c tests/config
test:
	@poetry run pytest tests/stairlight -v --cov=src
install-test:
	@make install
	@make test
test-report:
	-@rm -r htmlcov
	@poetry run pytest tests/stairlight -v --cov=src --cov-report=html
setup-test:
	@poetry run python scripts/setup_test.py
