lint:
	poetry run flake8 ./stairlight ./tests
	poetry run isort --check --diff ./stairlight ./tests
	poetry run black --check ./stairlight ./tests
format:
	poetry run isort ./stairlight ./tests
	poetry run black ./stairlight ./tests
