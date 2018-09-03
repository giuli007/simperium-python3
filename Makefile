
.PHONY: test install format

install:
	pipenv install

test:
	pipenv run mypy simperium/
	pipenv run pytest

format:
	pipenv run black simperium examples
