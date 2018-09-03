
.PHONY: test install fmt

install:
	pipenv install

test:
	pipenv run mypy simperium/
	pipenv run pytest

fmt:
	pipenv run black simperium examples
