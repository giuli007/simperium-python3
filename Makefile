
.PHONY: test install

install:
	pipenv install

test:
	pipenv run mypy simperium/
	pipenv run pytest
