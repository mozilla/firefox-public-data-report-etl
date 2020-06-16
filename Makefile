.PHONY: help test build

help:
	@echo "  lint -  check style with flake8"
	@echo "  test -  run tests quickly with the default Python version"
	@echo "  build - build the docker image"

lint:
	docker run -t firefox-public-data-report-etl:0.1 flake8 public_data_report tests --max-line-length 100

test:
	docker run -t firefox-public-data-report-etl:0.1 tox

build:
	docker build -t firefox-public-data-report-etl:0.1 .
