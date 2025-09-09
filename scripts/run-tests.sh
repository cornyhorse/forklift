#!/bin/bash

# Run all tests with S3 mocking (default mode) - excludes performance tests
pip install -e . && pytest --cache-clear && pytest -q --cov=forklift --cov-report=html -m "not performance"

# Run all tests including integration tests with real S3 (requires AWS credentials)
# pip install -e . && pytest --cache-clear && pytest -q --cov=forklift --cov-report=html --integration -m "not performance"

# Run tests with real S3 for unit tests AND integration tests
pip install -e . && pytest --cache-clear && pytest -q --cov=forklift --cov-report=html --no-s3-mock --integration -m "not performance"

# Run integration tests WITH S3 mocking (fast, no AWS credentials needed)
# pip install -e . && pytest --cache-clear && pytest -q --cov=forklift --cov-report=html --integration -m "not performance"

# Run ONLY performance tests when you need them
# pip install -e . && pytest --cache-clear && pytest -q --cov=forklift --cov-report=html -m "performance"

# Run ALL tests including performance tests
# pip install -e . && pytest --cache-clear && pytest -q --cov=forklift --cov-report=html

# OR OMIT integration tests entirely (with S3 mocking)
# pip install -e . \
#   && pytest --cache-clear \
#   && pytest -q --cov=forklift --cov-report=html --cov-omit="tests/integration-tests/*"
