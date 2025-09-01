#!/bin/bash

# Run all tests with S3 mocking (default mode)
pip install -e . && pytest --cache-clear &&  pytest -q --cov=forklift --cov-report=html

# Run all tests including integration tests with real S3 (requires AWS credentials)
# pip install -e . && pytest --cache-clear &&  pytest -q --cov=forklift --cov-report=html --integration

# Run tests with real S3 for unit tests AND integration tests
pip install -e . && pytest --cache-clear &&  pytest -q --cov=forklift --cov-report=html --no-s3-mock --integration

# OR OMIT integration tests entirely (with S3 mocking)
# pip install -e . \
#   && pytest --cache-clear \
#   && pytest -q --cov=forklift --cov-report=html --cov-omit="tests/integration-tests/*"
