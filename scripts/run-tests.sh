pip install -e . && pytest --cache-clear &&  pytest -q --cov=forklift --cov-report=html


# OR OMIT integration tests
pip install -e . \
  && pytest --cache-clear \
  && pytest -q --cov=forklift --cov-report=html --cov-omit="tests/integration-tests/*"