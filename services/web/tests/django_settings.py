"""Django settings for the tests: forklift_web.settings with defaults for the test services.

The defaults match tests/integration-tests/services/compose.yaml (PostgreSQL on 15432 with a
login of the tests' own, RustFS on 19000); set the FORKLIFT_* variables to use other services.
The tests create their own bucket and database (pytest-django's test_<name>) and remove both.
"""

import os

_DEFAULTS = {
    "FORKLIFT_SECRET_KEY": "forklift-web-tests-only-not-a-secret",
    "FORKLIFT_DB_HOST": "127.0.0.1",
    "FORKLIFT_DB_PORT": "15432",
    "FORKLIFT_DB_NAME": "forklift_web_test",
    "FORKLIFT_DB_USER": "forklift_web_test",
    "FORKLIFT_DB_PASSWORD": "forklift-web-test-secret",
    "FORKLIFT_S3_ENDPOINT_URL": "http://127.0.0.1:19000",
    "FORKLIFT_S3_ACCESS_KEY_ID": "forklift-test",
    "FORKLIFT_S3_SECRET_ACCESS_KEY": "forklift-test-secret",
    # Replaced by a bucket of the test session's own (conftest.py)
    "FORKLIFT_S3_BUCKET": "forklift-web-tests",
    # A fixed key for the tests only
    "FORKLIFT_SECRETS_KEYS": "q9FqU3cOVQ9v0lj6v7qP1m9t3sVqgV6mE3rW2bKXhYo=",
}
for _name, _value in _DEFAULTS.items():
    os.environ.setdefault(_name, _value)

from forklift_web.settings import *  # noqa: E402,F401,F403

# Hashing passwords slowly protects real accounts, not test fixtures.
PASSWORD_HASHERS = ["django.contrib.auth.hashers.MD5PasswordHasher"]
