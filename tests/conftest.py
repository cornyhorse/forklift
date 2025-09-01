"""Pytest configuration for forklift tests."""

import pytest


def pytest_addoption(parser):
    """Add custom command line options for pytest."""
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run integration tests that require real AWS/S3 access"
    )
    parser.addoption(
        "--s3-bucket",
        action="store",
        default=None,
        help="S3 bucket to use for integration tests (overrides mattstash config)"
    )


def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test requiring real AWS access"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection based on command line options."""
    if config.getoption("--integration"):
        # Integration tests are explicitly requested, run them
        return

    # Skip integration tests by default
    skip_integration = pytest.mark.skip(reason="need --integration option to run")
    for item in items:
        if "integration" in item.keywords:
            item.add_marker(skip_integration)


@pytest.fixture(scope="session")
def s3_test_bucket(request):
    """Provide S3 test bucket name from command line or mattstash."""
    bucket = request.config.getoption("--s3-bucket")
    if bucket:
        return bucket

    try:
        from mattstash import Stash
        stash = Stash()
        return stash.get('S3_TEST_BUCKET', 'forklift-test-bucket')
    except ImportError:
        return 'forklift-test-bucket'


@pytest.fixture(scope="session")
def aws_credentials():
    """Provide AWS credentials from mattstash if available."""
    try:
        from mattstash import Stash
        stash = Stash()
        return {
            'aws_access_key_id': stash.get('AWS_ACCESS_KEY_ID'),
            'aws_secret_access_key': stash.get('AWS_SECRET_ACCESS_KEY'),
            'aws_session_token': stash.get('AWS_SESSION_TOKEN'),
            'region_name': stash.get('AWS_DEFAULT_REGION', 'us-east-1')
        }
    except ImportError:
        pytest.skip("mattstash not available for AWS credentials")
