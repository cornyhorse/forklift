"""Pytest configuration for forklift tests."""

import pytest
import os
from pathlib import Path


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
        help="S3 bucket to use for integration tests (overrides .env config)"
    )
    parser.addoption(
        "--no-s3-mock",
        action="store_true",
        default=False,
        help="disable S3 mocking in unit tests (use real S3 operations)"
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


def _load_credentials_from_env():
    """Load S3 credentials from ~/.credentials/.env file."""
    try:
        from dotenv import load_dotenv

        # Load from ~/.credentials/.env
        credentials_file = Path.home() / ".credentials" / ".env"
        if credentials_file.exists():
            load_dotenv(credentials_file)
            return {
                'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
                'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
                'aws_session_token': os.getenv('AWS_SESSION_TOKEN'),
                'region_name': os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
                'endpoint_url': os.getenv('AWS_ENDPOINT_URL'),
            }
        else:
            # Fallback to system environment variables
            return {
                'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
                'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
                'aws_session_token': os.getenv('AWS_SESSION_TOKEN'),
                'region_name': os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
                'endpoint_url': os.getenv('AWS_ENDPOINT_URL'),
            }
    except ImportError:
        # If python-dotenv is not available, use system environment variables
        return {
            'aws_access_key_id': os.getenv('AWS_ACCESS_KEY_ID', ''),
            'aws_secret_access_key': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
            'aws_session_token': os.getenv('AWS_SESSION_TOKEN'),
            'region_name': os.getenv('AWS_DEFAULT_REGION', 'us-east-1'),
            'endpoint_url': os.getenv('AWS_ENDPOINT_URL'),
        }


@pytest.fixture(scope="session")
def s3_test_bucket(request):
    """Provide S3 test bucket name from command line or .env file."""
    bucket = request.config.getoption("--s3-bucket")
    if bucket:
        return bucket

    # Load from .env file or environment
    _load_credentials_from_env()  # This loads the .env file
    return os.getenv('S3_TEST_BUCKET', 'cornyhorse-data')


@pytest.fixture(scope="session")
def aws_credentials():
    """Provide AWS credentials from ~/.credentials/.env file."""
    credentials = _load_credentials_from_env()

    # Check if we have valid credentials
    if not credentials.get('aws_access_key_id') or not credentials.get('aws_secret_access_key'):
        pytest.skip("AWS credentials not available in ~/.credentials/.env or environment variables")

    return credentials


@pytest.fixture(scope="session")
def use_s3_mock(request):
    """Determine whether to use S3 mocking based on command line flags."""
    # If --no-s3-mock is set, don't use mocking
    if request.config.getoption("--no-s3-mock"):
        return False

    # If --integration is set, don't use mocking (for consistency)
    if request.config.getoption("--integration"):
        return False

    # Default: use mocking for unit tests
    return True


@pytest.fixture(scope="function")
def s3_mock_conditional(request, use_s3_mock, aws_credentials):
    """Conditionally provide S3 mocking based on configuration."""
    if use_s3_mock:
        # Use mocking
        from unittest.mock import patch, MagicMock
        with patch('boto3.Session') as mock_session:
            mock_client = MagicMock()
            mock_session.return_value.client.return_value = mock_client
            yield mock_session, mock_client
    else:
        # Use real S3 - check if credentials are available
        if not aws_credentials.get('aws_access_key_id') or not aws_credentials.get('aws_secret_access_key'):
            pytest.skip("Real S3 testing requested but AWS credentials not available")

        # Return None to indicate no mocking
        yield None, None
