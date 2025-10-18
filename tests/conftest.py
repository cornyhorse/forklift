"""Pytest configuration for forklift tests."""

import os
import sys
from pathlib import Path

import pytest

# Add the src directory to the Python path for imports
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


def pytest_configure(config):
    """Configure pytest with coverage settings."""
    # Register custom markers to avoid warnings
    config.addinivalue_line("markers", "s3: mark test as requiring S3 access")
    config.addinivalue_line("markers", "integration: mark test as integration test")
    config.addinivalue_line("markers", "slow: mark test as slow running")
    config.addinivalue_line("markers", "mock: mark test as using mocks")
    config.addinivalue_line("markers", "unit: mark test as unit test")
    config.addinivalue_line("markers", "excel: mark test as Excel-related")
    config.addinivalue_line("markers", "fwf: mark test as fixed-width file related")
    config.addinivalue_line("markers", "csv: mark test as CSV-related")
    config.addinivalue_line("markers", "sql: mark test as SQL-related")


def pytest_addoption(parser):
    """Add custom command line options for pytest."""
    parser.addoption(
        "--integration",
        action="store_true",
        default=False,
        help="run integration tests that require real AWS/S3 access",
    )
    parser.addoption(
        "--s3-bucket",
        action="store",
        default=None,
        help="S3 bucket to use for integration tests (overrides .env config)",
    )
    parser.addoption(
        "--no-s3-mock",
        action="store_true",
        default=False,
        help="disable S3 mocking in unit tests (use real S3 operations)",
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

        # Add slow marker to tests that might take longer
        if "integration" in item.nodeid or "slow" in item.name:
            item.add_marker(pytest.mark.slow)

        # Add integration marker to integration tests
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)


def _load_credentials_from_env():
    """Load S3 credentials from ~/.credentials/.env file."""
    try:
        from dotenv import load_dotenv

        # Load from ~/.credentials/.env
        credentials_file = Path.home() / ".credentials" / ".env"
        if credentials_file.exists():
            load_dotenv(credentials_file)
            return {
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
                "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
                "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
                "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
                "endpoint_url": os.getenv("AWS_ENDPOINT_URL"),
            }
        else:
            # Fallback to system environment variables
            return {
                "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
                "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
                "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
                "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
                "endpoint_url": os.getenv("AWS_ENDPOINT_URL"),
            }
    except ImportError:
        # If python-dotenv is not available, use system environment variables
        return {
            "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", ""),
            "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", ""),
            "aws_session_token": os.getenv("AWS_SESSION_TOKEN"),
            "region_name": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            "endpoint_url": os.getenv("AWS_ENDPOINT_URL"),
        }


@pytest.fixture(scope="session")
def s3_test_bucket(request):
    """Provide S3 test bucket name from command line or .env file."""
    bucket = request.config.getoption("--s3-bucket")
    if bucket:
        return bucket

    # Load from .env file or environment
    _load_credentials_from_env()  # This loads the .env file
    return os.getenv("S3_TEST_BUCKET", "cornyhorse-data")


@pytest.fixture(scope="session")
def aws_credentials():
    """Provide AWS credentials from ~/.credentials/.env file."""
    credentials = _load_credentials_from_env()

    # Check if we have valid credentials
    if not credentials.get("aws_access_key_id") or not credentials.get("aws_secret_access_key"):
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
def s3_mock_conditional(request, use_s3_mock):
    """Conditionally provide S3 mocking based on configuration."""
    if use_s3_mock:
        # Use mocking
        from unittest.mock import MagicMock, patch

        with patch("boto3.Session") as mock_session:
            mock_client = MagicMock()
            mock_session.return_value.client.return_value = mock_client

            # Mock the S3StreamingClient creation instead of the property
            with patch("forklift.io.s3_streaming.get_s3_client") as mock_get_s3_client:
                mock_s3_streaming_client = MagicMock()
                mock_get_s3_client.return_value = mock_s3_streaming_client
                yield mock_session, mock_s3_streaming_client
    else:
        # Use real S3 - load credentials only when needed
        credentials = _load_credentials_from_env()
        if not credentials.get("aws_access_key_id") or not credentials.get("aws_secret_access_key"):
            pytest.skip("Real S3 testing requested but AWS credentials not available")

        # Return None to indicate no mocking
        yield None, None
