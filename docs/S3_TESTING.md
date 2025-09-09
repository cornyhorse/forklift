# S3 Testing Configuration

This document explains how to configure S3 testing in the Forklift project, including the new conditional mocking system.

## Overview

Forklift supports two modes for S3 testing:

1. **Mocked S3 Testing** (default): Uses `unittest.mock` to simulate S3 operations without actual AWS calls
2. **Real S3 Testing**: Uses actual S3 buckets for testing (requires AWS credentials)

## Test Types

### Unit Tests (`tests/unit-tests/test_s3_streaming.py`)
- **Default behavior**: Uses mocking to simulate S3 operations
- **With `--no-s3-mock`**: Uses real S3 operations (requires credentials)

### Integration Tests (`tests/integration-tests/test_s3_integration.py`)
- **Always uses real S3 operations** (requires credentials and `--integration` flag)

## Command Line Options

### Running Tests with Mocking (Default)
```bash
# Run S3 unit tests with mocking (default)
pytest tests/unit-tests/test_s3_streaming.py

# Run all unit tests with mocking
pytest tests/unit-tests/
```

### Running Tests with Real S3
```bash
# Run S3 unit tests with real S3 operations
pytest tests/unit-tests/test_s3_streaming.py --no-s3-mock

# Run integration tests (always use real S3)
pytest tests/integration-tests/test_s3_integration.py --integration

# Run all tests with real S3
pytest --integration --no-s3-mock
```

### Additional Options
```bash
# Specify a custom S3 bucket for testing
pytest --integration --s3-bucket my-test-bucket

# Run integration tests without unit test mocking
pytest --integration --no-s3-mock
```

## AWS Credentials Setup

When using real S3 operations (either with `--no-s3-mock` or `--integration`), you need AWS credentials configured.

### Using mattstash (Recommended)
```bash
# Set up AWS credentials via mattstash
mattstash set AWS_ACCESS_KEY_ID your_access_key
mattstash set AWS_SECRET_ACCESS_KEY your_secret_key
mattstash set AWS_DEFAULT_REGION us-east-1
mattstash set S3_TEST_BUCKET your-test-bucket-name
```

### Using Environment Variables
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_DEFAULT_REGION=us-east-1
export S3_TEST_BUCKET=your-test-bucket-name
```

### Using AWS CLI Configuration
```bash
aws configure
```

## Test Behavior Differences

### Mocked S3 Testing
- **Pros**: Fast, no AWS costs, no network dependencies, predictable behavior
- **Cons**: Doesn't test real S3 integration, may miss AWS-specific issues
- **Use case**: Development, CI/CD pipelines, unit testing

### Real S3 Testing  
- **Pros**: Tests actual S3 integration, catches real-world issues
- **Cons**: Slower, requires AWS credentials, may incur AWS costs
- **Use case**: Integration testing, pre-production validation

## Example Test Configurations

### Development Workflow
```bash
# Fast unit tests during development
pytest tests/unit-tests/test_s3_streaming.py

# Occasional real S3 validation
pytest tests/unit-tests/test_s3_streaming.py --no-s3-mock
```

### CI/CD Pipeline
```bash
# Fast mocked tests for every commit
pytest tests/unit-tests/

# Real S3 integration tests for releases
pytest --integration --no-s3-mock
```

### Local Integration Testing
```bash
# Full integration test suite
pytest tests/integration-tests/ --integration

# Combined unit and integration with real S3
pytest --integration --no-s3-mock
```

## Test Safety Features

### Automatic Cleanup
- Real S3 tests include cleanup mechanisms to abort incomplete uploads
- Test objects use timestamped names to avoid conflicts

### Credential Validation
- Tests automatically skip if AWS credentials are not available
- Clear error messages guide setup when credentials are missing

### Bucket Isolation
- Uses dedicated test buckets (configurable via `S3_TEST_BUCKET`)
- Test objects are prefixed to avoid production data conflicts

## Implementation Details

The conditional mocking system is implemented using pytest fixtures:

- `use_s3_mock`: Session-scoped fixture that determines mocking behavior
- `s3_mock_conditional`: Function-scoped fixture that provides mocked or real clients
- `s3_client_with_mock`: Test-specific fixture that creates appropriate S3 clients

## Troubleshooting

### "AWS credentials not available"
- Ensure credentials are set via mattstash, environment variables, or AWS CLI
- Check that `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` are set

### "Cannot access real S3 bucket"
- Verify the test bucket exists and you have access
- Check that `S3_TEST_BUCKET` is set to a bucket you control
- Ensure IAM permissions allow S3 operations on the test bucket

### Tests skipped in real S3 mode
- Some tests are intentionally skipped in real S3 mode for safety
- These tests require specific S3 objects or may incur costs
- Use mocked mode for comprehensive unit test coverage
