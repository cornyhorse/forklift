# Forklift Developer Scripts Documentation

This directory contains shell scripts for common development tasks. All scripts include colored output, error handling, and comprehensive help documentation.

## Quick Reference

| Script | Purpose | Key Flags |
|--------|---------|-----------|
| `run-tests.sh` | Test runner with coverage by default | `--integration`, `--no-coverage`, `--no-html`, `--performance` |
| `lint.sh` | Code formatting and linting | `--apply-black`, `--black-only`, `--flake8-only` |
| `manage-databases.sh` | Database container management | `start`, `stop`, `wipe`, `status`, `logs` |
| `setup-dev.sh` | Development environment bootstrap | No flags (setup script) |

---

## 🧪 `run-tests.sh` - Comprehensive Test Runner

**Purpose**: Run tests with various configurations including unit tests, integration tests, performance tests, and coverage reporting. **Coverage with HTML report is now the default behavior.** Includes comprehensive S3 testing support with both mocking and real S3 operations.

### Usage
```bash
./scripts/run-tests.sh [options]
```

### Options
- `--integration` - Include integration tests (requires database containers)
- `--performance` - Include performance tests (normally excluded)
- `--no-s3-mock` - Use real S3 instead of mocking (uses Hetzner backend - see S3 Requirements below)
- `--s3-bucket BUCKET` - Specify custom S3 bucket for testing
- `--no-coverage` - Skip coverage reporting (just run tests)
- `--no-html` - Generate coverage report without HTML (terminal only)
- `--module MODULE` - Test specific module only (e.g. date_parser)
- `--verbose` - Verbose test output
- `--help` - Show help message

### S3 Testing Requirements
The Forklift test suite supports both mocked and real S3 testing as documented in `/docs/S3_TESTING.md`:

#### **Mocked S3 Testing (Default)**
- **Behavior**: Uses `unittest.mock` to simulate S3 operations
- **Pros**: Fast, no AWS costs, no network dependencies, predictable behavior
- **Cons**: Doesn't test real S3 integration, may miss AWS-specific issues
- **Use case**: Development, CI/CD pipelines, unit testing

#### **Real S3 Testing** 
- **Behavior**: Uses actual S3 buckets for testing
- **Pros**: Tests actual S3 integration, catches real-world issues
- **Cons**: Slower, requires AWS credentials, may incur AWS costs
- **Use case**: Integration testing, pre-production validation

#### **Current Configuration**: 
Tests are configured to use the repository owner's Hetzner Object Storage credentials when using `--no-s3-mock`.

#### **For Repository Owner**: 
Tests will run against the configured Hetzner bucket.

#### **For Other Developers**: 
Real S3 testing requires your own AWS-compatible storage credentials configured via:

1. **mattstash (Recommended)**:
   ```bash
   mattstash set AWS_ACCESS_KEY_ID your_access_key
   mattstash set AWS_SECRET_ACCESS_KEY your_secret_key
   mattstash set AWS_DEFAULT_REGION us-east-1
   mattstash set S3_TEST_BUCKET your-test-bucket-name
   ```

2. **Environment Variables**:
   ```bash
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   export S3_TEST_BUCKET=your-test-bucket-name
   ```

3. **AWS CLI Configuration**: `aws configure`

> **Note**: If you have your own S3-compatible storage (AWS S3, Hetzner, MinIO, etc.) and configure the correct credentials and bucket location, the tests will execute accordingly. Tests automatically skip when credentials are not available.

### Example Output
```
Forklift Test Runner
====================

Configuring real S3 testing...
Checking mattstash for AWS credentials...
✓ Using mattstash credentials
✓ Using S3 test bucket: my-test-bucket

Cleaning up previous coverage data...
Installing package in editable mode...
Clearing pytest cache...
Running command: pytest -q --cov=src/forklift --cov-report=html --cov-report=term-missing --cov-config=pyproject.toml --no-s3-mock --s3-bucket my-test-bucket -m "not performance"

✓ Tests completed successfully!
✓ HTML coverage report generated in htmlcov/index.html
To view the report, run: open htmlcov/index.html

Coverage Summary:
==================

Overall Coverage:
TOTAL    1234   123    90%

Files with lowest coverage (bottom 10):
src/forklift/some_file.py    45%
...

Test run complete!
```

### Common Usage Examples
```bash
# Basic unit tests with coverage + HTML report (DEFAULT - uses S3 mocking)
./scripts/run-tests.sh

# Just run tests without coverage (uses S3 mocking)
./scripts/run-tests.sh --no-coverage

# Integration tests with coverage + HTML (uses real S3 for integration tests)
./scripts/run-tests.sh --integration

# Unit tests with real S3 operations (requires credentials)
./scripts/run-tests.sh --no-s3-mock

# Integration tests with real S3 + coverage (requires credentials)
./scripts/run-tests.sh --integration --no-s3-mock

# Real S3 with custom bucket
./scripts/run-tests.sh --no-s3-mock --s3-bucket my-custom-test-bucket

# Coverage report to terminal only (uses S3 mocking)
./scripts/run-tests.sh --no-html

# Test specific module with verbose output and real S3
./scripts/run-tests.sh --module s3_streaming --verbose --no-s3-mock

# Full test suite including performance tests with coverage (uses S3 mocking)
./scripts/run-tests.sh --performance --integration

# Integration testing workflow with real S3
./scripts/run-tests.sh --integration --no-s3-mock --s3-bucket production-test-bucket
```

### S3 Testing Safety Features
- **Automatic Cleanup**: Real S3 tests include cleanup mechanisms to abort incomplete uploads
- **Credential Validation**: Tests automatically skip if AWS credentials are not available
- **Bucket Isolation**: Uses dedicated test buckets (configurable via `S3_TEST_BUCKET`)
- **Test Object Prefixes**: Test objects are prefixed to avoid production data conflicts
- **Graceful Degradation**: Missing credentials result in test skips, not failures
