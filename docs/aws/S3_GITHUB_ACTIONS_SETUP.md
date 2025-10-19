# S3 Integration Testing with GitHub Actions

This document explains how to set up S3 integration testing in GitHub Actions using Hetzner S3-compatible storage service.

## Overview

The Forklift project supports two modes for S3 testing:

1. **Local Development**: Uses `mattstash` to securely store and retrieve AWS credentials
2. **CI/CD (GitHub Actions)**: Uses GitHub repository secrets to provide AWS credentials

## GitHub Actions Setup

### Step 1: Configure Repository Secrets

Add the following secrets to the GitHub repository:

1. Go to the repository on GitHub
2. Click on **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret** for each of the following:

#### Required Secrets

| Secret Name | Description | Example Value                         |
|-------------|-------------|---------------------------------------|
| `AWS_ACCESS_KEY_ID` | S3 access key | `N/A`                                 |
| `AWS_SECRET_ACCESS_KEY` | S3 secret key | `N/A`                                 |
| `AWS_DEFAULT_REGION` | S3 region (use `hel1` for Hetzner) | `hel1`                                |
| `S3_TEST_BUCKET` | Bucket name for testing | `forklift`                            |
| `S3_ENDPOINT_URL` | Custom S3 endpoint URL | `https://hel1.your-objectstorage.com` |

#### How to Get These Values

If these values are already configured locally with mattstash, retrieve them using:

```bash
# Get current mattstash values
mattstash get AWS_ACCESS_KEY_ID --show-password
mattstash get AWS_SECRET_ACCESS_KEY --show-password  
mattstash get AWS_DEFAULT_REGION --show-password
mattstash get S3_TEST_BUCKET --show-password
```

### Step 2: GitHub Actions Workflows

The repository includes S3 integration testing in the following workflow:

#### `fast-test.yml` (Auto-Format and Test)
- Runs S3 integration tests with real S3 service alongside formatting and regular tests
- Triggered on push to main/develop/feature branches **and version branches (`v*`)**
- Only runs S3 tests on the main repository (not forks) when secrets are available
- Uses Python 3.12 for S3 integration tests
- Prevents resource contention by being the single workflow that accesses real S3

#### `test.yml` (Test Suite)
- Runs comprehensive unit tests with S3 mocking across multiple Python versions (3.9-3.12)
- Fast execution, no AWS costs, no S3 resource conflicts
- Focuses on broad compatibility testing

### Step 3: Security Considerations

#### Fork Safety
The S3 integration workflow includes protection against running on forks:

```yaml
if: github.repository == 'matt/forklift' && (github.event_name != 'pull_request' || github.event.pull_request.head.repo.full_name == github.repository)
```

This prevents:
- Forks from accessing your S3 credentials
- External contributors from running costly S3 operations
- Credential exposure in pull requests from forks

#### Resource Contention Prevention
- S3 integration tests run only in the `fast-test.yml` workflow
- Prevents multiple workflows from simultaneously accessing the same S3 bucket
- Ensures test isolation and prevents conflicts during object creation/deletion

## Local Development vs CI

### Local Development (with mattstash)
```bash
# Run S3 tests locally (uses mattstash)
./scripts/run-tests.sh --no-s3-mock --module s3_streaming

# Run integration tests locally
./scripts/run-tests.sh --integration --no-s3-mock
```

### CI Environment Detection
The test suite automatically detects the environment:

- **Local**: Uses mattstash when `FORKLIFT_CI_MODE` is not set
- **CI**: Uses environment variables when `FORKLIFT_CI_MODE=true`

## Workflow Triggers

### S3 Integration Tests Run On (via `fast-test.yml`):
1. **Push** to main, develop, feature/*, feat/*, bugfix/*, hotfix/* branches
2. **Push** to version branches (`v*`) ← **Key benefit for version releases**
3. **Pull requests** to main or develop (only from same repo)

### Regular Unit Tests Run On (via `test.yml`):
- Push to main, develop, feature/*, bugfix/*, hotfix/* branches  
- Pull requests to main or develop
- Uses mocked S3 (fast, no costs, no resource conflicts)

## Test Structure

### S3 Test Categories (Single Workflow Only)

1. **Unit Tests with Real S3** (`test_s3_streaming.py --no-s3-mock`)
   - Tests S3StreamingClient functionality
   - Creates and cleans up test objects
   - Validates read/write operations

2. **Integration Tests with Real S3** (`--integration --no-s3-mock`)
   - Tests full data pipeline with S3
   - End-to-end workflows
   - Real-world scenarios

### Test Data Management

All tests use the `forklift/` folder in the S3 bucket:
- Objects: `s3://forklift/forklift/test-*` (based on S3_TEST_BUCKET setting)
- Automatic cleanup after each test
- Unique names to avoid conflicts
- Single workflow prevents concurrent access conflicts

## Monitoring and Debugging

### GitHub Actions Logs
Check the Actions tab in your repository for:
- Test execution logs (in "Auto-Format and Test" workflow)
- S3 operation details  
- Coverage reports
- Error messages

### Environment Variable Validation
The workflow validates all required environment variables and will fail fast if any are missing.

## Cost Optimization

### Single Workflow Design
- S3 integration tests run only in `fast-test.yml` workflow
- Prevents resource contention and reduces costs
- Uses Python 3.12 only for S3 tests (vs 3.9-3.12 for unit tests)

### Efficient Test Design
- Each test creates minimal test data
- Immediate cleanup prevents storage accumulation
- Tests designed to be independent and avoid conflicts

## Troubleshooting

### Common Issues

1. **"AWS_ACCESS_KEY_ID environment variable is required"**
   - Secret not set in repository settings
   - Typo in secret name
   - Running on a fork (expected behavior)

2. **"Failed to connect to S3 endpoint"**
   - Check `S3_ENDPOINT_URL` secret value
   - Verify Hetzner service availability

3. **"Access denied" errors**
   - Verify access key has permissions for the test bucket
   - Check bucket name in `S3_TEST_BUCKET` secret

4. **Resource contention errors**
   - Should not occur with single workflow design
   - If seen, verify only `fast-test.yml` has S3 integration tests

### Debug Mode
Add debugging to workflow by setting verbose mode in the pytest commands:

```yaml
- name: Run S3 streaming tests (debug)
  run: |
    python -m pytest tests/unit-tests/test_s3_streaming.py --no-s3-mock -vvv --tb=long
```

## Architecture Summary

### Current Setup (Prevents Resource Contention)
- **`fast-test.yml`**: Auto-formatting + regular tests + **S3 integration tests**
- **`test.yml`**: Comprehensive unit tests with S3 mocking (3.9-3.12 Python versions)

### Key Benefits
- ✅ S3 tests run on version branches (`v*`) 
- ✅ No resource contention between workflows
- ✅ Cost-effective single S3 test execution
- ✅ Fork protection maintains security
- ✅ Comprehensive coverage across both mocked and real S3

The setup is now optimized to prevent S3 resource conflicts while ensuring thorough testing on all important branches!
