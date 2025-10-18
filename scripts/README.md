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

**Purpose**: Run tests with various configurations including unit tests, integration tests, performance tests, and coverage reporting. **Coverage with HTML report is now the default behavior.**

### Usage
```bash
./scripts/run-tests.sh [options]
```

### Options
- `--integration` - Include integration tests (requires database containers)
- `--performance` - Include performance tests (normally excluded)
- `--no-s3-mock` - Use real S3 instead of mocking (uses Hetzner backend - see S3 Requirements below)
- `--no-coverage` - Skip coverage reporting (just run tests)
- `--no-html` - Generate coverage report without HTML (terminal only)
- `--module MODULE` - Test specific module only (e.g. date_parser)
- `--verbose` - Verbose test output
- `--help` - Show help message

### S3 Testing Requirements
The `--no-s3-mock` flag configures tests to use a real S3-compatible backend (currently Hetzner Cloud Object Storage):

- **Current Configuration**: Tests are configured to use the repository owner's Hetzner credentials
- **For Repository Owner**: Tests will run against the configured Hetzner bucket
- **For Other Developers**: This flag is currently only executable by the repository owner
- **Custom S3 Setup**: To use your own S3 bucket, configure the appropriate S3 credentials and bucket location in your environment

> **Note**: If you have your own S3-compatible storage (AWS S3, Hetzner, MinIO, etc.) and configure the correct credentials and bucket location, the tests will execute accordingly.

### Example Output
```
Forklift Test Runner
====================

Cleaning up previous coverage data...
Installing package in editable mode...
Clearing pytest cache...
Running command: pytest -q --cov=forklift --cov-report=html --cov-report=term-missing -m "not performance"

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

Files with highest coverage (top 5):
src/forklift/core.py         98%
...

Coverage Details:
[Detailed coverage report with missing lines]

Test run complete!
```

### Common Usage Examples
```bash
# Basic unit tests with coverage + HTML report (NEW DEFAULT)
./scripts/run-tests.sh

# Just run tests without coverage (old default behavior)
./scripts/run-tests.sh --no-coverage

# Integration tests with coverage + HTML (default coverage enabled)
./scripts/run-tests.sh --integration

# Integration tests with real S3 + coverage (repository owner only)
./scripts/run-tests.sh --integration --no-s3-mock

# Coverage report to terminal only (no HTML)
./scripts/run-tests.sh --no-html

# Test specific module with verbose output and coverage
./scripts/run-tests.sh --module date_parser --verbose

# Full test suite including performance tests with coverage
./scripts/run-tests.sh --performance --integration
```

---

## 🔍 `lint.sh` - Code Quality Checks

**Purpose**: Run Black code formatting and flake8 linting checks. Can check code quality or automatically apply formatting fixes.

### Usage
```bash
./scripts/lint.sh [options]
```

### Options
- `--black-only` - Run only Black formatting check (skip flake8)
- `--flake8-only` - Run only flake8 linting check (skip Black)
- `--apply-black` - Apply Black formatting automatically
- `--help` - Show help message

### Example Output
```
Forklift Code Quality Checks
============================

Running Black Code Formatting Check
====================================

Checking code formatting with Black...
✓ All Python files are properly formatted with Black!

Running flake8 Linting Check
============================

Running flake8 linting (src only, matching GitHub workflow)...
✓ All Python files pass flake8 linting!

Final Summary:
==============
✓ Black formatting check: PASSED
✓ flake8 linting check: PASSED

All code quality checks passed!
```

### Common Usage Examples
```bash
# Run both Black and flake8 checks
./scripts/lint.sh

# Check only Black formatting
./scripts/lint.sh --black-only

# Auto-format code with Black
./scripts/lint.sh --apply-black

# Check only flake8 linting
./scripts/lint.sh --flake8-only
```

---

## 🗄️ `manage-databases.sh` - Database Container Management

**Purpose**: Manage Docker containers for database integration tests. Provides granular control over container lifecycle.

### Usage
```bash
./scripts/manage-databases.sh [command]
```

### Commands
- `start` - Start database containers for integration tests
- `stop` - Stop database containers (preserves data)
- `wipe` - Stop and remove containers and volumes (destroys data)
- `status` - Show status of database containers
- `logs` - Show logs from database containers
- `restart` - Stop and start containers (preserves data)
- `--help` - Show help message

### Example Output
```
Starting Database Testing Containers
====================================

Starting database containers...
✓ Database containers started successfully
Use './scripts/manage-databases.sh status' to check container health
Use './scripts/manage-databases.sh stop' to stop containers
Use './scripts/manage-databases.sh wipe' to remove containers and data
```

### Common Usage Examples
```bash
# Start databases for testing
./scripts/manage-databases.sh start

# Check container status
./scripts/manage-databases.sh status

# View container logs
./scripts/manage-databases.sh logs

# Stop containers but keep data
./scripts/manage-databases.sh stop

# Clean slate (remove everything)
./scripts/manage-databases.sh wipe
```

---

## 🚀 `setup-dev.sh` - Development Environment Setup

**Purpose**: Bootstrap development environment with pre-commit hooks, development dependencies, and script permissions.

### Usage
```bash
./scripts/setup-dev.sh
```

### What It Does
1. Installs pre-commit hooks for code quality
2. Installs development dependencies (black, isort, flake8, pytest, pytest-cov)
3. Makes all scripts executable
4. Provides guidance on available tools

### Example Output
```
🚀 Setting up Forklift development environment...

✅ pre-commit already installed
🔧 Installing pre-commit hooks...
📚 Installing development dependencies...
🔐 Making scripts executable...

✅ Setup complete! Your development environment is ready.

🎯 What happens now:
  • Black and isort will auto-format your code before each commit
  • flake8 will check for linting issues before commits
  • Tests will run before pushes (pre-push hook)
  • GitHub Actions will also auto-format and test on push

💡 Available developer scripts:
  • ./scripts/run-tests.sh          # Run tests with coverage by default
  • ./scripts/lint.sh               # Code formatting and linting
  • ./scripts/manage-databases.sh   # Manage database containers

📖 Common workflows:
  • ./scripts/run-tests.sh --help   # See all testing options
  • ./scripts/lint.sh --apply-black # Auto-format code with Black
  • ./scripts/run-tests.sh --no-html # Coverage report to terminal only
  • ./scripts/run-tests.sh --integration  # Integration tests with coverage
  • ./scripts/manage-databases.sh start  # Start database containers for integration tests
  • ./scripts/manage-databases.sh wipe   # Clean up database containers and data
```

---

## 📜 Legacy Scripts

### `run_coverage.sh` (Root Level) - DEPRECATED
This script has been replaced by the enhanced `run-tests.sh`. It shows deprecation warnings and redirects to appropriate new functionality:

- `run_coverage.sh --html` → `./scripts/run-tests.sh` (HTML coverage is now default)
- `run_coverage.sh --black-only` → `./scripts/lint.sh --black-only`
- `run_coverage.sh --apply-black` → `./scripts/lint.sh --apply-black`
- `run_coverage.sh --module MODULE` → `./scripts/run-tests.sh --module MODULE`

### Legacy Database Scripts
- `start-testing-containers.sh` → `./scripts/manage-databases.sh start`
- `wipe-databases.sh` → `./scripts/manage-databases.sh wipe`

These legacy scripts still work but redirect to the new consolidated tools.

---

## 🔄 Common Workflows

### Daily Development
```bash
# Set up environment (once)
./scripts/setup-dev.sh

# Before committing changes
./scripts/lint.sh --apply-black
./scripts/run-tests.sh  # Now includes coverage + HTML by default

# Quick test run without coverage
./scripts/run-tests.sh --no-coverage
```

### Integration Testing
```bash
# Start database containers
./scripts/manage-databases.sh start

# Run integration tests with coverage (coverage is default)
./scripts/run-tests.sh --integration

# Clean up when done
./scripts/manage-databases.sh wipe
```

### Code Quality Review
```bash
# Full quality check
./scripts/lint.sh
./scripts/run-tests.sh --integration --performance  # Includes coverage by default
```

### Debugging
```bash
# Check container status
./scripts/manage-databases.sh status
./scripts/manage-databases.sh logs

# Verbose test output with coverage
./scripts/run-tests.sh --verbose --module specific_module
```
