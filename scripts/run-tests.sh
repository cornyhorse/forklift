#!/bin/bash

# Test runner script for Forklift project
# Usage: ./run-tests.sh [options]
# Options:
#   --integration    Include integration tests
#   --performance    Include performance tests
#   --no-s3-mock     Use real S3 instead of mocking (requires AWS credentials)
#   --s3-bucket BUCKET  Specify custom S3 bucket for testing
#   --no-coverage    Skip coverage reporting (just run tests)
#   --no-html        Generate coverage report without HTML (terminal only)
#   --module MODULE  Test specific module only
#   --verbose        Verbose output
#   --help           Show this help message

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
INCLUDE_INTEGRATION=false
INCLUDE_PERFORMANCE=false
USE_REAL_S3=false
GENERATE_COVERAGE=true
HTML_REPORT=true
SPECIFIC_MODULE=""
VERBOSE=false
S3_TEST_BUCKET=""
PROJECT_ROOT="/Users/matt/PycharmProjects/forklift"

# Function to show help
show_help() {
    echo "Forklift Test Runner"
    echo ""
    echo "Usage: ./run-tests.sh [options]"
    echo ""
    echo "Options:"
    echo "  --integration       Include integration tests"
    echo "  --performance       Include performance tests (normally excluded)"
    echo "  --no-s3-mock        Use real S3 instead of mocking (uses Hetzner backend)"
    echo "  --s3-bucket BUCKET  Specify custom S3 bucket for testing"
    echo "  --no-coverage       Skip coverage reporting (just run tests)"
    echo "  --no-html           Generate coverage report without HTML (terminal only)"
    echo "  --module MODULE     Test specific module only (e.g. date_parser)"
    echo "  --verbose           Verbose test output"
    echo "  --help              Show this help message"
    echo ""
    echo "S3 Testing Modes:"
    echo "  Default: Uses S3 mocking for unit tests (fast, no AWS costs)"
    echo "  --no-s3-mock: Uses real S3 (requires AWS credentials)"
    echo "  --integration: Always uses real S3 for integration tests"
    echo ""
    echo "Examples:"
    echo "  ./run-tests.sh                                    # Basic tests with coverage + HTML report (DEFAULT)"
    echo "  ./run-tests.sh --no-coverage                      # Just run tests, no coverage"
    echo "  ./run-tests.sh --integration                      # Integration tests with coverage + HTML"
    echo "  ./run-tests.sh --integration --no-s3-mock        # Integration tests with real S3 + coverage"
    echo "  ./run-tests.sh --no-s3-mock --s3-bucket my-test-bucket  # Real S3 with custom bucket"
    echo "  ./run-tests.sh --performance                      # Include performance tests + coverage"
    echo "  ./run-tests.sh --no-html                          # Coverage report to terminal only"
    echo "  ./run-tests.sh --module date_parser --verbose     # Test specific module with verbose output"
    echo "  ./run-tests.sh --integration --no-coverage        # Integration tests without coverage"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --integration)
            INCLUDE_INTEGRATION=true
            shift
            ;;
        --performance)
            INCLUDE_PERFORMANCE=true
            shift
            ;;
        --no-s3-mock)
            USE_REAL_S3=true
            shift
            ;;
        --s3-bucket)
            S3_TEST_BUCKET="$2"
            shift 2
            ;;
        --no-coverage)
            GENERATE_COVERAGE=false
            HTML_REPORT=false
            shift
            ;;
        --no-html)
            HTML_REPORT=false
            shift
            ;;
        --module)
            SPECIFIC_MODULE="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=true
            shift
            ;;
        --help)
            show_help
            exit 0
            ;;
        # Legacy compatibility
        --coverage)
            GENERATE_COVERAGE=true
            shift
            ;;
        --html)
            HTML_REPORT=true
            GENERATE_COVERAGE=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Change to project root
cd "$PROJECT_ROOT"

echo -e "${BLUE}Forklift Test Runner${NC}"
echo -e "${BLUE}===================${NC}"
echo ""

# Clean up previous coverage data if generating coverage
if [[ "$GENERATE_COVERAGE" == true ]]; then
    echo -e "${YELLOW}Cleaning up previous coverage data...${NC}"
    rm -f .coverage
    rm -rf htmlcov/
fi

# Install package in editable mode
echo -e "${YELLOW}Installing package in editable mode...${NC}"
pip install -e .

# Clear pytest cache
echo -e "${YELLOW}Clearing pytest cache...${NC}"
pytest --cache-clear > /dev/null 2>&1

# Build pytest command
PYTEST_CMD="pytest"

# Add verbosity
if [[ "$VERBOSE" == true ]]; then
    PYTEST_CMD="$PYTEST_CMD -v"
else
    PYTEST_CMD="$PYTEST_CMD -q"
fi

# Add coverage options
if [[ "$GENERATE_COVERAGE" == true ]]; then
    PYTEST_CMD="$PYTEST_CMD --cov=src/forklift"
    if [[ "$HTML_REPORT" == true ]]; then
        PYTEST_CMD="$PYTEST_CMD --cov-report=html --cov-report=term-missing"
    else
        PYTEST_CMD="$PYTEST_CMD --cov-report=term-missing"
    fi
    # Add coverage config to ensure proper module measurement
    PYTEST_CMD="$PYTEST_CMD --cov-config=pyproject.toml"
else
    # Explicitly disable coverage when not requested
    PYTEST_CMD="$PYTEST_CMD --no-cov"
fi

# Add integration test flag
if [[ "$INCLUDE_INTEGRATION" == true ]]; then
    PYTEST_CMD="$PYTEST_CMD --integration"
fi

# Add S3 mocking flag
if [[ "$USE_REAL_S3" == true ]]; then
    PYTEST_CMD="$PYTEST_CMD --no-s3-mock"
fi

# Add S3 bucket flag if specified
if [[ -n "$S3_TEST_BUCKET" ]]; then
    PYTEST_CMD="$PYTEST_CMD --s3-bucket $S3_TEST_BUCKET"
fi

# Add test markers for performance tests
if [[ "$INCLUDE_PERFORMANCE" == true ]]; then
    # Include all tests
    echo -e "${YELLOW}Including performance tests...${NC}"
else
    # Exclude performance tests (default)
    PYTEST_CMD="$PYTEST_CMD -m \"not performance\""
fi

# Add specific module if requested
if [[ -n "$SPECIFIC_MODULE" ]]; then
    echo -e "${YELLOW}Testing module: ${SPECIFIC_MODULE}${NC}"
    # Try different possible test file locations
    if [[ -f "tests/unit-tests/test_${SPECIFIC_MODULE}.py" ]]; then
        PYTEST_CMD="$PYTEST_CMD tests/unit-tests/test_${SPECIFIC_MODULE}.py"
    elif [[ -f "tests/test_${SPECIFIC_MODULE}.py" ]]; then
        PYTEST_CMD="$PYTEST_CMD tests/test_${SPECIFIC_MODULE}.py"
    else
        echo -e "${RED}Test file not found for module: ${SPECIFIC_MODULE}${NC}"
        echo -e "${BLUE}Looked for:${NC}"
        echo -e "${BLUE}  - tests/unit-tests/test_${SPECIFIC_MODULE}.py${NC}"
        echo -e "${BLUE}  - tests/test_${SPECIFIC_MODULE}.py${NC}"
        exit 1
    fi
else
    # When not testing a specific module, use standard test discovery
    PYTEST_CMD="$PYTEST_CMD tests/"
fi

# Add coverage omit for integration tests if not including them
if [[ "$INCLUDE_INTEGRATION" == false && "$GENERATE_COVERAGE" == true ]]; then
    # Use coverage configuration file or environment variable instead of pytest --cov-omit
    # The --cov-omit flag doesn't exist for pytest, we need to handle this differently
    echo -e "${YELLOW}Excluding integration tests from coverage...${NC}"
    # We'll rely on the .coveragerc file or pyproject.toml configuration for omitting files
fi

echo -e "${YELLOW}Running command: ${PYTEST_CMD}${NC}"
echo ""

# Run the tests
if eval "$PYTEST_CMD"; then
    echo ""
    echo -e "${GREEN}✓ Tests completed successfully!${NC}"

    if [[ "$HTML_REPORT" == true ]]; then
        echo -e "${GREEN}✓ HTML coverage report generated in htmlcov/index.html${NC}"
        echo -e "${BLUE}To view the report, run: open htmlcov/index.html${NC}"
    fi
else
    echo ""
    echo -e "${RED}✗ Tests failed!${NC}"
    exit 1
fi

# Show enhanced coverage summary (from coverage.sh) if coverage was generated
if [[ "$GENERATE_COVERAGE" == true ]]; then
    echo ""
    echo -e "${BLUE}Coverage Summary:${NC}"
    echo -e "${BLUE}==================${NC}"

    # Show overall summary with just the TOTAL line - suppress broken pipe errors
    echo -e "${YELLOW}Overall Coverage:${NC}"
    python -m coverage report --show-missing 2>/dev/null | grep "TOTAL" || true

    echo ""
    echo -e "${YELLOW}Files with lowest coverage (bottom 10):${NC}"
    python -m coverage report --show-missing --sort=cover 2>/dev/null | head -n 15 | tail -n 10 || true

    echo ""
    echo -e "${YELLOW}Files with highest coverage (top 5):${NC}"
    python -m coverage report --show-missing --sort=cover 2>/dev/null | tail -n 10 | head -n 5 || true

    # Show files with missing lines if any - suppress broken pipe errors
    echo ""
    echo -e "${YELLOW}Coverage Details:${NC}"
    python -m coverage report --show-missing 2>/dev/null | head -n 20 || true

    echo ""
fi

echo -e "${GREEN}Test run complete!${NC}"
