#!/bin/bash

# Coverage script for Forklift project
# Usage: ./run_coverage.sh [options]
# Options:
#   --html    Generate HTML coverage report
#   --module  Specify specific module to test (e.g. --module date_parser)
#   --help    Show this help message

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
HTML_REPORT=false
SPECIFIC_MODULE=""
PROJECT_ROOT="/Users/matt/PycharmProjects/forklift"

# Function to show help
show_help() {
    echo "Forklift Coverage Test Runner"
    echo ""
    echo "Usage: ./run_coverage.sh [options]"
    echo ""
    echo "Options:"
    echo "  --html              Generate HTML coverage report"
    echo "  --module MODULE     Test specific module (e.g. date_parser, batch_processor)"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./run_coverage.sh                           # Run all tests with terminal report"
    echo "  ./run_coverage.sh --html                    # Run all tests with HTML report"
    echo "  ./run_coverage.sh --module date_parser      # Test only date_parser module"
    echo "  ./run_coverage.sh --module batch_processor --html  # Test batch_processor with HTML"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --html)
            HTML_REPORT=true
            shift
            ;;
        --module)
            SPECIFIC_MODULE="$2"
            shift 2
            ;;
        --help)
            show_help
            exit 0
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

echo -e "${BLUE}Forklift Coverage Analysis${NC}"
echo -e "${BLUE}=========================${NC}"
echo ""

# Clean up previous coverage data
echo -e "${YELLOW}Cleaning up previous coverage data...${NC}"
rm -f .coverage
rm -rf htmlcov/

# Build coverage command
if [[ -n "$SPECIFIC_MODULE" ]]; then
    echo -e "${YELLOW}Running coverage for module: ${SPECIFIC_MODULE}${NC}"
    TEST_PATTERN="tests/test_${SPECIFIC_MODULE}.py"
    COV_SOURCE="src/forklift/"
else
    echo -e "${YELLOW}Running coverage for all modules...${NC}"
    TEST_PATTERN="tests/"
    COV_SOURCE="src/forklift/"
fi

# Base coverage command
COV_CMD="python -m pytest $TEST_PATTERN --cov=$COV_SOURCE --cov-report=term-missing"

# Add HTML report if requested
if [[ "$HTML_REPORT" == true ]]; then
    COV_CMD="$COV_CMD --cov-report=html"
fi

# Add verbose output
COV_CMD="$COV_CMD -v"

echo -e "${YELLOW}Running command: ${COV_CMD}${NC}"
echo ""

# Run the coverage command
if eval "$COV_CMD"; then
    echo ""
    echo -e "${GREEN}✓ Coverage analysis completed successfully!${NC}"

    if [[ "$HTML_REPORT" == true ]]; then
        echo -e "${GREEN}✓ HTML report generated in htmlcov/index.html${NC}"
        echo -e "${BLUE}To view the report, run: open htmlcov/index.html${NC}"
    fi
else
    echo ""
    echo -e "${RED}✗ Coverage analysis failed!${NC}"
    exit 1
fi

# Show coverage summary
echo ""
echo -e "${BLUE}Coverage Summary:${NC}"
echo -e "${BLUE}==================${NC}"

# Show overall summary with just the TOTAL line
echo -e "${YELLOW}Overall Coverage:${NC}"
python -m coverage report --show-missing | grep "TOTAL"

echo ""
echo -e "${YELLOW}Files with lowest coverage (bottom 10):${NC}"
python -m coverage report --show-missing --sort=cover | head -n 15 | tail -n 10

echo ""
echo -e "${YELLOW}Files with highest coverage (top 5):${NC}"
python -m coverage report --show-missing --sort=cover | tail -n 10 | head -n 5

echo ""
echo -e "${GREEN}Coverage analysis complete!${NC}"
