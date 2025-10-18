#!/bin/bash

# Coverage script for Forklift project
# Usage: ./run_coverage.sh [options]
# Options:
#   --html    Generate HTML coverage report
#   --module  Specify specific module to test (e.g. --module date_parser)
#   --black   Run Black code formatting check
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
RUN_BLACK=true  # Default to running Black linting
BLACK_ONLY=false  # New option for Black-only mode
APPLY_BLACK=false  # New option to apply Black formatting instead of just checking
RUN_FLAKE8=false  # New option for flake8 linting
FLAKE8_ONLY=false  # New option for flake8-only mode
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
    echo "  --black             Run Black code formatting check (default: enabled)"
    echo "  --black-only        Run only Black code formatting check (skip tests/coverage)"
    echo "  --no-black          Skip Black code formatting check"
    echo "  --apply-black       Apply Black code formatting automatically"
    echo "  --flake8            Run flake8 linting check"
    echo "  --flake8-only       Run only flake8 linting check (skip tests/coverage)"
    echo "  --help              Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./run_coverage.sh                           # Run all tests with terminal report + Black"
    echo "  ./run_coverage.sh --html                    # Run all tests with HTML report + Black"
    echo "  ./run_coverage.sh --module date_parser      # Test only date_parser module + Black"
    echo "  ./run_coverage.sh --no-black               # Run tests without Black formatting check"
    echo "  ./run_coverage.sh --black-only             # Run only Black formatting check"
    echo "  ./run_coverage.sh --apply-black            # Apply Black formatting automatically"
    echo "  ./run_coverage.sh --flake8                 # Run tests + Black + flake8"
    echo "  ./run_coverage.sh --flake8-only            # Run only flake8 linting check"
    echo "  ./run_coverage.sh --module batch_processor --html  # Test batch_processor with HTML + Black"
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
        --black)
            RUN_BLACK=true
            shift
            ;;
        --black-only)
            BLACK_ONLY=true
            RUN_BLACK=true
            shift
            ;;
        --no-black)
            RUN_BLACK=false
            shift
            ;;
        --apply-black)
            APPLY_BLACK=true
            RUN_BLACK=true
            shift
            ;;
        --flake8)
            RUN_FLAKE8=true
            shift
            ;;
        --flake8-only)
            FLAKE8_ONLY=true
            RUN_FLAKE8=true
            shift
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

# Function to run Black formatting check
run_black_check() {
    echo -e "${BLUE}Running Black Code Formatting Check${NC}"
    echo -e "${BLUE}====================================${NC}"
    echo ""

    # Check if Black is installed
    if ! command -v black &> /dev/null; then
        echo -e "${YELLOW}Black is not installed. Installing Black...${NC}"
        pip install black
    fi

    echo -e "${YELLOW}Checking code formatting with Black...${NC}"

    # Run Black in check mode (don't modify files, just report issues)
    # Only check directories and files that exist
    if black --check --diff --color src/ tests/ examples/; then
        echo -e "${GREEN}✓ All Python files are properly formatted with Black!${NC}"
        return 0
    else
        echo -e "${RED}✗ Code formatting issues found!${NC}"
        echo -e "${YELLOW}To fix formatting issues, run: black src/ tests/ examples/${NC}"
        return 1
    fi
}

# Function to apply Black formatting
apply_black_formatting() {
    echo -e "${BLUE}Applying Black Code Formatting${NC}"
    echo -e "${BLUE}=============================${NC}"
    echo ""

    # Check if Black is installed
    if ! command -v black &> /dev/null; then
        echo -e "${YELLOW}Black is not installed. Installing Black...${NC}"
        pip install black
    fi

    echo -e "${YELLOW}Formatting code with Black...${NC}"

    # Run Black to format the code
    if black src/ tests/ examples/; then
        echo -e "${GREEN}✓ Code formatting applied successfully!${NC}"
    else
        echo -e "${RED}✗ Failed to apply code formatting.${NC}"
        exit 1
    fi
}

# Function to run flake8 linting check
run_flake8_check() {
    echo -e "${BLUE}Running flake8 Linting Check${NC}"
    echo -e "${BLUE}============================${NC}"
    echo ""

    # Check if flake8 is installed
    if ! command -v flake8 &> /dev/null; then
        echo -e "${YELLOW}flake8 is not installed. Installing flake8...${NC}"
        pip install flake8
    fi

    echo -e "${YELLOW}Running flake8 linting (src only, matching GitHub workflow)...${NC}"

    # Run flake8 with the same settings as GitHub workflow
    # flake8 src/ --max-line-length=99 --extend-ignore=E203,W503
    if flake8 src/ --max-line-length=99 --extend-ignore=E203,W503; then
        echo -e "${GREEN}✓ All Python files pass flake8 linting!${NC}"
        return 0
    else
        echo -e "${RED}✗ flake8 linting issues found!${NC}"
        echo -e "${YELLOW}To see details, run: flake8 src/ --max-line-length=99 --extend-ignore=E203,W503${NC}"
        return 1
    fi
}

# Change to project root
cd "$PROJECT_ROOT"

# Update title based on mode
if [[ "$BLACK_ONLY" == true ]]; then
    echo -e "${BLUE}Forklift Black Code Formatting Check${NC}"
    echo -e "${BLUE}====================================${NC}"
elif [[ "$FLAKE8_ONLY" == true ]]; then
    echo -e "${BLUE}Forklift flake8 Linting Check${NC}"
    echo -e "${BLUE}=============================${NC}"
elif [[ "$APPLY_BLACK" == true ]]; then
    echo -e "${BLUE}Forklift Apply Black Formatting${NC}"
    echo -e "${BLUE}==============================${NC}"
else
    echo -e "${BLUE}Forklift Coverage Analysis${NC}"
    echo -e "${BLUE}=========================${NC}"
fi
echo ""

# Run Black formatting check first if enabled
BLACK_PASSED=true
if [[ "$RUN_BLACK" == true ]]; then
    if ! run_black_check; then
        BLACK_PASSED=false
        if [[ "$BLACK_ONLY" == true ]]; then
            echo -e "${RED}Black formatting check failed.${NC}"
        else
            echo -e "${RED}Black formatting check failed. Continuing with tests...${NC}"
        fi
        echo ""
    else
        echo ""
    fi
fi

# Run flake8 linting check if enabled
FLAKE8_PASSED=true
if [[ "$RUN_FLAKE8" == true ]]; then
    if ! run_flake8_check; then
        FLAKE8_PASSED=false
        if [[ "$FLAKE8_ONLY" == true ]]; then
            echo -e "${RED}flake8 linting check failed.${NC}"
        else
            echo -e "${RED}flake8 linting check failed. Continuing with tests...${NC}"
        fi
        echo ""
    else
        echo ""
    fi
fi

# If BLACK_ONLY mode, skip tests and coverage
if [[ "$BLACK_ONLY" == true ]]; then
    echo -e "${BLUE}Black-only mode: Skipping tests and coverage analysis${NC}"

    # Final summary for Black-only mode
    echo ""
    echo -e "${BLUE}Final Summary:${NC}"
    echo -e "${BLUE}==============${NC}"
    if [[ "$BLACK_PASSED" == true ]]; then
        echo -e "${GREEN}✓ Black formatting check: PASSED${NC}"
        echo -e "${GREEN}All Python files are properly formatted!${NC}"
        exit 0
    else
        echo -e "${RED}✗ Black formatting check: FAILED${NC}"
        echo -e "${YELLOW}  Run 'black src/ tests/ examples/' to fix formatting${NC}"
        exit 1
    fi
fi

# If FLAKE8_ONLY mode, skip tests and coverage
if [[ "$FLAKE8_ONLY" == true ]]; then
    echo -e "${BLUE}flake8-only mode: Skipping tests and coverage analysis${NC}"

    # Final summary for flake8-only mode
    echo ""
    echo -e "${BLUE}Final Summary:${NC}"
    echo -e "${BLUE}==============${NC}"
    if [[ "$FLAKE8_PASSED" == true ]]; then
        echo -e "${GREEN}✓ flake8 linting check: PASSED${NC}"
        echo -e "${GREEN}All Python files pass flake8 linting!${NC}"
        exit 0
    else
        echo -e "${RED}✗ flake8 linting check: FAILED${NC}"
        echo -e "${YELLOW}  Run 'flake8 src/ --max-line-length=99 --extend-ignore=E203,W503' to see details${NC}"
        exit 1
    fi
fi

# If APPLY_BLACK mode, apply formatting and exit
if [[ "$APPLY_BLACK" == true ]]; then
    apply_black_formatting
    echo ""
    echo -e "${GREEN}✓ Code formatting applied successfully!${NC}"
    exit 0
fi

# Continue with normal test/coverage flow if not BLACK_ONLY or APPLY_BLACK
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

# Final summary including Black results
echo ""
echo -e "${BLUE}Final Summary:${NC}"
echo -e "${BLUE}==============${NC}"
if [[ "$RUN_BLACK" == true ]]; then
    if [[ "$BLACK_PASSED" == true ]]; then
        echo -e "${GREEN}✓ Black formatting check: PASSED${NC}"
    else
        echo -e "${RED}✗ Black formatting check: FAILED${NC}"
        echo -e "${YELLOW}  Run 'black src/ tests/ examples/' to fix formatting${NC}"
    fi
else
    echo -e "${YELLOW}⚬ Black formatting check: SKIPPED${NC}"
fi

if [[ "$RUN_FLAKE8" == true ]]; then
    if [[ "$FLAKE8_PASSED" == true ]]; then
        echo -e "${GREEN}✓ flake8 linting check: PASSED${NC}"
    else
        echo -e "${RED}✗ flake8 linting check: FAILED${NC}"
        echo -e "${YELLOW}  Run 'flake8 src/ --max-line-length=99 --extend-ignore=E203,W503' to see details${NC}"
    fi
else
    echo -e "${YELLOW}⚬ flake8 linting check: SKIPPED${NC}"
fi

# Exit with appropriate code
if [[ "$RUN_BLACK" == true && "$BLACK_PASSED" == false ]]; then
    echo -e "${RED}Script completed with formatting issues. Please fix Black formatting errors.${NC}"
    exit 1
fi

if [[ "$RUN_FLAKE8" == true && "$FLAKE8_PASSED" == false ]]; then
    echo -e "${RED}Script completed with linting issues. Please fix flake8 linting errors.${NC}"
    exit 1
fi
