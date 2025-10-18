#!/bin/bash

# Linting script for Forklift project
# Usage: ./lint.sh [options]
# Options:
#   --black-only     Run only Black formatting check
#   --flake8-only    Run only flake8 linting check
#   --apply-black    Apply Black formatting automatically
#   --help           Show this help message

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default values
BLACK_ONLY=false
FLAKE8_ONLY=false
APPLY_BLACK=false
PROJECT_ROOT="/Users/matt/PycharmProjects/forklift"

# Function to show help
show_help() {
    echo "Forklift Code Linting and Formatting"
    echo ""
    echo "Usage: ./lint.sh [options]"
    echo ""
    echo "Options:"
    echo "  --black-only     Run only Black formatting check (skip flake8)"
    echo "  --flake8-only    Run only flake8 linting check (skip Black)"
    echo "  --apply-black    Apply Black formatting automatically"
    echo "  --help           Show this help message"
    echo ""
    echo "Examples:"
    echo "  ./lint.sh                    # Run both Black and flake8 checks"
    echo "  ./lint.sh --black-only       # Run only Black formatting check"
    echo "  ./lint.sh --flake8-only      # Run only flake8 linting check"
    echo "  ./lint.sh --apply-black      # Apply Black formatting automatically"
}

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --black-only)
            BLACK_ONLY=true
            shift
            ;;
        --flake8-only)
            FLAKE8_ONLY=true
            shift
            ;;
        --apply-black)
            APPLY_BLACK=true
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
    if black --check --diff --color src/ tests/ examples/; then
        echo -e "${GREEN}✓ All Python files are properly formatted with Black!${NC}"
        return 0
    else
        echo -e "${RED}✗ Code formatting issues found!${NC}"
        echo -e "${YELLOW}To fix formatting issues, run: ./lint.sh --apply-black${NC}"
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
    if flake8 src/ --max-line-length=99 --extend-ignore=E203,W503; then
        echo -e "${GREEN}✓ All Python files pass flake8 linting!${NC}"
        return 0
    else
        echo -e "${RED}✗ flake8 linting issues found!${NC}"
        return 1
    fi
}

# Change to project root
cd "$PROJECT_ROOT"

echo -e "${BLUE}Forklift Code Quality Checks${NC}"
echo -e "${BLUE}============================${NC}"
echo ""

# Handle special modes
if [[ "$APPLY_BLACK" == true ]]; then
    apply_black_formatting
    exit 0
fi

# Track results
BLACK_PASSED=true
FLAKE8_PASSED=true

# Run Black check unless flake8-only mode
if [[ "$FLAKE8_ONLY" != true ]]; then
    if ! run_black_check; then
        BLACK_PASSED=false
    fi
    echo ""
fi

# Run flake8 check unless black-only mode
if [[ "$BLACK_ONLY" != true ]]; then
    if ! run_flake8_check; then
        FLAKE8_PASSED=false
    fi
    echo ""
fi

# Final summary
echo -e "${BLUE}Final Summary:${NC}"
echo -e "${BLUE}==============${NC}"

if [[ "$FLAKE8_ONLY" != true ]]; then
    if [[ "$BLACK_PASSED" == true ]]; then
        echo -e "${GREEN}✓ Black formatting check: PASSED${NC}"
    else
        echo -e "${RED}✗ Black formatting check: FAILED${NC}"
        echo -e "${YELLOW}  Run './lint.sh --apply-black' to fix formatting${NC}"
    fi
fi

if [[ "$BLACK_ONLY" != true ]]; then
    if [[ "$FLAKE8_PASSED" == true ]]; then
        echo -e "${GREEN}✓ flake8 linting check: PASSED${NC}"
    else
        echo -e "${RED}✗ flake8 linting check: FAILED${NC}"
    fi
fi

# Exit with appropriate code
if [[ "$BLACK_PASSED" == false || "$FLAKE8_PASSED" == false ]]; then
    exit 1
fi

echo -e "${GREEN}All code quality checks passed!${NC}"
