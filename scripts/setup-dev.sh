#!/bin/bash

# Setup script for Forklift development environment
# This script sets up pre-commit hooks and installs development dependencies

set -e  # Exit on any error

# Colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}🚀 Setting up Forklift development environment...${NC}"
echo ""

# Everything below runs from the repository root (where .pre-commit-config.yaml lives)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel 2>/dev/null || (cd "$SCRIPT_DIR/.." && pwd))"

# Install pre-commit if not already installed
if ! command -v pre-commit &> /dev/null; then
    echo -e "${YELLOW}📦 Installing pre-commit...${NC}"
    pip install pre-commit
else
    echo -e "${GREEN}✅ pre-commit already installed${NC}"
fi

# Install the pre-commit hooks
echo -e "${YELLOW}🔧 Installing pre-commit hooks...${NC}"
pre-commit install
pre-commit install --hook-type pre-push

# Install the package (editable) with every optional format plus the development tooling
# (same set as requirements-dev.txt; the pre-commit hooks are configured in .pre-commit-config.yaml)
echo -e "${YELLOW}📚 Installing development dependencies...${NC}"
pip install -e ".[all,dev]"

# Make scripts executable
echo -e "${YELLOW}🔐 Making scripts executable...${NC}"
chmod +x scripts/*.sh

echo ""
echo -e "${GREEN}✅ Setup complete! Your development environment is ready.${NC}"
echo ""
echo -e "${BLUE}🎯 What happens now:${NC}"
echo -e "  • Black and isort will auto-format your code before each commit"
echo -e "  • flake8 will check for linting issues before commits"
echo -e "  • Tests will run before pushes (pre-push hook)"
echo -e "  • GitHub Actions check formatting on pull requests (and auto-format feature-branch pushes), and run the tests on Python 3.12, 3.13 and 3.14"
echo ""
echo -e "${BLUE}💡 Available developer scripts:${NC}"
echo -e "  • ${YELLOW}./scripts/run-tests.sh${NC}          # Run tests with coverage by default"
echo -e "  • ${YELLOW}./scripts/lint.sh${NC}               # Code formatting and linting"
echo -e "  • ${YELLOW}./scripts/manage-databases.sh${NC}   # Manage database containers"
echo ""
echo -e "${BLUE}📖 Common workflows:${NC}"
echo -e "  • ${YELLOW}./scripts/run-tests.sh --help${NC}   # See all testing options"
echo -e "  • ${YELLOW}./scripts/lint.sh --apply-black${NC} # Auto-format code with Black"
echo -e "  • ${YELLOW}./scripts/run-tests.sh --no-html${NC} # Coverage report to terminal only"
echo -e "  • ${YELLOW}./scripts/run-tests.sh --integration${NC}  # Integration tests with coverage"
echo -e "  • ${YELLOW}./scripts/manage-databases.sh start${NC}  # Start database containers for integration tests"
echo -e "  • ${YELLOW}./scripts/manage-databases.sh wipe${NC}   # Clean up database containers and data"
