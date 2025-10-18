#!/bin/bash

# Setup script for Forklift development environment
# This installs pre-commit hooks for automatic code formatting

echo "🚀 Setting up Forklift development environment..."

# Install pre-commit if not already installed
if ! command -v pre-commit &> /dev/null; then
    echo "📦 Installing pre-commit..."
    pip install pre-commit
else
    echo "✅ pre-commit already installed"
fi

# Install the pre-commit hooks
echo "🔧 Installing pre-commit hooks..."
pre-commit install
pre-commit install --hook-type pre-push

# Install development dependencies
echo "📚 Installing development dependencies..."
pip install black isort flake8 pytest pytest-cov

echo ""
echo "✅ Setup complete! Your development environment is ready."
echo ""
echo "🎯 What happens now:"
echo "  • Black and isort will auto-format your code before each commit"
echo "  • flake8 will check for linting issues before commits"
echo "  • Tests will run before pushes (pre-push hook)"
echo "  • GitHub Actions will also auto-format and test on push"
echo ""
echo "💡 Manual commands:"
echo "  • Format code:  black src/ tests/"
echo "  • Sort imports: isort src/ tests/"
echo "  • Run tests:    python -m pytest tests/"
echo "  • Run coverage: ./tests/run_coverage.sh"
