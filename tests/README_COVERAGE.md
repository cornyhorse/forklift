# Coverage Testing Scripts

This directory contains scripts to easily run coverage analysis on the Forklift project.

## Available Scripts

### 1. `run_coverage.sh` (Recommended)
A comprehensive shell script with colorized output and multiple options.

**Usage:**
```bash
# From the tests directory:
cd tests

# Run all tests with terminal coverage report
./run_coverage.sh

# Run all tests with HTML coverage report
./run_coverage.sh --html

# Run specific module (e.g., batch_processor)
./run_coverage.sh --module batch_processor

# Run specific module with HTML report
./run_coverage.sh --module batch_processor --html

# Show help
./run_coverage.sh --help
```

**Features:**
- Colorized output for better readability
- Automatic cleanup of previous coverage data
- Support for HTML and terminal reports
- Module-specific testing
- Comprehensive error handling
- Coverage summary display

### 2. `run_coverage_simple.py`
A simpler Python script for basic coverage analysis.

**Usage:**
```bash
# From the tests directory:
cd tests

# Run all tests
python run_coverage_simple.py

# Run specific module
python run_coverage_simple.py batch_processor
```

## Coverage Commands Reference

### Best Commands for Different Scenarios

**Full project coverage with HTML report:**
```bash
./run_coverage.sh --html
```

**Quick check of specific module:**
```bash
./run_coverage.sh --module batch_processor
```

**Detailed analysis of specific module:**
```bash
./run_coverage.sh --module batch_processor --html
```

### Direct pytest Commands

If you prefer to run pytest directly:

```bash
# From project root (/Users/matt/PycharmProjects/forklift):

# All tests with terminal report
python -m pytest tests/ --cov=src/forklift/ --cov-report=term-missing -v

# Specific module with HTML report
python -m pytest tests/test_batch_processor.py --cov=src/forklift/ --cov-report=html --cov-report=term-missing -v

# Focus coverage on specific source module
python -m pytest tests/test_batch_processor.py --cov=src/forklift/engine/processors/batch_processor.py --cov-report=term-missing -v
```

## Output Explanation

### Terminal Report
Shows coverage percentages and missing line numbers for each file:
```
Name                                           Stmts   Miss  Cover   Missing
--------------------------------------------------------------------------
src/forklift/engine/processors/batch_processor.py   172     35    80%   60-62, 95-104, 109
```

### HTML Report
Generated in `htmlcov/index.html` - provides:
- Interactive file-by-file coverage
- Line-by-line highlighting of covered/uncovered code
- Detailed statistics and graphs

To view HTML report:
```bash
open htmlcov/index.html
```

## Tips

1. **Use the shell script** (`run_coverage.sh`) for the best experience
2. **Run with --html** when you need detailed analysis
3. **Test specific modules** when working on particular features
4. **Check the htmlcov folder** is created in the project root after HTML reports
5. **Run from the tests directory** for convenience

## Coverage Goals

- **batch_processor.py**: Currently at 80% - excellent coverage  
- **Overall project**: **Currently at 96% - OUTSTANDING!** 🎉
- **Total lines**: 8,143 lines of code with only 299 lines missing coverage
- **Test suite**: 2,571 tests passing, 80 skipped
- **Focus areas**: The remaining 4% consists of edge cases and error handling paths

## Viewing Full Coverage Report

The terminal output shows only the **last portion** of the coverage report. To see the complete details:

### Full Terminal Report
```bash
# Pipe to less for scrollable viewing
./run_coverage.sh | less

# Save full report to file
./run_coverage.sh > coverage_report.txt 2>&1
```

### HTML Report (Recommended)
```bash
# Generate and view interactive HTML report
./run_coverage.sh --html
open htmlcov/index.html
```

The HTML report provides:
- **Interactive file browser** with coverage percentages
- **Line-by-line highlighting** of covered/uncovered code  
- **Detailed statistics** and branch coverage
- **Search and filtering** capabilities

### Coverage Summary Display

The script now provides an intelligent summary showing:
- **Overall coverage percentage** for the entire project
- **Files with lowest coverage** - areas that need attention
- **Files with highest coverage** - examples of good test coverage

This replaces the previous simple display that only showed the last few files alphabetically.
