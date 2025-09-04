#!/usr/bin/env python3
"""
Script to run date_parser tests and check coverage
"""
import subprocess
import sys
import os

def run_coverage():
    """Run coverage analysis on date_parser tests"""
    print("Running date_parser tests with coverage analysis...")

    # Change to the project directory
    os.chdir('/Users/matt/PycharmProjects/forklift')

    try:
        # Run pytest with coverage
        result = subprocess.run([
            sys.executable, '-m', 'pytest',
            'test_date_parser.py',
            '--cov=src/forklift/utils/date_parser',
            '--cov-report=term-missing',
            '-v'
        ], capture_output=True, text=True, timeout=60)

        print("STDOUT:")
        print(result.stdout)
        print("\nSTDERR:")
        print(result.stderr)
        print(f"\nReturn code: {result.returncode}")

    except subprocess.TimeoutExpired:
        print("Test execution timed out")
    except Exception as e:
        print(f"Error running tests: {e}")

if __name__ == "__main__":
    run_coverage()
