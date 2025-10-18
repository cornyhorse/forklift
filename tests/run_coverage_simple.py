#!/usr/bin/env python3
"""
Simple coverage runner for Forklift project
Usage: python run_coverage_simple.py [module_name]
"""
import subprocess
import sys
import os
from pathlib import Path

def main():
    """Run coverage analysis"""
    # Change to project root
    project_root = Path(__file__).parent.parent
    os.chdir(project_root)
    
    # Determine what to test
    if len(sys.argv) > 1:
        module = sys.argv[1]
        test_pattern = f"tests/test_{module}.py"
        cov_source = f"src/forklift"
        print(f"Running coverage for module: {module}")
    else:
        test_pattern = "tests/"
        cov_source = "src/forklift/"
        print("Running coverage for all modules")
    
    # Build and run command
    cmd = [
        sys.executable, "-m", "pytest",
        test_pattern,
        f"--cov={cov_source}",
        "--cov-report=term-missing",
        "--cov-report=html",
        "-v"
    ]
    
    print(f"Command: {' '.join(cmd)}")
    print("-" * 50)
    
    try:
        result = subprocess.run(cmd, check=True)
        print("\n✓ Coverage completed successfully!")
        print("✓ HTML report available at: htmlcov/index.html")
        return 0
    except subprocess.CalledProcessError as e:
        print(f"\n✗ Coverage failed with exit code: {e.returncode}")
        return e.returncode

if __name__ == "__main__":
    sys.exit(main())
