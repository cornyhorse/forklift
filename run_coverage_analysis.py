#!/usr/bin/env python3
"""Script to run coverage analysis and identify areas needing improvement."""

import subprocess
import sys
import os
from pathlib import Path

def main():
    """Run coverage analysis and report results."""
    # Change to project directory
    project_dir = Path(__file__).parent
    os.chdir(project_dir)
    
    print("Running coverage analysis...")
    
    # Run pytest with coverage
    cmd = [
        sys.executable, "-m", "pytest", 
        "--cov=src", 
        "--cov-report=term-missing",
        "--cov-report=html",
        "tests/",
        "-v"
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        print("STDOUT:")
        print(result.stdout)
        print("\nSTDERR:")
        print(result.stderr)
        print(f"\nReturn code: {result.returncode}")
        
        # Also try to get coverage report
        if result.returncode == 0:
            print("\n" + "="*50)
            print("COVERAGE REPORT")
            print("="*50)
            
            coverage_cmd = [sys.executable, "-m", "coverage", "report", "--show-missing"]
            cov_result = subprocess.run(coverage_cmd, capture_output=True, text=True)
            print(cov_result.stdout)
            if cov_result.stderr:
                print("Coverage stderr:", cov_result.stderr)
                
    except subprocess.TimeoutExpired:
        print("Test execution timed out after 5 minutes")
    except Exception as e:
        print(f"Error running tests: {e}")

if __name__ == "__main__":
    main()
