"""Test data for metadata generation functionality."""

import csv
import os
import tempfile

# Create test CSV data with various data types and patterns
test_data = [
    ["id", "name", "age", "salary", "category", "active", "score", "description"],
    [1, "John Doe", 25, "$50,000", "A", True, 85.5, "Software Engineer with expertise in Python"],
    [2, "Jane Smith", 30, "$75,000", "B", True, 92.0, "Data Scientist specializing in ML"],
    [
        3,
        "Bob Johnson",
        35,
        "$60,000",
        "A",
        False,
        78.5,
        "Product Manager with 10+ years experience",
    ],
    [4, "Alice Brown", 28, "$55,000", "C", True, 88.0, "UI/UX Designer focusing on mobile apps"],
    [5, "Charlie Davis", 42, "$85,000", "B", True, 95.5, "Senior Developer and tech lead"],
    [6, "Diana Wilson", 33, "$70,000", "A", False, 82.0, "DevOps Engineer with cloud expertise"],
    [7, "Frank Miller", 29, "$52,000", "C", True, 79.5, "QA Engineer with automation skills"],
    [8, "Grace Lee", 26, "$48,000", "B", True, 86.5, "Junior Developer learning full-stack"],
    [
        9,
        "Henry Taylor",
        38,
        "$90,000",
        "A",
        True,
        91.0,
        "Architect with distributed systems expertise",
    ],
    [10, "Ivy Chen", 31, "$65,000", "C", False, 83.5, "Business Analyst with domain knowledge"],
]

# Create temporary CSV file
temp_dir = tempfile.mkdtemp()
test_csv_path = os.path.join(temp_dir, "test_data.csv")

with open(test_csv_path, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerows(test_data)

print(f"Test data created at: {test_csv_path}")
print("Sample data for metadata generation testing:")
for i, row in enumerate(test_data[:3]):
    print(f"Row {i}: {row}")
