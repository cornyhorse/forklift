"""Debug test to understand why bad rows aren't being added."""

import pyarrow as pa
from unittest.mock import Mock, patch
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from forklift.processors.enhanced_processor import EnhancedDataProcessor
from forklift.processors.base import ValidationResult

# Create a simple test to debug the issue
def debug_bad_rows():
    # Mock dependencies
    with patch('forklift.processors.enhanced_processor.SchemaValidator') as mock_schema_validator, \
         patch('forklift.processors.enhanced_processor.ConstraintValidator') as mock_constraint_validator, \
         patch('forklift.processors.enhanced_processor.create_constraint_config_from_schema') as mock_create_config:

        mock_create_config.return_value = Mock()
        mock_schema_validator.return_value = Mock()
        mock_constraint_validator_instance = Mock()
        mock_constraint_validator_instance.get_all_violations.return_value = []
        mock_constraint_validator.return_value = mock_constraint_validator_instance

        # Create schema
        test_schema = pa.schema([
            pa.field("id", pa.int64(), nullable=False),
            pa.field("name", pa.string(), nullable=True),
            pa.field("age", pa.int64(), nullable=True),
            pa.field("email", pa.string(), nullable=True)
        ])

        processor = EnhancedDataProcessor(test_schema)

        # Create batch with null/invalid values
        test_batch = pa.RecordBatch.from_pydict({
            'id': [1, None, 3],
            'name': ['Alice', 'Bob', None],
            'age': [25, 30, None],
            'email': ['alice@test.com', None, 'charlie@test.com']
        })

        print(f"Batch size: {test_batch.num_rows}")
        print(f"Batch columns: {test_batch.num_columns}")

        # Create validation result for row index 1
        validation_results = [
            ValidationResult(False, "NULL_ERROR", "Null value not allowed", "id", 1)
        ]

        print(f"Validation results: {[f'is_valid={r.is_valid}, row_index={r.row_index}, type={type(r.row_index)}' for r in validation_results]}")

        # Check if row index is within bounds
        for result in validation_results:
            print(f"Row index {result.row_index} < batch size {test_batch.num_rows}: {result.row_index < test_batch.num_rows}")

        # Call the actual method
        print("Calling _handle_bad_rows...")
        try:
            processor._handle_bad_rows(test_batch, test_batch, validation_results)
            print(f"Bad row count after processing: {processor.bad_rows_handler.get_bad_row_count()}")
            print(f"Bad rows: {processor.bad_rows_handler.bad_rows}")
        except Exception as e:
            print(f"Error in _handle_bad_rows: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    debug_bad_rows()
