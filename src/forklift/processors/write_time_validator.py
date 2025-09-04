            return results

        # Check if schemas match
        if not batch.schema.equals(self.config.expected_schema):
            error_msg = f"Schema mismatch. Expected: {self.config.expected_schema}, Got: {batch.schema}"

            if self.config.fail_on_schema_mismatch:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=error_msg,
                    error_code="SCHEMA_MISMATCH"
                ))
"""Write-time validation processor for ensuring data quality before writing."""
                # Just warn about schema differences
                results.append(ValidationResult(
                    is_valid=True,
                    error_message=f"Schema warning: {error_msg}",
                    error_code="SCHEMA_WARNING"
                ))

        return results

    def _validate_required_columns(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that all required columns are present."""
        results = []

        if not self.config.required_columns:
            return results

        present_columns = set(batch.schema.names)
        missing_columns = set(self.config.required_columns) - present_columns

        if missing_columns:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Missing required columns: {sorted(missing_columns)}",
                error_code="MISSING_REQUIRED_COLUMNS"
            ))

        return results

    def _validate_null_percentages(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate null percentages don't exceed thresholds."""
        results = []

        if batch.num_rows == 0:
            return results

        for i, column_name in enumerate(batch.schema.names):
            column = batch.column(i)
            null_count = pc.sum(pc.is_null(column)).as_py()
            null_percentage = (null_count / batch.num_rows) * 100

            if null_percentage > self.config.max_null_percentage:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Column '{column_name}' has {null_percentage:.1f}% null values, exceeds threshold of {self.config.max_null_percentage}%",
                    error_code="EXCESSIVE_NULLS",
                    column_name=column_name
                ))

        return results

    def _validate_primary_key_nulls(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that primary key columns don't contain nulls."""
        results = []

        schema_names = batch.schema.names

        for pk_column in self.config.primary_key_columns:
            if pk_column not in schema_names:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Primary key column '{pk_column}' not found in schema",
                    error_code="MISSING_PRIMARY_KEY_COLUMN",
                    column_name=pk_column
                ))
                continue

            column_index = schema_names.index(pk_column)
            column = batch.column(column_index)
            null_count = pc.sum(pc.is_null(column)).as_py()

            if null_count > 0:
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Primary key column '{pk_column}' contains {null_count} null values",
                    error_code="NULL_PRIMARY_KEY",
                    column_name=pk_column
                ))

        return results

    def _validate_duplicate_rows(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that there are no duplicate rows based on primary key."""
        results = []

        if not self.config.primary_key_columns or batch.num_rows == 0:
            return results

        schema_names = batch.schema.names

        # Check if all primary key columns exist
        missing_pk_columns = set(self.config.primary_key_columns) - set(schema_names)
        if missing_pk_columns:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Primary key columns not found: {sorted(missing_pk_columns)}",
                error_code="MISSING_PRIMARY_KEY_COLUMNS"
            ))
            return results

        # Extract primary key values
        pk_indices = [schema_names.index(col) for col in self.config.primary_key_columns]

        duplicate_rows = []
        current_batch_keys = set()

        for row_idx in range(batch.num_rows):
            # Create primary key tuple for this row
            pk_values = tuple(
                batch.column(col_idx)[row_idx].as_py() if not batch.column(col_idx)[row_idx].is_valid else None
                for col_idx in pk_indices
            )

            # Check for duplicates within this batch
            if pk_values in current_batch_keys:
                duplicate_rows.append(row_idx)
            else:
                current_batch_keys.add(pk_values)

            # Check for duplicates across batches
            if pk_values in self._seen_primary_keys:
                duplicate_rows.append(row_idx)
            else:
                self._seen_primary_keys.add(pk_values)

        if duplicate_rows:
            results.append(ValidationResult(
                is_valid=False,
                error_message=f"Found {len(duplicate_rows)} duplicate primary key values in rows: {duplicate_rows[:10]}{'...' if len(duplicate_rows) > 10 else ''}",
                error_code="DUPLICATE_PRIMARY_KEYS"
            ))

        return results

    def _validate_write_readiness(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that data is ready for writing (general checks)."""
        results = []

        # Check for unsupported data types that might cause write issues
        for field in batch.schema:
            if field.type == pa.null():
                results.append(ValidationResult(
                    is_valid=False,
                    error_message=f"Column '{field.name}' has null type which may cause write issues",
                    error_code="NULL_TYPE_COLUMN",
                    column_name=field.name
                ))

        # Check for extremely large strings that might cause issues
        for i, field in enumerate(batch.schema):
            if field.type == pa.string():
                column = batch.column(i)
                try:
                    max_length = pc.max(pc.utf8_length(column)).as_py()
                    if max_length and max_length > 1000000:  # 1MB limit
                        results.append(ValidationResult(
                            is_valid=False,
                            error_message=f"Column '{field.name}' contains very large strings (max: {max_length} chars) that may cause write issues",
                            error_code="LARGE_STRING_VALUES",
                            column_name=field.name
                        ))
                except Exception:
                    # Skip if we can't compute string lengths
                    pass

        return results

    def reset_state(self):
        """Reset internal state for processing new datasets."""
        self._seen_primary_keys.clear()


def create_basic_write_validator(primary_key_columns: Optional[List[str]] = None) -> WriteTimeValidator:
    """Create a basic write-time validator with common settings.

    Args:
        primary_key_columns: List of primary key column names

    Returns:
        WriteTimeValidator with basic configuration
    """
    config = WriteTimeConfig(
        check_empty_tables=True,
        check_duplicate_rows=bool(primary_key_columns),
        check_null_primary_keys=bool(primary_key_columns),
        primary_key_columns=primary_key_columns or [],
        max_null_percentage=90.0  # Allow high null percentage for basic validation
    )
    return WriteTimeValidator(config)


def create_strict_write_validator(
    primary_key_columns: List[str],
    required_columns: List[str],
    expected_schema: Optional[pa.Schema] = None
) -> WriteTimeValidator:
    """Create a strict write-time validator for production use.

    Args:
        primary_key_columns: List of primary key column names
        required_columns: List of required column names
        expected_schema: Expected schema for validation

    Returns:
        WriteTimeValidator with strict configuration
    """
    config = WriteTimeConfig(
        check_empty_tables=True,
        check_schema_compliance=True,
        check_duplicate_rows=True,
        check_null_primary_keys=True,
        primary_key_columns=primary_key_columns,
        required_columns=required_columns,
        max_null_percentage=10.0,
        fail_on_schema_mismatch=True,
        expected_schema=expected_schema,
        validate_write_readiness=True
    )
    return WriteTimeValidator(config)
        check_null_primary_keys: Whether to validate primary key columns are not null
        primary_key_columns: List of column names that form the primary key
        required_columns: List of column names that are required to be present
        max_null_percentage: Maximum percentage of null values allowed per column
        fail_on_schema_mismatch: Whether to fail if schema doesn't match expected
        expected_schema: Expected PyArrow schema for validation
        validate_write_readiness: Whether to perform write-readiness checks
    """
    check_empty_tables: bool = True
    check_schema_compliance: bool = True
    check_duplicate_rows: bool = True
    check_null_primary_keys: bool = True
    primary_key_columns: Optional[List[str]] = None
    required_columns: Optional[List[str]] = None
    max_null_percentage: float = 50.0
    fail_on_schema_mismatch: bool = False
    expected_schema: Optional[pa.Schema] = None
    validate_write_readiness: bool = True
    
    def __post_init__(self):
        if self.primary_key_columns is None:
            self.primary_key_columns = []
        if self.required_columns is None:
            self.required_columns = []


class WriteTimeValidator(BaseProcessor):
    """Validates data quality and consistency before writing.

    This processor performs final validation checks before data is written
    to ensure data quality, schema compliance, and write-readiness.

    Examples:
        # Basic write-time validation
        config = WriteTimeConfig(
            primary_key_columns=['id'],
            required_columns=['id', 'name', 'created_at']
        )

        # Schema validation with expected schema
        config = WriteTimeConfig(
            expected_schema=expected_schema,
            fail_on_schema_mismatch=True
        )

        # Comprehensive validation
        config = WriteTimeConfig(
            check_empty_tables=True,
            check_duplicate_rows=True,
            primary_key_columns=['state_id', 'county_code'],
            max_null_percentage=10.0
        )
    """
    
    def __init__(self, config: WriteTimeConfig):
        """Initialize the write-time validator.

        Args:
            config: Write-time validation configuration
        """
        self.config = config
        self._seen_primary_keys: Set[Tuple] = set()
    
    def process_batch(self, batch: pa.RecordBatch) -> Tuple[pa.RecordBatch, List[ValidationResult]]:
        """Process a batch by performing write-time validation.

        Args:
            batch: PyArrow RecordBatch to validate

        Returns:
            Tuple of (validated_batch, validation_results)
        """
        validation_results = []
        
        try:
            # Check if table is empty
            if self.config.check_empty_tables:
                validation_results.extend(self._validate_not_empty(batch))
            
            # Check schema compliance
            if self.config.check_schema_compliance:
                validation_results.extend(self._validate_schema_compliance(batch))
            
            # Check required columns are present
            validation_results.extend(self._validate_required_columns(batch))
            
            # Check null percentages
            validation_results.extend(self._validate_null_percentages(batch))
            
            # Check primary key constraints
            if self.config.check_null_primary_keys and self.config.primary_key_columns:
                validation_results.extend(self._validate_primary_key_nulls(batch))
            
            # Check for duplicate rows
            if self.config.check_duplicate_rows:
                validation_results.extend(self._validate_duplicate_rows(batch))
            
            # Check write readiness
            if self.config.validate_write_readiness:
                validation_results.extend(self._validate_write_readiness(batch))
            
            return batch, validation_results
            
        except Exception as e:
            validation_results.append(ValidationResult(
                is_valid=False,
                error_message=f"Write-time validation failed: {str(e)}",
                error_code="WRITE_VALIDATION_ERROR"
            ))
            return batch, validation_results
    
    def _validate_not_empty(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate that the batch is not empty."""
        results = []
        
        if batch.num_rows == 0:
            results.append(ValidationResult(
                is_valid=False,
                error_message="Cannot write empty table",
                error_code="EMPTY_TABLE"
            ))
        
        return results
    
    def _validate_schema_compliance(self, batch: pa.RecordBatch) -> List[ValidationResult]:
        """Validate schema compliance against expected schema."""
        results = []
        
        if self.config.expected_schema is None:
            else:
                # Handle processors that might not follow the standard interface
                current_batch = processor.transform(current_batch)

        # After all transformations, validate constraints
        if self.write_time_validator:
            valid_batch, invalid_batch, constraint_results = self.write_time_validator.validate_and_split(current_batch)
            all_validation_results.extend(constraint_results)
            return valid_batch, invalid_batch, all_validation_results
        else:
            # No constraint validation, return all data as valid
            return current_batch, None, all_validation_results

    def finalize(self) -> Dict[str, Any]:
        """Finalize processing and return summary."""
        if self.write_time_validator:
            return self.write_time_validator.finalize()
        else:
            return {"constraint_validation_passed": True, "total_violations": 0}
