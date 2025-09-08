================================================================================== FAILURES ===================================================================================
______________________________________________________________________ test_password_resolution_from_env ______________________________________________________________________

tmp_path = PosixPath('/private/var/folders/v6/nzt3xh4j0yjgh_wq46slzmj40000gn/T/pytest-of-matt/pytest-79/test_password_resolution_from_0')

    def test_password_resolution_from_env(tmp_path: Path):
        """Test password resolution from environment variable"""
        db_path = tmp_path / "test_db.kdbx"
        sidecar_path = db_path.parent / ".mattstash.txt"

        # Ensure sidecar doesn't exist so it falls back to env
        if sidecar_path.exists():
            sidecar_path.unlink()

        with patch.dict(os.environ, {'KDBX_PASSWORD': 'env_password'}, clear=False):
            ms = MattStash(path=str(db_path), password=None)  # Force password resolution
            # Password should come from explicit resolution, not bootstrap
            resolved_password = ms._resolve_password()
>           assert resolved_password == 'env_password'
E           AssertionError: assert 'Nkbl8g11OZmv...AhdI5R8dcmeIU' == 'env_password'
E
E             - env_password
E             + Nkbl8g11OZmv_Gapl7z1G8G8B8DXbuAhdI5R8dcmeIU

tests/test_advanced_coverage.py:88: AssertionError
---------------------------------------------------------------------------- Captured stderr call -----------------------------------------------------------------------------
[MattStash] Created new KeePass DB at /private/var/folders/v6/nzt3xh4j0yjgh_wq46slzmj40000gn/T/pytest-of-matt/pytest-79/test_password_resolution_from_0/test_db.kdbx and sidecar /private/var/folders/v6/nzt3xh4j0yjgh_wq46slzmj40000gn/T/pytest-of-matt/pytest-79/test_password_resolution_from_0/.mattstash.txt
[MattStash] Loaded password from sidecar file /private/var/folders/v6/nzt3xh4j0yjgh_wq46slzmj40000gn/T/pytest-of-matt/pytest-79/test_password_resolution_from_0/.mattstash.txt
[MattStash] Loaded password from sidecar file /private/var/folders/v6/nzt3xh4j0yjgh_wq46slzmj40000gn/T/pytest-of-matt/pytest-79/test_password_resolution_from_0/.mattstash.txt
_____________________________________________________________________ test_password_resolution_no_sources _____________________________________________________________________

tmp_path = PosixPath('/private/var/folders/v6/nzt3xh4j0yjgh_wq46slzmj40000gn/T/pytest-of-matt/pytest-79/test_password_resolution_no_so0')

    def test_password_resolution_no_sources(tmp_path: Path):
        """Test password resolution when no sources available"""
        db_path = tmp_path / "test_db.kdbx"
        sidecar_path = db_path.parent / ".mattstash.txt"

        # Ensure sidecar doesn't exist
        if sidecar_path.exists():
            sidecar_path.unlink()

        # Ensure no environment variable and test resolution directly
        env_backup = os.environ.get('KDBX_PASSWORD')
        if 'KDBX_PASSWORD' in os.environ:
            del os.environ['KDBX_PASSWORD']

        try:
            with patch('builtins.print') as mock_print:
                ms = MattStash(path=str(db_path), password=None)
                resolved = ms._resolve_password()
                # Should return None when no sources available
>               assert resolved is None
E               AssertionError: assert 'HJ17bVeQu5MfS33Eof-z5B-fSrWeEs_CzHXhMD2W7Mk' is None

tests/test_advanced_coverage.py:110: AssertionError
__________________________________________________________________________ test_main_delete_success ___________________________________________________________________________

temp_db = PosixPath('/private/var/folders/v6/nzt3xh4j0yjgh_wq46slzmj40000gn/T/pytest-of-matt/pytest-79/test_main_delete_success0/mattstash/test.kdbx')

    def test_main_delete_success(temp_db: Path):
        """Test main function delete command success"""
        # First create an entry
        main(["--db", str(temp_db), "put", "to-delete", "--value", "test"])

        # Verify it was created successfully by attempting to get it
        result = main(["--db", str(temp_db), "get", "to-delete"])
        assert result == 0  # Should succeed

        # Then delete it - capture stdout to verify success message
        import io
        from contextlib import redirect_stdout

        output = io.StringIO()
        with redirect_stdout(output):
            result = main([
                "--db", str(temp_db),
                "delete", "to-delete"
            ])

>       assert result == 0
E       assert 2 == 0

tests/test_cli_coverage.py:124: AssertionError
"""Tests for schema validator processor backward compatibility."""

import pytest


class TestSchemaValidatorBackwardCompatibility:
    """Test backward compatibility of schema validator processor module."""

    def test_schema_validator_imports(self):
        """Test that all schema validator classes can be imported from the main module."""
        from forklift.processors.schema_validator import (
            SchemaValidator,
            SchemaValidatorConfig,
            SchemaValidationMode,
            NullabilityMode,
            ColumnSchema,
            create_schema_validator_from_json,
            create_schema_from_batch
        )

        # Verify all classes and functions are available
        assert SchemaValidator is not None
        assert SchemaValidatorConfig is not None
        assert SchemaValidationMode is not None
        assert NullabilityMode is not None
        assert ColumnSchema is not None
        assert callable(create_schema_validator_from_json)
        assert callable(create_schema_from_batch)

    def test_schema_validator_all_exports(self):
        """Test that __all__ contains expected exports."""
        import forklift.processors.schema_validator as sv_module

        expected_exports = [
            'SchemaValidator',
            'SchemaValidatorConfig',
            'SchemaValidationMode',
            'NullabilityMode',
            'ColumnSchema',
            'create_schema_validator_from_json',
            'create_schema_from_batch'
        ]

        assert hasattr(sv_module, '__all__')
        assert set(sv_module.__all__) == set(expected_exports)

    def test_schema_validator_module_docstring(self):
        """Test that the module has proper documentation."""
        import forklift.processors.schema_validator as sv_module

        assert sv_module.__doc__ is not None
        assert "Backward compatibility wrapper" in sv_module.__doc__
        assert "schema validator" in sv_module.__doc__

    def test_schema_validator_classes_are_callable(self):
        """Test that imported classes are actually callable."""
        from forklift.processors.schema_validator import (
            SchemaValidator,
            SchemaValidatorConfig,
            SchemaValidationMode,
            NullabilityMode,
            ColumnSchema
        )

        # Verify classes are callable (can be instantiated)
        assert callable(SchemaValidator)
        assert callable(SchemaValidatorConfig)
        assert callable(SchemaValidationMode)
        assert callable(NullabilityMode)
        assert callable(ColumnSchema)

    def test_schema_validator_utility_functions(self):
        """Test that utility functions are callable."""
        from forklift.processors.schema_validator import (
            create_schema_validator_from_json,
            create_schema_from_batch
        )

        # Verify functions are callable
        assert callable(create_schema_validator_from_json)
        assert callable(create_schema_from_batch)
