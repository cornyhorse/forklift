"""``SchemaGenerator`` outputs: clipboard support detection, unsupported output targets,
metadata without a separate file and validation of a generated schema against its data."""

import importlib.util
import json
import sys
import types

import pyarrow as pa
import pytest

from forklift.schema.generator import core
from forklift.schema.generator.core import (
    FileType,
    OutputTarget,
    SchemaGenerationConfig,
    SchemaGenerator,
)


def _csv(tmp_path):
    path = tmp_path / "people.csv"
    path.write_text("id,name\n1,Ann\n2,Bob\n")
    return path


def _load_core(monkeypatch, pyperclip_module):
    """Execute core.py as a separate module while ``import pyperclip`` gives the stand-in
    (``None`` makes the import fail)."""
    monkeypatch.setitem(sys.modules, "pyperclip", pyperclip_module)
    name = "forklift.schema.generator._core_clipboard_probe"
    spec = importlib.util.spec_from_file_location(name, core.__file__)
    module = importlib.util.module_from_spec(spec)
    monkeypatch.setitem(sys.modules, name, module)
    spec.loader.exec_module(module)
    return module


class TestClipboardSupportDetection:
    def test_installed_pyperclip_enables_copying_to_the_clipboard(
        self, tmp_path, monkeypatch, capsys
    ):
        copied = []
        fake = types.ModuleType("pyperclip")
        fake.copy = copied.append
        module = _load_core(monkeypatch, fake)

        generator = module.SchemaGenerator(
            module.SchemaGenerationConfig(
                input_path=_csv(tmp_path),
                file_type=module.FileType.CSV,
                output_target=module.OutputTarget.CLIPBOARD,
            )
        )
        generator.output_schema({"title": "people"})

        assert module.CLIPBOARD_AVAILABLE is True
        assert module.pyperclip is fake
        assert [json.loads(text) for text in copied] == [{"title": "people"}]
        assert capsys.readouterr().out == "Schema copied to clipboard\n"

    def test_missing_pyperclip_falls_back_to_stdout(self, tmp_path, monkeypatch, capsys):
        module = _load_core(monkeypatch, None)

        generator = module.SchemaGenerator(
            module.SchemaGenerationConfig(
                input_path=_csv(tmp_path),
                file_type=module.FileType.CSV,
                output_target=module.OutputTarget.CLIPBOARD,
            )
        )
        generator.output_schema({"title": "people"})

        assert module.CLIPBOARD_AVAILABLE is False
        assert module.pyperclip is None
        out = capsys.readouterr().out
        assert out.startswith("Pyperclip not available. Falling back to stdout:\n")
        assert json.loads(out.split("\n", 1)[1]) == {"title": "people"}


class TestOutputTargets:
    def test_unsupported_output_target_is_rejected(self, tmp_path, capsys):
        generator = SchemaGenerator(
            SchemaGenerationConfig(
                input_path=_csv(tmp_path), file_type=FileType.CSV, output_target="printer"
            )
        )

        with pytest.raises(ValueError, match="Unsupported output target: printer"):
            generator.output_schema({"title": "people"})
        assert capsys.readouterr().out == ""


class TestMetadataWithoutSeparateFile:
    def test_metadata_is_not_saved_without_an_output_path(self, tmp_path):
        generator = SchemaGenerator(
            SchemaGenerationConfig(input_path=_csv(tmp_path), file_type=FileType.CSV)
        )
        table = generator._read_sample_data()

        assert generator.generate_and_save_metadata(table) is None
        assert sorted(p.name for p in tmp_path.iterdir()) == ["people.csv"]

    def test_generated_schema_embeds_the_metadata(self, tmp_path):
        generator = SchemaGenerator(
            SchemaGenerationConfig(input_path=_csv(tmp_path), file_type=FileType.CSV)
        )

        schema = generator.generate_schema()

        assert schema["x-metadata"]["table_metadata"]["row_count"] == 2
        assert set(schema["x-metadata"]["column_metadata"]) == {"id", "name"}


class TestValidateGeneratedSchema:
    @pytest.fixture
    def generated(self, tmp_path):
        generator = SchemaGenerator(
            SchemaGenerationConfig(input_path=_csv(tmp_path), file_type=FileType.CSV)
        )
        table = generator._read_sample_data()
        return generator, generator._generate_schema_from_table(table), table

    def test_schema_generated_from_the_data_is_valid(self, generated):
        generator, schema, table = generated

        assert generator.validate_generated_schema(schema, table) == (True, [])

    def test_problems_from_every_check_are_collected(self, generated):
        generator, schema, table = generated
        del schema["$schema"]
        schema["x-transformations"] = {"global_settings": "strict"}
        table = table.append_column("extra", pa.array(["x", "y"]))

        valid, issues = generator.validate_generated_schema(schema, table)

        assert valid is False
        assert issues == [
            "Missing required field: $schema",
            "Extra columns in data: extra",
            "global_settings must be a dictionary",
        ]

    def test_schema_without_transformations_skips_that_check(self, generated):
        generator, schema, table = generated
        del schema["x-transformations"]

        assert generator.validate_generated_schema(schema, table) == (True, [])
