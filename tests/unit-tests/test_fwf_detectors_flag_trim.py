"""FwfSchemaDetector: the flag column's ``trim`` setting decides how flags are compared."""

from __future__ import annotations

from forklift.inputs.config import FwfConditionalSchema, FwfFieldSpec, FwfInputConfig
from forklift.inputs.fwf import FwfSchemaDetector


def _detector(trim: bool) -> FwfSchemaDetector:
    schemas = [
        FwfConditionalSchema("A", "padded", [FwfFieldSpec("v", 3, 3)]),
        FwfConditionalSchema("A ", "exact", [FwfFieldSpec("v", 3, 3)]),
    ]
    flag = FwfFieldSpec("kind", 1, 2, trim=trim)
    return FwfSchemaDetector(FwfInputConfig(conditional_schemas=schemas, flag_column=flag))


class TestFlagTrimming:
    def test_untrimmed_flag_keeps_its_padding(self):
        assert _detector(trim=False).detect_conditional_schema("A 123").description == "exact"

    def test_trimmed_flag_drops_its_padding(self):
        assert _detector(trim=True).detect_conditional_schema("A 123").description == "padded"
