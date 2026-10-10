"""FWF config validation accepts parameterised type names and the schema honours them."""

from __future__ import annotations

import pyarrow as pa
import pytest

from forklift.inputs.config import FwfFieldSpec, FwfInputConfig
from forklift.inputs.fwf import FwfConfigValidator, FwfInputHandler


class TestParameterisedTypeNames:
    @pytest.mark.parametrize(
        "parquet_type, arrow_type",
        [
            ("timestamp[ms]", pa.timestamp("ms")),
            ("duration[s]", pa.duration("s")),
            ("list<string>", pa.list_(pa.string())),
        ],
    )
    def test_type_with_parameters_is_valid_and_reaches_the_schema(self, parquet_type, arrow_type):
        config = FwfInputConfig(fields=[FwfFieldSpec("f", 1, 4, parquet_type=parquet_type)])

        FwfConfigValidator.validate_config(config)  # does not raise

        assert FwfInputHandler(config).get_arrow_schema().field("f").type == arrow_type

    @pytest.mark.parametrize("parquet_type", ["timestamp[ms", "duration(s)", "list<string"])
    def test_malformed_parameterised_type_is_rejected(self, parquet_type):
        config = FwfInputConfig(fields=[FwfFieldSpec("f", 1, 4, parquet_type=parquet_type)])
        with pytest.raises(ValueError, match=r"^Invalid data type: "):
            FwfConfigValidator.validate_config(config)
