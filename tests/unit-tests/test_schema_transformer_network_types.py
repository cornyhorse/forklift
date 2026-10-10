"""SchemaBasedTransformer: IP and MAC address x-special-type columns, malformed configuration."""

from __future__ import annotations

import pyarrow as pa
import pytest

from forklift.processors.transformations.schema_transformer import SchemaBasedTransformer


def special(**types):
    return SchemaBasedTransformer(
        {"properties": {name: {"x-special-type": kind} for name, kind in types.items()}}
    )


class TestIpAddressColumns:
    def test_each_ip_flavour_keeps_only_its_own_addresses(self):
        transformer = special(v4="ipv4", v6="ipv6", either="ip")
        batch = pa.RecordBatch.from_pydict(
            {
                "v4": ["192.168.1.1", "::1"],
                "v6": ["2001:0db8:0000:0000:0000:0000:0000:0001", "1.2.3.4"],
                "either": ["10.0.0.1", "2001:db8::1"],
            }
        )

        out, results = transformer.process_batch(batch)

        assert out.column("v4").to_pylist() == ["192.168.1.1", None]
        assert out.column("v6").to_pylist() == ["2001:db8::1", None]
        assert out.column("either").to_pylist() == ["10.0.0.1", "2001:db8::1"]
        assert [(r.column_name, r.row_index, r.error_code) for r in results] == [
            ("v4", 1, "INVALID_SPECIAL_VALUE"),
            ("v6", 1, "INVALID_SPECIAL_VALUE"),
        ]
        assert results[0].error_message == "Column 'v4' has a value that is not a valid 'ipv4'"


class TestMacAddressColumns:
    def test_addresses_are_normalised_and_invalid_ones_reported(self):
        transformer = special(mac="mac-address")
        batch = pa.RecordBatch.from_pydict({"mac": ["AA-BB-CC-DD-EE-FF", "aabb.ccdd.eeff", "zz"]})

        out, results = transformer.process_batch(batch)

        assert out.column("mac").to_pylist() == ["aa:bb:cc:dd:ee:ff", "aa:bb:cc:dd:ee:ff", None]
        assert [(r.row_index, r.error_code) for r in results] == [(2, "INVALID_SPECIAL_VALUE")]


class TestMalformedConfiguration:
    def test_x_transformations_must_be_an_object(self):
        with pytest.raises(ValueError, match="^x-transformations must be an object$"):
            SchemaBasedTransformer({"x-transformations": ["trim"]})
