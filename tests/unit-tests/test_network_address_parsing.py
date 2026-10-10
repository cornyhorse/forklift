"""IP and MAC address parsing rules of ``transformations.format.network``."""

import pyarrow as pa
import pytest

from forklift.utils.transformations.configs import IPAddressConfig, MACAddressConfig
from forklift.utils.transformations.format.network import (
    IPAddressFormatter,
    MACAddressFormatter,
)


class TestIPAddressWithoutValidation:
    def test_invalid_address_passes_through_stripped(self):
        formatter = IPAddressFormatter(IPAddressConfig(validate=False))

        assert formatter.format_value(" not-an-ip ") == "not-an-ip"

    @pytest.mark.parametrize("ip_version", ["ipv4", "ipv6", "both"])
    def test_invalid_address_is_rejected_when_validating(self, ip_version):
        formatter = IPAddressFormatter(IPAddressConfig(ip_version=ip_version))

        with pytest.raises(ValueError, match="Invalid"):
            formatter.format_value("not-an-ip")


class TestMACAddressSeparators:
    @pytest.mark.parametrize(
        "value",
        [
            "00/11/22/33/44/55",  # one consistent separator, but not ':', '-', '.' or ' '
            "0011::2233:4455",  # an empty group between two separators
            "00:11:22:33:44:55:",  # a trailing separator
            "00:11:22:33:44:5G",  # a non-hex group
        ],
    )
    def test_malformed_address_is_rejected(self, value):
        formatter = MACAddressFormatter(MACAddressConfig())

        with pytest.raises(ValueError, match="exactly 12 hexadecimal digits"):
            formatter.format_value(value)

    def test_malformed_addresses_become_null_in_a_column(self):
        formatter = MACAddressFormatter(MACAddressConfig())

        result = formatter.apply_formatting(pa.array(["00/11/22/33/44/55", "00-11-22-33-44-55"]))

        assert result.to_pylist() == [None, "00:11:22:33:44:55"]

    def test_malformed_address_passes_through_without_validation(self):
        formatter = MACAddressFormatter(MACAddressConfig(validate=False))

        assert formatter.format_value(" 00/11/22/33/44/55 ") == "00/11/22/33/44/55"
