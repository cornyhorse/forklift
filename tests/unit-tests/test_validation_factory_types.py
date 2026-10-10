"""ValidationFactory: validator types that are neither a ValidatorType nor its value."""

from __future__ import annotations

import pytest

from forklift.processors.validation_factory import ValidationFactory


class TestUnsupportedValidatorTypes:
    @pytest.mark.parametrize("validator_type", [42, None])
    def test_a_value_of_another_type_is_rejected(self, validator_type):
        with pytest.raises(ValueError) as error:
            ValidationFactory.create_validator(validator_type)

        assert str(error.value) == f"Unsupported validator type: {validator_type}"
