"""Case handling of ``transformations.format.email.EmailFormatter``."""

from forklift.utils.transformations.configs import EmailConfig
from forklift.utils.transformations.format.email import EmailFormatter


class TestNormalizeCase:
    def test_case_is_kept_when_normalisation_is_off(self):
        formatter = EmailFormatter(EmailConfig(normalize_case=False))

        assert formatter.format_value(" John.Doe@Example.COM ") == "John.Doe@Example.COM"

    def test_case_is_lowered_by_default(self):
        formatter = EmailFormatter(EmailConfig())

        assert formatter.format_value(" John.Doe@Example.COM ") == "john.doe@example.com"
