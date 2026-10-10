"""Tests for scrub_secrets in forklift.engine.importers.redaction."""

from forklift.engine.importers.redaction import scrub_secrets


class TestScrubSecretsWithoutInput:
    def test_text_is_returned_unchanged_without_a_connection_string(self):
        message = "connection refused by db.example.com"

        assert scrub_secrets(message, None) == message
        assert scrub_secrets(message, "") == message

    def test_empty_text_stays_empty(self):
        assert scrub_secrets("", "Server=db;Pwd=hunter22") == ""


class TestScrubSecretsRemovesPasswords:
    def test_password_value_is_replaced_in_free_text(self):
        text = "login failed for password hunter22 on db"

        assert scrub_secrets(text, "Server=db;Uid=bob;Pwd=hunter22") == (
            "login failed for password *** on db"
        )
