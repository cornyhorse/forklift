"""``forklift.schema.validation_utils``: JSON type resolution and regex pattern checks."""

from forklift.schema.validation_utils import regex_error, resolve_json_types


class TestResolveJsonTypes:
    def test_repeated_types_are_listed_once(self):
        definition = {"type": ["string", "string", "null"]}

        assert resolve_json_types(definition) == (["string"], [], [])

    def test_type_repeated_across_union_branches_is_listed_once(self):
        definition = {"anyOf": [{"type": "integer"}, {"type": "integer"}, {"type": "null"}]}

        assert resolve_json_types(definition) == (["integer"], [], [])


class TestRegexError:
    def test_valid_pattern_has_no_error(self):
        assert regex_error(r"^[A-Z]{2}\d+$") is None

    def test_non_string_pattern_is_reported(self):
        assert regex_error(42) == "must be a string"

    def test_malformed_pattern_is_reported(self):
        assert regex_error("[unclosed") == "is not a valid regular expression"

    def test_pathologically_nested_pattern_is_reported(self):
        # Deep enough that the regex compiler exceeds the recursion limit
        pattern = "(" * 5000 + "a" + ")" * 5000

        assert regex_error(pattern) == "is not a valid regular expression"
