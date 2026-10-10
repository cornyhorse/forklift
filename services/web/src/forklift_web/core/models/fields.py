"""Model fields."""

from __future__ import annotations

import json

from django.db import models


class OrderedJSONField(models.TextField):
    """A JSON value stored as its text, so object keys keep the order they were written in.

    Django's JSONField is ``jsonb`` on PostgreSQL, which sorts object keys (shortest first) and
    drops duplicates. The engine reads meaning from that order: a file without a header row gets
    its column names from the order of the schema's ``properties``. Schema documents and job specs
    are therefore kept as text and read back with their keys in the original order.
    """

    description = "A JSON value whose object keys keep their order"

    def from_db_value(self, value, expression, connection):
        return None if value is None else json.loads(value)

    def to_python(self, value):
        if isinstance(value, str):
            return json.loads(value)
        return value

    def get_prep_value(self, value):
        if value is None:
            return None
        return json.dumps(value, ensure_ascii=False, separators=(",", ":"))

    def value_to_string(self, obj):
        return self.get_prep_value(self.value_from_object(obj))
