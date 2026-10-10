"""Schema documents and job specs keep their key order through the database.

The engine names the columns of a file without a header row after the order of the schema's
``properties``; jsonb (Django's JSONField on PostgreSQL) would sort them (shortest key first) and
put values under the wrong names. These fields are stored as text instead.
"""

from __future__ import annotations

import pytest
from world import make_job, make_upload, make_user

from forklift_web.core.choices import Role
from forklift_web.core.models import Job, SchemaVersion
from forklift_web.core.models.fields import OrderedJSONField
from forklift_web.errors import Conflict
from forklift_web.policy import Actor
from forklift_web.services import installation, schemas, specs

pytestmark = pytest.mark.django_db

# Not the order jsonb would store: it sorts keys by length, then bytewise
PROPERTIES = {
    "zeta_last_name": {"type": "string"},
    "a": {"type": "integer"},
    "middle": {"type": "string"},
}
DOCUMENT = {"type": "object", "properties": PROPERTIES, "required": ["a"]}


class TestField:
    def test_values_round_trip_through_their_text(self):
        field = OrderedJSONField()
        text = field.get_prep_value({"b": 1, "a": [1, "é"]})

        assert text == '{"b":1,"a":[1,"é"]}'
        assert list(field.from_db_value(text, None, None)) == ["b", "a"]
        assert field.to_python(text) == {"b": 1, "a": [1, "é"]}
        assert field.to_python({"already": "python"}) == {"already": "python"}

    def test_null_stays_null(self):
        field = OrderedJSONField(null=True)

        assert field.get_prep_value(None) is None
        assert field.from_db_value(None, None, None) is None

    def test_serialisation_writes_the_text(self):
        version = SchemaVersion(document={"y": 1, "x": 2})

        assert SchemaVersion._meta.get_field("document").value_to_string(version) == (
            '{"y":1,"x":2}'
        )


class TestStoredOrder:
    def test_a_schema_version_keeps_its_properties_order(self):
        author = make_user(Role.AUTHOR)
        schema = schemas.create_schema(Actor.for_user(author), name="ordered", document=DOCUMENT)

        stored = SchemaVersion.objects.get(schema=schema, number=1)

        assert list(stored.document) == ["type", "properties", "required"]
        assert list(stored.document["properties"]) == list(PROPERTIES)

    def test_a_job_spec_keeps_the_order_and_so_does_the_spec_a_worker_leases(self):
        owner = make_user(Role.OPERATOR)
        job = make_job(owner, make_upload(owner))
        job.spec["schema"] = DOCUMENT
        job.save()

        stored = Job.objects.get(pk=job.pk)
        rendered = specs.render(stored, installation.current())

        assert list(stored.spec["schema"]["properties"]) == list(PROPERTIES)
        assert list(rendered["schema"]["properties"]) == list(PROPERTIES)

    def test_reordering_properties_makes_a_new_version(self):
        author = make_user(Role.AUTHOR)
        actor = Actor.for_user(author)
        schema = schemas.create_schema(actor, name="reordered", document=DOCUMENT)
        reordered = {**DOCUMENT, "properties": dict(reversed(list(PROPERTIES.items())))}

        version = schemas.create_version(actor, schema.id, document=reordered)

        assert version.number == 2
        assert list(version.document["properties"]) == list(reversed(list(PROPERTIES)))
        with pytest.raises(Conflict):  # the same order again is still "unchanged"
            schemas.create_version(actor, schema.id, document=reordered)
