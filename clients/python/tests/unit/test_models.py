"""The models: decoding the API's JSON (times, nested resources, lists, unknown and missing
fields) and encoding it back."""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional, Tuple

import pytest
from fake_api import _job_doc

from forklift_client import Job, Model, Page, UnexpectedResponse, Upload, UploadTicket
from forklift_client._model import parse_datetime


@dataclasses.dataclass(frozen=True)
class Sample(Model):
    count: int
    ratio: float
    when: datetime
    tags: Tuple[str, ...]
    meta: Dict[str, Any]
    anything: Any
    keyword: str = dataclasses.field(default="", metadata={"json": "from"})
    maybe: Optional[int] = None


GOOD = {
    "count": 3,
    "ratio": 1,
    "when": "2026-10-11T08:30:00Z",
    "tags": ["a", "b"],
    "meta": {"x": [1]},
    "anything": [None],
    "from": "json key",
}


class TestDatetimes:
    @pytest.mark.parametrize(
        "text, expected",
        [
            ("2026-10-11T08:30:00Z", datetime(2026, 10, 11, 8, 30, tzinfo=timezone.utc)),
            ("2026-10-11t08:30:00z", datetime(2026, 10, 11, 8, 30, tzinfo=timezone.utc)),
            ("2026-10-11 08:30", datetime(2026, 10, 11, 8, 30, tzinfo=timezone.utc)),
            (
                "2026-10-11T08:30:00.5+02:00",
                datetime(2026, 10, 11, 8, 30, 0, 500000, tzinfo=timezone(timedelta(hours=2))),
            ),
            (
                "2026-10-11T08:30:00.123456789-0130",
                datetime(
                    2026,
                    10,
                    11,
                    8,
                    30,
                    0,
                    123456,
                    tzinfo=timezone(-timedelta(hours=1, minutes=30)),
                ),
            ),
        ],
    )
    def test_iso_8601_as_the_api_writes_it(self, text, expected):
        parsed = parse_datetime(text)
        assert parsed == expected and parsed.utcoffset() == expected.utcoffset()

    @pytest.mark.parametrize("text", ["", "2026-10-11", "yesterday", "2026-10-11T08:30:00+2"])
    def test_other_text(self, text):
        with pytest.raises(ValueError, match="is not an ISO 8601 date and time"):
            parse_datetime(text)


class TestDecode:
    def test_every_kind_of_field(self):
        sample = Sample.from_dict({**GOOD, "unknown": "ignored"})
        assert sample.count == 3 and sample.ratio == 1.0 and isinstance(sample.ratio, float)
        assert sample.when == datetime(2026, 10, 11, 8, 30, tzinfo=timezone.utc)
        assert sample.tags == ("a", "b") and sample.meta == {"x": [1]}
        assert sample.anything == [None] and sample.keyword == "json key"
        assert sample.maybe is None

    def test_optional_values(self):
        assert Sample.from_dict({**GOOD, "maybe": None}).maybe is None
        assert Sample.from_dict({**GOOD, "maybe": 4}).maybe == 4

    @pytest.mark.parametrize(
        "field, value, message",
        [
            ("count", "3", r"Sample.count should be int, not str"),
            ("count", True, r"Sample.count should be int, not bool"),
            ("ratio", "x", r"Sample.ratio should be float, not str"),
            ("when", "noon", r"Sample.when should be a date and time: 'noon'"),
            ("when", 5, r"Sample.when should be a date and time: 5"),
            ("tags", "a", r"Sample.tags should be a list, not str"),
            ("tags", ["a", 1], r"Sample.tags\[1\] should be str, not int"),
            ("meta", [], r"Sample.meta should be an object, not list"),
        ],
    )
    def test_wrong_types(self, field, value, message):
        with pytest.raises(UnexpectedResponse, match=message):
            Sample.from_dict({**GOOD, field: value})

    def test_a_missing_required_field(self):
        document = dict(GOOD)
        del document["when"]
        with pytest.raises(UnexpectedResponse, match="Sample has no 'when'"):
            Sample.from_dict(document)

    def test_nested_resources(self):
        job = _job_doc("failed")
        decoded = Job.from_dict(job)
        assert decoded.error is not None and decoded.error.code == "BAD_ROWS_THRESHOLD_EXCEEDED"
        with pytest.raises(UnexpectedResponse, match="Job.error should be an object, not str"):
            Job.from_dict({**job, "error": "boom"})
        with pytest.raises(UnexpectedResponse, match="UploadTicket.parts\\[0\\] should be"):
            UploadTicket.from_dict(
                {
                    "upload": _upload(),
                    "method": "PUT",
                    "url": None,
                    "headers": {},
                    "expires_at": "2026-10-11T08:30:00Z",
                    "part_size": 5,
                    "part_count": 1,
                    "parts": ["not an object"],
                }
            )

    def test_unknown_enumeration_values_are_kept(self):
        """A newer gateway may add a status; the client passes it on."""
        document = _job_doc("queued")
        document["status"] = "paused"
        assert Job.from_dict(document).status == "paused"


class TestEncode:
    def test_to_dict_round_trips_and_is_json_ready(self):
        import json

        job = Job.from_dict(_job_doc("succeeded"))
        data = job.to_dict()
        json.dumps(data)
        assert Job.from_dict(data) == job
        assert isinstance(data["created_at"], str) and data["created_at"].endswith("+00:00")
        sample = Sample.from_dict(GOOD).to_dict()
        assert sample["from"] == "json key" and sample["tags"] == ["a", "b"]

    def test_resources_are_frozen(self):
        job = Job.from_dict(_job_doc())
        with pytest.raises(dataclasses.FrozenInstanceError):
            job.status = "failed"  # type: ignore[misc]


class TestPage:
    def test_page(self):
        page = Page(items=(1, 2), count=5, offset=2, limit=2)
        assert list(page) == [1, 2] and len(page) == 2
        assert page.has_more and page.next_offset == 4
        assert not Page(items=(5,), count=5, offset=4, limit=2).has_more
        assert not Page(items=(), count=5, offset=9, limit=2).has_more


def _upload() -> dict:
    return {
        "id": "u",
        "filename": "a.csv",
        "content_type": "",
        "size": 1,
        "declared_sha256": "",
        "classification": "internal",
        "status": "pending",
        "uploaded_by_id": None,
        "created_at": "2026-10-11T08:30:00Z",
        "completed_at": None,
        "deleted_at": None,
        "multipart": False,
    }


def test_an_upload_decodes():
    assert Upload.from_dict(_upload()).status == "pending"
