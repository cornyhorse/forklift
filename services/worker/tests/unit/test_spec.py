"""Checking a leased spec and planning how its input reaches the engine."""

from __future__ import annotations

import pytest
from fake_gateway import make_spec

from forklift_worker.gateway import Lease
from forklift_worker.spec import SpecRefused, format_bytes, plan_job, staged_name


def presigned(size: int = 10, url: str = "http://store:9000/uploads/1/People%20List.csv?sig=x"):
    return {"type": "presigned_url", "url": url, "size": size, "etag": '"abc"'}


def lease(spec: dict, job_id: str = "j1") -> Lease:
    return Lease(job_id, 1, 60.0, 100, spec)


def plan(spec: dict, *, network: bool = True, stage_max: int = 100, hosts=()):
    return plan_job(
        lease(spec), stage_max_bytes=stage_max, network_allowed=network, store_hosts=hosts
    )


def refused(spec: dict, **kwargs) -> SpecRefused:
    with pytest.raises(SpecRefused) as caught:
        plan(spec, **kwargs)
    return caught.value


def test_a_small_input_is_staged_and_its_location_rewritten():
    spec = make_spec("j1", presigned())
    result = plan(spec, hosts=["store:9000"])
    assert result.spec["input"]["location"] == {"type": "file", "path": "in/People_List.csv"}
    assert spec["input"]["location"]["type"] == "presigned_url", "the lease's spec is untouched"
    (item,) = result.staged
    assert item.host == "store:9000" and item.size == 10 and item.etag == '"abc"'
    assert result.stream_hosts == [] and not result.uses_sql
    assert result.output_dirs == ["out"]


def test_a_large_input_is_streamed():
    spec = make_spec("j1", presigned(size=500, url="https://store/key"))
    result = plan(spec)
    assert result.spec["input"]["location"]["type"] == "presigned_url"
    assert result.stream_hosts == ["store"] and result.stream_ports == [443]


def test_limits():
    spec = make_spec("j1", presigned(size=50))
    spec["limits"] = {"max_input_bytes": 20, "max_seconds": 30}
    error = refused(spec)
    assert error.code == "LIMIT_EXCEEDED" and "limits.max_input_bytes" in error.message
    spec["limits"] = {"max_seconds": 30}
    assert plan(spec).max_seconds == 30.0
    spec["limits"] = None
    assert plan(spec).max_seconds is None
    for bad in ({"max_seconds": 0}, {"max_seconds": True}, {"max_input_bytes": "1"}):
        spec["limits"] = bad
        assert refused(spec).code == "SPEC_INVALID"
    spec["limits"] = []
    assert "limits is not an object" in refused(spec).message


def test_sql_locations_need_the_network():
    spec = make_spec("j1", {"type": "sql", "connection_string": "Driver={X}"})
    spec["output"] = {
        "location": {"type": "sql_table", "connection_string": "Driver={X}", "table": "t"},
        "parquet": {"type": "file", "path": "out/data"},
    }
    result = plan(spec)
    assert result.uses_sql and result.output_dirs == ["out/data"]
    assert "no-network isolation profile" in refused(spec, network=False).message


def test_the_no_network_profile_refuses_streaming():
    error = refused(make_spec("j1", presigned(size=500)), network=False)
    assert error.code == "LIMIT_EXCEEDED"
    assert "no-network isolation profile" in error.message


@pytest.mark.parametrize(
    "change, phrase",
    [
        (lambda s: s.update(spec_version=2), "spec_version 2"),
        (lambda s: s.update(job_id="other"), "does not match"),
        (lambda s: s.update(kind="explode"), "kind 'explode'"),
        (lambda s: s.update(input=None), "no input object"),
        (lambda s: s.update(output=[]), "neither an object nor null"),
        (lambda s: s["input"].pop("location"), "input.location is missing"),
        (lambda s: s["input"].update(location="file.csv"), "not a location object"),
        (lambda s: s["input"].update(location={"type": "s3", "uri": "s3://b/k"}), "s3 location"),
        (lambda s: s["input"].update(location={"type": "file", "path": "../x"}), "'..'"),
        (lambda s: s["input"].update(location={"type": "file", "path": "/etc/x"}), "relative"),
        (lambda s: s["input"].update(location={"type": "file"}), "no usable path"),
        (lambda s: s["output"].update(location=presigned()), "inputs only"),
        (lambda s: s["input"]["location"].update(url="file:///etc/passwd"), "not an http"),
        (lambda s: s["input"]["location"].update(url=None), "not an http"),
        (lambda s: s["input"]["location"].update(size=-1), "size is not"),
        (lambda s: s["input"]["location"].update(size=True), "size is not"),
        (lambda s: s["input"]["location"].update(etag=5), "etag is not a string"),
    ],
)
def test_specs_the_worker_refuses(change, phrase):
    spec = make_spec("j1", presigned())
    change(spec)
    error = refused(spec)
    assert error.code == "SPEC_INVALID"
    assert phrase in error.message


def test_store_hosts_limit_where_presigned_urls_may_point():
    error = refused(make_spec("j1", presigned()), hosts=["rustfs:9000"])
    assert "store:9000, which is not an object store" in error.message


def test_an_input_file_location_and_a_null_output_are_accepted():
    spec = make_spec("j1", {"type": "file", "path": "in/x.csv"})
    spec["output"] = None
    spec["limits"] = {"max_seconds": 5}
    result = plan(spec)
    assert result.staged == [] and result.output_dirs == []
    spec["output"] = {"location": {"type": "file", "path": "./"}}
    assert plan(spec).output_dirs == ["."]


def test_names_and_sizes():
    assert staged_name("http://s/uploads/1/..hidden.csv") == "hidden.csv"
    assert staged_name("http://s/") == "input"
    assert staged_name("http://s/" + "a" * 300 + ".csv").endswith("a.csv")
    assert format_bytes(10) == "10 bytes"
    assert format_bytes(2048) == "2.0 KiB (2048 bytes)"
    assert format_bytes(3 * 1024**3) == "3.0 GiB (3221225472 bytes)"
    assert format_bytes(5 * 1024**5).startswith("5120.0 TiB")


def test_two_streamed_inputs_from_one_store_allow_its_host_once():
    spec = make_spec("j1", presigned(size=500))
    spec["input"]["companion"] = presigned(size=600)
    result = plan(spec)
    assert result.stream_hosts == ["store:9000"] and result.stream_ports == [9000]
