"""Fresh input URLs: the engine of a streamed input asks, the supervisor answers from the gateway.

The fake engine's ``input_url`` mode asks the way ``forklift run-job --input-url-requests`` does
(one ``{"type": "input_url"}`` line on stdout, one answer line read from stdin) and reports the
answers it got; the fake gateway answers ``input-url`` or the faults a test queues.
"""

from __future__ import annotations

import json
import logging
import time

import pytest

from forklift_worker import input_url
from forklift_worker.engine import input_url_request
from forklift_worker.gateway import GatewayClient, GatewayRejected
from forklift_worker.redact import REDACTED, Redactor
from forklift_worker.supervisor import EXIT_GATEWAY
from forklift_worker.transport import HttpClient


def streamed_job(gateway, **behaviour):
    """A job whose input is streamed (larger than the staging limit), in fake-engine mode
    ``input_url``."""
    gateway.stage_max_bytes = 4
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "input_url", **behaviour}}
    return job


def report(gateway, job) -> dict:
    return json.loads(gateway.objects[f"jobs/{job.job_id}/attempt-1/report.json"])


def answer(body, status: int = 200):
    """A fault that answers with ``body`` as JSON."""

    def respond(handler) -> None:
        handler._send(status, json.dumps(body).encode(), {"Content-Type": "application/json"})

    return respond


def test_a_streamed_engine_gets_fresh_input_urls(gateway, run_worker):
    job = streamed_job(gateway, requests=2)
    run_worker()

    assert job.completed["result"]["status"] == "succeeded"
    seen = report(gateway, job)
    original = job.spec["input"]["location"]["url"]
    assert seen["answers"] == [{"url": f"{original}&fresh=1"}, {"url": f"{original}&fresh=2"}]
    argv = seen["argv"]
    assert "--input-url-requests" in argv
    # three attempts of at most --http-timeout (10 s here) and their waits, plus room
    assert argv[argv.index("--input-url-timeout") + 1] == "72"
    calls = gateway.calls("input-url")
    assert [call["body"] for call in calls] == [{"attempt": 1}, {"attempt": 1}]
    assert calls[0]["path"] == f"/internal/v1/jobs/{job.job_id}/input-url"
    # The engine read the input from the last fresh URL
    assert gateway.calls("get")[-1]["path"].endswith("&fresh=2")


def test_only_input_url_lines_are_requests():
    assert input_url_request(b'{"type": "input_url"}\n')
    for line in (b"not json\n", b"[1]\n", b'{"rows_read": 1}\n', b'{"type": "other"}\n'):
        assert not input_url_request(line)


def test_a_staged_input_leaves_the_engines_stdin_alone(gateway, run_worker):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "probe"}}
    run_worker()

    argv = json.loads(gateway.objects[f"jobs/{job.job_id}/attempt-1/report.json"])["argv"]
    assert "--input-url-requests" not in argv and "--input-url-timeout" not in argv


def test_the_gateway_is_asked_for_a_bounded_number_of_urls(gateway, run_worker, monkeypatch):
    monkeypatch.setattr(input_url, "MAX_REQUESTS", 2)
    job = streamed_job(gateway, requests=3)
    run_worker()

    answers = report(gateway, job)["answers"]
    assert [set(given) for given in answers] == [{"url"}, {"url"}, {"error"}]
    assert answers[2]["error"] == "the engine already asked for 2 fresh input URLs"
    assert len(gateway.calls("input-url")) == 2


def test_an_unavailable_gateway_is_asked_again(gateway, run_worker):
    job = streamed_job(gateway)
    gateway.fail("input-url", 503)
    run_worker()

    assert report(gateway, job)["answers"][0]["url"].endswith("&fresh=2")
    assert len(gateway.calls("input-url")) == 2


@pytest.mark.parametrize(
    "fault, phrase",
    [
        (503, "the gateway gave no fresh input URL: The gateway answered HTTP 503"),
        ("drop", "the gateway gave no fresh input URL: The gateway at 127.0.0.1"),
        (410, "the gateway gave no fresh input URL: The gateway refused /jobs/"),
        (answer({"location": {"type": "sql"}}), "location is not a presigned_url location"),
        (answer({"location": {"type": "presigned_url"}}), "the location has no url"),
        (answer({"location": None}), "location is not a presigned_url location"),
    ],
)
def test_without_a_fresh_url_the_engine_gets_an_error(gateway, run_worker, fault, phrase):
    job = streamed_job(gateway)
    gateway.fail("input-url", fault, times=3)
    run_worker()

    [given] = report(gateway, job)["answers"]
    assert set(given) == {"error"} and phrase in given["error"]
    assert job.completed["result"]["status"] == "succeeded", "the job itself goes on"


@pytest.mark.parametrize(
    "url, phrase",
    [
        ("http://other.example:9000/store/k?sig", "points at other.example:9000, not at the"),
        ("file:///etc/passwd", "is not an http(s) URL"),
        ("http://{host}/store/k?" + "x" * 20000, "is longer than 16384 bytes"),
    ],
)
def test_a_fresh_url_off_the_store_is_not_passed_on(gateway, run_worker, url, phrase):
    job = streamed_job(gateway)
    location = {"type": "presigned_url", "url": url.format(host=gateway.host)}
    gateway.fail("input-url", answer({"location": location}))
    run_worker()

    [given] = report(gateway, job)["answers"]
    assert phrase in given["error"] and "sig" not in given["error"]


def test_a_lost_lease_is_answered_and_stops_the_job(gateway, run_worker, caplog):
    job = streamed_job(gateway)
    gateway.fail("input-url", 409)
    caplog.set_level(logging.WARNING, logger="forklift_worker")
    supervisor = run_worker()

    assert job.completed is None, "nothing is reported for a job that is taken away"
    assert "no longer leased to this worker" in caplog.text
    assert "job abandoned; nothing uploaded or reported" in caplog.text
    assert supervisor.exit_code == 0


def test_a_refused_token_stops_the_worker(gateway, run_worker):
    job = streamed_job(gateway)
    gateway.fail("input-url", 401)
    supervisor = run_worker()

    assert supervisor.exit_code == EXIT_GATEWAY
    assert job.completed is None


def test_a_job_cancelled_while_the_gateway_is_asked_again(gateway, run_worker, caplog):
    job = streamed_job(gateway)

    def cancel_and_fail(handler):
        job.cancel = True
        handler._json(503, {"detail": "busy"})

    gateway.fail("input-url", cancel_and_fail)
    caplog.set_level(logging.WARNING, logger="forklift_worker")
    run_worker()

    assert job.completed["result"]["status"] == "cancelled"
    assert "no fresh input URL for the engine: the job is being stopped" in caplog.text


def test_fresh_urls_never_reach_results_or_logs(gateway, run_worker, caplog):
    job = streamed_job(gateway, then="crash")
    secret = "session-token-0123456789"
    location = {**job.spec["input"]["location"]}
    location["url"] += f"&token={secret}"
    gateway.fail("input-url", answer({"location": location}))
    caplog.set_level(logging.DEBUG, logger="forklift_worker")
    run_worker()

    result = job.completed["result"]
    assert result["status"] == "failed" and "fake engine: answers" in result["error"]["message"]
    assert REDACTED in result["error"]["message"]
    assert secret not in json.dumps(job.completed) and secret not in caplog.text


def test_an_engine_that_does_not_wait_for_its_answer(gateway, run_worker):
    job = streamed_job(gateway, then="exit")

    def slow(handler):
        time.sleep(0.5)  # the engine has exited by the time the answer is written
        handler._json(200, {"location": job.spec["input"]["location"]})

    gateway.fail("input-url", slow)
    run_worker()

    assert job.completed is not None, "the job was reported"


def test_the_client_returns_the_url(gateway, tmp_path):
    token = tmp_path / "token"
    token.write_text(gateway.token)
    client = GatewayClient(
        HttpClient(timeout=5, user_agent="test"),
        gateway.url + "/internal/v1",
        token,
        worker_id="w1",
        lanes=["batch"],
        engine_version="0.2.0",
    )
    job = gateway.enqueue(job_id="a/b")
    client.lease()
    assert client.input_url("a/b", 1) == job.spec["input"]["location"]["url"] + "&fresh=1"
    assert gateway.calls("input-url")[0]["path"] == "/internal/v1/jobs/a%2Fb/input-url"
    gateway.fail("input-url", answer([]))
    with pytest.raises(GatewayRejected, match="answer to an input-url request is malformed"):
        client.input_url("a/b", 1)


def test_the_redactor_learns_new_secrets():
    redactor = Redactor(["first-secret"])
    redactor.add(["https://store/k?token=abc123", "token=abc123", "x"])
    assert redactor("https://store/k?token=abc123 first-secret token=abc123 x") == (
        f"{REDACTED} {REDACTED} {REDACTED} x"
    )
