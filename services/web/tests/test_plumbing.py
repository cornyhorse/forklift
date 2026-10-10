"""Settings from the environment, JSON logs, the store client and the contract loader."""

from __future__ import annotations

import importlib
import json
import logging
import sys

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError
from django.core.exceptions import ImproperlyConfigured

from forklift_web import contracts, storage
from forklift_web.conf import env_bool, env_int, env_list, env_optional, env_str
from forklift_web.errors import StoreUnavailable
from forklift_web.logs import JsonFormatter

# --------------------------------------------------------------------------- environment


def test_env_helpers():
    environ = {"A": "x", "EMPTY": "", "ON": " Yes ", "OFF": "0", "N": "12", "L": " a, ,b "}
    assert env_str("A", environ=environ) == "x" and env_str("B", "d", environ=environ) == "d"
    with pytest.raises(ImproperlyConfigured, match="B must be set"):
        env_str("B", environ=environ)
    assert env_optional("EMPTY", environ=environ) is None and env_optional("A", environ=environ)
    assert env_bool("ON", False, environ=environ) is True
    assert env_bool("OFF", True, environ=environ) is False
    assert env_bool("B", True, environ=environ) is True
    with pytest.raises(ImproperlyConfigured, match="must be a boolean"):
        env_bool("A", False, environ=environ)
    assert env_int("N", 1, environ=environ) == 12 and env_int("EMPTY", 3, environ=environ) == 3
    assert env_int("B", 4, environ=environ) == 4
    with pytest.raises(ImproperlyConfigured, match="must be an integer; got 'x'"):
        env_int("A", 1, environ=environ)
    with pytest.raises(ImproperlyConfigured, match="must be at least 20; got 12"):
        env_int("N", 1, minimum=20, environ=environ)
    assert env_list("L", environ=environ) == ["a", "b"] and env_list("B", environ=environ) == []


def test_settings_module_reads_the_environment(monkeypatch):
    monkeypatch.setenv("FORKLIFT_BEHIND_TLS_PROXY", "1")
    monkeypatch.setenv("FORKLIFT_S3_PUBLIC_ENDPOINT_URL", "https://files.example.org")
    monkeypatch.setenv("FORKLIFT_S3_DELETE_ACCESS_KEY_ID", "sweeper")
    monkeypatch.setenv("FORKLIFT_S3_DELETE_SECRET_ACCESS_KEY", "sweeper-secret")
    monkeypatch.setenv("FORKLIFT_API_DOCS", "0")
    module = importlib.import_module("forklift_web.settings")
    try:
        reloaded = importlib.reload(module)
        assert reloaded.SECURE_PROXY_SSL_HEADER == ("HTTP_X_FORWARDED_PROTO", "https")
        assert reloaded.SESSION_COOKIE_SECURE is True
        store = reloaded.FORKLIFT_STORE
        assert store["public_endpoint_url"] == "https://files.example.org"
        assert store["worker_endpoint_url"] == store["endpoint_url"]
        assert store["credentials"]["delete"] == ("sweeper", "sweeper-secret")
        assert store["credentials"]["upload"] != ("sweeper", "sweeper-secret")
        assert reloaded.FORKLIFT_API_DOCS is False
    finally:
        monkeypatch.undo()
        importlib.reload(module)
    assert module.SECURE_PROXY_SSL_HEADER is None


def test_the_api_documentation_can_be_switched_off(monkeypatch, settings):
    settings.FORKLIFT_API_DOCS = False
    saved = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "forklift_web.api" or name.startswith("forklift_web.api.")
    }
    try:
        api_module = importlib.import_module("forklift_web.api")
        assert api_module.api.docs_url is None
    finally:
        for name in list(sys.modules):
            if name == "forklift_web.api" or name.startswith("forklift_web.api."):
                del sys.modules[name]
        sys.modules.update(saved)


def test_json_log_lines():
    formatter = JsonFormatter()
    record = logging.makeLogRecord(
        {
            "name": "forklift_web",
            "levelname": "INFO",
            "msg": "Leased %s",
            "args": ("job",),
            "job_id": "j-1",
        }
    )
    line = json.loads(formatter.format(record))
    assert line["message"] == "Leased job" and line["job_id"] == "j-1"
    assert line["level"] == "INFO" and "exception" not in line
    try:
        raise ValueError("boom")
    except ValueError:
        record.exc_info = sys.exc_info()
    assert "ValueError: boom" in json.loads(formatter.format(record))["exception"]


# --------------------------------------------------------------------------- the store


def test_content_disposition():
    assert storage.content_disposition("data.parquet") == (
        "attachment; filename=\"data.parquet\"; filename*=UTF-8''data.parquet"
    )
    header = storage.content_disposition('ré"port\n.json')
    assert header.startswith('attachment; filename="r__port_.json"')
    assert "filename*=UTF-8''r%C3%A9%22port%0A.json" in header


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "nope"}}, "Op")


class _Failing:
    def __init__(self, error):
        self.error = error

    def __getattr__(self, name):
        def fail(*args, **kwargs):
            raise self.error

        return fail


@pytest.fixture
def bucket():
    return storage.Bucket(
        bucket="b",
        region="us-east-1",
        addressing_style="path",
        endpoints={},
        credentials={p: ("k", "s") for p in storage.Purpose},
    )


@pytest.mark.parametrize(
    "operation,call",
    [
        ("HEAD", lambda b: b.head("k")),
        ("starting a multipart upload", lambda b: b.create_multipart("k")),
        ("completing the multipart upload", lambda b: b.complete_multipart("k", "u", [])),
        ("aborting the multipart upload", lambda b: b.abort_multipart("k", "u")),
        ("DELETE", lambda b: b.delete("k")),
        ("copying to", lambda b: b.copy_from(b, "k", "k2")),
        ("HEAD", lambda b: b.check_access()),
    ],
)
@pytest.mark.parametrize(
    "error,reason",
    [
        (_client_error("AccessDenied"), "AccessDenied: nope"),
        (EndpointConnectionError(endpoint_url="http://x"), "EndpointConnectionError"),
    ],
)
def test_store_errors_name_the_operation_and_reason(
    bucket, monkeypatch, operation, call, error, reason
):
    monkeypatch.setattr(bucket, "client", lambda purpose, audience=None: _Failing(error))
    with pytest.raises(StoreUnavailable) as raised:
        call(bucket)
    assert operation in raised.value.message and reason in raised.value.message


def test_missing_objects_and_uploads_are_not_errors(bucket, monkeypatch):
    for code in ("404", "NoSuchKey", "NotFound"):
        monkeypatch.setattr(bucket, "client", lambda p, a=None, c=code: _Failing(_client_error(c)))
        assert bucket.head("k") is None
    monkeypatch.setattr(
        bucket, "client", lambda p, a=None: _Failing(_client_error("NoSuchUpload"))
    )
    bucket.abort_multipart("k", "u")
    monkeypatch.setattr(bucket, "client", lambda p, a=None: _Failing(ClientError({}, "Op")))
    with pytest.raises(StoreUnavailable, match=r"\(error\)"):
        bucket.delete("k")


def test_store_clients_are_cached_per_purpose_and_audience(bucket):
    upload = bucket.client(storage.Purpose.UPLOAD, storage.Audience.PUBLIC)
    assert bucket.client(storage.Purpose.UPLOAD, storage.Audience.PUBLIC) is upload
    assert bucket.client(storage.Purpose.UPLOAD, storage.Audience.WORKER) is not upload
    assert bucket.host(storage.Audience.WORKER) is None
    connection = storage.connection_bucket(
        {"bucket": "ext", "endpoint_url": "https://s3.example.org", "prefix": "p"},
        {"access_key_id": "a", "secret_access_key": "s", "session_token": "t"},
    )
    assert (
        connection.host(storage.Audience.PUBLIC) == "s3.example.org" and connection.prefix == "p"
    )
    url = connection.presign_get("k", expires=10**9, audience=storage.Audience.PUBLIC)
    assert "X-Amz-Expires=604800" in url and "X-Amz-Security-Token=t" in url


# --------------------------------------------------------------------------- contracts


def test_contract_location(settings, tmp_path, monkeypatch):
    settings.FORKLIFT_CONTRACTS_DIR = str(tmp_path)
    assert contracts.contracts_dir() == tmp_path
    settings.FORKLIFT_CONTRACTS_DIR = None
    packaged = tmp_path / "packaged"
    packaged.mkdir()
    monkeypatch.setattr(contracts, "_PACKAGED", packaged)
    assert contracts.contracts_dir() == contracts._REPOSITORY
    (packaged / contracts.JOBSPEC).write_text("{}")
    assert contracts.contracts_dir() == packaged


def test_contract_validation_messages_never_repeat_values(tmp_path):
    (tmp_path / "t.json").write_text(
        json.dumps(
            {
                "type": "object",
                "properties": {
                    "needed": {},
                    "url": {"type": "string", "pattern": "^https://"},
                    "big": {"enum": ["x" * 200]},
                    "either": {
                        "anyOf": [
                            {"type": "null"},
                            {"type": "object", "properties": {"n": {"type": "integer"}}},
                        ]
                    },
                },
                "required": ["needed"],
                "additionalProperties": False,
            }
        )
    )
    with pytest.raises(contracts.ContractViolation) as raised:
        contracts.validate(
            "t.json",
            {
                "url": "http://secret.example/?sig=abc",
                "big": "y",
                "either": {"n": "five"},
                "extra": 1,
            },
            directory=tmp_path,
        )
    message = str(raised.value)
    assert "sig=abc" not in message and "five" not in message
    assert "url: fails 'pattern' (expected '^https://')" in message
    assert "big: fails 'enum'" in message and "..." in message
    assert "either/n: fails 'type' (expected 'integer')" in message
    assert "'needed' is a required property" in message and "'extra' was unexpected" in message
    assert len(raised.value.errors) == 5
    contracts.validate("t.json", {"needed": 1}, directory=tmp_path)


def test_a_missing_contract_is_a_configuration_error(tmp_path):
    contracts.clear_cache()
    with pytest.raises(ImproperlyConfigured, match="set FORKLIFT_CONTRACTS_DIR"):
        contracts.validate("absent.json", {}, directory=tmp_path)
