"""Secrets are removed from everything the worker passes on."""

from __future__ import annotations

from forklift_worker.redact import REDACTED, Redactor, secrets_of


def spec_with(*locations, output=None) -> dict:
    return {"input": {"location": locations[0], "extra": list(locations[1:])}, "output": output}


def test_a_jobs_secrets_are_its_connection_strings_passwords_and_presigned_urls():
    odbc = "Driver={PostgreSQL};Server=db;Uid=u;PWD={p;w0rd!};Database=x"
    url_style = "postgresql://loader:s3cr3t-pass@db:5432/x"
    presigned = "https://store/key.csv?X-Amz-Signature=abcdef123456"
    spec = spec_with(
        {"type": "sql", "connection_string": odbc},
        {"type": "presigned_url", "url": presigned},
        output={"location": {"type": "sql_table", "connection_string": url_style}},
    )
    found = secrets_of(spec)
    assert odbc in found and "p;w0rd!" in found
    assert url_style in found and "s3cr3t-pass" in found
    assert presigned in found and "X-Amz-Signature=abcdef123456" in found
    assert found == sorted(found, key=len, reverse=True)


def test_odd_specs_have_no_secrets():
    assert secrets_of(None) == []
    assert secrets_of({"input": {"location": {"type": "sql", "connection_string": ""}}}) == []
    assert secrets_of({"input": {"location": {"type": "file", "url": ""}}}) == []
    broken = {"input": {"location": {"type": "sql", "connection_string": "http://[::1"}}}
    assert secrets_of(broken) == ["http://[::1"]
    tiny = {"input": {"location": {"type": "sql", "connection_string": "Pwd=ab"}}}
    assert secrets_of(tiny) == ["Pwd=ab"], "fragments under four characters are not secrets"


def test_redaction_replaces_known_secrets_and_generic_patterns():
    redact = Redactor(["hunter2-secret", "ab"])
    text = (
        "login failed for Pwd=hunter2-secret; also password = other-pass and "
        "postgresql://u:pw123@host/db and https://s/k?X-Amz-Credential=AKIA/x&X-Amz-Signature=ff"
    )
    cleaned = redact(text)
    assert "hunter2-secret" not in cleaned and "other-pass" not in cleaned
    assert "pw123" not in cleaned and "AKIA" not in cleaned and "=ff" not in cleaned
    assert cleaned.count(REDACTED) >= 5
    assert "ab" in redact("ab"), "short strings are not treated as secrets"


def test_deep_redaction_keeps_the_shape():
    redact = Redactor(["topsecret"])
    value = {"error": {"message": "topsecret here", "retryable": False}, "warnings": ["topsecret"]}
    assert redact.deep(value) == {
        "error": {"message": f"{REDACTED} here", "retryable": False},
        "warnings": [REDACTED],
    }


def test_a_presigned_url_without_a_query_is_a_secret_on_its_own():
    spec = spec_with({"type": "presigned_url", "url": "https://store/key.csv"})
    assert secrets_of(spec) == ["https://store/key.csv"]
