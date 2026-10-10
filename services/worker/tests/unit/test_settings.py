"""Settings from flags and FORKLIFT_WORKER_* variables, and the README table that lists them."""

from __future__ import annotations

import re
import sys
from pathlib import Path

import pytest

from forklift_worker import settings as settings_module
from forklift_worker.settings import (
    OPTIONS,
    Settings,
    SettingsError,
    load_settings,
    parse_duration,
    parse_size,
)

README = Path(__file__).resolve().parents[2] / "README.md"
REQUIRED = [
    "--gateway",
    "http://gateway:8081",
    "--token-file",
    "/run/secrets/t",
    "--scratch",
    "/s",
]


def test_the_required_settings_and_the_defaults():
    settings = load_settings(REQUIRED, {})
    assert settings.gateway == "http://gateway:8081/internal/v1"
    assert settings.token_file == Path("/run/secrets/t")
    assert settings.scratch == Path("/s")
    assert settings.lanes == ["batch"]
    assert settings.isolation == "standard"
    assert settings.landlock == "auto"
    assert settings.concurrency == 1
    assert settings.engine_command == [sys.executable, "-I", "-m", "forklift"]
    assert settings.engine_env == [] and settings.store_host == []
    assert settings.max_job_seconds == 86400
    assert settings.limit_address_space == "auto"
    assert settings.limit_open_files == 1024
    assert settings.allow_root is False
    assert settings.heartbeat_seconds is None and settings.stage_max_bytes is None
    assert re.match(r"^[A-Za-z0-9][A-Za-z0-9._-]*-[0-9a-f]{6}$", settings.worker_id)
    assert settings.user_agent == "forklift-worker/0.1.0"


def test_variables_fill_in_and_flags_win():
    environ = {
        "FORKLIFT_WORKER_GATEWAY": "https://gateway.internal:8443/internal/v1/",
        "FORKLIFT_WORKER_TOKEN_FILE": "/run/secrets/worker-token",
        "FORKLIFT_WORKER_SCRATCH": "/scratch",
        "FORKLIFT_WORKER_LANES": "interactive, batch",
        "FORKLIFT_WORKER_ENGINE_ENV": "ODBCSYSINI,LD_LIBRARY_PATH",
        "FORKLIFT_WORKER_STORE_HOSTS": "RustFS:9000, store.example.org",
        "FORKLIFT_WORKER_ALLOW_ROOT": "yes",
        "FORKLIFT_WORKER_CONCURRENCY": "3",
        "FORKLIFT_WORKER_STAGE_MAX_BYTES": "2GiB",
        "FORKLIFT_WORKER_LIMIT_CPU_SECONDS": "unlimited",
        "FORKLIFT_WORKER_MAX_JOB_SECONDS": "2h",
        "FORKLIFT_WORKER_ISOLATION": "standard",
        "FORKLIFT_WORKER_LOG_FORMAT": "TEXT",
        "FORKLIFT_WORKER_ID": "   ",
    }
    settings = load_settings(["--concurrency", "2", "--engine-env", "TZ"], environ)
    assert settings.gateway == "https://gateway.internal:8443/internal/v1"
    assert settings.lanes == ["interactive", "batch"]
    assert settings.concurrency == 2, "the flag wins over the variable"
    assert settings.engine_env == ["TZ"], "a repeated flag replaces the variable's list"
    assert settings.store_host == ["rustfs:9000", "store.example.org"]
    assert settings.allow_root is True
    assert settings.stage_max_bytes == 2 * 1024**3
    assert settings.limit_cpu_seconds is None
    assert settings.max_job_seconds == 7200
    assert settings.log_format == "text"


def test_switches_and_repeated_flags():
    settings = load_settings(
        REQUIRED
        + ["--allow-root", "--engine-read-path", "/opt/a", "--engine-read-path", "/opt/b"]
        + ["--engine-command", "{python} -m forklift --verbose", "--limit-file-size", "10G"]
        + ["--worker-id", "worker-a.example:1"],
        {},
    )
    assert settings.worker_id == "worker-a.example:1"
    assert settings.allow_root is True
    assert settings.engine_read_path == [Path("/opt/a"), Path("/opt/b")]
    assert settings.engine_command == [sys.executable, "-m", "forklift", "--verbose"]
    assert settings.limit_file_size == 10 * 1000**3


def test_a_missing_required_setting_names_the_flag_and_the_variable():
    with pytest.raises(SettingsError, match="--token-file / FORKLIFT_WORKER_TOKEN_FILE"):
        load_settings(["--gateway", "http://g", "--scratch", "/s"], {})


@pytest.mark.parametrize(
    "flags, message",
    [
        (["--gateway", "ftp://gateway"], "not an http:// or https:// URL"),
        (["--gateway", "http://user:pw@gateway"], "must not carry"),
        (["--gateway", "http://gateway/?x=1"], "must not carry"),
        (["--token-file", "relative/token"], "not an absolute path"),
        (["--scratch", " "], "the path is empty"),
        (["--lanes", " , "], "at least one lane"),
        (["--lanes", "Batch"], "is not a lane name"),
        (["--lanes", "batch,batch"], "more than once"),
        (["--isolation", "sandboxed"], "is not one of standard, no-network"),
        (["--concurrency", "0"], "must be 1 or more"),
        (["--concurrency", "two"], "not a whole number"),
        (["--max-jobs", "-1"], "must be 0 or more"),
        (["--max-jobs", "x"], "not a whole number"),
        (["--engine-command", "'unclosed"], "not a valid command line"),
        (["--engine-command", "  "], "the engine command is empty"),
        (["--engine-env", "AWS_SECRET_ACCESS_KEY"], "never passed to the engine"),
        (["--engine-env", "MY_API_KEY"], "never passed to the engine"),
        (["--engine-env", "1BAD"], "not an environment variable name"),
        (["--store-host", "bad host"], "not a host name"),
        (["--worker-id=-starts-with-a-dash"], "is not a worker id"),
        (["--worker-id", "has space"], "is not a worker id"),
        (["--stage-max-bytes", "auto"], "is not a size"),
        (["--stage-max-bytes", "0"], "must be more than zero"),
        (["--limit-address-space", "lots"], "is not a size"),
        (["--limit-address-space", "2i"], "is not a size"),
        (["--limit-cpu-seconds", "ten"], "is not a duration"),
        (["--kill-grace-seconds", "0"], "must be more than zero"),
        (["--allow-root", "--log-level", "loud"], "is not one of debug"),
    ],
)
def test_invalid_values_are_explained(flags, message):
    # The last occurrence of a flag wins, so these override REQUIRED's values.
    with pytest.raises(SettingsError, match=re.escape(message)):
        load_settings(REQUIRED + flags, {})


def test_an_invalid_variable_names_the_variable():
    with pytest.raises(SettingsError, match="FORKLIFT_WORKER_ALLOW_ROOT: 'maybe' is not a bool"):
        load_settings(REQUIRED, {"FORKLIFT_WORKER_ALLOW_ROOT": "maybe"})


def test_settings_that_contradict_each_other(tmp_path):
    base = {"gateway": "http://g/internal/v1", "token_file": tmp_path, "scratch": tmp_path}
    with pytest.raises(SettingsError, match="--idle-min-seconds"):
        Settings(**base, idle_min_seconds=5, idle_max_seconds=1)
    with pytest.raises(SettingsError, match="cannot serve the sql lane"):
        Settings(**base, isolation="no-network", lanes=["sql"])


def test_sizes_and_durations():
    assert parse_size("512") == 512
    assert parse_size("1.5 KiB") == 1536
    assert parse_size("2M") == 2_000_000
    assert parse_size("1tib") == 1024**4
    assert parse_size("10b") == 10
    assert parse_duration("1.5h") == 5400
    assert parse_duration("90s") == 90
    assert parse_duration("5m") == 300


def test_bools(monkeypatch):
    for word in ("1", "true", "on", "YES"):
        assert settings_module._bool(word) is True
    for word in ("0", "false", "off", "no", ""):
        assert settings_module._bool(word) is False


def test_version_and_help(capsys):
    with pytest.raises(SystemExit) as version:
        load_settings(["--version"], {})
    assert version.value.code == 0
    assert "forklift-worker 0.1.0" in capsys.readouterr().out
    with pytest.raises(SystemExit):
        load_settings(["--help"], {})
    text = capsys.readouterr().out
    for option in OPTIONS:
        assert option.flag in text


def test_the_readme_lists_every_setting():
    readme = README.read_text(encoding="utf-8")
    for option in OPTIONS:
        rows = readme.splitlines()
        row = next((line for line in rows if line.startswith(f"| `{option.flag}` |")), None)
        assert row is not None, f"README.md has no row for {option.flag}"
        assert f"`{option.env}`" in row
        assert option.default_text() in row, f"README.md shows another default for {option.flag}"


def test_the_default_worker_id_is_made_of_allowed_characters(monkeypatch):
    monkeypatch.setattr(settings_module.socket, "gethostname", lambda: "..my host_é")
    assert re.match(r"^my-host_--[0-9a-f]{6}$", settings_module.default_worker_id())
    monkeypatch.setattr(settings_module.socket, "gethostname", lambda: "")
    assert re.match(r"^worker-[0-9a-f]{6}$", settings_module.default_worker_id())
