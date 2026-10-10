"""Isolation profiles: what the engine can and cannot reach, checked from inside the engine."""

from __future__ import annotations

import json
import os
import socket
import sys
from pathlib import Path

import pytest
from fake_gateway import make_spec

from forklift_worker import isolation as isolation_module
from forklift_worker import linux
from forklift_worker.isolation import Isolation, IsolationError, Platform
from forklift_worker.spec import JobPlan

LANDLOCK = linux.landlock_abi()
needs_landlock = pytest.mark.skipif(not LANDLOCK, reason="this kernel has no Landlock")
NETNS = isolation_module.probe_network_namespace()[0]
needs_netns = pytest.mark.skipif(
    not NETNS, reason="this platform does not allow unprivileged network namespaces"
)


def probe_report(gateway, job) -> dict:
    assert job.completed and job.completed["result"]["status"] == "succeeded", job.completed
    return json.loads(gateway.objects[f"jobs/{job.job_id}/attempt-1/report.json"])


@pytest.fixture
def listener():
    """A TCP port on 127.0.0.1 that accepts connections (and is not the gateway's)."""
    server = socket.create_server(("127.0.0.1", 0))
    yield server.getsockname()[1]
    server.close()


# --------------------------------------------------------------------------- Landlock


@needs_landlock
def test_landlock_confines_the_engine_to_its_scratch_directory(
    gateway, run_worker, tmp_path, listener
):
    outside = tmp_path / "another-jobs-file.csv"
    outside.write_text("id\n1\n")
    token_file = tmp_path / "secrets" / "worker-token"
    job = gateway.enqueue()
    job.spec["options"] = {
        "fake_engine": {
            "mode": "probe",
            "read": [str(token_file), str(outside), "/etc/passwd", sys.executable],
            "write": [str(tmp_path / "escape.txt")],
            "connect": [["127.0.0.1", listener]],
            "signal_parent": True,
        }
    }
    run_worker()

    report = probe_report(gateway, job)
    assert report["read"][str(token_file)] == "PermissionError"
    assert report["read"][str(outside)] == "PermissionError"
    assert report["read"]["/etc/passwd"] == "ok"
    assert report["read"][sys.executable] == "ok"
    assert report["write"][str(tmp_path / "escape.txt")] == "PermissionError"
    assert not (tmp_path / "escape.txt").exists()
    if LANDLOCK >= 4:  # staged input: no TCP at all
        assert report["connect"][f"127.0.0.1:{listener}"] == "PermissionError"
    if LANDLOCK >= 6:  # no signals to the supervisor
        assert report["signal_parent"] == "PermissionError"


@pytest.mark.skipif(LANDLOCK < 4, reason="Landlock TCP rules need ABI 4 (Linux 6.7)")
def test_a_streaming_engine_may_connect_only_to_the_store(gateway, run_worker, listener):
    gateway.stage_max_bytes = 4
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "probe", "connect": [["127.0.0.1", listener]]}}
    run_worker()

    report = probe_report(gateway, job)
    assert report["connect"][f"127.0.0.1:{listener}"] == "PermissionError"
    # ... while it read its input from the store's port (the job succeeded with its data).
    assert gateway.objects[f"jobs/{job.job_id}/attempt-1/data.parquet"] == b"ID,NAME\n1,A\n"


def test_landlock_off_leaves_the_file_system_open(gateway, run_worker, tmp_path):
    token_file = tmp_path / "secrets" / "worker-token"
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "probe", "read": [str(token_file)]}}
    run_worker(landlock="off")

    assert probe_report(gateway, job)["read"][str(token_file)] == "ok"


# --------------------------------------------------------------------------- no-network


@needs_netns
def test_the_no_network_profile_gives_the_engine_no_network(gateway, run_worker, listener):
    job = gateway.enqueue()
    job.spec["options"] = {"fake_engine": {"mode": "probe", "connect": [["127.0.0.1", listener]]}}
    run_worker(isolation="no-network")

    report = probe_report(gateway, job)
    assert report["interfaces"] == ["lo"]
    assert report["connect"][f"127.0.0.1:{listener}"] != "ok"


@needs_netns
def test_the_no_network_profile_refuses_a_streamed_input(gateway, run_worker):
    gateway.stage_max_bytes = 4
    job = gateway.enqueue()
    run_worker(isolation="no-network")

    error = job.completed["result"]["error"]
    assert error["code"] == "LIMIT_EXCEEDED"
    assert "no-network isolation profile" in error["message"]
    assert gateway.calls("get") == []


@needs_netns
def test_the_no_network_profile_refuses_sql_locations(gateway, run_worker):
    spec = make_spec("sql", {"type": "sql", "connection_string": "Driver={X};Pwd=abcdef"})
    job = gateway.enqueue(spec)
    run_worker(isolation="no-network")

    assert "no-network isolation profile" in job.completed["result"]["error"]["message"]


# --------------------------------------------------------------------------- startup checks


def platform(**overrides) -> Platform:
    values = {
        "landlock_abi": 6,
        "network_namespace": True,
        "network_namespace_problem": "",
        "euid": 1000,
    }
    values.update(overrides)
    return Platform(**values)


def test_root_is_refused_unless_allowed(make_settings):
    with pytest.raises(IsolationError, match="running as root"):
        Isolation(make_settings(allow_root=False), platform(euid=0)).check()
    Isolation(make_settings(allow_root=True), platform(euid=0)).check()


def test_no_network_needs_a_network_namespace(make_settings):
    settings = make_settings(isolation="no-network")
    unavailable = platform(network_namespace=False, network_namespace_problem="EPERM")
    with pytest.raises(IsolationError, match="EPERM"):
        Isolation(settings, unavailable).check()


def test_landlock_required_needs_the_kernel_to_have_it(make_settings):
    with pytest.raises(IsolationError, match="no Landlock support"):
        Isolation(make_settings(landlock="required"), platform(landlock_abi=0)).check()


def test_a_kernel_without_landlock_is_reported(make_settings):
    warnings = Isolation(make_settings(), platform(landlock_abi=0)).check()
    assert any("no Landlock support" in warning for warning in warnings)
    assert Isolation(make_settings(landlock="off"), platform(landlock_abi=0)).check() == []


def test_a_token_or_scratch_inside_a_readable_path_is_reported(make_settings, tmp_path):
    settings = make_settings(engine_read_path=[tmp_path])
    warnings = Isolation(settings, platform()).check()
    assert any("worker token file" in warning for warning in warnings)
    assert any("concurrent jobs could read" in warning for warning in warnings)


def test_describe_lists_what_is_enforced(make_settings):
    described = Isolation(make_settings(isolation="no-network"), platform(landlock_abi=4))
    assert described.describe() == {
        "profile": "no-network",
        "non_root": True,
        "network_namespace": True,
        "landlock_abi": 4,
        "filesystem_sandbox": True,
        "tcp_restricted": True,
        "signal_scoped": False,
    }
    assert Isolation(make_settings(landlock="off"), platform()).describe()["landlock_abi"] == 0


def test_the_platform_probe(monkeypatch):
    assert Platform.probe("standard").network_namespace is False
    probed = Platform.probe("no-network")
    assert probed.network_namespace is NETNS

    def broken(*args, **kwargs):
        raise OSError("no such interpreter")

    monkeypatch.setattr(isolation_module.subprocess, "run", broken)
    available, problem = isolation_module.probe_network_namespace()
    assert not available and "no such interpreter" in problem


def test_the_engine_command_directory_is_readable(make_settings, tmp_path):
    tool = tmp_path / "bin" / "engine"
    tool.parent.mkdir()
    tool.write_text("#!/bin/sh\n")
    tool.chmod(0o755)
    isolation = Isolation(make_settings(engine_command=[str(tool)]), platform())
    assert str(tool.parent) in isolation.read_paths
    missing = Isolation(make_settings(engine_command=["no-such-engine-anywhere"]), platform())
    assert str(tool.parent) not in missing.read_paths


# --------------------------------------------------------------------------- per job


def test_proxies_reach_only_streaming_engines_and_never_with_credentials(make_settings, tmp_path):
    isolation = Isolation(make_settings(), platform())
    environ = {
        "PATH": "/usr/bin",
        "HTTPS_PROXY": "http://proxy.internal:3128",
        "http_proxy": "http://user:password@proxy.internal:3128",
        "NO_PROXY": "localhost",
        "AWS_SECRET_ACCESS_KEY": "secret",
    }
    staged = isolation.environment(tmp_path, JobPlan(spec={}), environ)
    assert "HTTPS_PROXY" not in staged and "AWS_SECRET_ACCESS_KEY" not in staged
    streamed_plan = JobPlan(spec={}, stream_hosts=["store:9000"], stream_ports=[9000])
    streamed = isolation.environment(tmp_path, streamed_plan, environ)
    assert streamed["HTTPS_PROXY"] == "http://proxy.internal:3128"
    assert streamed["NO_PROXY"] == "localhost"
    assert "http_proxy" not in streamed
    assert isolation._tcp_ports(streamed_plan, streamed) == [3128, 9000]
    assert isolation.environment(tmp_path, JobPlan(spec={}), {})["PATH"] == os.defpath


def test_tcp_rules_need_abi_4_and_leave_sql_jobs_alone(make_settings):
    plan = JobPlan(spec={}, stream_hosts=["store"], stream_ports=[443])
    assert Isolation(make_settings(), platform(landlock_abi=3))._tcp_ports(plan, {}) is None
    sql = JobPlan(spec={}, uses_sql=True)
    assert Isolation(make_settings(), platform())._tcp_ports(sql, {}) is None
    proxied = {"HTTPS_PROXY": "proxy.internal", "HTTP_PROXY": "http://proxy:bad"}
    assert Isolation(make_settings(), platform())._tcp_ports(plan, proxied) == [80, 443]


def test_resource_limits(make_settings):
    explicit = Isolation(
        make_settings(
            limit_address_space=1 << 30,
            limit_cpu_seconds=90.5,
            limit_file_size=1 << 20,
            limit_open_files=64,
            kill_grace_seconds=2.5,
        ),
        platform(),
    ).rlimits(timeout=10)
    assert explicit == {
        "core": [0, 0],
        "as": [1 << 30, 1 << 30],
        "cpu": [91, 94],
        "fsize": [1 << 20, 1 << 20],
        "nofile": [64, 64],
    }
    unlimited = Isolation(
        make_settings(
            limit_address_space=None,
            limit_cpu_seconds=None,
            limit_file_size=None,
            limit_open_files=None,
        ),
        platform(),
    ).rlimits(timeout=10)
    assert unlimited == {
        "core": [0, 0],
        "as": [-1, -1],
        "cpu": [-1, -1],
        "fsize": [-1, -1],
        "nofile": [-1, -1],
    }
    settings = make_settings()
    settings.scratch.mkdir()
    automatic = Isolation(settings, platform()).rlimits(timeout=10)
    assert automatic["cpu"][0] == 10 * len(os.sched_getaffinity(0))
    assert automatic["as"][0] == os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    assert automatic["fsize"][0] > 0


def test_the_sandbox_configuration(make_settings, tmp_path):
    plan = JobPlan(spec={})
    make_settings().scratch.mkdir()
    with_landlock = Isolation(make_settings(), platform()).sandbox_config(tmp_path, plan, {}, 5)
    assert with_landlock["landlock"]["write"] == [str(tmp_path)]
    assert with_landlock["landlock"]["tcp_ports"] == []
    assert with_landlock["landlock"]["scope"] is True
    assert with_landlock["parent_pid"] == os.getpid()
    without = Isolation(make_settings(), platform(landlock_abi=0)).sandbox_config(
        tmp_path, plan, {}, 5
    )
    assert without["landlock"] is None
    assert Path(sys.prefix).as_posix() in Isolation(make_settings(), platform()).read_paths
