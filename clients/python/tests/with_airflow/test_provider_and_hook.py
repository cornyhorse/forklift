"""The provider entry point, the ``forklift`` connection type and the hook, in a real Airflow."""

from __future__ import annotations

import asyncio
import json

import pytest
from airflow.sdk import Connection
from airflow.sdk.exceptions import AirflowException
from fake_api import TOKEN

from forklift_client import ForkliftClient, RetryPolicy
from forklift_client.airflow.hooks import ForkliftHook, client_for
from forklift_client.airflow.provider import get_provider_info


def make(**fields):
    return Connection(conn_id="forklift_test", conn_type="forklift", **fields)


class TestProvider:
    def test_airflow_finds_the_provider_and_the_connection_type(self):
        from airflow.providers_manager import ProvidersManager
        from airflow.sdk.providers_manager_runtime import ProvidersManagerTaskRuntime

        manager = ProvidersManager()
        assert "forklift-client" in manager.providers
        assert manager.field_behaviours["forklift"]["relabeling"] == {
            "host": "Base URL",
            "password": "API token",
        }
        assert {"extra__forklift__ca_file", "extra__forklift__timeout"} <= set(
            manager.connection_form_widgets
        )
        hook = ProvidersManagerTaskRuntime().hooks["forklift"]
        assert hook.hook_class_name == "forklift_client.airflow.hooks.ForkliftHook"
        assert hook.connection_id_attribute_name == "forklift_conn_id"

    def test_the_provider_info_follows_airflows_schema(self):
        from pathlib import Path

        import jsonschema
        from airflow import __file__ as airflow_init

        schema = json.loads((Path(airflow_init).parent / "provider_info.schema.json").read_text())
        jsonschema.validate(get_provider_info(), schema)

    def test_the_modules_it_lists_exist(self):
        import importlib

        info = get_provider_info()
        for group in ("hooks", "operators", "sensors", "triggers"):
            for module in info[group][0]["python-modules"]:
                importlib.import_module(module)


class TestClientFor:
    def test_a_url_host(self):
        client = client_for(make(host="https://forklift.example.org", password=TOKEN))
        assert isinstance(client, ForkliftClient)
        assert client.base_url == "https://forklift.example.org"
        assert client.timeout == 30.0

    def test_a_host_name_with_a_port_is_https(self):
        client = client_for(make(host="forklift.example.org", port=8443, password=TOKEN))
        assert client.base_url == "https://forklift.example.org:8443"
        assert client_for(make(host="h.example", password=TOKEN)).base_url == "https://h.example"

    def test_extras(self, tmp_path):
        bundle = tmp_path / "ca.pem"
        bundle.write_text("")  # an empty bundle: the client refuses it when it loads it
        extra = json.dumps({"timeout": "12.5", "ca_file": ""})
        client = client_for(make(host="https://h.example", password=TOKEN, extra=extra))
        assert client.timeout == 12.5
        with pytest.raises(AirflowException, match="is not a readable PEM bundle"):
            extra = json.dumps({"ca_file": str(bundle)})
            client_for(make(host="https://h", password=TOKEN, extra=extra))
        blank = json.dumps({"timeout": ""})
        assert client_for(make(host="https://h", password=TOKEN, extra=blank)).timeout == 30.0

    def test_the_retry_policy(self):
        policy = RetryPolicy(attempts=2)
        assert client_for(make(host="https://h", password=TOKEN), retry=policy).retry is policy

    @pytest.mark.parametrize(
        "fields, message",
        [
            ({"password": TOKEN}, "has no host: set it to the forklift gateway's URL"),
            ({"host": "https://h"}, "has no password: set it to a forklift API token"),
            ({"host": "ftp://h", "password": TOKEN}, "base_url must be"),
            (
                {"host": "https://h", "password": TOKEN, "extra": '{"timeout": "soon"}'},
                "the extra 'timeout' must be a number of seconds",
            ),
        ],
    )
    def test_connections_that_cannot_be_used(self, fields, message):
        with pytest.raises(AirflowException, match=message) as raised:
            client_for(make(**fields))
        assert "forklift_test" in str(raised.value) and TOKEN not in str(raised.value)


class TestHook:
    def test_get_conn_reads_the_connection_once(self, connection, fake):
        hook = ForkliftHook()
        assert hook.get_conn() is hook.get_conn()
        assert hook.get_conn().base_url == fake.url

    def test_aget_conn(self, connection, fake):
        client = asyncio.run(ForkliftHook().aget_conn())
        assert client.base_url == fake.url

    def test_test_connection(self, connection, fake):
        ok, message = ForkliftHook().test_connection()
        assert ok and message == f"Connected to {fake.url} as ana (author)"

    def test_test_connection_with_a_bad_token(self, fake, monkeypatch):
        value = {"conn_type": "forklift", "host": fake.url, "password": "fkl_wrong"}
        monkeypatch.setenv("AIRFLOW_CONN_FORKLIFT_BAD", json.dumps(value))
        ok, message = ForkliftHook("forklift_bad").test_connection()
        assert not ok and "HTTP 401 (not_authenticated)" in message and "fkl_wrong" not in message

    def test_test_connection_without_a_host(self, monkeypatch):
        value = {"conn_type": "forklift", "password": TOKEN}
        monkeypatch.setenv("AIRFLOW_CONN_FORKLIFT_EMPTY", json.dumps(value))
        ok, message = ForkliftHook("forklift_empty").test_connection()
        assert not ok and "has no host" in message
