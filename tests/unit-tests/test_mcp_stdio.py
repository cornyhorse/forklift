"""milestone M2 end to end: an agent drives a real ``forklift-mcp`` process over stdio.

The client is the MCP SDK's own stdio client, as an agent host uses it: it starts the command,
initializes, and calls tools. The flow: preview a local file, generate a schema and write it,
read it back, tighten it, validate it, preview the typed rows, apply it (run_job), read the
result again (get_job), then run a stricter schema that fails, explain the failure, follow the
documentation page it names, and page through the rejected rows.
"""

from __future__ import annotations

import copy
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip(
    "mcp.server.mcpserver", reason="needs the MCP Python SDK: pip install 'forklift-etl[mcp]'"
)

import anyio  # noqa: E402
import pyarrow.parquet as pq  # noqa: E402
from mcp import Client, StdioServerParameters  # noqa: E402

SRC = Path(__file__).resolve().parents[2] / "src"
ORDERS = (
    "order_id,customer,amount,ordered_on\n"
    "1,Ann,12.50,2026-01-03\n"
    "2,Bob,7.00,2026-01-04\n"
    "3,Cy,-3.25,2026-01-04\n"
    "4,Di,99.99,2026-01-05\n"
    "5,Ed,15.00,2026-01-06\n"
    "6,Flo,1.10,2026-01-07\n"
    "7,Gus,8.80,2026-01-08\n"
    "8,Hal,4.40,2026-01-09\n"
)


def _server(*args):
    """``forklift-mcp ARGS`` as installed next to this Python (``python -m`` in a checkout)."""
    script = shutil.which("forklift-mcp", path=os.path.dirname(sys.executable))
    if script is not None:
        return StdioServerParameters(command=script, args=list(args))
    return StdioServerParameters(
        command=sys.executable,
        args=["-m", "forklift.mcp", *args],
        env={"PYTHONPATH": str(SRC)},
    )


def _session(parameters, scenario, cwd):
    parameters = parameters.model_copy(update={"cwd": str(cwd)})

    async def main():
        with anyio.fail_after(300):
            async with Client(parameters, mode="legacy") as client:
                return await scenario(client)

    return anyio.run(main)


async def _tool(client, name, **arguments):
    result = await client.call_tool(name, arguments)
    assert not result.is_error, result.content[0].text
    return result.structured_content


@pytest.fixture
def work(tmp_path):
    work = tmp_path / "work"
    (work / "in").mkdir(parents=True)
    (work / "in" / "orders.csv").write_text(ORDERS)
    return work


def test_m2_generate_validate_apply_and_explain_a_failed_run(work, tmp_path):
    async def agent(client):
        names = [tool.name for tool in (await client.list_tools()).tools]
        assert {"generate_schema", "validate_schema", "run_job", "explain_failure"} <= set(names)
        assert "list_connections" not in names and "list_datasets" not in names

        raw = await _tool(client, "preview", input_path="in/orders.csv", rows=3)
        assert raw["columns"] == ["order_id", "customer", "amount", "ordered_on"]
        assert raw["rows"][0] == ["1", "Ann", "12.50", "2026-01-03"] and raw["truncated"]

        generated = await _tool(
            client,
            "generate_schema",
            input_path="in/orders.csv",
            output_path="schemas/orders.json",
        )
        assert generated["status"] == "succeeded" and generated["written"] is True
        stored = await _tool(client, "get_schema", path="schemas/orders.json", max_bytes=1048576)
        assert stored["summary"]["column_count"] == 4 and stored["truncated"] is False
        types = {column["name"]: column["type"] for column in stored["summary"]["columns"]}
        assert types["order_id"] == "integer" and types["amount"] == "number"

        schema = copy.deepcopy(stored["document"])
        schema["required"] = ["order_id", "amount"]
        checked = await _tool(client, "validate_schema", input_path="in/orders.csv", schema=schema)
        assert checked["valid"] is True and checked["error"] is None
        assert checked["schema_columns_not_in_input"] == []

        typed = await _tool(client, "preview", input_path="in/orders.csv", schema=schema, rows=2)
        assert typed["mode"] == "typed" and typed["rows"][0][:3] == [1, "Ann", 12.5]

        applied = await _tool(
            client, "run_job", input_path="in/orders.csv", output_dir="out/orders", schema=schema
        )
        assert applied["status"] == "succeeded", applied["error"]
        assert applied["counts"]["valid_rows"] == 8
        assert (await _tool(client, "get_job", output_dir="out/orders"))["status"] == "succeeded"

        strict = copy.deepcopy(schema)
        strict["x-validation"] = {
            "badRowsHandling": {"maxBadRowsPercent": 5},
            "fieldValidations": {"amount": {"range": {"min": 0}}},
        }
        failed = await _tool(
            client, "run_job", input_path="in/orders.csv", output_dir="out/strict", schema=strict
        )
        assert failed["status"] == "failed"
        assert failed["error"]["code"] == "BAD_ROWS_THRESHOLD_EXCEEDED"

        explained = await _tool(client, "explain_failure", output_dir="out/strict")
        assert explained["summary"] == (
            "The run failed with BAD_ROWS_THRESHOLD_EXCEEDED after reading 8 rows. 1 rejected "
            "row is kept; most common reasons: VALIDATION_ERROR:amount (1)."
        )
        assert explained["error"]["code"] == "BAD_ROWS_THRESHOLD_EXCEEDED"
        assert explained["error"]["message"].startswith(
            "Bad rows (12.5%) exceed threshold (5.0%): 1 of 8 rows"
        )
        assert explained["error"]["meaning"] and explained["error"]["next_steps"]
        assert explained["rejected"]["reasons"] == [
            {
                "rule": "VALIDATION_ERROR",
                "column": "amount",
                "reason": "VALIDATION_ERROR:amount",
                "rows": 1,
            }
        ]
        assert explained["rejected"]["first_rows"]["rows"] == [
            ["3", "Cy", "-3.25", "2026-01-04", "VALIDATION_ERROR:amount"]
        ]
        page = "forklift://docs/schemas/X_VALIDATION_DOCUMENTATION.md"
        assert explained["docs"][0] == page
        documentation = await client.read_resource(page)
        assert "maxBadRowsPercent" in documentation.contents[0].text

        rejected = await _tool(client, "get_bad_rows", output_dir="out/strict", columns=["amount"])
        assert rejected["rows"] == [["-3.25"]] and rejected["next_offset"] is None
        return explained

    _session(_server("--root", str(work)), agent, cwd=tmp_path)

    data = pq.read_table(str(work / "out" / "orders" / "data.parquet"))
    assert data.num_rows == 8 and data.column("amount").to_pylist()[2] == -3.25
    assert (work / "out" / "orders" / "forklift-job.json").is_file()
    assert (work / "out" / "strict" / "bad_rows.parquet").is_file()
    assert not (work / "out" / "strict" / "data.parquet").exists()


def test_read_only_server_refuses_writes_and_serves_the_docs(work, tmp_path):
    async def agent(client):
        refused = await client.call_tool(
            "run_job", {"input_path": "in/orders.csv", "output_dir": "out"}
        )
        assert refused.is_error and "--read-only" in refused.content[0].text
        outside = await client.call_tool("preview", {"input_path": str(tmp_path / "x.csv")})
        assert outside.is_error and "outside the directories" in outside.content[0].text
        preview = await _tool(client, "preview", input_path="in/orders.csv", rows=1)
        assert preview["row_count"] == 1
        pages = [str(resource.uri) for resource in (await client.list_resources()).resources]
        assert "forklift://docs/schemas/README.md" in pages
        assert "read-only" in client.instructions

    _session(_server("--root", str(work), "--read-only"), agent, cwd=tmp_path)
    assert sorted(os.listdir(work)) == ["in"]


def test_the_protocol_version_is_negotiated_automatically(work, tmp_path):
    parameters = _server("--root", str(work)).model_copy(update={"cwd": str(tmp_path)})

    async def main():
        with anyio.fail_after(120):
            async with Client(parameters) as client:
                return (await client.call_tool("get_job", {"output_dir": "in"})).is_error

    assert anyio.run(main) is True


def test_a_server_that_cannot_start_says_why(tmp_path):
    parameters = _server("--root", str(tmp_path / "missing"))
    environment = dict(os.environ, **(parameters.env or {}))
    done = subprocess.run(
        [parameters.command, *parameters.args],
        capture_output=True,
        text=True,
        env=environment,
        timeout=120,
        stdin=subprocess.DEVNULL,
    )
    assert done.returncode == 2
    assert done.stderr.strip() == f"forklift-mcp: --root {tmp_path / 'missing'} is not a directory"
    assert done.stdout == ""
