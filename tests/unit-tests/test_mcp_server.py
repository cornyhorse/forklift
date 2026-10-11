"""forklift-mcp through the MCP protocol, in process (the SDK's in-memory client and server).

Tool listing and input schemas, structured results, refusals as tool errors, crashes that keep
their details on the server, the schema documentation resources and the server's instructions.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

pytest.importorskip(
    "mcp.server.mcpserver", reason="needs the MCP Python SDK: pip install 'forklift-etl[mcp]'"
)

import anyio  # noqa: E402
from mcp import Client  # noqa: E402
from mcp.server.mcpserver import MCPServer  # noqa: E402
from mcp.shared.exceptions import MCPError  # noqa: E402

from forklift import __version__  # noqa: E402
from forklift.mcp import server as server_module  # noqa: E402
from forklift.mcp.server import PARAMETERS, TOOLS, create_server, instructions, serve  # noqa: E402
from forklift.mcp.tools import Toolbox  # noqa: E402
from forklift.mcp.workspace import Workspace  # noqa: E402

REPO_DOCS = Path(__file__).resolve().parents[2] / "docs" / "schemas"
READ_ONLY_TOOLS = {
    "get_schema",
    "validate_schema",
    "preview",
    "get_job",
    "explain_failure",
    "get_bad_rows",
}


@pytest.fixture
def root(tmp_path):
    root = tmp_path / "root"
    (root / "in").mkdir(parents=True)
    (root / "in" / "people.csv").write_text("id,name\n1,Ann\n2,Bob\n")
    return root


def _with_client(server, scenario):
    async def main():
        with anyio.fail_after(120):
            async with Client(server, mode="legacy") as client:
                return await scenario(client)

    return anyio.run(main)


def _call(server, name, arguments):
    return _with_client(server, lambda client: client.call_tool(name, arguments))


class TestTools:
    def test_the_tool_set(self, root):
        listed = _with_client(create_server(Workspace([root])), lambda c: c.list_tools()).tools
        assert [tool.name for tool in listed] == [
            "get_schema",
            "generate_schema",
            "validate_schema",
            "preview",
            "run_job",
            "get_job",
            "explain_failure",
            "get_bad_rows",
        ]
        for tool in listed:
            assert tool.title and tool.description
            assert tool.annotations.read_only_hint is (tool.name in READ_ONLY_TOOLS)
            assert tool.annotations.open_world_hint is False
            assert tool.output_schema["type"] == "object"
            properties = tool.input_schema["properties"]
            assert set(properties) == set(PARAMETERS[tool.name])
            assert all(spec.get("description") for spec in properties.values())
        preview = next(tool for tool in listed if tool.name == "preview")
        rows = preview.input_schema["properties"]["rows"]
        assert (rows["minimum"], rows["maximum"], rows["default"]) == (1, 500, 20)
        assert preview.input_schema["properties"]["format"]["enum"] == ["csv", "excel", None]
        assert preview.input_schema["required"] == ["input_path"]

    def test_results_are_structured_json(self, root):
        server = create_server(Workspace([root]))
        result = _call(server, "preview", {"input_path": "in/people.csv", "rows": 1})
        assert not result.is_error
        expected = Toolbox(Workspace([root])).preview("in/people.csv", rows=1)
        assert result.structured_content == expected
        assert json.loads(result.content[0].text) == expected

    def test_an_inline_schema_argument(self, root):
        schema = {"properties": {"id": {"type": "integer"}}, "required": ["id"]}
        result = _call(
            create_server(Workspace([root])),
            "validate_schema",
            {"input_path": "in/people.csv", "schema": schema},
        )
        assert result.structured_content["valid"] is True

    def test_refusals_are_tool_errors_with_the_reason(self, root):
        result = _call(create_server(Workspace([root])), "preview", {"input_path": "/etc/hosts"})
        assert result.is_error
        assert result.content[0].text.startswith(
            "Error executing tool preview: input_path '/etc/hosts' is outside the directories"
        )

    def test_read_only(self, root):
        server = create_server(Workspace([root], read_only=True))
        refused = _call(server, "run_job", {"input_path": "in/people.csv", "output_dir": "out"})
        assert refused.is_error and "--read-only" in refused.content[0].text
        planned = _call(
            server,
            "run_job",
            {"input_path": "in/people.csv", "output_dir": "out", "dry_run": True},
        )
        assert planned.structured_content["writable"] is False
        assert not (root / "out").exists()

    def test_arguments_that_do_not_fit_the_input_schema(self, root):
        result = _call(
            create_server(Workspace([root])),
            "preview",
            {"input_path": "in/people.csv", "rows": "x"},
        )
        assert result.is_error and "rows" in result.content[0].text

    def test_a_crash_keeps_its_details_on_the_server(self, root, monkeypatch):
        def crash(self, path, max_bytes=0):
            raise RuntimeError("the cell value 123-45-6789")

        monkeypatch.setattr(Toolbox, "get_schema", crash)
        result = _call(create_server(Workspace([root])), "get_schema", {"path": "x.json"})
        assert result.is_error
        assert result.content[0].text == "Error executing tool get_schema"


class TestResources:
    def test_the_schema_pages(self, root):
        server = create_server(Workspace([root]))
        resources = _with_client(server, lambda c: c.list_resources()).resources
        names = sorted(path.name for path in REPO_DOCS.glob("*.md"))
        assert sorted(resource.name for resource in resources) == names
        readme = resources[0]
        assert str(readme.uri) == "forklift://docs/schemas/README.md"
        assert readme.mime_type == "text/markdown"
        assert readme.title == "Forklift Extended JSON Schema (x-attributes) Documentation"

        async def read(client):
            return await client.read_resource(
                "forklift://docs/schemas/X_VALIDATION_DOCUMENTATION.md"
            )

        contents = _with_client(server, read).contents
        assert contents[0].text == (REPO_DOCS / "X_VALIDATION_DOCUMENTATION.md").read_text()

    def test_an_unknown_page(self, root):
        async def read(client):
            with pytest.raises(MCPError, match="Unknown resource"):
                await client.read_resource("forklift://docs/schemas/NOPE.md")

        _with_client(create_server(Workspace([root])), read)

    def test_no_pages(self, root, monkeypatch):
        monkeypatch.setattr(server_module, "schema_pages", lambda: [])
        server = create_server(Workspace([root]))
        assert _with_client(server, lambda c: c.list_resources()).resources == []


class TestServer:
    def test_identity_and_instructions(self, root):
        server = create_server(Workspace([root]))

        async def connect(client):
            return client.server_info, client.instructions

        info, text = _with_client(server, connect)
        assert info.name == "forklift" and info.version == __version__
        assert str(root) in text and "never replaces files unless overwrite=true" in text
        assert "forklift://docs/schemas/<page>.md" in text
        assert "read-only" in instructions(Workspace([root], read_only=True))

    def test_serve_runs_over_stdio(self, root, monkeypatch):
        calls = []
        monkeypatch.setattr(MCPServer, "run", lambda self, transport: calls.append(transport))
        serve(Workspace([root]), log_level="ERROR")
        assert calls == ["stdio"]

    def test_every_tool_has_a_parameter_table(self):
        assert set(TOOLS) == set(PARAMETERS)
