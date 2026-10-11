# forklift-mcp: the remote MCP server (forklift-mcp-server). Build from the repository root, so that
# forklift-client (clients/python) and the schema documentation pages (docs/schemas) go in:
#
#   docker build -f services/mcp/Dockerfile -t forklift-mcp .
#
# Streamable HTTP at /mcp on port 8000 (FORKLIFT_MCP_PORT) and GET /healthz; settings in
# services/mcp/README.md (FORKLIFT_MCP_API_URL and FORKLIFT_MCP_ALLOWED_HOSTS are required).
# It holds no credentials: every request brings its caller's forklift API token, which is passed
# on to the gateway and nowhere else. Runs as uid/gid 10001 and writes nothing, so a read-only
# root file system works (give it a tmpfs at /tmp for Python's temporary files). No apt packages.
# PYTHON_IMAGE picks another registry for the base image (e.g. a mirror).

ARG PYTHON_IMAGE=python:3.13.16-slim-trixie

FROM ${PYTHON_IMAGE} AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /src
COPY docs/schemas docs/schemas
COPY clients/python/pyproject.toml clients/python/README.md clients/python/
COPY clients/python/src clients/python/src
COPY services/mcp/pyproject.toml services/mcp/README.md services/mcp/constraints.txt services/mcp/
COPY services/mcp/src services/mcp/src
# The wheel takes the pages from ../../docs/schemas (pyproject.toml); exact versions of the rest
RUN python -m venv /opt/forklift-mcp \
    && /opt/forklift-mcp/bin/pip install -c services/mcp/constraints.txt \
        ./clients/python ./services/mcp

FROM ${PYTHON_IMAGE}
RUN groupadd --system --gid 10001 forklift \
    && useradd --system --uid 10001 --gid 10001 --no-create-home --home-dir /nonexistent \
        --shell /usr/sbin/nologin forklift
COPY --from=build /opt/forklift-mcp /opt/forklift-mcp
ENV PATH=/opt/forklift-mcp/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FORKLIFT_MCP_PORT=8000
USER 10001:10001
EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=5s --retries=3 --start-period=10s \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.environ['FORKLIFT_MCP_PORT'] + '/healthz', timeout=4)"]
ENTRYPOINT ["forklift-mcp-server"]
