# forklift-worker: the supervisor (forklift-worker) and the engine (forklift-etl), no Django.
#
#   docker build -f services/worker/Dockerfile -t forklift-worker .     # from the repository root
#
# The image runs as uid 10001 and works with a read-only root file system: the only path it
# writes is the scratch volume (/scratch). tini is PID 1, so processes an engine leaves behind are
# reaped. See services/worker/README.md for the settings and the isolation profiles.

ARG PYTHON_VERSION=3.13

FROM python:${PYTHON_VERSION}-slim-bookworm AS build
ENV PIP_NO_CACHE_DIR=1 PIP_DISABLE_PIP_VERSION_CHECK=1
WORKDIR /src
COPY pyproject.toml README.md LICENSE ./
COPY src ./src
COPY services/worker/pyproject.toml services/worker/README.md ./services/worker/
COPY services/worker/src ./services/worker/src
# The engine from this checkout (with Excel and ODBC support), then the worker; pip takes the
# local forklift-etl for the worker's dependency instead of fetching one from PyPI.
RUN python -m venv /opt/forklift \
    && /opt/forklift/bin/pip install ".[excel,sql]" ./services/worker \
    && /opt/forklift/bin/python -m compileall -q /opt/forklift

FROM python:${PYTHON_VERSION}-slim-bookworm
# unixODBC with the PostgreSQL and MariaDB (MySQL) drivers for sql-lane jobs. Add Microsoft's
# ODBC Driver 18 for SQL Server or Oracle Instant Client in an image built FROM this one.
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini unixodbc odbc-postgresql odbc-mariadb \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system --gid 10001 forklift \
    && useradd --system --uid 10001 --gid 10001 --home-dir /scratch --no-create-home \
        --shell /usr/sbin/nologin forklift \
    && install -d -o 10001 -g 10001 -m 0700 /scratch
COPY --from=build /opt/forklift /opt/forklift
ENV PATH=/opt/forklift/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    FORKLIFT_WORKER_SCRATCH=/scratch \
    FORKLIFT_WORKER_TOKEN_FILE=/run/secrets/worker-token
USER 10001:10001
WORKDIR /scratch
VOLUME ["/scratch"]
ENTRYPOINT ["/usr/bin/tini", "--", "forklift-worker"]
