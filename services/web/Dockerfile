# forklift-web: the gateway (Django). Build from the repository root, so that the job contract's
# JSON Schemas (contracts/) are packaged with it:
#
#   docker build -f services/web/Dockerfile -t forklift-web .
#
# One gunicorn serves both ports: the public one (UI, /api/v1) and the internal one
# (/internal/v1, workers only; never route it from the ingress). Requests are routed by the port
# their connection arrived on. Run `forklift-web migrate` (same image) before starting it.
# No engine and no pyarrow in this image: tests/test_trust_boundary.py checks the package.
# PYTHON_IMAGE picks another registry for the base image (e.g. a mirror).

ARG PYTHON_IMAGE=python:3.13-slim

FROM ${PYTHON_IMAGE} AS build
WORKDIR /build
RUN pip install --no-cache-dir --upgrade pip build
COPY services/web/pyproject.toml services/web/
COPY services/web/src services/web/src
COPY contracts/jobspec.schema.json contracts/jobresult.schema.json \
     services/web/src/forklift_web/contracts/
RUN python -m build --wheel --outdir /wheels services/web

FROM ${PYTHON_IMAGE}
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DJANGO_SETTINGS_MODULE=forklift_web.settings \
    FORKLIFT_PUBLIC_PORT=8080 \
    FORKLIFT_INTERNAL_PORT=8081
RUN useradd --system --uid 10001 --no-create-home --home-dir /nonexistent forklift
COPY --from=build /wheels /wheels
RUN pip install --no-cache-dir /wheels/*.whl && rm -rf /wheels
USER 10001
EXPOSE 8080 8081
HEALTHCHECK --interval=10s --timeout=5s --retries=5 \
    CMD ["python", "-c", "import os, urllib.request; urllib.request.urlopen('http://127.0.0.1:' + os.environ['FORKLIFT_PUBLIC_PORT'] + '/readyz', timeout=4)"]
CMD ["sh", "-c", "exec gunicorn forklift_web.wsgi:application --bind 0.0.0.0:${FORKLIFT_PUBLIC_PORT} --bind 0.0.0.0:${FORKLIFT_INTERNAL_PORT} --workers ${FORKLIFT_WEB_WORKERS:-2} --threads ${FORKLIFT_WEB_THREADS:-8} --timeout ${FORKLIFT_WEB_TIMEOUT:-120} --access-logfile -"]
