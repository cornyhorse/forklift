"""forklift-worker: leases jobs from the forklift gateway and runs the engine in a child process.

The supervisor (this package) is the trusted half of a worker: it talks to the gateway's internal
API, stages inputs through presigned URLs, starts ``forklift run-job`` in a sandboxed child process
with no credentials, uploads the artifacts and reports the result. See README.md.
"""

__version__ = "0.1.0"
