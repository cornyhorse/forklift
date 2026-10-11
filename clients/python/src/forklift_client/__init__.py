"""forklift-client: a typed Python client for the forklift gateway's API (``/api/v1``).

    from forklift_client import ForkliftClient

    client = ForkliftClient("https://forklift.example.org", token)
    upload = client.upload_file("people.csv")
    job = client.submit_job("run", upload_id=upload.id, format="csv", schema=schema)
    job = client.wait_for_job(job, raise_on_failure=True)
    for artifact in client.list_artifacts(job):
        client.download_artifact(artifact, "out/")

The standard library is its only run-time dependency. ``forklift_client.airflow`` (extra
``airflow``) has an Airflow hook, operator, sensor and trigger. See docs/platform/client.md.
"""

__version__ = "0.1.0"

from ._downloads import ArtifactStream  # noqa: E402
from ._model import Model, Page  # noqa: E402
from ._transport import NO_RETRIES, RetryPolicy  # noqa: E402
from .client import ACTIVE_STATUSES, TERMINAL_STATUSES, ForkliftClient  # noqa: E402
from .errors import (  # noqa: E402
    ApiError,
    BadRequest,
    Conflict,
    Forbidden,
    ForkliftError,
    Gone,
    JobFailed,
    JobTimeout,
    NotFound,
    ServerError,
    StoreError,
    TooManyRequests,
    TransportError,
    Unauthorized,
    UnexpectedResponse,
    VerificationError,
)
from .models import (  # noqa: E402
    Artifact,
    Connection,
    Dataset,
    Download,
    Job,
    JobError,
    JobEvent,
    JobSummary,
    Me,
    PartUrl,
    Schedule,
    Schema,
    SchemaVersion,
    Token,
    Upload,
    UploadTicket,
    User,
    Webhook,
    WebhookDelivery,
)
from .webhooks import verify_webhook_signature  # noqa: E402

__all__ = [
    "ACTIVE_STATUSES",
    "NO_RETRIES",
    "TERMINAL_STATUSES",
    "ApiError",
    "Artifact",
    "ArtifactStream",
    "BadRequest",
    "Conflict",
    "Connection",
    "Dataset",
    "Download",
    "Forbidden",
    "ForkliftClient",
    "ForkliftError",
    "Gone",
    "Job",
    "JobError",
    "JobEvent",
    "JobFailed",
    "JobSummary",
    "JobTimeout",
    "Me",
    "Model",
    "NotFound",
    "Page",
    "PartUrl",
    "RetryPolicy",
    "Schedule",
    "Schema",
    "SchemaVersion",
    "ServerError",
    "StoreError",
    "Token",
    "TooManyRequests",
    "TransportError",
    "Unauthorized",
    "UnexpectedResponse",
    "Upload",
    "UploadTicket",
    "User",
    "VerificationError",
    "Webhook",
    "WebhookDelivery",
    "verify_webhook_signature",
    "__version__",
]
