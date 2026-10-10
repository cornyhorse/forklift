"""/api/v1/webhooks: the caller's webhooks, their signing secrets, test events and deliveries
(admins see and disable everyone's under /api/v1/admin/webhooks)."""

import uuid
from typing import Literal, Optional

from ninja import Router, Status
from ninja.pagination import paginate

from forklift_web.api.common import responses
from forklift_web.api.payloads import (
    WebhookCreatedOut,
    WebhookDeliveryOut,
    WebhookIn,
    WebhookOut,
    WebhookPatch,
)
from forklift_web.services import webhooks

router = Router(tags=["webhooks"])


@router.get("", response=responses({200: list[WebhookOut]}), summary="My webhooks")
@paginate
def list_webhooks(request):
    return webhooks.list_webhooks(request.auth)


@router.post(
    "",
    response=responses({201: WebhookCreatedOut}),
    summary="Create a webhook (its signing secret is returned only now)",
)
def create_webhook(request, payload: WebhookIn):
    webhook, secret = webhooks.create_webhook(request.auth, **payload.model_dump())
    webhook.secret = secret
    return Status(201, webhook)


@router.get("/{webhook_id}", response=responses({200: WebhookOut}), summary="A webhook")
def get_webhook(request, webhook_id: uuid.UUID):
    return webhooks.get_webhook(request.auth, webhook_id)


@router.patch(
    "/{webhook_id}",
    response=responses({200: WebhookOut}),
    summary="Change a webhook, or disable or enable it",
)
def update_webhook(request, webhook_id: uuid.UUID, payload: WebhookPatch):
    return webhooks.update_webhook(
        request.auth, webhook_id, **payload.model_dump(exclude_unset=True)
    )


@router.delete(
    "/{webhook_id}", response=responses({204: None}), summary="Delete a webhook and its log"
)
def delete_webhook(request, webhook_id: uuid.UUID):
    webhooks.delete_webhook(request.auth, webhook_id)
    return Status(204, None)


@router.post(
    "/{webhook_id}/rotate-secret",
    response=responses({200: WebhookCreatedOut}),
    summary="Replace the signing secret (the new one is returned only now)",
)
def rotate_secret(request, webhook_id: uuid.UUID):
    webhook, secret = webhooks.rotate_secret(request.auth, webhook_id)
    webhook.secret = secret
    return webhook


@router.post(
    "/{webhook_id}/test",
    response=responses({202: WebhookDeliveryOut}),
    summary="Queue a webhook.test event (202); its delivery shows how it went once it was sent",
)
def send_test(request, webhook_id: uuid.UUID):
    return Status(202, webhooks.send_test(request.auth, webhook_id))


@router.get(
    "/{webhook_id}/deliveries",
    response=responses({200: list[WebhookDeliveryOut]}),
    summary="The webhook's deliveries, newest first",
)
@paginate
def list_deliveries(
    request,
    webhook_id: uuid.UUID,
    status: Optional[Literal["pending", "delivered", "failed", "skipped"]] = None,
):
    return webhooks.list_deliveries(request.auth, webhook_id, status=status)


@router.post(
    "/{webhook_id}/deliveries/{delivery_id}/redeliver",
    response=responses({200: WebhookDeliveryOut}),
    summary="Send a delivery again (queued now, with the same id and all its retries)",
)
def redeliver(request, webhook_id: uuid.UUID, delivery_id: uuid.UUID):
    return webhooks.redeliver(request.auth, webhook_id, delivery_id)
