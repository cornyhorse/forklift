"""The signed-in user's webhooks (/account/webhooks/) and the admin list of everyone's.

A webhook's signing secret is shown once, on the page that answers its creation or a rotation;
the webhook's page shows its settings, the test button and the log of its deliveries. A test
event is queued for the dispatcher (the gateway sends nothing while it answers a request): while
one waits, HTMX polls the delivery log until it was sent.
"""

from __future__ import annotations

from django.contrib import messages
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from forklift_web.core.choices import DeliveryStatus
from forklift_web.errors import Conflict, ServiceError
from forklift_web.policy import Action, allowed, check
from forklift_web.services import datasets, webhooks
from forklift_web.ui.forms import WebhookForm
from forklift_web.ui.views.base import back, form_failed, page, paginate


def _form(actor, data=None, webhook=None) -> WebhookForm:
    visible = datasets.list_datasets(actor) if allowed(actor, Action.DATASET_VIEW) else []
    return WebhookForm(
        data,
        datasets=visible,
        all_jobs=allowed(actor, Action.WEBHOOK_ALL_JOBS),
        webhook=webhook,
    )


def _secret_page(request, webhook, secret: str, *, created: bool):
    """The page that shows a webhook's signing secret: the only time anyone sees it."""
    response = render(
        request,
        "ui/account/webhook_secret.html",
        {"webhook": webhook, "secret": secret, "created": created},
    )
    response["Cache-Control"] = "no-store"
    return response


def _list_page(request, actor, form, status=200):
    return render(
        request,
        "ui/account/webhooks.html",
        {"webhooks": webhooks.list_webhooks(actor), "form": form},
        status=status,
    )


@never_cache
@page
def webhook_list(request, actor):
    """Own webhooks; POST creates one and shows its secret, once."""
    check(actor, Action.WEBHOOK_VIEW)
    if request.method != "POST":
        return _list_page(request, actor, _form(actor))
    check(actor, Action.WEBHOOK_MANAGE)
    form = _form(actor, request.POST)
    status = 400
    if form.is_valid():
        try:
            webhook, secret = webhooks.create_webhook(actor, **form.service_values())
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            return _secret_page(request, webhook, secret, created=True)
    return _list_page(request, actor, form, status)


def _log(request, actor, webhook) -> dict:
    """The delivery log's context: a page of deliveries, whether a test event is waiting (the
    log then refreshes itself) and the latest test event, to announce how it went."""
    found = webhooks.list_deliveries(actor, webhook.pk)
    tests = found.filter(job__isnull=True)
    return {
        "webhook": webhook,
        "deliveries": paginate(request, found),
        "testing": tests.filter(status=DeliveryStatus.PENDING).exists(),
        "latest_test": tests.first(),
        "now": timezone.now(),
    }


def _detail_page(request, actor, webhook, form, status=200):
    context = {**_log(request, actor, webhook), "form": form}
    return render(request, "ui/account/webhook.html", context, status=status)


@require_GET
@page
def webhook_deliveries(request, actor, webhook_id):
    """The delivery log alone (what the webhook's page polls while a test event waits)."""
    webhook = webhooks.get_webhook(actor, webhook_id)
    return render(request, "ui/account/_webhook_deliveries.html", _log(request, actor, webhook))


@never_cache
@page
def webhook_detail(request, actor, webhook_id):
    """A webhook's settings and delivery log; POST saves the settings."""
    webhook = webhooks.get_webhook(actor, webhook_id)
    if request.method != "POST":
        return _detail_page(request, actor, webhook, _form(actor, webhook=webhook))
    form = _form(actor, request.POST, webhook=webhook)
    status = 400
    if form.is_valid():
        try:
            saved = webhooks.update_webhook(actor, webhook_id, **form.service_values())
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"The webhook {saved.name!r} was saved.")
            return redirect("ui:webhook", webhook_id=webhook_id)
    return _detail_page(request, actor, webhook, form, status)


@require_POST
@page
def webhook_rotate(request, actor, webhook_id):
    webhook, secret = webhooks.rotate_secret(actor, webhook_id)
    return _secret_page(request, webhook, secret, created=False)


@require_POST
@page
def webhook_test(request, actor, webhook_id):
    try:
        webhooks.send_test(actor, webhook_id)
    except Conflict as waiting:
        messages.warning(request, waiting.message)
    else:
        messages.success(
            request,
            "The test event is queued; the dispatcher sends it within moments, and the delivery "
            "log below shows how it went.",
        )
    return redirect("ui:webhook", webhook_id=webhook_id)


@require_POST
@page
def webhook_delete(request, actor, webhook_id):
    webhook = webhooks.get_webhook(actor, webhook_id, Action.WEBHOOK_MANAGE)
    webhooks.delete_webhook(actor, webhook_id)
    messages.success(request, f"The webhook {webhook.name!r} and its delivery log were deleted.")
    return redirect("ui:webhooks")


@require_POST
@page
def webhook_redeliver(request, actor, webhook_id, delivery_id):
    webhooks.redeliver(actor, webhook_id, delivery_id)
    webhook = webhooks.get_webhook(actor, webhook_id)
    if webhook.backoff_until is not None and webhook.backoff_until > timezone.now():
        when = f"once its backoff ends ({webhook.backoff_until:%Y-%m-%d %H:%M} UTC)"
    else:
        when = "on its next pass"
    messages.success(request, f"The delivery is queued again; the dispatcher sends it {when}.")
    return redirect("ui:webhook", webhook_id=webhook_id)


# --------------------------------------------------------------------------- administration


@require_GET
@page
def admin_webhooks(request, actor):
    found = webhooks.list_all_webhooks(actor).order_by("-consecutive_failures", "name", "id")
    return render(request, "ui/admin/webhooks.html", {"webhooks": paginate(request, found)})


@require_POST
@page
def admin_webhook_disable(request, actor, webhook_id):
    webhook = webhooks.disable_webhook(actor, webhook_id)
    messages.success(
        request,
        f"The webhook {webhook.name!r} of {webhook.owner.username} is disabled; its owner can "
        "enable it again.",
    )
    return back(request, reverse("ui:admin-webhooks"))
