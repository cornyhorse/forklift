"""The webhook pages: the account list and its form, the secret shown once, a webhook's page
(settings, test, rotation, deletion, the delivery log, redelivery) and the admin list."""

from __future__ import annotations

from datetime import timedelta

import pytest
from django.urls import reverse
from django.utils import timezone
from webhook_support import Receiver, make_webhook
from world import World, make_job

from forklift_web.core.choices import WebhookScope
from forklift_web.core.models import AuditLog, Webhook, WebhookDelivery
from forklift_web.services import webhooks

pytestmark = pytest.mark.django_db


@pytest.fixture
def world():
    return World.build()


def signed_in(client, user):
    client.force_login(user)
    return client


def form(**fields) -> dict:
    return {
        "name": "ci",
        "url": "https://hooks.example.org/in?key=private",
        "events": ["job.failed", "job.succeeded"],
        "scope": "own_jobs",
        "dataset": "",
        "kinds": ["run"],
        **fields,
    }


def test_the_account_area_links_to_webhooks(world, client):
    page = signed_in(client, world.viewer).get(reverse("ui:webhooks")).content.decode()
    assert f'<a href="{reverse("ui:webhooks")}" aria-current="page">Webhooks</a>' in page
    assert "You have no webhooks yet." in page
    assert 'value="all_jobs"' not in page  # every job: admins only
    admin_page = signed_in(client, world.admin).get(reverse("ui:webhooks")).content.decode()
    assert 'value="all_jobs"' in admin_page
    assert f'href="{reverse("ui:admin-webhooks")}"' in (
        client.get(reverse("ui:admin")).content.decode()
    )


def test_creating_a_webhook_shows_its_secret_once(world, client):
    signed_in(client, world.viewer)
    response = client.post(
        reverse("ui:webhooks"), form(scope="dataset", dataset=str(world.dataset.pk))
    )
    assert response.status_code == 200 and "no-store" in response["Cache-Control"]
    webhook = Webhook.objects.get()
    page = response.content.decode()
    secret = page.split('id="secret-value" aria-labelledby="secret-label">')[1].split("<")[0]
    assert secret.startswith("fkwh_") and webhook.secret_prefix == secret[:12]
    assert "only time it is shown" in page and webhook.dataset == world.dataset
    for url in (reverse("ui:webhooks"), reverse("ui:webhook", args=[webhook.pk])):
        assert secret not in client.get(url).content.decode()
    assert (
        f"{webhook.secret_prefix}…"
        in client.get(reverse("ui:webhook", args=[webhook.pk])).content.decode()
    )


def test_the_form_shows_what_was_wrong(world, client):
    signed_in(client, world.viewer)
    response = client.post(reverse("ui:webhooks"), form(url="http://hooks.example.org/"))
    assert response.status_code == 400
    assert "must start with https://" in response.content.decode()
    assert client.post(reverse("ui:webhooks"), form(events=[])).status_code == 400
    assert not Webhook.objects.exists()


def test_a_webhooks_page_and_its_delivery_log(world, client):
    webhook = world.webhook()
    signed_in(client, world.operator)
    page = client.get(reverse("ui:webhook", args=[webhook.pk])).content.decode()
    assert "Failed attempts in a row" in page and 'class="badge failed"' in page
    redeliver = reverse("ui:webhook-redeliver", args=[webhook.pk, webhook.deliveries.get().pk])
    assert redeliver in page
    Webhook.objects.filter(pk=webhook.pk).update(active=False, disabled_reason="failures")
    page = client.get(reverse("ui:webhook", args=[webhook.pk])).content.decode()
    assert "Disabled after repeated failed deliveries" in page and redeliver not in page
    empty = make_webhook(world.operator, name="quiet")
    page = client.get(reverse("ui:webhook", args=[empty.pk])).content.decode()
    assert "Nothing was sent yet." in page


def test_saving_a_webhooks_settings(world, client):
    webhook = world.webhook()
    signed_in(client, world.operator)
    url = reverse("ui:webhook", args=[webhook.pk])
    response = client.post(url, form(name="alerts", active="on"), follow=True)
    assert "The webhook &#x27;alerts&#x27; was saved." in response.content.decode()
    off = client.post(url, form(name="alerts"))  # "active" left unticked
    assert off.status_code == 302 and Webhook.objects.get(pk=webhook.pk).disabled_reason == "owner"
    bad = client.post(url, form(url="https://127.0.0.1/"))
    assert bad.status_code == 400 and "not a publicly routable" in bad.content.decode()
    incomplete = client.post(url, form(events=[]))
    assert incomplete.status_code == 400 and "Please correct the fields" in (
        incomplete.content.decode()
    )
    other = signed_in(client, world.author).post(url, form())
    assert other.status_code == 403


def test_a_test_event_is_queued_and_the_log_follows_it(world, client, settings):
    receiver = Receiver(200)
    settings.FORKLIFT_WEBHOOK_ALLOW_HTTP = True
    settings.FORKLIFT_WEBHOOK_ALLOWED_HOSTS = ["127.0.0.1"]
    try:
        webhook = make_webhook(world.operator, url=f"http://127.0.0.1:{receiver.port}/")
        signed_in(client, world.operator)
        test = reverse("ui:webhook-test", args=[webhook.pk])
        log = reverse("ui:webhook-deliveries", args=[webhook.pk])
        page = client.post(test, follow=True).content.decode()
        assert "The test event is queued" in page and receiver.requests == []
        assert f'hx-get="{log}?testing=1" hx-trigger="every 2s"' in page  # it polls
        again = client.post(test, follow=True).content.decode()
        assert "is still waiting for the dispatcher" in again
        waiting = client.get(log + "?testing=1").content.decode()
        assert 'hx-trigger="every 2s"' in waiting and "queued, the next pass" in waiting
        assert webhooks.deliver_due()["delivered"] == 1
        done = client.get(log + "?testing=1").content.decode()
        assert "hx-trigger" not in done  # polling stops
        assert (
            'hx-swap-oob="innerHTML">The test event was delivered: the receiver answered 200.'
            in (done)
        )
        assert "hx-swap-oob" not in client.get(log).content.decode()  # announced once
    finally:
        receiver.stop()
    client.post(test)
    webhooks.deliver_due()
    failed = client.get(log + "?testing=1").content.decode()
    assert "The test event was not delivered." in failed and "refused the connection" in failed


def test_a_webhook_that_backs_off_says_so(world, client):
    webhook = world.webhook()
    later = timezone.now() + timedelta(hours=2)
    Webhook.objects.filter(pk=webhook.pk).update(consecutive_failures=4, backoff_until=later)
    job = make_job(world.operator, world.upload)
    WebhookDelivery.objects.create(
        webhook=webhook, job=job, event="job.failed", payload="{}", next_attempt_at=job.created_at
    )
    signed_in(client, world.operator)
    page = client.get(reverse("ui:webhook", args=[webhook.pk])).content.decode()
    assert "nothing is sent before" in page and "(backing off)" in page
    delivery = webhook.deliveries.get(status="failed")
    page = client.post(
        reverse("ui:webhook-redeliver", args=[webhook.pk, delivery.pk]), follow=True
    ).content.decode()
    assert "the dispatcher sends it once its backoff ends" in page


def test_rotating_deleting_and_redelivering(world, client):
    webhook = world.webhook()
    signed_in(client, world.operator)
    rotated = client.post(reverse("ui:webhook-rotate", args=[webhook.pk]))
    assert "A new signing secret for hook" in rotated.content.decode()
    assert rotated["Cache-Control"] == "no-store"
    delivery = webhook.deliveries.get()
    page = client.post(
        reverse("ui:webhook-redeliver", args=[webhook.pk, delivery.pk]), follow=True
    ).content.decode()
    assert "The delivery is queued again; the dispatcher sends it on its next pass." in page
    assert WebhookDelivery.objects.get(pk=delivery.pk).status == "pending"
    page = client.post(reverse("ui:webhook-delete", args=[webhook.pk]), follow=True)
    assert "and its delivery log were deleted" in page.content.decode()
    assert not Webhook.objects.filter(pk=webhook.pk).exists()


def test_the_admin_list_and_disabling(world, client):
    failing = make_webhook(
        world.viewer,
        name="flaky",
        url="https://hooks.example.org/in?key=private",
        consecutive_failures=7,
    )
    make_webhook(world.author, name="calm", scope=WebhookScope.DATASET, dataset=world.dataset)
    signed_in(client, world.admin)
    page = client.get(reverse("ui:admin-webhooks")).content.decode()
    assert page.index("flaky") < page.index("calm")  # the most failing first
    assert "https://hooks.example.org/in" in page and "key=private" not in page
    assert 'aria-current="page">Webhooks</a>' in page
    response = client.post(reverse("ui:admin-webhook-disable", args=[failing.pk]), follow=True)
    assert "is disabled; its owner can enable it again." in response.content.decode()
    assert Webhook.objects.get(pk=failing.pk).disabled_reason == "admin"
    assert AuditLog.objects.filter(action="admin.webhook.disable").count() == 1
    again = client.post(reverse("ui:admin-webhook-disable", args=[failing.pk]))
    assert again.status_code == 409
