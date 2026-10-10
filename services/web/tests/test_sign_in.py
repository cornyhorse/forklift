"""The sign-in throttle: failed password checks counted per username and per client address,
locks that refuse an attempt before its password is checked, the password-change path, what
admins see and clear, the settings and the sweeper."""

from __future__ import annotations

import logging
import re
import threading
import time
from datetime import timedelta

import pytest
from django.contrib.auth.backends import ModelBackend
from django.core.serializers.json import DjangoJSONEncoder
from django.db import connection
from django.test import Client, override_settings
from django.urls import reverse
from django.utils import timezone
from world import PASSWORD

from forklift_web.core.choices import Role
from forklift_web.core.models import AuditLog, SignInThrottle
from forklift_web.errors import InvalidRequest, NotFound, PermissionDenied, TooManyAttempts
from forklift_web.policy import Actor
from forklift_web.services import installation, retention, sign_in

pytestmark = pytest.mark.django_db

T0 = timezone.now().replace(second=0, microsecond=0)
ATTACKER = Actor.anonymous(ip="203.0.113.7")
ELSEWHERE = Actor.anonymous(ip="198.51.100.1")
SYSTEM = Actor.for_system("test")
NEW_PASSWORD = "a much better passphrase 7"
WRONG = "not-the-password-6f1c"


def at(minutes: float):
    return T0 + timedelta(minutes=minutes)


def counts() -> dict:
    return {(row.kind, row.key): row.failures for row in SignInThrottle.objects.all()}


def fail(actor: Actor, username: str, times: int = 1, *, now=T0) -> None:
    """``times`` wrong passwords that stay below the limits."""
    for _ in range(times):
        sign_in.failed(sign_in.reserve(actor, username, now=now))


def lock(actor: Actor, username: str, *, now=T0) -> TooManyAttempts:
    """Fail until the username limit (5 by default) locks ``username``; returns the refusal."""
    fail(actor, username, 4, now=now)
    with pytest.raises(TooManyAttempts) as refused:
        fail(actor, username, now=now)
    return refused.value


def encoded(value) -> str:
    return DjangoJSONEncoder().default(value)


def configure(**settings) -> None:
    installation.update(SYSTEM, settings)


# --------------------------------------------------------------------------- counting


def test_failures_count_per_normalised_username_and_per_address():
    fail(ATTACKER, "Alice")
    fail(ELSEWHERE, " ALICE ")  # trimmed, in any letter case
    fail(ATTACKER, "Ａｌｉｃｅ")  # full-width letters: NFKC makes them "Alice"
    fail(ATTACKER, "bob")
    assert counts() == {
        ("user", "alice"): 3,
        ("user", "bob"): 1,
        ("ip", "203.0.113.7"): 3,
        ("ip", "198.51.100.1"): 1,
    }
    assert not AuditLog.objects.exists()  # single failures are not audited


def test_counting_starts_again_when_the_window_ends():
    fail(ATTACKER, "alice", 4)
    fail(ATTACKER, "alice", now=at(15))  # sign_in_window_seconds (900) after the first
    row = SignInThrottle.objects.get(kind="user", key="alice")
    assert (row.failures, row.window_started_at, row.locked_until) == (1, at(15), None)
    configure(sign_in_window_seconds=3600)
    fail(ATTACKER, "alice", 3, now=at(70))  # within the longer window
    assert counts()[("user", "alice")] == 4


def test_a_lock_starts_at_the_limit_and_ends_after_the_lock_time():
    fail(ATTACKER, "alice", 3, now=at(0))
    fail(ATTACKER, "alice", now=at(4))  # 4 failures: not locked yet
    fifth = sign_in.reserve(ATTACKER, "alice", now=at(5))
    with pytest.raises(TooManyAttempts) as locking:
        sign_in.failed(fifth)
    assert (locking.value.status, locking.value.code) == (429, "too_many_attempts")
    assert locking.value.retry_at == at(20)  # sign_in_lock_seconds (900) from the 5th
    assert locking.value.message == (
        f"Too many failed sign-in attempts. Try again in 15 minutes, after "
        f"{at(20):%Y-%m-%d %H:%M} UTC."
    )
    # Refused, counting nothing, for that username in any letter case, from any address,
    # until the lock ends
    with pytest.raises(TooManyAttempts, match="Try again in 1 minute, after"):
        sign_in.reserve(ELSEWHERE, "ALICE", now=at(19.5))
    assert counts() == {("user", "alice"): 5, ("ip", "203.0.113.7"): 5, ("ip", "198.51.100.1"): 0}
    fail(ELSEWHERE, "alice", now=at(20))  # counting starts again
    row = SignInThrottle.objects.get(kind="user", key="alice")
    assert (row.failures, row.window_started_at, row.locked_until) == (1, at(20), None)
    entry = AuditLog.objects.get()
    assert (entry.action, entry.actor, entry.object_repr, entry.ip) == (
        "account.sign_in_locked",
        None,
        "user alice",
        "203.0.113.7",
    )
    assert entry.details == {
        "kind": "user",
        "username": "alice",
        "failures": 5,
        "locked_until": encoded(at(20)),
    }


def test_an_address_lock_refuses_every_username_from_that_address():
    configure(sign_in_ip_max_failures=3)
    fail(ATTACKER, "a")
    fail(ATTACKER, "b")
    with pytest.raises(TooManyAttempts):
        fail(ATTACKER, "c")
    with pytest.raises(TooManyAttempts):
        sign_in.reserve(ATTACKER, "someone else", now=T0)
    sign_in.reserve(ELSEWHERE, "a", now=T0)  # the usernames are not locked
    sign_in.reserve(ATTACKER, "a", by_address=False, now=T0)  # password changes
    entry = AuditLog.objects.get(action="account.sign_in_locked")
    assert (entry.object_repr, entry.details["kind"], entry.details["username"]) == (
        "ip 203.0.113.7",
        "ip",
        "c",
    )


def test_addresses_are_counted_by_ipv4_or_the_ipv6_64():
    fail(Actor.anonymous(ip="2001:db8:1:2::5"), "a")
    fail(Actor.anonymous(ip="2001:db8:1:2:ffff::9"), "b")  # the same /64
    fail(Actor.anonymous(ip="::ffff:192.0.2.1"), "c")  # IPv4-mapped
    fail(Actor.anonymous(), "d")  # no address known: only the username counts
    assert {key: n for (kind, key), n in counts().items() if kind == "ip"} == {
        "2001:db8:1:2::/64": 2,
        "192.0.2.1": 1,
    }
    assert counts()[("user", "d")] == 1
    assert sign_in.address_key("unix-socket") == "unix-socket"


@pytest.mark.parametrize(
    "address,key",
    [
        ("203.0.113.5:5555", "203.0.113.5"),
        ("[2001:db8:1:2::5]:443", "2001:db8:1:2::/64"),
        ("[2001:db8:1:2::5]", "2001:db8:1:2::/64"),
        ("2001:db8:1:2::5", "2001:db8:1:2::/64"),
        ("[::ffff:192.0.2.1]:80", "192.0.2.1"),
        ("proxy.example:80", "proxy.example:80"),
        ("[not-an-address]:80", "[not-an-address]:80"),
    ],
)
def test_address_keys_ignore_the_port_and_brackets_a_proxy_adds(address, key):
    assert sign_in.address_key(address) == key


def test_a_correct_password_clears_the_username_but_not_the_address():
    fail(ATTACKER, "alice", 3)
    sign_in.succeeded(sign_in.reserve(ATTACKER, "ALICE", now=T0))
    assert counts() == {("ip", "203.0.113.7"): 3}  # its own attempt given back


def test_a_correct_password_ends_its_own_lock_but_never_another_attempts():
    fail(ATTACKER, "alice", 4)
    sign_in.succeeded(sign_in.reserve(ATTACKER, "alice", now=T0))  # the 5th, right
    assert counts() == {("ip", "203.0.113.7"): 4}  # no lock is left
    configure(sign_in_ip_max_failures=6)
    fail(ATTACKER, "bob")
    sign_in.succeeded(sign_in.reserve(ATTACKER, "carol", now=T0))  # the address's 6th, right
    assert not SignInThrottle.objects.exclude(locked_until=None).exists()
    assert counts()[("ip", "203.0.113.7")] == 5

    held = sign_in.reserve(ELSEWHERE, "erin", now=T0)  # still being checked while...
    others = Actor.anonymous(ip="192.0.2.99")
    fail(others, "erin", 3)
    with pytest.raises(TooManyAttempts):
        fail(others, "erin")  # ...other attempts reach the limit
    sign_in.succeeded(held)
    with pytest.raises(TooManyAttempts):
        sign_in.reserve(ELSEWHERE, "erin", now=T0)
    assert counts()[("user", "erin")] == 5
    assert counts()[("ip", "198.51.100.1")] == 0  # its address attempt was given back
    assert AuditLog.objects.filter(action="account.sign_in_locked").count() == 1


@pytest.mark.django_db(transaction=True)
def test_concurrent_failures_are_all_counted():
    """Two attempts failing at the same moment count twice, for a new counter (both insert)
    and for an existing one (both update)."""
    configure(sign_in_max_failures=100, sign_in_ip_max_failures=100)
    errors = []

    def attempt(barrier):
        try:
            barrier.wait()
            fail(ATTACKER, "alice", now=timezone.now())
        except Exception as error:  # reported below, with the thread's error
            errors.append(error)
        finally:
            connection.close()

    for _ in range(5):
        barrier = threading.Barrier(2)
        threads = [threading.Thread(target=attempt, args=(barrier,)) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=30)
    assert errors == []
    assert counts() == {("user", "alice"): 10, ("ip", "203.0.113.7"): 10}


# --------------------------------------------------------------------------- settings


def test_the_limits_are_installation_settings(admin_actor):
    described = installation.describe(admin_actor)
    assert {key: described[key]["value"] for key in described if key.startswith("sign_in")} == {
        "sign_in_max_failures": 5,
        "sign_in_ip_max_failures": 30,
        "sign_in_window_seconds": 900,
        "sign_in_lock_seconds": 900,
    }
    configure(sign_in_max_failures=2, sign_in_lock_seconds=60)
    fail(ATTACKER, "alice")
    with pytest.raises(TooManyAttempts) as refused:
        fail(ATTACKER, "alice")
    assert refused.value.retry_at == at(1)


@pytest.mark.parametrize(
    "changes,message",
    [
        ({"sign_in_max_failures": 0}, "at least 1 and at most 1000"),
        ({"sign_in_max_failures": "5"}, "must be an integer"),
        ({"sign_in_ip_max_failures": 100_001}, "at least 1 and at most 100000"),
        ({"sign_in_window_seconds": 59}, "at least 60 and at most 86400"),
        ({"sign_in_lock_seconds": 86_401}, "at least 60 and at most 86400"),
    ],
)
def test_sign_in_settings_are_validated(admin_actor, changes, message):
    with pytest.raises(InvalidRequest, match=message):
        installation.update(admin_actor, changes)


# --------------------------------------------------------------------------- the sign-in page


def post_sign_in(client: Client, username: str, password: str, **extra):
    return client.post(
        reverse("forklift-login"), {"username": username, "password": password}, **extra
    )


def alert(response) -> str:
    found = re.search(r'role="alert">\s*<p>(.*?)</p>', response.content.decode(), re.S)
    return found.group(1) if found else ""


def test_a_locked_username_is_refused_with_429_before_the_password_is_checked(make_user, caplog):
    caplog.set_level(logging.INFO, logger="forklift_web.services.sign_in")
    user = make_user(Role.VIEWER)
    client = Client()
    for _ in range(4):
        wrong = post_sign_in(client, user.username, WRONG)
        assert wrong.status_code == 200 and "did not match" in alert(wrong)
    locking = post_sign_in(client, user.username, WRONG)
    assert locking.status_code == 429
    assert alert(locking).startswith("Too many failed sign-in attempts. Try again in 15 minutes")
    assert 890 <= int(locking["Retry-After"]) <= 900
    checked = AuditLog.objects.filter(action="user.login_failed").count()
    assert checked == 5  # Django's own audit of each checked password

    # The right password is refused just like a wrong one: a lock is no password oracle.
    right = post_sign_in(client, user.username, PASSWORD)
    again = post_sign_in(client, user.username, WRONG)
    assert (right.status_code, alert(right)) == (again.status_code, alert(again))
    assert right.status_code == 429 and right.has_header("Retry-After")
    assert AuditLog.objects.filter(action="user.login_failed").count() == checked  # unchecked
    assert client.get("/").status_code == 302  # not signed in
    user.refresh_from_db()
    assert user.last_login is None
    assert AuditLog.objects.filter(action="account.sign_in_locked").count() == 1

    # Logged without the password
    messages = [record.getMessage() for record in caplog.records]
    assert messages.count("Sign-in failed") == 5
    assert messages.count("Sign-in refused while locked") == 2
    assert "Sign-in locked" in messages
    assert all(WRONG not in str(vars(record)) for record in caplog.records)
    assert all(PASSWORD not in str(vars(record)) for record in caplog.records)
    failed = next(record for record in caplog.records if record.getMessage() == "Sign-in failed")
    assert (failed.levelname, failed.username, failed.ip) == ("INFO", user.username, "127.0.0.1")


def test_unknown_and_known_usernames_get_the_same_responses(make_user):
    make_user(Role.VIEWER, username="known-user")
    seen = {}
    for number, username in enumerate(("known-user", "nobody-has-this-name")):
        client = Client(REMOTE_ADDR=f"192.0.2.{number + 1}")
        responses = [post_sign_in(client, username, WRONG) for _ in range(6)]
        # The numbers in a refusal are times, which may differ by a minute between the two
        seen[username] = [
            (r.status_code, re.sub(r"\d", "#", alert(r)), r.has_header("Retry-After"))
            for r in responses
        ]
    assert seen["known-user"] == seen["nobody-has-this-name"]
    assert [status for status, _, _ in seen["known-user"]] == [200] * 4 + [429] * 2


def test_a_successful_sign_in_resets_the_count_and_a_lock_ends(make_user):
    user = make_user(Role.VIEWER)
    client = Client()
    for _ in range(4):
        post_sign_in(client, user.username, WRONG)
    assert post_sign_in(client, user.username, PASSWORD).status_code == 302
    client.post(reverse("forklift-logout"))
    for _ in range(4):
        assert post_sign_in(client, user.username, WRONG).status_code == 200
    assert post_sign_in(client, user.username, WRONG).status_code == 429
    SignInThrottle.objects.update(locked_until=timezone.now() - timedelta(seconds=1))
    assert post_sign_in(client, user.username, PASSWORD).status_code == 302


def test_a_form_without_a_password_checks_nothing(make_user):
    user = make_user(Role.VIEWER)
    response = post_sign_in(Client(), user.username, "")
    assert response.status_code == 200 and "did not match" in alert(response)
    assert not SignInThrottle.objects.exists()


@override_settings(FORKLIFT_TRUSTED_PROXIES=1)
def test_an_address_lock_uses_the_client_address_behind_a_proxy(make_user):
    configure(sign_in_ip_max_failures=3)
    user = make_user(Role.VIEWER)
    proxy = Client(REMOTE_ADDR="10.0.0.2")
    guesser = {"HTTP_X_FORWARDED_FOR": "203.0.113.50"}
    assert post_sign_in(proxy, "a", WRONG, **guesser).status_code == 200
    assert post_sign_in(proxy, "b", WRONG, **guesser).status_code == 200
    assert post_sign_in(proxy, "c", WRONG, **guesser).status_code == 429
    assert post_sign_in(proxy, user.username, PASSWORD, **guesser).status_code == 429
    other = {"HTTP_X_FORWARDED_FOR": "203.0.113.51"}
    assert post_sign_in(proxy, user.username, PASSWORD, **other).status_code == 302
    assert counts()[("ip", "203.0.113.50")] == 3


# --------------------------------------------------------------------------- parallel attempts

CHECK_SECONDS = 0.3  # about what PBKDF2 takes, so that attempts overlap


@pytest.fixture
def slow_checks(monkeypatch):
    """Every password check takes CHECK_SECONDS and is counted (the usernames it checked)."""
    checked = []
    original = ModelBackend.authenticate

    def check(self, request, username=None, password=None, **kwargs):
        checked.append(username)
        time.sleep(CHECK_SECONDS)
        return original(self, request, username=username, password=password, **kwargs)

    monkeypatch.setattr(ModelBackend, "authenticate", check)
    return checked


def in_parallel(calls) -> list:
    """Start the calls at the same moment, each in its own thread; their results, in order."""
    barrier = threading.Barrier(len(calls))
    results = [None] * len(calls)

    def run(index, call):
        try:
            barrier.wait()
            results[index] = call()
        except Exception as error:  # reported by the caller's assertions
            results[index] = error
        finally:
            connection.close()

    threads = [threading.Thread(target=run, args=item) for item in enumerate(calls)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=60)
    return results


@pytest.mark.django_db(transaction=True)
@pytest.mark.parametrize("burst", ["one username", "one address"])
def test_a_parallel_burst_gets_no_more_password_checks_than_the_limit(
    make_user, slow_checks, burst
):
    configure(sign_in_ip_max_failures=5 if burst == "one address" else 30)
    user = make_user(Role.VIEWER)
    if burst == "one username":
        names = [user.username] * 16
    else:
        names = [f"guess-{n}" for n in range(16)]
    calls = [lambda name=name: post_sign_in(Client(), name, WRONG) for name in names]
    responses = in_parallel(calls)
    assert len(slow_checks) == 5  # sign_in_max_failures / the lowered address limit
    assert sorted(response.status_code for response in responses) == [200] * 4 + [429] * 12
    assert AuditLog.objects.filter(action="account.sign_in_locked").count() == 1


@pytest.mark.django_db(transaction=True)
def test_a_success_that_ends_after_a_lock_started_keeps_the_lock(make_user, monkeypatch):
    user = make_user(Role.VIEWER)
    checking, release = threading.Event(), threading.Event()
    original = ModelBackend.authenticate

    def check(self, request, username=None, password=None, **kwargs):
        if password == PASSWORD and not release.is_set():
            checking.set()
            release.wait(30)  # the right password is still being checked...
        return original(self, request, username=username, password=password, **kwargs)

    monkeypatch.setattr(ModelBackend, "authenticate", check)
    right = []

    def sign_in_right():
        try:
            right.append(post_sign_in(Client(REMOTE_ADDR="192.0.2.80"), user.username, PASSWORD))
        finally:
            connection.close()

    thread = threading.Thread(target=sign_in_right)
    thread.start()
    assert checking.wait(30)
    # ...while wrong passwords reach the limit
    statuses = [post_sign_in(Client(), user.username, WRONG).status_code for _ in range(5)]
    release.set()
    thread.join(timeout=30)
    assert right[0].status_code == 302  # it was checked within the limit
    assert post_sign_in(Client(), user.username, PASSWORD).status_code == 429  # still locked
    assert statuses == [200, 200, 200, 429, 429]  # the right one held the first attempt


@override_settings(FORKLIFT_TRUSTED_PROXIES=1)
def test_an_address_counts_whatever_port_the_proxy_writes():
    configure(sign_in_ip_max_failures=3)
    proxy = Client(REMOTE_ADDR="10.0.0.2")
    for port, expected in ((5001, 200), (5002, 200), (5003, 429)):
        forwarded = {"HTTP_X_FORWARDED_FOR": f"203.0.113.50:{port}"}
        assert post_sign_in(proxy, f"guess-{port}", WRONG, **forwarded).status_code == expected
    assert counts()[("ip", "203.0.113.50")] == 3
    assert set(AuditLog.objects.exclude(ip=None).values_list("ip", flat=True)) == {"203.0.113.50"}


# --------------------------------------------------------------------------- password change


def change(client: Client, old: str, new: str = NEW_PASSWORD):
    return client.post(
        reverse("ui:password"),
        {"old_password": old, "new_password1": new, "new_password2": new},
    )


def test_wrong_old_passwords_count_against_the_username(make_user):
    user = make_user(Role.VIEWER)
    client = Client()
    client.force_login(user)
    for _ in range(4):
        assert change(client, WRONG).status_code == 200
    locked = change(client, WRONG)
    assert locked.status_code == 429 and locked.has_header("Retry-After")
    assert b"Too many failed sign-in attempts. Try again in 15 minutes" in locked.content
    assert change(client, PASSWORD).status_code == 429  # refused before it is checked
    user.refresh_from_db()
    assert user.check_password(PASSWORD)  # unchanged
    assert post_sign_in(Client(), user.username, PASSWORD).status_code == 429  # the same limit
    assert client.get("/").status_code == 200  # the session stays
    entry = AuditLog.objects.get(action="account.sign_in_locked")
    assert (entry.actor, entry.details["kind"]) == (user, "user")
    # The address is not counted (and the refused sign-in counted nothing)
    assert counts() == {("user", user.username): 5, ("ip", "127.0.0.1"): 0}


def test_a_correct_old_password_clears_the_count(make_user):
    user = make_user(Role.VIEWER)
    client = Client()
    client.force_login(user)
    for _ in range(4):
        change(client, WRONG)
    weak = change(client, PASSWORD, new="12345678")  # the old one was right
    assert weak.status_code == 200 and b"errorlist" in weak.content
    assert counts() == {}
    assert change(client, PASSWORD).status_code == 302


# --------------------------------------------------------------------------- admins


def test_an_admin_sees_a_lock_on_the_user_page_and_unlocks_it(make_user, admin):
    user = make_user(Role.VIEWER)
    lock(ATTACKER, user.username.upper(), now=timezone.now())
    client = Client()
    client.force_login(admin)
    overview = client.get(reverse("ui:admin")).content.decode()
    assert "<strong>1</strong> <a" in overview and "sign-in lock</a> active" in overview
    page_url = reverse("ui:admin-user", kwargs={"user_id": user.pk})
    page = client.get(page_url).content.decode()
    assert "Signing in is locked" in page and "5 wrong passwords for this username" in page
    unlock_url = reverse("ui:admin-user-unlock", kwargs={"user_id": user.pk})
    assert f'action="{unlock_url}"' in page

    unlocked = client.post(unlock_url, follow=True)
    assert unlocked.redirect_chain == [(page_url, 302)]
    assert f"{user.username} can sign in again." in unlocked.content.decode()
    assert "Signing in is locked" not in unlocked.content.decode()
    entry = AuditLog.objects.get(action="account.sign_in_unlocked")
    assert (entry.actor, entry.object_repr, entry.details["kind"], entry.details["failures"]) == (
        admin,
        f"user {user.username}",
        "user",
        5,
    )
    assert post_sign_in(Client(), user.username, PASSWORD).status_code == 302
    again = client.post(unlock_url, follow=True).content.decode()
    assert f"Signing in as {user.username} was not locked." in again
    assert AuditLog.objects.filter(action="account.sign_in_unlocked").count() == 1
    overview = client.get(reverse("ui:admin")).content.decode()
    assert "<strong>0</strong> sign-in locks active" in overview


def test_the_admin_api_lists_and_clears_locks(as_user, admin, make_user):
    now = timezone.now()
    lock(ATTACKER, "alice", now=now)
    fail(ELSEWHERE, "bob", now=now)  # counted, not locked
    caller = as_user(admin)
    listed = caller.get("/api/v1/admin/sign-in-locks").json()
    assert [(i["kind"], i["key"], i["failures"]) for i in listed["items"]] == [
        ("user", "alice", 5)
    ]
    item = listed["items"][0]
    until = now + timedelta(minutes=15)
    assert item["locked_until"].startswith(until.strftime("%Y-%m-%dT%H:%M"))
    cleared = caller.delete(f"/api/v1/admin/sign-in-locks/{item['id']}")
    assert cleared.status_code == 204
    assert caller.get("/api/v1/admin/sign-in-locks").json()["items"] == []
    gone = caller.delete(f"/api/v1/admin/sign-in-locks/{item['id']}")
    assert (gone.status_code, gone.json()["code"]) == (404, "not_found")
    entry = AuditLog.objects.get(action="account.sign_in_unlocked")
    assert (entry.actor, entry.object_repr) == (admin, "user alice")
    sign_in.reserve(ATTACKER, "alice")  # signing in works again
    viewer = Actor.for_user(make_user(Role.VIEWER))
    with pytest.raises(PermissionDenied):
        sign_in.active_locks(viewer)
    with pytest.raises(PermissionDenied):
        sign_in.unlock_user(Actor.for_user(make_user(Role.AUTHOR)), admin)
    with pytest.raises(NotFound, match="no sign-in lock with id 0"):
        sign_in.clear(Actor.for_user(admin), 0)


# --------------------------------------------------------------------------- the sweeper


def test_the_sweeper_drops_counters_whose_window_and_lock_have_ended():
    now = timezone.now()
    for key, started, until in (
        ("window-ended", 16, None),
        ("lock-ended", 40, -5),
        ("counting", 5, None),
        ("locked", 20, 5),
    ):
        SignInThrottle.objects.create(
            kind="user",
            key=key,
            failures=2,
            window_started_at=now - timedelta(minutes=started),
            locked_until=None if until is None else now + timedelta(minutes=until),
        )
    assert retention.sweep(SYSTEM, dry_run=True).sign_in_throttles == 2
    assert SignInThrottle.objects.count() == 4
    assert retention.sweep(SYSTEM).sign_in_throttles == 2
    assert set(SignInThrottle.objects.values_list("key", flat=True)) == {"counting", "locked"}
    assert not AuditLog.objects.exists()  # housekeeping, not a retention deletion
