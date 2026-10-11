"""Sign-in throttling: wrong passwords counted per username and per client address.

Every password check of the UI (the sign-in form, and the old password of the password-change
form) first takes an attempt with :func:`reserve`, then reports the outcome with
:func:`failed` or :func:`succeeded`. Attempts are counted per normalised username, whether or
not an account has it, so the throttle tells nobody which usernames exist; sign-ins are also
counted per client address (``request.client_ip``, which honours FORKLIFT_TRUSTED_PROXIES). The
attempt that reaches ``sign_in_max_failures`` or ``sign_in_ip_max_failures`` within
``sign_in_window_seconds`` (from the first one) locks that username or address for
``sign_in_lock_seconds``. Because the attempt is counted before the slow password check, a burst
of parallel attempts gets no more checks than the limit; a refused attempt is not checked at all,
so a lock cannot be used to test passwords. A correct password gives its attempt back (an address
counts only failures) and clears its username's count, but never a lock another attempt started.

The counters are database rows that every gateway process and replica shares, changed under row
locks. Locks and unlocks are audited; this module only logs single failures (never with the
password), and refused attempts are not audited at all (core.signals audits each password Django
checked). API tokens are random 256-bit values and need no throttle.
"""

from __future__ import annotations

import ipaddress
import logging
import math
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timedelta
from datetime import timezone as dt_timezone
from typing import Optional

from django.db import transaction
from django.db.models import F, Q
from django.utils import timezone

from forklift_web.core.choices import SignInThrottleKind
from forklift_web.core.models import SignInThrottle, User
from forklift_web.errors import NotFound, TooManyAttempts
from forklift_web.middleware import parse_address
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit, installation

logger = logging.getLogger(__name__)

USER, IP = SignInThrottleKind.USER, SignInThrottleKind.IP
KEY_LENGTH = 150


def username_key(username: str) -> str:
    """The key a username is counted by: NFKC-normalised, trimmed, in any letter case."""
    return unicodedata.normalize("NFKC", username).strip().casefold()[:KEY_LENGTH]


def address_key(ip: str) -> str:
    """The key a client address is counted by, without a port: an IPv6 address by its /64
    network (what one subscriber usually gets), an IPv4-mapped one as IPv4, anything that is
    no address as it is."""
    address = parse_address(ip)
    if address is None:
        return ip[:KEY_LENGTH]
    if address.version == 6:
        if address.ipv4_mapped is not None:
            return str(address.ipv4_mapped)
        return str(ipaddress.ip_network(f"{address}/64", strict=False))
    return str(address)


def _counters(actor: Actor, username: str, by_address: bool) -> list:
    """(kind, key, the setting with its limit) of each counter an attempt counts against."""
    found = [(USER, username_key(username), "sign_in_max_failures")]
    if by_address and actor.ip:
        found.append((IP, address_key(actor.ip), "sign_in_ip_max_failures"))
    return found


def _refusal(until: datetime, now: datetime) -> TooManyAttempts:
    """The same message for every lock: it does not say whether the username or the address
    is locked, nor whether the account exists."""
    minutes = math.ceil((until - now).total_seconds() / 60)
    after = datetime.fromtimestamp(math.ceil(until.timestamp() / 60) * 60, tz=dt_timezone.utc)
    wait = "1 minute" if minutes == 1 else f"{minutes} minutes"
    return TooManyAttempts(
        f"Too many failed sign-in attempts. Try again in {wait}, after "
        f"{after:%Y-%m-%d %H:%M} UTC.",
        retry_at=until,
    )


# --------------------------------------------------------------------------- password checks


@dataclass(frozen=True)
class Reservation:
    """The attempt a password check took: each counter as the reservation left it, and whether
    the reservation started that counter's lock."""

    actor: Actor
    username: str
    now: datetime
    held: tuple  # ((SignInThrottle, started_lock), ...)


def _counter(kind: str, key: str, now, window: timedelta) -> SignInThrottle:
    """A counter, row-locked until the transaction ends. One whose lock or window is over
    starts again, so afterwards ``locked_until`` is set only while it is locked."""
    row, _ = SignInThrottle.objects.select_for_update().get_or_create(
        kind=kind, key=key, defaults={"window_started_at": now, "updated_at": now}
    )
    lock_over = row.locked_until is not None and row.locked_until <= now
    if lock_over or (row.locked_until is None and row.window_started_at + window <= now):
        row.failures, row.window_started_at, row.locked_until = 0, now, None
    return row


def _take(row: SignInThrottle, limit: int, now, lock: timedelta) -> tuple:
    """Count one attempt on a row-locked counter; returns (row, whether it started the lock)."""
    row.failures += 1
    started = row.failures >= limit
    if started:
        row.locked_until = now + lock
    row.updated_at = now
    row.save()
    return row, started


def reserve(actor: Actor, username: str, *, by_address: bool = True, now=None) -> Reservation:
    """Take one attempt for ``username`` (and, with ``by_address``, the actor's address) before
    the password is checked. Raises TooManyAttempts, counting nothing, while either is locked;
    the attempt that reaches a limit starts the lock, so later ones are refused unchecked."""
    now = now or timezone.now()
    rules = installation.current()
    window = timedelta(seconds=rules["sign_in_window_seconds"])
    lock = timedelta(seconds=rules["sign_in_lock_seconds"])
    with transaction.atomic():  # the username's row first, then the address's: no deadlocks
        rows = [
            (_counter(kind, key, now, window), rules[limit])
            for kind, key, limit in _counters(actor, username, by_address)
        ]
        locks = [row.locked_until for row, _ in rows if row.locked_until is not None]
        held = () if locks else tuple(_take(row, limit, now, lock) for row, limit in rows)
    if locks:
        logger.info(
            "Sign-in refused while locked",
            extra={"username": username_key(username), "ip": actor.ip},
        )
        raise _refusal(max(locks), now)
    return Reservation(actor, username, now, held)


def failed(reservation: Reservation) -> None:
    """The password was wrong: the attempt stays counted. Raises TooManyAttempts when this
    attempt started a lock, which is audited now (a right password would have given it back)."""
    username = username_key(reservation.username)
    started = [row for row, starts in reservation.held if starts]
    for row in started:
        audit.record(
            reservation.actor,
            "account.sign_in_locked",
            row,
            {
                "kind": row.kind,
                "username": username,
                "failures": row.failures,
                "locked_until": row.locked_until,
            },
        )
        logger.warning(
            "Sign-in locked",
            extra={"kind": row.kind, "key": row.key, "locked_until": row.locked_until.isoformat()},
        )
    logger.info(
        "Sign-in failed",
        extra={
            "username": username,
            "ip": reservation.actor.ip,
            "failures": {row.kind: row.failures for row, _ in reservation.held},
        },
    )
    if started:
        raise _refusal(max(row.locked_until for row in started), reservation.now)


def succeeded(reservation: Reservation) -> None:
    """The password was right: clear the username's count and give the address its attempt
    back, each only while no other attempt has locked it since (this attempt's own lock goes)."""
    for row, _ in reservation.held:
        # locked_until as reserved: None, or the lock this attempt started
        mine = SignInThrottle.objects.filter(
            Q(locked_until=None) | Q(locked_until=row.locked_until), pk=row.pk
        )
        if row.kind == USER:
            mine.delete()
        else:
            mine.filter(window_started_at=row.window_started_at).update(
                failures=F("failures") - 1, locked_until=None
            )


# --------------------------------------------------------------------------- admin


def active_locks(actor: Actor, *, now=None):
    """The counters that refuse sign-ins now (admins)."""
    check(actor, Action.USER_VIEW)
    return SignInThrottle.objects.filter(locked_until__gt=now or timezone.now())


def user_lock(actor: Actor, user: User, *, now=None) -> Optional[SignInThrottle]:
    """The active lock on ``user``'s username, if there is one."""
    return active_locks(actor, now=now).filter(kind=USER, key=username_key(user.username)).first()


def _unlock(actor: Actor, row: SignInThrottle) -> None:
    with transaction.atomic():
        audit.record(
            actor,
            "account.sign_in_unlocked",
            row,
            {"kind": row.kind, "failures": row.failures, "locked_until": row.locked_until},
        )
        row.delete()


def clear(actor: Actor, lock_id: int) -> SignInThrottle:
    """Remove a counter with its lock, so that signing in works again at once (admins)."""
    check(actor, Action.USER_MANAGE)
    row = SignInThrottle.objects.filter(pk=lock_id).first()
    if row is None:
        raise NotFound(
            f"There is no sign-in lock with id {lock_id}; it may have ended and been swept."
        )
    _unlock(actor, row)
    return row


def unlock_user(actor: Actor, user: User) -> Optional[SignInThrottle]:
    """Clear the failures and any lock of ``user``'s username; None when there were none."""
    check(actor, Action.USER_MANAGE)
    row = SignInThrottle.objects.filter(kind=USER, key=username_key(user.username)).first()
    if row is not None:
        _unlock(actor, row)
    return row


def sweep(*, dry_run: bool = False, now=None) -> int:
    """Delete the counters whose window and lock have both ended (the retention sweeper's
    housekeeping, not audited); returns how many there were."""
    now = now or timezone.now()
    window = timedelta(seconds=installation.get("sign_in_window_seconds"))
    ended = SignInThrottle.objects.filter(
        Q(locked_until=None) | Q(locked_until__lte=now), window_started_at__lte=now - window
    )
    if dry_run:
        return ended.count()
    return ended.delete()[0]
