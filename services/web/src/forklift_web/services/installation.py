"""Installation settings: values admins change at run time, stored in the database.

Each setting has a default here; ``current()`` returns every value (stored or default).
Deployment settings (database, store, keys) are environment variables instead (settings.py).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from django.db import transaction
from django.utils import timezone

from forklift_web.core.choices import Classification, Lane
from forklift_web.core.models import InstallationSetting
from forklift_web.errors import InvalidRequest
from forklift_web.policy import Action, Actor, check
from forklift_web.services import audit

KIB, MIB, GIB, TIB = 1024, 1024**2, 1024**3, 1024**4
DAY = 24 * 3600


@dataclass(frozen=True)
class Setting:
    default: Any
    description: str
    validate: Callable[[str, Any], Any]


def _integer(minimum: int, maximum: int | None = None, *, nullable: bool = False):
    def validate(key: str, value: Any):
        if value is None and nullable:
            return None
        if isinstance(value, bool) or not isinstance(value, int):
            kind = "an integer or null" if nullable else "an integer"
            raise InvalidRequest(f"The setting {key} must be {kind}.")
        if value < minimum or (maximum is not None and value > maximum):
            upper = f" and at most {maximum}" if maximum is not None else ""
            raise InvalidRequest(f"The setting {key} must be at least {minimum}{upper}.")
        return value

    return validate


def _choice(choices):
    def validate(key: str, value: Any):
        if value not in choices:
            raise InvalidRequest(f"The setting {key} must be one of: {', '.join(choices)}.")
        return value

    return validate


LIMIT_KEYS = ("max_input_bytes", "max_seconds", "max_rows")


def _lane_limits(key: str, value: Any):
    if not isinstance(value, dict) or set(value) - set(Lane.values):
        raise InvalidRequest(
            f"The setting {key} must map lanes ({', '.join(Lane.values)}) to limits."
        )
    merged = {lane: dict(limits) for lane, limits in DEFAULT_LANE_LIMITS.items()}
    for lane, limits in value.items():
        if not isinstance(limits, dict) or set(limits) - set(LIMIT_KEYS):
            raise InvalidRequest(
                f"The limits of lane {lane} in {key} may only set {', '.join(LIMIT_KEYS)}."
            )
        for name, number in limits.items():
            merged[lane][name] = _integer(1, nullable=True)(f"{key}.{lane}.{name}", number)
    return merged


DEFAULT_LANE_LIMITS = {
    Lane.INTERACTIVE.value: {"max_input_bytes": 256 * MIB, "max_seconds": 120, "max_rows": None},
    Lane.BATCH.value: {"max_input_bytes": None, "max_seconds": DAY, "max_rows": None},
    Lane.SQL.value: {"max_input_bytes": None, "max_seconds": DAY, "max_rows": None},
}

SETTINGS: dict[str, Setting] = {
    "stage_max_bytes": Setting(
        2 * GIB,
        "Inputs up to this size are staged into the worker's scratch directory; larger ones "
        "are streamed to the engine through presigned URLs (ADR 0006).",
        _integer(0),
    ),
    "lease_seconds": Setting(
        60,
        "How long a lease lasts without a heartbeat before the job returns to the queue.",
        _integer(10, 3600),
    ),
    "max_attempts": Setting(
        3, "Leases a job gets before an expired lease fails it.", _integer(1, 20)
    ),
    "default_classification": Setting(
        Classification.INTERNAL.value,
        "Classification of uploads and ad-hoc jobs that do not name one.",
        _choice(Classification.values),
    ),
    "upload_max_bytes": Setting(5 * TIB, "Largest upload accepted.", _integer(1)),
    "upload_url_seconds": Setting(
        3600, "Lifetime of presigned upload URLs.", _integer(60, 7 * DAY)
    ),
    "multipart_threshold_bytes": Setting(
        256 * MIB,
        "Uploads and job outputs larger than this use a multipart upload (one presigned URL "
        "per part).",
        _integer(5 * MIB, 5 * GIB),
    ),
    "multipart_part_bytes": Setting(
        64 * MIB,
        "Part size of multipart uploads and outputs (raised when needed to stay within 10,000 "
        "parts).",
        _integer(5 * MIB, 5 * GIB),
    ),
    "download_url_seconds": Setting(
        300, "Lifetime of presigned download URLs.", _integer(10, 7 * DAY)
    ),
    "input_url_margin_seconds": Setting(
        900,
        "Input URLs handed to workers stay valid for the job's max_seconds plus this margin.",
        _integer(0, DAY),
    ),
    "output_url_seconds": Setting(
        3600, "Lifetime of the URLs workers upload outputs with.", _integer(60, 7 * DAY)
    ),
    "output_max_bytes": Setting(
        1 * TIB,
        "Largest single output file a worker may upload (those above multipart_threshold_bytes "
        "go up in parts; at most 5 TiB, the largest object S3 stores).",
        _integer(1, 5 * TIB),
    ),
    "validate_wait_seconds": Setting(
        10,
        "How long POST /schemas/validate waits for its job before answering 202.",
        _integer(0, 60),
    ),
    "schema_max_bytes": Setting(
        1 * MIB, "Largest schema document (as compact JSON).", _integer(1 * KIB, 64 * MIB)
    ),
    "token_max_days": Setting(
        None,
        "Longest lifetime of a new API token in days (null: tokens may never expire).",
        _integer(1, 3650, nullable=True),
    ),
    "sign_in_max_failures": Setting(
        5,
        "Wrong passwords for one username (in any letter case, whether or not the account "
        "exists; sign-ins and password changes) within sign_in_window_seconds before signing "
        "in with it is refused for sign_in_lock_seconds.",
        _integer(1, 1000),
    ),
    "sign_in_ip_max_failures": Setting(
        30,
        "Failed sign-ins from one client address (an IPv6 address counts by its /64) within "
        "sign_in_window_seconds before signing in from it is refused for sign_in_lock_seconds.",
        _integer(1, 100_000),
    ),
    "sign_in_window_seconds": Setting(
        900,
        "How long failed sign-ins are counted, from the first one; then counting starts again.",
        _integer(60, DAY),
    ),
    "sign_in_lock_seconds": Setting(
        900,
        "How long signing in is refused once a sign-in limit is reached (admins can unlock).",
        _integer(60, DAY),
    ),
    "webhook_disable_after_failures": Setting(
        10,
        "A webhook is disabled once this many delivery attempts in a row failed (with the "
        "growing waits between them, 10 take about two and a half days); its owner can enable "
        "it again.",
        _integer(1, 1000),
    ),
    "webhook_max_per_owner": Setting(
        25, "The most webhooks one user may have.", _integer(1, 1000)
    ),
    "schedule_catch_up_seconds": Setting(
        3600,
        "After the dispatcher was not running, a schedule's most recent missed run still starts "
        "if it is at most this many seconds late; older ones are recorded as missed (0: no "
        "catching up).",
        _integer(0, 7 * DAY),
    ),
    "lane_limits": Setting(
        DEFAULT_LANE_LIMITS,
        "Per-lane limits for jobs (max_input_bytes, max_seconds, max_rows; null for none). "
        "A request can only lower them.",
        _lane_limits,
    ),
}


def current() -> dict:
    """Every installation setting, stored values over defaults."""
    values = {key: definition.default for key, definition in SETTINGS.items()}
    for stored in InstallationSetting.objects.filter(key__in=SETTINGS):
        values[stored.key] = stored.value
    return values


def get(key: str) -> Any:
    stored = InstallationSetting.objects.filter(key=key).first()
    return stored.value if stored is not None else SETTINGS[key].default


def describe(actor: Actor) -> dict:
    """Every setting with its value, default and description (admins)."""
    check(actor, Action.SETTINGS_VIEW)
    values = current()
    return {
        key: {
            "value": values[key],
            "default": definition.default,
            "description": definition.description,
        }
        for key, definition in SETTINGS.items()
    }


def update(actor: Actor, changes: dict) -> dict:
    """Set the given settings (null resets one to its default); all or nothing."""
    check(actor, Action.SETTINGS_MANAGE)
    unknown = sorted(set(changes) - set(SETTINGS))
    if unknown:
        raise InvalidRequest(
            f"Unknown installation settings: {', '.join(unknown)} (known: "
            f"{', '.join(SETTINGS)})."
        )
    validated = {
        key: (None if value is None else SETTINGS[key].validate(key, value))
        for key, value in changes.items()
    }
    previous = current()
    with transaction.atomic():
        for key, value in validated.items():
            if value is None:
                InstallationSetting.objects.filter(key=key).delete()
            else:
                InstallationSetting.objects.update_or_create(
                    key=key,
                    defaults={
                        "value": value,
                        "updated_at": timezone.now(),
                        "updated_by": actor.user,
                    },
                )
        audit.record(
            actor,
            "settings.update",
            details={
                key: {
                    "from": previous[key],
                    "to": SETTINGS[key].default if value is None else value,
                }
                for key, value in validated.items()
            },
        )
    return describe(actor)
