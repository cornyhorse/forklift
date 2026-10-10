"""The admin screens (/admin/): overview, users, API tokens, worker tokens and workers,
connections, retention, the audit log, installation settings and all jobs.

Each screen calls the service functions of its area, which check the admin actions
(``user.view``, ``connection.manage``, ...) and audit every change.
"""

from __future__ import annotations

import csv
import json
from datetime import timedelta

from django.contrib import messages
from django.db.models import Count, Min, Q
from django.http import StreamingHttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.utils import timezone
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from forklift_web.core.choices import (
    Classification,
    JobStatus,
    Lane,
    RetentionKind,
    RetentionScope,
    Role,
)
from forklift_web.errors import InvalidRequest, NotFound, ServiceError
from forklift_web.policy import ALL_SCOPES, Action, check
from forklift_web.services import (
    accounts,
    audit,
    connections,
    datasets,
    installation,
    jobs,
    retention,
    workers,
)
from forklift_web.ui.forms import (
    CONNECTION_FORMS,
    AdminJobFilterForm,
    AdminTokenForm,
    AuditFilterForm,
    PasswordPairForm,
    RetentionForm,
    SettingForm,
    UserCreateForm,
    UserEditForm,
    WorkerTokenForm,
    retention_prefix,
)
from forklift_web.ui.views.base import back, form_failed, is_htmx, page, paginate, token_created
from forklift_web.ui.views.work import job_rows

# A worker counts as online when it called within twice the lease length (at least this).
ONLINE_SECONDS = 120
# Characters that make a spreadsheet treat a CSV cell as a formula.
FORMULA_START = ("=", "+", "-", "@", "\t", "\r")


def online_since(settings: dict):
    seconds = max(2 * settings["lease_seconds"], ONLINE_SECONDS)
    return timezone.now() - timedelta(seconds=seconds)


# --------------------------------------------------------------------------- overview


@require_GET
@page
def overview(request, actor):
    check(actor, Action.WORKER_VIEW)
    since = online_since(installation.current())
    seen = list(workers.list_workers(actor))
    queued = (
        jobs.list_jobs(actor, status=JobStatus.QUEUED)
        .order_by()
        .values("lane")
        .annotate(count=Count("id"), oldest=Min("created_at"))
    )
    lanes = {lane: {"lane": lane, "count": 0, "oldest": None} for lane in Lane.values}
    for row in queued:
        lanes[row["lane"]] = row
    day_ago = timezone.now() - timedelta(days=1)
    failed = jobs.list_jobs(actor, status=JobStatus.FAILED)
    return render(
        request,
        "ui/admin/overview.html",
        {
            "online": [worker for worker in seen if worker.last_seen_at >= since],
            "offline": [worker for worker in seen if worker.last_seen_at < since],
            "lanes": list(lanes.values()),
            "running": jobs.list_jobs(actor, status=JobStatus.RUNNING)[:20],
            "failed": failed.order_by("-finished_at")[:10],
            "failed_today": failed.filter(finished_at__gte=day_ago).count(),
            "warnings": retention.overview(actor)["warnings"],
        },
    )


@require_GET
@page
def store_status(request, actor):
    """The installation's bucket, checked on demand (the overview loads this lazily, so a
    store that does not answer cannot slow the page down)."""
    result = connections.check_store(actor)
    template = "ui/admin/_store.html" if is_htmx(request) else "ui/admin/store.html"
    return render(request, template, {"store": result})


# --------------------------------------------------------------------------- users


@require_GET
@page
def users(request, actor):
    found = accounts.list_users(actor)
    search = request.GET.get("q", "").strip()
    role = request.GET.get("role", "")
    state = request.GET.get("state", "")
    if search:
        found = found.filter(
            Q(username__icontains=search)
            | Q(email__icontains=search)
            | Q(first_name__icontains=search)
            | Q(last_name__icontains=search)
        )
    if role in Role.values:
        found = found.filter(role=role)
    if state in {"active", "inactive"}:
        found = found.filter(is_active=state == "active")
    context = {
        "page": paginate(request, found),
        "search": search,
        "role": role,
        "state": state,
        "roles": Role.choices,
    }
    template = "ui/admin/_users_table.html" if is_htmx(request) else "ui/admin/users.html"
    return render(request, template, context)


@page
def user_new(request, actor):
    check(actor, Action.USER_MANAGE)
    form = UserCreateForm(request.POST if request.method == "POST" else None)
    status = 400 if form.is_bound else 200
    if form.is_valid():
        try:
            user = accounts.create_user(actor, **form.service_values())
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"The user {user.username!r} was created.")
            return redirect("ui:admin-user", user_id=user.pk)
    return render(request, "ui/admin/user_new.html", {"form": form}, status=status)


def _user_page(request, actor, user, *, edit_form=None, password_form=None, status=200):
    others = accounts.list_users(actor).filter(role=Role.ADMIN, is_active=True).exclude(pk=user.pk)
    initial = {field: getattr(user, field) for field in accounts.USER_FIELDS}
    return render(
        request,
        "ui/admin/user_detail.html",
        {
            "person": user,
            "edit_form": edit_form or UserEditForm(initial=initial),
            "password_form": password_form or PasswordPairForm(),
            "tokens": accounts.list_all_tokens(actor, owner_id=user.pk),
            "last_admin": user.role == Role.ADMIN and user.is_active and not others.exists(),
        },
        status=status,
    )


@page
def user_detail(request, actor, user_id):
    user = accounts.get_user(actor, user_id)
    if request.method != "POST":
        return _user_page(request, actor, user)
    check(actor, Action.USER_MANAGE)
    form = UserEditForm(request.POST)
    status = 400
    if form.is_valid():
        try:
            accounts.update_user(actor, user.pk, **form.cleaned_data)
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"{user.username} was saved.")
            return redirect("ui:admin-user", user_id=user.pk)
    return _user_page(request, actor, user, edit_form=form, status=status)


@require_POST
@page
def user_password(request, actor, user_id):
    check(actor, Action.USER_MANAGE)
    user = accounts.get_user(actor, user_id)
    form = PasswordPairForm(request.POST)
    status = 400
    if form.is_valid():
        try:
            accounts.set_password(actor, user.pk, form.cleaned_data["password1"])
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"The password of {user.username} was set.")
            return redirect("ui:admin-user", user_id=user.pk)
    return _user_page(request, actor, user, password_form=form, status=status)


# --------------------------------------------------------------------------- API tokens


def _token_state(token, now) -> str:
    if token.revoked_at is not None:
        return "revoked"
    if token.expires_at is not None and token.expires_at <= now:
        return "expired"
    return "active"


def with_states(found) -> list:
    now = timezone.now()
    rows = list(found)
    for token in rows:
        token.state = _token_state(token, now)
    return rows


def _admin_token_form(actor, data=None, owner: str = "") -> AdminTokenForm:
    return AdminTokenForm(
        data,
        users=accounts.list_users(actor).filter(is_active=True),
        scopes=ALL_SCOPES,
        max_days=installation.get("token_max_days"),
        initial={"owner": owner},
    )


@never_cache
@page
def tokens(request, actor):
    owner = request.GET.get("owner", "")
    found = accounts.list_all_tokens(actor, owner_id=int(owner) if owner.isdigit() else None)
    if request.method == "POST":
        check(actor, Action.ANY_TOKEN_MANAGE)
        form = _admin_token_form(actor, request.POST)
        status = 400
        if form.is_valid():
            try:
                token, raw = accounts.create_token_for(
                    actor,
                    owner_id=form.cleaned_data["owner"],
                    name=form.cleaned_data["name"],
                    scopes=form.cleaned_data["scopes"],
                    expires_at=form.expires_at(),
                )
            except ServiceError as error:
                status = form_failed(form, error)
            else:
                return token_created(request, token, raw, back="ui:admin-tokens")
    else:
        form, status = _admin_token_form(actor, owner=owner), 200
    current = paginate(request, found)
    return render(
        request,
        "ui/admin/tokens.html",
        {"page": current, "rows": with_states(current), "form": form, "owner": owner},
        status=status,
    )


@require_POST
@page
def token_revoke(request, actor, token_id):
    token = accounts.revoke_any_token(actor, token_id)
    messages.success(
        request, f"The token {token.name!r} ({token.prefix}...) of {token.owner} was revoked."
    )
    return back(request, reverse("ui:admin-tokens"))


# --------------------------------------------------------------------------- workers


@never_cache
@page
def workers_page(request, actor):
    check(actor, Action.WORKER_VIEW)
    form = WorkerTokenForm(request.POST if request.method == "POST" else None)
    status = 200
    if form.is_bound:
        check(actor, Action.WORKER_MANAGE)
        status = 400
        if form.is_valid():  # the form requires what the service would refuse
            token, raw = workers.create_worker_token(
                actor, name=form.cleaned_data["name"], expires_at=form.expires_at()
            )
            return token_created(request, token, raw, back="ui:admin-workers", what="worker token")
    since = online_since(installation.current())
    running: dict = {}
    for job in jobs.list_jobs(actor, status=JobStatus.RUNNING):
        running.setdefault(job.lease_worker_id, []).append(job)
    seen = list(workers.list_workers(actor))
    for worker in seen:
        worker.online = worker.last_seen_at >= since
        worker.current_jobs = running.get(worker.pk, [])
    return render(
        request,
        "ui/admin/workers.html",
        {
            "form": form,
            "worker_tokens": with_states(workers.list_worker_tokens(actor)),
            "workers": seen,
        },
        status=status,
    )


@require_POST
@page
def worker_token_revoke(request, actor, token_id):
    token = workers.revoke_worker_token(actor, token_id)
    messages.success(request, f"The worker token {token.name!r} ({token.prefix}...) was revoked.")
    return redirect("ui:admin-workers")


# --------------------------------------------------------------------------- connections


@require_GET
@page
def connections_page(request, actor):
    check(actor, Action.CONNECTION_MANAGE)
    return render(
        request,
        "ui/admin/connections.html",
        {"connections": connections.list_connections(actor), "kinds": CONNECTION_FORMS},
    )


def _connection_form_class(kind: str):
    if kind not in CONNECTION_FORMS:
        raise NotFound(
            f"There is no connection kind {kind!r}; kinds: {', '.join(CONNECTION_FORMS)}."
        )
    return CONNECTION_FORMS[kind]


def _connection_page(request, form, connection=None, status=200):
    return render(
        request,
        "ui/admin/connection_form.html",
        {"form": form, "connection": connection, "kind": form.kind},
        status=status,
    )


@page
def connection_new(request, actor, kind):
    check(actor, Action.CONNECTION_MANAGE)
    form_class = _connection_form_class(kind)
    form = form_class(request.POST if request.method == "POST" else None)
    status = 400 if form.is_bound else 200
    if form.is_valid():
        try:
            connection = connections.create_connection(
                actor,
                name=form.cleaned_data["name"],
                kind=kind,
                config=form.config(),
                secrets=form.secrets(),
                description=form.cleaned_data["description"],
                allowed_roles=form.cleaned_data["allowed_roles"],
            )
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"The connection {connection.name!r} was created.")
            return redirect("ui:admin-connection", connection_id=connection.pk)
    return _connection_page(request, form, status=status)


@page
def connection_detail(request, actor, connection_id):
    check(actor, Action.CONNECTION_MANAGE)
    connection = connections.get_connection(actor, connection_id)
    form_class = CONNECTION_FORMS[connection.kind]
    if request.method != "POST":
        return _connection_page(request, form_class(connection=connection), connection)
    form = form_class(request.POST, connection=connection)
    status = 400
    if form.is_valid():
        try:
            connections.update_connection(
                actor,
                connection.pk,
                name=form.cleaned_data["name"],
                description=form.cleaned_data["description"],
                config=form.config(),
                secrets=form.secrets(),
                allowed_roles=form.cleaned_data["allowed_roles"],
            )
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"The connection {form.cleaned_data['name']!r} was saved.")
            return redirect("ui:admin-connection", connection_id=connection.pk)
    return _connection_page(request, form, connection, status)


@require_POST
@page
def connection_test(request, actor, connection_id):
    result = connections.check_connection(actor, connection_id)
    if is_htmx(request):
        return render(request, "ui/admin/_connection_test.html", {"result": result})
    levels = {True: messages.SUCCESS, False: messages.ERROR, None: messages.INFO}
    messages.add_message(request, levels[result.ok], result.message)
    return redirect("ui:admin-connection", connection_id=connection_id)


@require_POST
@page
def connection_delete(request, actor, connection_id):
    check(actor, Action.CONNECTION_MANAGE)
    connection = connections.get_connection(actor, connection_id)
    try:
        connections.delete_connection(actor, connection.pk)
    except ServiceError as error:
        messages.error(request, error.message)
        return redirect("ui:admin-connection", connection_id=connection.pk)
    messages.success(request, f"The connection {connection.name!r} was deleted.")
    return redirect("ui:admin-connections")


# --------------------------------------------------------------------------- retention


def _retention_levels(actor, policies) -> list:
    """Every level an admin can set, with its form: installation, classifications, datasets
    that have a policy, and one more to add a dataset policy."""
    by_level = {}
    for policy in policies:
        target = policy.classification or (str(policy.dataset_id) if policy.dataset_id else "")
        by_level[(policy.scope, target)] = policy
    levels = [(RetentionScope.INSTALLATION, "", "The installation")]
    levels += [
        (RetentionScope.CLASSIFICATION, value, f"{label} data")
        for value, label in Classification.choices
    ]
    levels += [
        (RetentionScope.DATASET, str(policy.dataset_id), f"Dataset {policy.dataset.name}")
        for policy in policies
        if policy.scope == RetentionScope.DATASET
    ]
    found = []
    for scope, target, title in levels:
        policy = by_level.get((scope, target))
        days = policy.days if policy else {}
        prefix = retention_prefix(scope, target)
        found.append(
            {
                "scope": scope,
                "target": target,
                "title": title,
                "prefix": prefix,
                "policy": policy,
                "cells": [_retention_cell(days, kind) for kind in RetentionKind.values],
                "form": RetentionForm(days=days, prefix=prefix),
            }
        )
    return found


def _retention_cell(days: dict, kind: str) -> str:
    if kind not in days:
        return ""
    value = days[kind]
    if value is None:
        return "until deleted"
    return "1 day" if value == 1 else f"{value} days"


def _retention_page(request, actor, *, bound=None, report=None, status=200):
    summary = retention.overview(actor)
    levels = _retention_levels(actor, summary["policies"])
    with_policy = {level["target"] for level in levels if level["scope"] == RetentionScope.DATASET}
    new_dataset = {
        "prefix": retention_prefix(RetentionScope.DATASET, "new"),
        "datasets": [d for d in datasets.list_datasets(actor) if str(d.pk) not in with_policy],
    }
    new_dataset["form"] = RetentionForm(prefix=new_dataset["prefix"])
    unplaced = bound
    for level in levels + [new_dataset]:
        if bound is not None and level["prefix"] == bound.prefix:
            level["form"], unplaced = bound, None
    return render(
        request,
        "ui/admin/retention.html",
        {
            "kinds": RetentionKind.choices,
            "warnings": summary["warnings"],
            "levels": levels,
            "new_dataset": new_dataset,
            "unplaced": unplaced,
            "report": report,
        },
        status=status,
    )


def _retention_target(request) -> dict:
    scope = request.POST.get("scope", "")
    target = request.POST.get("target", "")
    if scope == RetentionScope.CLASSIFICATION:
        return {"scope": scope, "classification": target}
    if scope == RetentionScope.DATASET:
        return {"scope": scope, "dataset_id": target or None}
    return {"scope": scope}


@require_GET
@page
def retention_page(request, actor):
    return _retention_page(request, actor)


@require_POST
@page
def retention_save(request, actor):
    check(actor, Action.RETENTION_MANAGE)
    target = _retention_target(request)
    form = RetentionForm(request.POST, prefix=request.POST.get("prefix") or None)
    status = 400
    if form.is_valid():
        try:
            retention.set_policy(actor, target.pop("scope"), days=form.days(), **target)
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, "The retention policy was saved.")
            return redirect("ui:admin-retention")
    return _retention_page(request, actor, bound=form, status=status)


@require_POST
@page
def retention_delete(request, actor):
    target = _retention_target(request)
    retention.delete_policy(actor, target.pop("scope"), **target)
    messages.success(request, "The retention policy was removed; its level inherits again.")
    return redirect("ui:admin-retention")


@require_POST
@page
def retention_sweep(request, actor):
    dry_run = request.POST.get("dry_run") != "0"
    report = retention.sweep(actor, dry_run=dry_run)
    return _retention_page(request, actor, report=report)


# --------------------------------------------------------------------------- audit log


def _audit_form(actor, data) -> AuditFilterForm:
    entries = audit.list_entries(actor)
    actions = entries.order_by("action").values_list("action", flat=True).distinct()
    types = entries.exclude(object_type="").order_by("object_type")
    return AuditFilterForm(
        data or None,
        actions=list(actions),
        object_types=list(types.values_list("object_type", flat=True).distinct()),
    )


def _audit_entries(actor, form: AuditFilterForm):
    if form.is_bound and not form.is_valid():
        raise InvalidRequest(
            "The filters are not valid: "
            + " ".join(f"{name}: {' '.join(errors)}" for name, errors in form.errors.items())
        )
    filters = dict(form.cleaned_data) if form.is_bound else {}
    username = filters.pop("actor", "")
    if username:
        user = accounts.list_users(actor).filter(username=username).first()
        filters["actor_id"] = user.pk if user is not None else -1
    return audit.list_entries(actor, **{key: value for key, value in filters.items() if value})


@require_GET
@page
def audit_page(request, actor):
    form = _audit_form(actor, request.GET)
    try:
        entries = _audit_entries(actor, form)
    except InvalidRequest as error:
        entries, problem = audit.list_entries(actor).none(), error.message
    else:
        problem = ""
    context = {"form": form, "page": paginate(request, entries), "problem": problem}
    template = "ui/admin/_audit_table.html" if is_htmx(request) else "ui/admin/audit.html"
    return render(request, template, context)


class _Echo:
    def write(self, value):
        return value


def spreadsheet_safe(value) -> str:
    """A CSV cell that a spreadsheet will not run as a formula (OWASP's CSV injection advice)."""
    text = "" if value is None else str(value)
    return "'" + text if text.startswith(FORMULA_START) else text


AUDIT_COLUMNS = [
    "id",
    "created_at",
    "actor",
    "token_prefix",
    "action",
    "object_type",
    "object_id",
    "object",
    "details",
    "request_id",
    "ip",
]


def _audit_rows(entries):
    writer = csv.writer(_Echo())
    yield writer.writerow(AUDIT_COLUMNS)
    for entry in entries.iterator():
        yield writer.writerow(
            spreadsheet_safe(value)
            for value in (
                entry.id,
                entry.created_at.isoformat(),
                entry.actor_label,
                entry.token_prefix,
                entry.action,
                entry.object_type,
                entry.object_id,
                entry.object_repr,
                json.dumps(entry.details, sort_keys=True, default=str),
                entry.request_id,
                entry.ip,
            )
        )


@require_GET
@page
def audit_export(request, actor):
    entries = _audit_entries(actor, _audit_form(actor, request.GET))
    response = StreamingHttpResponse(_audit_rows(entries), content_type="text/csv; charset=utf-8")
    stamp = timezone.now().strftime("%Y%m%d-%H%M%S")
    response["Content-Disposition"] = f'attachment; filename="forklift-audit-{stamp}.csv"'
    return response


# --------------------------------------------------------------------------- settings


def _settings_page(request, actor, *, bound=None, status=200):
    described = installation.describe(actor)
    rows = []
    for key, setting in described.items():
        form = bound if bound is not None and bound.key == key else None
        rows.append(
            {
                "key": key,
                "setting": setting,
                "changed": setting["value"] != setting["default"],
                "form": form or SettingForm(key=key, setting=setting, prefix=key),
            }
        )
    return render(request, "ui/admin/settings.html", {"rows": rows}, status=status)


@require_GET
@page
def settings_page(request, actor):
    return _settings_page(request, actor)


def _setting(actor, key: str) -> dict:
    described = installation.describe(actor)
    if key not in described:
        raise NotFound(f"There is no installation setting {key!r}.")
    return described[key]


@require_POST
@page
def setting_save(request, actor, key):
    check(actor, Action.SETTINGS_MANAGE)
    form = SettingForm(request.POST, key=key, setting=_setting(actor, key), prefix=key)
    status = 400
    if form.is_valid():
        try:
            installation.update(actor, {key: form.setting_value()})
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            messages.success(request, f"{key} was saved.")
            return redirect(reverse("ui:admin-settings") + f"#setting-{key}")
    return _settings_page(request, actor, bound=form, status=status)


@require_POST
@page
def setting_reset(request, actor, key):
    check(actor, Action.SETTINGS_MANAGE)
    _setting(actor, key)
    installation.update(actor, {key: None})
    messages.success(request, f"{key} is back to its default.")
    return redirect(reverse("ui:admin-settings") + f"#setting-{key}")


# --------------------------------------------------------------------------- all jobs


@require_GET
@page
def all_jobs(request, actor):
    check(actor, Action.WORKER_VIEW)
    form = AdminJobFilterForm(request.GET or None)
    filters = form.cleaned_data if form.is_valid() else {}
    found = jobs.list_jobs(
        actor,
        status=filters.get("status") or None,
        kind=filters.get("kind") or None,
        mine=filters.get("mine", False),
    )
    if filters.get("lane"):
        found = found.filter(lane=filters["lane"])
    if filters.get("requester"):
        found = found.filter(requested_by__username=filters["requester"])
    current = paginate(request, found)
    context = {"form": form, "page": current, "rows": job_rows(actor, current), "admin": True}
    template = "ui/jobs/_table.html" if is_htmx(request) else "ui/admin/jobs.html"
    return render(request, template, context)
