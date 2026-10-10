"""The home page, changing one's own password, and one's own API tokens."""

from __future__ import annotations

from django.contrib import messages
from django.contrib.auth import views as auth_views
from django.shortcuts import redirect, render
from django.urls import reverse_lazy
from django.views.decorators.cache import never_cache
from django.views.decorators.http import require_GET, require_POST

from forklift_web.api.auth import actor_for_request
from forklift_web.core.choices import TERMINAL_STATUSES, UploadStatus
from forklift_web.errors import ServiceError
from forklift_web.policy import Action, allowed, check
from forklift_web.services import accounts, audit, installation, jobs, uploads
from forklift_web.ui.forms import TokenForm
from forklift_web.ui.views.base import form_failed, page, token_created

RECENT = 10


def recent(actor) -> dict:
    """The actor's latest jobs and files, as far as they may see them."""
    found: dict = {"recent_jobs": [], "recent_uploads": []}
    if allowed(actor, Action.JOB_VIEW):
        found["recent_jobs"] = list(
            jobs.list_jobs(actor, mine=True).select_related("upload")[:RECENT]
        )
    if allowed(actor, Action.UPLOAD_VIEW):
        found["recent_uploads"] = uploads.list_uploads(actor).filter(
            uploaded_by=actor.user, status=UploadStatus.COMPLETE
        )[:5]
    found["running"] = [job for job in found["recent_jobs"] if job.status not in TERMINAL_STATUSES]
    return found


@require_GET
@page
def home(request, actor):
    return render(request, "ui/home.html", recent(actor))


class PasswordChangeView(auth_views.PasswordChangeView):
    """Django's own view (old password, new one twice, the password validators), audited."""

    template_name = "ui/account/password.html"
    success_url = reverse_lazy("ui:home")

    def form_valid(self, form):
        response = super().form_valid(form)
        audit.record(actor_for_request(self.request), "user.change_own_password", form.user)
        messages.success(self.request, "Your password was changed.")
        return response


def _token_form(actor, data=None) -> TokenForm:
    return TokenForm(data, scopes=actor.scopes, max_days=installation.get("token_max_days"))


def _tokens_page(request, actor, form, status=200):
    return render(
        request,
        "ui/account/tokens.html",
        {"tokens": accounts.list_tokens(actor), "form": form},
        status=status,
    )


@never_cache
@page
def tokens(request, actor):
    """Own API tokens; POST creates one and shows its value, once."""
    check(actor, Action.TOKEN_VIEW)
    if request.method != "POST":
        return _tokens_page(request, actor, _token_form(actor))
    check(actor, Action.TOKEN_MANAGE)
    form = _token_form(actor, request.POST)
    status = 400
    if form.is_valid():
        try:
            token, raw = accounts.create_token(
                actor,
                name=form.cleaned_data["name"],
                scopes=form.cleaned_data["scopes"],
                expires_at=form.expires_at(),
            )
        except ServiceError as error:
            status = form_failed(form, error)
        else:
            return token_created(request, token, raw, back="ui:tokens")
    return _tokens_page(request, actor, form, status)


@require_POST
@page
def revoke_token(request, actor, token_id):
    token = accounts.revoke_token(actor, token_id)
    messages.success(request, f"The token {token.name!r} ({token.prefix}...) was revoked.")
    return redirect("ui:tokens")
