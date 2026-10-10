"""/api/v1/me and /api/v1/tokens: who am I, and my API tokens."""

import uuid

from ninja import Router, Status
from ninja.pagination import paginate

from forklift_web.api.common import responses
from forklift_web.api.payloads import MeOut, TokenCreatedOut, TokenIn, TokenOut
from forklift_web.policy import ROLE_SCOPES
from forklift_web.services import accounts

router = Router(tags=["me"])


@router.get("/me", response=responses({200: MeOut}), summary="The caller and their scopes")
def me(request):
    actor = request.auth
    return {
        "user": actor.user,
        "scopes": sorted(actor.scopes),
        "role_scopes": sorted(ROLE_SCOPES[actor.role]),
        "token": actor.token,
    }


@router.get("/tokens", response=responses({200: list[TokenOut]}), summary="My API tokens")
@paginate
def list_tokens(request):
    return accounts.list_tokens(request.auth).select_related("owner")


@router.post(
    "/tokens",
    response=responses({201: TokenCreatedOut}),
    summary="Create an API token (its value is returned only now)",
)
def create_token(request, payload: TokenIn):
    token, raw = accounts.create_token(
        request.auth, name=payload.name, scopes=payload.scopes, expires_at=payload.expires_at
    )
    token.token = raw
    return Status(201, token)


@router.delete("/tokens/{token_id}", response=responses({204: None}), summary="Revoke my token")
def revoke_token(request, token_id: uuid.UUID):
    accounts.revoke_token(request.auth, token_id)
    return Status(204, None)
