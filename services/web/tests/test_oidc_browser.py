"""Single sign-on in a real browser (Chromium, through Playwright) against pytest-django's live
server and the fake provider: the button, the provider's own sign-in page, back signed in;
then signing out of Forklift and of the provider."""

from __future__ import annotations

import pytest
from django.urls import reverse
from oidc_support import idp, sso  # noqa: F401  (the fixtures)
from playwright.sync_api import expect
from test_ui_browser import browser  # noqa: F401  (the fixture)

from forklift_web.core.choices import Role
from forklift_web.core.models import AuditLog, User

pytestmark = [pytest.mark.browser, pytest.mark.django_db(transaction=True)]


@pytest.fixture
def page(browser, live_server):  # noqa: F811
    # The fake provider's certificate comes from the tests' own CA, which the browser does
    # not know; the gateway itself verifies it (FORKLIFT_OIDC_CA_FILE).
    context = browser.new_context(base_url=live_server.url, ignore_https_errors=True)
    page = context.new_page()
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.errors = errors
    yield page
    context.close()


def test_sign_in_with_the_provider_and_out_again(page, idp, sso, live_server):  # noqa: F811
    sso(PROVIDER_LOGOUT="true")
    idp.person(
        "grace", preferred_username="grace", given_name="Grace", groups=["forklift-authors"]
    )

    page.goto(reverse("ui:schemas"))  # not signed in: the sign-in page, which keeps the way back
    page.get_by_role("link", name="Sign in with Fake IdP").click()
    expect(page.get_by_role("heading", level=1)).to_have_text("Fake IdP sign-in")
    page.get_by_label("Account").select_option("grace")
    page.get_by_role("button", name="Continue").click()

    page.wait_for_url(f"{live_server.url}{reverse('ui:schemas')}")
    expect(page.locator(".account")).to_contain_text("grace")
    expect(page.locator(".account")).to_contain_text("Author")
    expect(page.locator(".account").get_by_role("link", name="Password")).to_have_count(0)
    user = User.objects.get(username="grace")
    assert (user.role, user.first_name) == (Role.AUTHOR, "Grace")

    page.get_by_role("button", name="Sign out").click()
    # Forklift's page sends the browser on to the provider, which sends it back to sign in
    page.wait_for_url(f"{live_server.url}{reverse('forklift-login')}")
    expect(page.get_by_role("link", name="Sign in with Fake IdP")).to_be_visible()
    (logout,) = idp.logouts
    assert logout["post_logout_redirect_uri"] == f"{live_server.url}{reverse('forklift-login')}"
    assert logout["id_token_hint"].count(".") == 2
    assert list(
        AuditLog.objects.filter(actor=user).order_by("id").values_list("action", flat=True)
    ) == ["user.login", "user.logout"]
    assert page.errors == []


def test_without_the_password_form(page, idp, sso, settings):  # noqa: F811
    settings.FORKLIFT_PASSWORD_SIGN_IN = False
    page.goto(reverse("forklift-login"))
    expect(page.get_by_role("link", name="Sign in with Fake IdP")).to_be_visible()
    expect(page.get_by_label("Password")).to_have_count(0)
    expect(page.locator("main")).to_contain_text("Ask an admin if you cannot sign in with Fake")
