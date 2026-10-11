"""End-to-end: the sign-in throttle of a running Compose stack (run these as test_stack.py says).

A throwaway user is locked by wrong passwords and unlocked by an admin, through the sign-in
page and /api/v1/admin. The username limit is lowered to 2 for the test and reset afterwards,
so that a run adds only two failures to the address counter of the machine running the tests.
"""

from __future__ import annotations

import re
import urllib.error
import urllib.parse
import urllib.request
import uuid

from test_stack import Client, admin, settings  # noqa: F401  (the session fixtures)


def attempt_sign_in(client: Client, username: str, password: str):
    """One sign-in attempt: (status, Retry-After); a wrong password gets the form again (200)."""
    with client.opener.open(client.base_url + "/accounts/login/", timeout=30) as response:
        page = response.read().decode()
    csrf = re.search(r'name="csrfmiddlewaretoken" value="([^"]+)"', page).group(1)
    form = urllib.parse.urlencode(
        {"username": username, "password": password, "csrfmiddlewaretoken": csrf}
    ).encode()
    request = urllib.request.Request(client.base_url + "/accounts/login/", data=form)
    request.add_header("Referer", client.base_url + "/accounts/login/")
    try:
        with client.opener.open(request, timeout=30) as response:
            return response.status, response.headers.get("Retry-After")
    except urllib.error.HTTPError as error:
        return error.code, error.headers.get("Retry-After")


def signed_in(client: Client) -> bool:
    return any(cookie.name == "sessionid" for cookie in client.cookies)


def test_wrong_passwords_lock_a_username_until_an_admin_unlocks_it(admin, settings):  # noqa: F811
    username = f"e2e-locked-{uuid.uuid4().hex[:8]}"
    password = f"correct horse {uuid.uuid4().hex}"
    admin.request(
        "POST",
        "/api/v1/admin/users",
        {"username": username, "role": "viewer", "password": password},
        expect=(201,),
    )
    admin.request("PATCH", "/api/v1/admin/settings", {"sign_in_max_failures": 2})
    try:
        person = Client(settings["FORKLIFT_PUBLIC_URL"])
        assert attempt_sign_in(person, username, "wrong")[0] == 200
        status, retry_after = attempt_sign_in(person, username.upper(), "wrong again")
        assert status == 429 and 0 < int(retry_after) <= 900
        # Refused before the password is checked: the right one gets the same answer
        assert attempt_sign_in(person, username, password)[0] == 429
        assert not signed_in(person)

        _, locks = admin.request("GET", "/api/v1/admin/sign-in-locks")
        (lock,) = [item for item in locks["items"] if item["key"] == username]
        assert (lock["kind"], lock["failures"]) == ("user", 2)
        admin.request("DELETE", f"/api/v1/admin/sign-in-locks/{lock['id']}", expect=(204,))
        _, audit = admin.request("GET", "/api/v1/admin/audit?action=account.sign_in_unlocked")
        assert any(entry["object_repr"] == f"user {username}" for entry in audit["items"])

        person.sign_in(username, password)
        assert signed_in(person)
    finally:
        admin.request("PATCH", "/api/v1/admin/settings", {"sign_in_max_failures": None})
