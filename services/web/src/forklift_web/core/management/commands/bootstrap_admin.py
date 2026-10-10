"""Create the first admin account (idempotent: an existing admin of that name is left alone)."""

from __future__ import annotations

import os

from django.core.management.base import BaseCommand, CommandError

from forklift_web.core.choices import Role
from forklift_web.core.models import User
from forklift_web.errors import ServiceError
from forklift_web.policy import Actor
from forklift_web.services import accounts


class Command(BaseCommand):
    help = (
        "Create an admin account whose password is read from an environment variable "
        "(FORKLIFT_ADMIN_PASSWORD by default), so that it never appears in a process list or "
        "shell history. Does nothing when the account already exists as an admin."
    )

    def add_arguments(self, parser):
        parser.add_argument("--username", required=True)
        parser.add_argument("--email", default="")
        parser.add_argument(
            "--password-env",
            default="FORKLIFT_ADMIN_PASSWORD",
            help="Environment variable that holds the password (default FORKLIFT_ADMIN_PASSWORD)",
        )

    def handle(self, *args, username, email, password_env, **options):
        existing = User.objects.filter(username=username).first()
        if existing is not None:
            if existing.role != Role.ADMIN:
                raise CommandError(
                    f"A user named {username!r} exists with the role {existing.role}; "
                    "bootstrap_admin does not change existing accounts (use the admin API)."
                )
            self.stdout.write(f"The admin {username!r} already exists; nothing to do.")
            return
        password = os.environ.get(password_env)
        if not password:
            raise CommandError(
                f"Set the environment variable {password_env} to the new admin's password."
            )
        try:
            accounts.create_user(
                Actor.for_system("bootstrap_admin"),
                username=username,
                email=email,
                role=Role.ADMIN,
                password=password,
            )
        except ServiceError as error:
            raise CommandError(error.message) from None
        self.stdout.write(f"Created the admin {username!r}.")
