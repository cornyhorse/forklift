from django.apps import AppConfig


class CoreConfig(AppConfig):
    """Models, migrations and management commands of the gateway."""

    name = "forklift_web.core"
    label = "core"
    verbose_name = "Forklift gateway"
    default_auto_field = "django.db.models.BigAutoField"

    def ready(self) -> None:
        from forklift_web.core import signals  # noqa: F401  (connects the login audit)
