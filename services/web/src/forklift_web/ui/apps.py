from django.apps import AppConfig


class UiConfig(AppConfig):
    """The HTML user interface: views, templates, static files and forms."""

    name = "forklift_web.ui"
    label = "ui"
    verbose_name = "Forklift user interface"
