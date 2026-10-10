"""Template tags and filters of the UI: static assets, sizes, JSON, query strings, events."""

from __future__ import annotations

import json

from django import template
from django.templatetags.static import static

from forklift_web import __version__

register = template.Library()

_UNITS = ("bytes", "KiB", "MiB", "GiB", "TiB", "PiB")


@register.simple_tag
def asset(path: str) -> str:
    """The URL of a static file with the version as a cache-busting query string."""
    return f"{static(path)}?v={__version__}"


@register.filter
def filesize(value) -> str:
    """Bytes in binary units: 1536 -> '1.5 KiB'; None -> an em dash."""
    if value is None or value == "":
        return "—"
    size, unit = float(value), 0
    while abs(size) >= 1024 and unit < len(_UNITS) - 1:
        size /= 1024
        unit += 1
    if unit == 0:
        return "1 byte" if int(size) == 1 else f"{int(size)} bytes"
    return f"{size:.1f} {_UNITS[unit]}"


@register.filter
def pretty_json(value) -> str:
    return json.dumps(value, indent=2, ensure_ascii=False, default=str)


@register.filter
def compact_json(value) -> str:
    return json.dumps(value, ensure_ascii=False, default=str, separators=(", ", ": "))


@register.simple_tag(takes_context=True)
def query(context, **changes) -> str:
    """The current query string with ``changes`` applied (pagination keeps the filters)."""
    params = context["request"].GET.copy()
    for key, value in changes.items():
        params[key] = value
    return "?" + params.urlencode()


@register.filter
def event_text(event) -> str:
    """A job event's payload as 'key: value' pairs (payloads hold no cell values)."""
    return ", ".join(
        f"{key.replace('_', ' ')}: {value}"
        for key, value in event.payload.items()
        if value is not None
    )
