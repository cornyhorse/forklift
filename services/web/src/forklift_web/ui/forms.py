"""Forms of the UI: they turn what people type into the service layer's arguments.

Forms check what a browser cannot be trusted with (types, JSON syntax, required fields, two
passwords that match); every rule about what may be done, and every rule about valid values,
is the service layer's, whose messages the views show next to the form.
"""

from __future__ import annotations

import json
from datetime import timedelta

from django import forms
from django.utils import timezone

from forklift_web.core.choices import (
    Classification,
    InputFormat,
    JobKind,
    JobStatus,
    Lane,
    RetentionKind,
    Role,
    SqlDialect,
)
from forklift_web.services.connections import DIALECTS, SECRET_FIELDS
from forklift_web.services.datasets import TABLE_MODES

COMPRESSIONS = [(name, name) for name in ("snappy", "zstd", "gzip", "brotli", "lz4", "none")]
UPLOAD_FORMATS = [(fmt.value, fmt.label) for fmt in InputFormat if fmt != InputFormat.SQL]
HEADER_MODES = [
    ("", "Default (present)"),
    ("present", "Present: the first row is the header"),
    ("absent", "Absent: the schema names the columns"),
    ("auto", "Detect it"),
]
EXPIRY_DAYS = (7, 30, 90, 365)
SECRET_LABELS = {
    "access_key_id": "Access key ID",
    "secret_access_key": "Secret access key",
    "session_token": "Session token",
    "password": "Password",
}
DATETIME_FORMATS = ["%Y-%m-%dT%H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"]


def _textarea(rows: int = 3, **attrs) -> forms.Textarea:
    return forms.Textarea(attrs={"rows": rows, **attrs})


def _password() -> forms.PasswordInput:
    return forms.PasswordInput(render_value=False, attrs={"autocomplete": "new-password"})


class JSONObjectField(forms.CharField):
    """A JSON object typed into a text area; an empty one is ``{}`` unless it is required."""

    def __init__(self, *, rows: int = 4, **kwargs):
        kwargs.setdefault("widget", _textarea(rows, spellcheck="false", **{"class": "code"}))
        kwargs.setdefault("strip", True)
        super().__init__(**kwargs)

    def to_python(self, value):
        text = super().to_python(value)
        if not text:
            return {}
        try:
            data = json.loads(text)
        except json.JSONDecodeError as error:
            raise forms.ValidationError(
                f"This is not valid JSON: {error.msg} (line {error.lineno}, column "
                f"{error.colno})."
            ) from None
        if not isinstance(data, dict):
            raise forms.ValidationError("This must be a JSON object: {...}.")
        return data

    def prepare_value(self, value):
        if isinstance(value, dict):
            return json.dumps(value, indent=2, ensure_ascii=False) if value else ""
        return value


def expiry_choices(max_days, *, allow_never: bool = True) -> list:
    """Lifetimes a token form offers, within ``max_days`` when the installation sets one."""
    days = [d for d in EXPIRY_DAYS if max_days is None or d <= max_days]
    if max_days is not None and max_days not in days:
        days.append(max_days)
    choices = [(str(d), "1 year" if d == 365 else f"{d} days") for d in days]
    if max_days is None and allow_never:
        choices.append(("", "Never"))
    return choices


def expires_at(value: str):
    return timezone.now() + timedelta(days=int(value)) if value else None


# --------------------------------------------------------------------------- accounts


class TokenForm(forms.Form):
    name = forms.CharField(max_length=100, help_text="What the token is for, e.g. 'airflow'.")
    scopes = forms.MultipleChoiceField(widget=forms.CheckboxSelectMultiple)
    expires_in = forms.ChoiceField(label="Expires after", required=False)

    def __init__(self, *args, scopes, max_days=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["scopes"].choices = [(scope, scope) for scope in sorted(scopes)]
        choices = expiry_choices(max_days)
        self.fields["expires_in"].choices = choices
        values = [value for value, _ in choices]
        self.fields["expires_in"].initial = "90" if "90" in values else values[-1]

    def expires_at(self):
        return expires_at(self.cleaned_data["expires_in"])


class AdminTokenForm(TokenForm):
    owner = forms.TypedChoiceField(coerce=int, help_text="Typically a service account.")

    field_order = ["owner", "name", "scopes", "expires_in"]

    def __init__(self, *args, users, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["owner"].choices = [
            (user.pk, f"{user.username} ({user.get_role_display()})") for user in users
        ]


class WorkerTokenForm(forms.Form):
    name = forms.CharField(max_length=100, help_text="The worker pool, e.g. 'batch workers'.")
    expires_in = forms.ChoiceField(label="Expires after", required=False)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["expires_in"].choices = expiry_choices(None)
        self.fields["expires_in"].initial = ""

    def expires_at(self):
        return expires_at(self.cleaned_data["expires_in"])


class PasswordPairForm(forms.Form):
    password1 = forms.CharField(label="New password", strip=False, widget=_password())
    password2 = forms.CharField(label="New password again", strip=False, widget=_password())

    def clean(self):
        cleaned = super().clean()
        if cleaned.get("password1") != cleaned.get("password2"):
            raise forms.ValidationError("The two passwords are not the same.")
        return cleaned


class UserCreateForm(forms.Form):
    username = forms.CharField(max_length=150)
    role = forms.ChoiceField(choices=Role.choices, initial=Role.VIEWER)
    email = forms.EmailField(required=False)
    first_name = forms.CharField(max_length=150, required=False)
    last_name = forms.CharField(max_length=150, required=False)
    can_view_raw_rows = forms.BooleanField(
        required=False,
        label="May view raw rows of sensitive data",
        help_text="Previews and downloads of data, bad rows and previews of sensitive jobs.",
    )
    is_service_account = forms.BooleanField(
        required=False,
        label="Service account",
        help_text="Signs in only with API tokens, never with a password.",
    )
    password1 = forms.CharField(
        label="Password",
        strip=False,
        required=False,
        widget=_password(),
        help_text="Leave empty for service accounts (or to set it later).",
    )
    password2 = forms.CharField(
        label="Password again", strip=False, required=False, widget=_password()
    )

    def clean(self):
        cleaned = super().clean()
        if cleaned.get("password1") != cleaned.get("password2"):
            raise forms.ValidationError("The two passwords are not the same.")
        return cleaned

    def service_values(self) -> dict:
        data = dict(self.cleaned_data)
        password = data.pop("password1") or None
        data.pop("password2")
        return {**data, "password": password}


class UserEditForm(forms.Form):
    email = forms.EmailField(required=False)
    first_name = forms.CharField(max_length=150, required=False)
    last_name = forms.CharField(max_length=150, required=False)
    role = forms.ChoiceField(choices=Role.choices)
    can_view_raw_rows = UserCreateForm.base_fields["can_view_raw_rows"]
    is_active = forms.BooleanField(
        required=False, label="Active", help_text="Inactive users cannot sign in."
    )


# --------------------------------------------------------------------------- schemas


class SchemaCreateForm(forms.Form):
    name = forms.CharField(max_length=200)
    description = forms.CharField(required=False, widget=_textarea(2))
    document = JSONObjectField(
        rows=24,
        label="Schema document (JSON)",
        error_messages={"required": "A schema needs a document: a JSON object."},
    )
    notes = forms.CharField(required=False, label="Notes for version 1", widget=_textarea(2))


class SchemaMetaForm(forms.Form):
    name = forms.CharField(max_length=200)
    description = forms.CharField(required=False, widget=_textarea(2))


class VersionForm(forms.Form):
    document = SchemaCreateForm.base_fields["document"]
    notes = forms.CharField(
        required=False, label="What changed in this version", widget=_textarea(2)
    )


class ValidateForm(forms.Form):
    """A draft document checked against an upload's header (or a dataset's source)."""

    source = forms.ChoiceField(label="Check against")
    delimiter = forms.CharField(max_length=1, required=False, strip=False)
    document = forms.CharField(strip=False)

    def __init__(self, *args, uploads, datasets, **kwargs):
        super().__init__(*args, **kwargs)
        choices = []
        if uploads:
            choices.append(
                ("Your uploads", [(f"upload:{u.pk}", f"{u.filename} ({u.pk})") for u in uploads])
            )
        if datasets:
            choices.append(("Datasets", [(f"dataset:{d.pk}", d.name) for d in datasets]))
        self.fields["source"].choices = choices

    def clean_document(self):
        try:
            return json.loads(self.cleaned_data["document"])
        except json.JSONDecodeError as error:
            raise forms.ValidationError(
                f"The document is not valid JSON: {error.msg} (line {error.lineno}, column "
                f"{error.colno})."
            ) from None

    def target(self) -> dict:
        kind, _, pk = self.cleaned_data["source"].partition(":")
        return {f"{kind}_id": pk}

    def input_options(self) -> dict:
        delimiter = self.cleaned_data["delimiter"]
        return {"delimiter": delimiter} if delimiter else {}


# --------------------------------------------------------------------------- datasets


def version_choices(versions) -> list:
    """Schema versions grouped by schema, newest version first."""
    groups: dict = {}
    for version in versions:
        groups.setdefault(version.schema.name, []).append(
            (str(version.pk), f"{version.schema.name} v{version.number}")
        )
    return list(groups.items())


class DatasetForm(forms.Form):
    name = forms.CharField(max_length=200)
    description = forms.CharField(required=False, widget=_textarea(2))
    classification = forms.ChoiceField(
        choices=Classification.choices, initial=Classification.INTERNAL
    )
    schema_version = forms.ChoiceField(label="Schema version")
    input_format = forms.ChoiceField(choices=InputFormat.choices, initial=InputFormat.CSV)
    input_options = JSONObjectField(required=False, label="Input options (JSON)")
    source_connection = forms.ChoiceField(required=False, label="Source")
    source_path = forms.CharField(
        required=False,
        max_length=1024,
        help_text="s3 sources: the object's key, relative to the connection's prefix.",
    )
    destination_connection = forms.ChoiceField(required=False, label="Destination")
    destination_prefix = forms.CharField(
        required=False,
        max_length=1024,
        help_text="s3 destinations: a key prefix; each run writes under <prefix>/<job id>/.",
    )
    destination_table = forms.CharField(required=False, label="Table (sql destinations)")
    destination_schema_name = forms.CharField(
        required=False, label="Database schema (sql destinations)"
    )
    destination_mode = forms.ChoiceField(
        required=False,
        label="Table mode (sql destinations)",
        choices=[("", "append (default)")] + [(mode, mode) for mode in TABLE_MODES],
    )
    destination_key_columns = forms.CharField(
        required=False,
        label="Key columns (sql destinations)",
        help_text="Comma-separated; needed for upsert.",
    )
    compression = forms.ChoiceField(choices=COMPRESSIONS, initial="snappy")
    options = JSONObjectField(required=False, label="Job options (JSON)")

    def __init__(self, *args, versions, connections, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["schema_version"].choices = version_choices(versions)
        usable = [(str(c.pk), f"{c.name} ({c.kind})") for c in connections]
        self.fields["source_connection"].choices = [
            ("", "Uploads: each run names a file")
        ] + usable
        self.fields["destination_connection"].choices = [
            ("", "None: keep the outputs as job artifacts")
        ] + usable

    @staticmethod
    def initial_for(dataset) -> dict:
        options = dataset.destination_options
        return {
            "name": dataset.name,
            "description": dataset.description,
            "classification": dataset.classification,
            "schema_version": str(dataset.schema_version_id),
            "input_format": dataset.input_format,
            "input_options": dataset.input_options,
            "source_connection": str(dataset.source_connection_id or ""),
            "source_path": dataset.source_path,
            "destination_connection": str(dataset.destination_connection_id or ""),
            "destination_prefix": dataset.destination_prefix,
            "destination_table": options.get("table", ""),
            "destination_schema_name": options.get("schema_name", ""),
            "destination_mode": options.get("mode", ""),
            "destination_key_columns": ", ".join(options.get("key_columns") or []),
            "compression": dataset.compression,
            "options": dataset.options,
        }

    def service_values(self) -> dict:
        data = self.cleaned_data
        destination = {
            "table": data["destination_table"].strip(),
            "schema_name": data["destination_schema_name"].strip(),
            "mode": data["destination_mode"],
            "key_columns": [
                column.strip()
                for column in data["destination_key_columns"].split(",")
                if column.strip()
            ],
        }
        return {
            "name": data["name"],
            "description": data["description"],
            "classification": data["classification"],
            "schema_version_id": data["schema_version"],
            "input_format": data["input_format"],
            "input_options": data["input_options"],
            "source_connection_id": data["source_connection"] or None,
            "source_path": data["source_path"].strip(),
            "destination_connection_id": data["destination_connection"] or None,
            "destination_prefix": data["destination_prefix"].strip(),
            "destination_options": {key: value for key, value in destination.items() if value},
            "compression": data["compression"],
            "options": data["options"],
        }


class DatasetRunForm(forms.Form):
    upload = forms.ChoiceField(label="File to run", required=False)
    idempotency_key = forms.CharField(widget=forms.HiddenInput, required=False)

    def __init__(self, *args, uploads, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["upload"].choices = [(str(u.pk), f"{u.filename} ({u.pk})") for u in uploads]


# --------------------------------------------------------------------------- uploads and jobs

UPLOAD_ACTIONS = [
    ("run", "Run it with a schema version"),
    ("validate", "Validate a schema version against it first"),
    ("generate", "Generate a schema from it"),
    ("preview", "Preview its first rows"),
    ("dataset", "Run a dataset that reads uploads"),
]


class UploadRunForm(forms.Form):
    """What to do with an uploaded file, and how to read it."""

    action = forms.ChoiceField(
        choices=UPLOAD_ACTIONS, widget=forms.RadioSelect, label="What to do", initial="run"
    )
    schema_version = forms.ChoiceField(required=False, label="Schema version (to run or validate)")
    dataset = forms.ChoiceField(required=False, label="Dataset (to run a dataset)")
    format = forms.ChoiceField(choices=UPLOAD_FORMATS, initial=InputFormat.CSV)
    delimiter = forms.CharField(max_length=1, required=False, strip=False, label="CSV delimiter")
    encoding = forms.CharField(max_length=40, required=False, label="CSV encoding")
    header_mode = forms.ChoiceField(choices=HEADER_MODES, required=False, label="CSV header")
    sheet = forms.CharField(
        max_length=100, required=False, label="Excel sheet", help_text="A name or a 0-based index."
    )
    more_options = JSONObjectField(
        required=False,
        rows=3,
        label="More input options (JSON)",
        help_text='For example {"excess_column_mode": "reject"}.',
    )
    idempotency_key = forms.CharField(widget=forms.HiddenInput, required=False)

    def __init__(self, *args, versions, datasets, actions, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["action"].choices = [c for c in UPLOAD_ACTIONS if c[0] in actions]
        self.fields["schema_version"].choices = [("", "Choose a schema version")] + (
            version_choices(versions)
        )
        self.fields["dataset"].choices = [("", "Choose a dataset")] + [
            (str(d.pk), d.name) for d in datasets
        ]

    def clean(self):
        cleaned = super().clean()
        action = cleaned.get("action")
        if action in {"run", "validate"} and not cleaned.get("schema_version"):
            self.add_error("schema_version", "Choose the schema version to use.")
        if action == "dataset" and not cleaned.get("dataset"):
            self.add_error("dataset", "Choose the dataset to run.")
        return cleaned

    def input_options(self) -> dict:
        data = self.cleaned_data
        options = dict(data["more_options"])
        for name in ("delimiter", "encoding", "header_mode"):
            if data[name]:
                options[name] = data[name]
        if data["sheet"]:
            options["sheet"] = int(data["sheet"]) if data["sheet"].isdigit() else data["sheet"]
        return options

    @property
    def kind(self) -> str:
        return {
            "run": JobKind.RUN,
            "validate": JobKind.VALIDATE_SCHEMA,
            "generate": JobKind.GENERATE_SCHEMA,
            "preview": JobKind.PREVIEW,
        }[self.cleaned_data["action"]]


class JobFilterForm(forms.Form):
    status = forms.ChoiceField(
        required=False, choices=[("", "Any status")] + list(JobStatus.choices)
    )
    kind = forms.ChoiceField(required=False, choices=[("", "Any kind")] + list(JobKind.choices))
    mine = forms.BooleanField(required=False, label="Only my jobs")


class AdminJobFilterForm(JobFilterForm):
    lane = forms.ChoiceField(required=False, choices=[("", "Any lane")] + list(Lane.choices))
    requester = forms.CharField(required=False, label="Requested by (username)")


# --------------------------------------------------------------------------- connections


class ConnectionForm(forms.Form):
    """Common fields of a connection; subclasses add their kind's configuration. Secrets are
    write-only: their inputs are always empty, an empty one keeps the stored value."""

    kind = ""
    config_fields: tuple = ()

    name = forms.CharField(max_length=100)
    description = forms.CharField(required=False, widget=_textarea(2))
    allowed_roles = forms.MultipleChoiceField(
        choices=Role.choices,
        required=False,
        widget=forms.CheckboxSelectMultiple,
        label="Roles that may use it in datasets",
        help_text="Admins always may.",
        initial=[Role.AUTHOR.value, Role.ADMIN.value],
    )

    def __init__(self, *args, connection=None, **kwargs):
        if connection is not None:
            kwargs.setdefault("initial", self.initial_for(connection))
        super().__init__(*args, **kwargs)
        self.connection = connection
        spec = SECRET_FIELDS[self.kind]
        self.secret_names = sorted(spec["required"]) + sorted(spec["optional"])
        for name in self.secret_names:
            is_set = connection is not None and name in connection.secret_fields
            self.fields[f"secret_{name}"] = forms.CharField(
                label=SECRET_LABELS[name],
                required=connection is None and name in spec["required"],
                strip=False,
                widget=_password(),
                help_text=(
                    "Set. Type a new value to replace it, or leave this empty to keep it."
                    if is_set
                    else ("Not set." if connection is not None else "")
                ),
            )
            if is_set and name in spec["optional"]:
                self.fields[f"remove_{name}"] = forms.BooleanField(
                    required=False, label=f"Remove the {SECRET_LABELS[name].lower()}"
                )

    def config_boundfields(self) -> list:
        return [self[name] for name in self.config_fields]

    def secret_boundfields(self) -> list:
        return [self[name] for name in self.fields if name.startswith(("secret_", "remove_"))]

    @classmethod
    def initial_for(cls, connection) -> dict:
        config = connection.config
        return {
            "name": connection.name,
            "description": connection.description,
            "allowed_roles": connection.allowed_roles,
            **{name: config.get(name, "") for name in cls.config_fields},
        }

    def config(self) -> dict:
        return {name: self.cleaned_data[name] for name in self.config_fields}

    def secrets(self) -> dict:
        """New values (and, when editing, None for each secret to remove)."""
        found = {}
        for name in self.secret_names:
            value = self.cleaned_data[f"secret_{name}"]
            if value:
                found[name] = value
            elif self.cleaned_data.get(f"remove_{name}"):
                found[name] = None
        return found


class S3ConnectionForm(ConnectionForm):
    kind = "s3"
    config_fields = ("bucket", "endpoint_url", "prefix", "region", "addressing_style")

    bucket = forms.CharField(max_length=63)
    endpoint_url = forms.CharField(
        required=False,
        label="Endpoint URL",
        help_text="For example https://s3.example.org; leave it empty for AWS S3.",
    )
    prefix = forms.CharField(required=False, help_text="A key prefix inside the bucket.")
    region = forms.CharField(required=False, initial="us-east-1")
    addressing_style = forms.ChoiceField(
        choices=[("path", "path"), ("virtual", "virtual"), ("auto", "auto")], initial="path"
    )


class LocalfsConnectionForm(ConnectionForm):
    kind = "localfs"
    config_fields = ("root_path",)

    root_path = forms.CharField(
        label="Root path", help_text="An absolute path that workers mount, e.g. /data/exports."
    )


class SqlConnectionForm(ConnectionForm):
    kind = "sql"
    config_fields = ("dialect", "host", "port", "database", "username", "driver", "options")

    dialect = forms.ChoiceField(choices=SqlDialect.choices)
    host = forms.CharField()
    port = forms.IntegerField(
        required=False,
        min_value=1,
        max_value=65535,
        help_text="Default: "
        + ", ".join(f"{d.label} {DIALECTS[d].port}" for d in SqlDialect)
        + ".",
    )
    database = forms.CharField(help_text="Oracle: the service name.")
    username = forms.CharField(label="Login")
    driver = forms.CharField(
        required=False,
        label="ODBC driver",
        help_text="Default: " + "; ".join(f"{d.label}: {DIALECTS[d].driver}" for d in SqlDialect),
    )
    options = JSONObjectField(
        required=False,
        rows=2,
        label="ODBC attributes (JSON)",
        help_text='For example {"Encrypt": "yes"}.',
    )

    def config(self) -> dict:
        config = super().config()
        return {key: value for key, value in config.items() if value not in (None, "", {})}


CONNECTION_FORMS = {
    form.kind: form for form in (S3ConnectionForm, LocalfsConnectionForm, SqlConnectionForm)
}


# --------------------------------------------------------------------------- retention

RETENTION_MODES = [
    ("inherit", "Inherit"),
    ("forever", "Keep until deleted"),
    ("days", "Keep n days"),
]


def retention_prefix(scope: str, target: str = "") -> str:
    return f"{scope}-{target}" if target else scope


class RetentionForm(forms.Form):
    """A policy: for each retention kind, inherit, keep until deleted, or keep n days."""

    def __init__(self, *args, days=None, **kwargs):
        super().__init__(*args, **kwargs)
        days = days or {}
        for kind, label in RetentionKind.choices:
            mode = "inherit" if kind not in days else ("forever" if days[kind] is None else "days")
            self.fields[f"{kind}_mode"] = forms.ChoiceField(
                choices=RETENTION_MODES, label=label, initial=mode
            )
            self.fields[f"{kind}_days"] = forms.IntegerField(
                required=False,
                min_value=0,
                label=f"{label}: days to keep",
                initial=days.get(kind),
            )

    def kinds(self):
        """(label, mode field, days field) for each retention kind, for the template."""
        return [
            (label, self[f"{kind}_mode"], self[f"{kind}_days"])
            for kind, label in RetentionKind.choices
        ]

    def clean(self):
        cleaned = super().clean()
        for kind, label in RetentionKind.choices:
            if cleaned.get(f"{kind}_mode") == "days" and cleaned.get(f"{kind}_days") is None:
                self.add_error(f"{kind}_days", f"Give the number of days to keep {label}.")
        return cleaned

    def days(self) -> dict:
        found = {}
        for kind in RetentionKind.values:
            mode = self.cleaned_data[f"{kind}_mode"]
            if mode == "forever":
                found[kind] = None
            elif mode == "days":
                found[kind] = self.cleaned_data[f"{kind}_days"]
        return found


# --------------------------------------------------------------------------- audit


class AuditFilterForm(forms.Form):
    action = forms.ChoiceField(required=False)
    actor = forms.CharField(required=False, label="Actor (username)")
    object_type = forms.ChoiceField(required=False, label="Object type")
    object_id = forms.CharField(required=False, label="Object id")
    since = forms.DateTimeField(
        required=False,
        label="From (UTC)",
        input_formats=DATETIME_FORMATS,
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}, format="%Y-%m-%dT%H:%M"),
    )
    until = forms.DateTimeField(
        required=False,
        label="Until (UTC)",
        input_formats=DATETIME_FORMATS,
        widget=forms.DateTimeInput(attrs={"type": "datetime-local"}, format="%Y-%m-%dT%H:%M"),
    )

    def __init__(self, *args, actions, object_types, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["action"].choices = [("", "Any action")] + [(a, a) for a in actions]
        self.fields["object_type"].choices = [("", "Any type")] + [(t, t) for t in object_types]


# --------------------------------------------------------------------------- settings

SETTING_CHOICES = {"default_classification": Classification.choices}


class SettingForm(forms.Form):
    """One installation setting, with a field that fits its default's type."""

    def __init__(self, *args, key: str, setting: dict, **kwargs):
        super().__init__(*args, **kwargs)
        self.key = key
        default, value = setting["default"], setting["value"]
        if key in SETTING_CHOICES:
            self.shape = "choice"
            self.fields["value"] = forms.ChoiceField(
                choices=SETTING_CHOICES[key], initial=value, label="New value"
            )
        elif isinstance(default, dict) and all(isinstance(v, dict) for v in default.values()):
            self.shape = "table"
            self.columns = list(next(iter(default.values())))
            self.groups = list(default)
            for group, limits in default.items():
                for name in limits:
                    self.fields[f"{group}__{name}"] = forms.IntegerField(
                        required=False,
                        min_value=0,
                        label=f"{group}: {name}",
                        initial=value.get(group, {}).get(name),
                    )
        elif default is None:
            self.shape = "optional_integer"
            self.fields["value"] = forms.IntegerField(
                required=False, initial=value, label="New value", help_text="Empty: no limit."
            )
        elif isinstance(default, int) and not isinstance(default, bool):
            self.shape = "integer"
            self.fields["value"] = forms.IntegerField(initial=value, label="New value")
        else:
            self.shape = "json"
            self.fields["value"] = forms.CharField(
                initial=json.dumps(value),
                label="New value (JSON)",
                widget=_textarea(2, **{"class": "code"}),
            )

    def table(self) -> list:
        """(group, [bound fields in column order]) rows of a table-shaped setting."""
        return [
            (group, [self[f"{group}__{name}"] for name in self.columns]) for group in self.groups
        ]

    def clean_value(self):
        value = self.cleaned_data["value"]
        if self.shape != "json":
            return value
        try:
            return json.loads(value)
        except json.JSONDecodeError as error:
            raise forms.ValidationError(f"This is not valid JSON: {error.msg}.") from None

    def setting_value(self):
        if self.shape != "table":
            return self.cleaned_data["value"]
        table: dict = {}
        for name, value in self.cleaned_data.items():
            group, _, limit = name.partition("__")
            table.setdefault(group, {})[limit] = value
        return table
