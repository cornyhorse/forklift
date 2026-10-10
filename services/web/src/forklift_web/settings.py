"""Django settings for forklift-web, read from ``FORKLIFT_...`` environment variables.

Deployment settings live here (database, object store, secrets, ports); settings an admin changes
at run time (``stage_max_bytes``, lease length, limits, ...) are installation settings in the
database (``forklift_web.services.installation``).

Required: ``FORKLIFT_SECRET_KEY``, ``FORKLIFT_SECRETS_KEYS``, ``FORKLIFT_DB_*`` (host, name, user,
password) and ``FORKLIFT_S3_*`` (bucket and credentials). Everything else has a default.
"""

from __future__ import annotations

from forklift_web.conf import env_bool, env_int, env_list, env_optional, env_str

SECRET_KEY = env_str("FORKLIFT_SECRET_KEY")
DEBUG = env_bool("FORKLIFT_DEBUG", False)
ALLOWED_HOSTS = env_list("FORKLIFT_ALLOWED_HOSTS", "localhost,127.0.0.1")
CSRF_TRUSTED_ORIGINS = env_list("FORKLIFT_CSRF_TRUSTED_ORIGINS")

INSTALLED_APPS = [
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django.contrib.sessions",
    "django.contrib.messages",
    "django.contrib.staticfiles",
    "ninja",
    "forklift_web.core",
]

MIDDLEWARE = [
    "forklift_web.middleware.SurfaceMiddleware",
    "forklift_web.middleware.RequestIdMiddleware",
    "django.middleware.security.SecurityMiddleware",
    "django.contrib.sessions.middleware.SessionMiddleware",
    "django.middleware.common.CommonMiddleware",
    "django.middleware.csrf.CsrfViewMiddleware",
    "django.contrib.auth.middleware.AuthenticationMiddleware",
    "django.contrib.messages.middleware.MessageMiddleware",
    "django.middleware.clickjacking.XFrameOptionsMiddleware",
]

# The public surface (UI, /api/v1) and the internal one (/internal/v1, workers only) are two URL
# configurations; forklift_web.wsgi decides per connection which one a request sees.
ROOT_URLCONF = "forklift_web.urls"
FORKLIFT_INTERNAL_URLCONF = "forklift_web.urls_internal"
FORKLIFT_PUBLIC_PORT = env_int("FORKLIFT_PUBLIC_PORT", 8080, minimum=1)
FORKLIFT_INTERNAL_PORT = env_int("FORKLIFT_INTERNAL_PORT", 8081, minimum=1)

WSGI_APPLICATION = "forklift_web.wsgi.application"

TEMPLATES = [
    {
        "BACKEND": "django.template.backends.django.DjangoTemplates",
        "DIRS": [],
        "APP_DIRS": True,
        "OPTIONS": {
            "context_processors": [
                "django.template.context_processors.request",
                "django.contrib.auth.context_processors.auth",
                "django.contrib.messages.context_processors.messages",
            ],
        },
    },
]

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.postgresql",
        "HOST": env_str("FORKLIFT_DB_HOST", "localhost"),
        "PORT": env_int("FORKLIFT_DB_PORT", 5432, minimum=1),
        "NAME": env_str("FORKLIFT_DB_NAME", "forklift"),
        "USER": env_str("FORKLIFT_DB_USER", "forklift"),
        "PASSWORD": env_str("FORKLIFT_DB_PASSWORD"),
        "CONN_MAX_AGE": env_int("FORKLIFT_DB_CONN_MAX_AGE", 60, minimum=0),
        "CONN_HEALTH_CHECKS": True,
        "OPTIONS": {"sslmode": env_str("FORKLIFT_DB_SSLMODE", "prefer")},
    }
}

AUTH_USER_MODEL = "core.User"
AUTH_PASSWORD_VALIDATORS = [
    {"NAME": "django.contrib.auth.password_validation.UserAttributeSimilarityValidator"},
    {
        "NAME": "django.contrib.auth.password_validation.MinimumLengthValidator",
        "OPTIONS": {"min_length": env_int("FORKLIFT_PASSWORD_MIN_LENGTH", 12, minimum=8)},
    },
    {"NAME": "django.contrib.auth.password_validation.CommonPasswordValidator"},
    {"NAME": "django.contrib.auth.password_validation.NumericPasswordValidator"},
]
LOGIN_URL = "forklift-login"
LOGIN_REDIRECT_URL = env_str("FORKLIFT_LOGIN_REDIRECT_URL", "/")
LOGOUT_REDIRECT_URL = "forklift-login"

# Behind a TLS-terminating ingress: trust its X-Forwarded-Proto and mark cookies secure.
FORKLIFT_BEHIND_TLS_PROXY = env_bool("FORKLIFT_BEHIND_TLS_PROXY", False)
SECURE_PROXY_SSL_HEADER = (
    ("HTTP_X_FORWARDED_PROTO", "https") if FORKLIFT_BEHIND_TLS_PROXY else None
)
SESSION_COOKIE_SECURE = env_bool("FORKLIFT_SECURE_COOKIES", FORKLIFT_BEHIND_TLS_PROXY)
CSRF_COOKIE_SECURE = SESSION_COOKIE_SECURE
SESSION_COOKIE_AGE = env_int("FORKLIFT_SESSION_SECONDS", 8 * 3600, minimum=60)
SESSION_COOKIE_HTTPONLY = True
SESSION_COOKIE_SAMESITE = "Lax"
X_FRAME_OPTIONS = "DENY"
SECURE_CONTENT_TYPE_NOSNIFF = True
SECURE_REFERRER_POLICY = "same-origin"
# Number of reverse proxies in front of the gateway whose X-Forwarded-For entries are trusted
# for the client address in the audit log (0: use the connection's address).
FORKLIFT_TRUSTED_PROXIES = env_int("FORKLIFT_TRUSTED_PROXIES", 0, minimum=0)

LANGUAGE_CODE = "en-us"
TIME_ZONE = "UTC"
USE_I18N = True
USE_TZ = True

STATIC_URL = "/static/"
STATIC_ROOT = env_optional("FORKLIFT_STATIC_ROOT")
DEFAULT_AUTO_FIELD = "django.db.models.BigAutoField"

# The interactive API documentation at /api/v1/docs (the OpenAPI document itself is always
# served at /api/v1/openapi.json and checked in as contracts/openapi.json).
FORKLIFT_API_DOCS = env_bool("FORKLIFT_API_DOCS", True)
NINJA_PAGINATION_PER_PAGE = 50
NINJA_PAGINATION_MAX_LIMIT = 500

# --------------------------------------------------------------------------- object store
#
# One S3-compatible bucket with uploads/, jobs/ and previews/ prefixes (design section 7.1). The
# gateway calls the store at FORKLIFT_S3_ENDPOINT_URL (HEAD, multipart bookkeeping, copy, delete)
# and signs URLs for two audiences that may reach the store under other names: browsers and API
# clients (FORKLIFT_S3_PUBLIC_ENDPOINT_URL) and workers (FORKLIFT_S3_WORKER_ENDPOINT_URL). The
# credential may be split by purpose: FORKLIFT_S3_{UPLOAD,DOWNLOAD,DELETE}_ACCESS_KEY_ID / ..._
# SECRET_ACCESS_KEY default to FORKLIFT_S3_ACCESS_KEY_ID / FORKLIFT_S3_SECRET_ACCESS_KEY.
_S3_ENDPOINT = env_optional("FORKLIFT_S3_ENDPOINT_URL")
_S3_KEY = env_str("FORKLIFT_S3_ACCESS_KEY_ID")
_S3_SECRET = env_str("FORKLIFT_S3_SECRET_ACCESS_KEY")
FORKLIFT_STORE = {
    "bucket": env_str("FORKLIFT_S3_BUCKET"),
    "region": env_str("FORKLIFT_S3_REGION", "us-east-1"),
    "addressing_style": env_str("FORKLIFT_S3_ADDRESSING_STYLE", "path"),
    "endpoint_url": _S3_ENDPOINT,
    "public_endpoint_url": env_optional("FORKLIFT_S3_PUBLIC_ENDPOINT_URL") or _S3_ENDPOINT,
    "worker_endpoint_url": env_optional("FORKLIFT_S3_WORKER_ENDPOINT_URL") or _S3_ENDPOINT,
    "credentials": {
        purpose: (
            env_optional(f"FORKLIFT_S3_{purpose.upper()}_ACCESS_KEY_ID") or _S3_KEY,
            env_optional(f"FORKLIFT_S3_{purpose.upper()}_SECRET_ACCESS_KEY") or _S3_SECRET,
        )
        for purpose in ("upload", "download", "delete")
    },
    "connect_timeout": env_int("FORKLIFT_S3_CONNECT_TIMEOUT", 5, minimum=1),
    "read_timeout": env_int("FORKLIFT_S3_READ_TIMEOUT", 30, minimum=1),
}

# --------------------------------------------------------------------------- secrets
#
# Connection secrets are encrypted with Fernet keys (comma-separated; the first one encrypts,
# all of them decrypt, so a key can be rotated with `forklift-web rotate_secrets`). Generate one
# with: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
FORKLIFT_SECRET_BACKEND = env_str("FORKLIFT_SECRET_BACKEND", "env")
FORKLIFT_SECRETS_KEYS = env_list("FORKLIFT_SECRETS_KEYS")

# --------------------------------------------------------------------------- contracts
#
# The job contract's JSON Schemas (jobspec.schema.json, jobresult.schema.json). Default: the
# copy packaged with forklift_web, else the repository's contracts/ directory.
FORKLIFT_CONTRACTS_DIR = env_optional("FORKLIFT_CONTRACTS_DIR")

LOGGING = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {"json": {"()": "forklift_web.logs.JsonFormatter"}},
    "handlers": {"console": {"class": "logging.StreamHandler", "formatter": "json"}},
    "root": {"handlers": ["console"], "level": env_str("FORKLIFT_LOG_LEVEL", "INFO")},
}
