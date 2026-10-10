"""``forklift-worker``: settings, logging, signal handling, then the supervisor loop.

Exit codes: 0 after a clean stop, 1 after an internal error, 2 for invalid settings or an
isolation profile this platform cannot provide, 3 when the gateway refuses the worker (its token,
or its internal API version).
"""

from __future__ import annotations

import os
import signal
import sys
from typing import NoReturn, Sequence

from . import linux, logs
from .settings import SettingsError, load_settings
from .supervisor import EXIT_CONFIG, StartupError, Supervisor

log = logs.logger("cli")


def main(argv: Sequence[str] | None = None) -> NoReturn:
    try:
        settings = load_settings(sys.argv[1:] if argv is None else argv, os.environ)
    except SettingsError as error:
        print(f"forklift-worker: {error}", file=sys.stderr)
        sys.exit(EXIT_CONFIG)
    logs.configure(settings.log_level, settings.log_format)
    # The supervisor holds the worker token: keep other processes of this user (the engine
    # among them) out of its memory, environment and file descriptors, and write no core dump.
    linux.set_dumpable(False)
    supervisor = Supervisor(settings)
    handlers = {
        number: signal.signal(number, lambda *_: supervisor.request_stop())
        for number in (signal.SIGTERM, signal.SIGINT)
    }
    try:
        code = supervisor.run()
    except StartupError as error:
        log.error("%s", error)
        code = EXIT_CONFIG
    finally:
        for number, handler in handlers.items():
            signal.signal(number, handler)
    sys.exit(code)
