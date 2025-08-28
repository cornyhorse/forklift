#!/usr/bin/env python3
from __future__ import annotations
import argparse
import subprocess
import os
import sys
from pathlib import Path
from typing import Iterable, List
import time
import socket

# Optional .env
try:
    from dotenv import load_dotenv  # type: ignore

    load_dotenv()
except Exception:
    pass

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

SCRIPT_DIR = Path(__file__).resolve().parent


def _wait_for_tcp(host: str, port: int, timeout: int = 180) -> bool:
    """Wait until a TCP port is accepting connections. Returns True if ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with socket.create_connection((host, port), timeout=2):
                return True
        except OSError:
            time.sleep(2)
    return False


def _wait_for_oracle(dsn: str, user: str, password: str, timeout: int = 900, interval: int = 5) -> None:
    """
    Poll the Oracle PDB until we can connect and run a trivial query.
    Prints each attempt and the error if it fails.
    Raises RuntimeError if it never becomes ready within timeout seconds.
    """
    import time
    import datetime
    import oracledb  # thin driver
    last = None
    attempt = 0
    deadline = time.time() + timeout
    while time.time() < deadline:
        attempt += 1
        if attempt == 1:
            try:
                import oracledb
                print(f"[oracle] python-oracledb version: {getattr(oracledb, '__version__', 'unknown')}")
            except Exception:
                pass
        now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[oracle] connect attempt {attempt}: {user}@{dsn} at {now}")
        try:
            with oracledb.connect(user=user, password=password, dsn=dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT 1 FROM dual")
                    cur.fetchone()
            print("[oracle] PDB connection successful; database is ready.")
            return
        except Exception as e:
            last = e
            # Print a concise error and retry info
            print(f"[oracle] attempt {attempt} failed: {e.__class__.__name__}: {e}")
            print(f"[oracle] retrying in {interval}s...")
            time.sleep(interval)
    raise RuntimeError(f"Oracle never became ready after {attempt} attempts. Last error: {last}")


def find_sql_root(cli_override: str | None) -> Path:
    # Priority: CLI -> ENV -> common locals (correct) -> legacy (previous buggy path)
    if cli_override:
        p = Path(cli_override).resolve()
        if p.exists():
            return p
        print(f"[warn] --sql-root '{p}' not found, ignoring.", file=sys.stderr)

    env_root = os.environ.get("SQL_SOURCE_ROOT")
    if env_root:
        p = Path(env_root).resolve()
        if p.exists():
            return p
        print(f"[warn] SQL_SOURCE_ROOT '{p}' not found, ignoring.", file=sys.stderr)

    candidates = [
        SCRIPT_DIR / "source-sql-ddl-and-data",  # ✅ correct for your layout
        SCRIPT_DIR.parent / "source-sql-ddl-and-data",  # one level up, just in case
        SCRIPT_DIR / "tests" / "test-files" / "sql" / "source-sql-ddl-and-data",
    ]
    for c in candidates:
        if c.exists():
            return c.resolve()

    # Nothing found
    print("[error] Could not locate SQL root. Tried:\n  " + "\n  ".join(map(str, candidates)), file=sys.stderr)
    sys.exit(2)


def _read(path: Path) -> str:
    with path.open("r", encoding="utf-8") as f:
        return f.read()


def _split_pg_mysql(sql: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    for line in sql.splitlines():
        buf.append(line)
        if line.rstrip().endswith(";"):
            parts.append("\n".join(buf).strip().rstrip(";"))
            buf = []
    if buf:
        tail = "\n".join(buf).strip()
        if tail:
            parts.append(tail)
    return [p for p in parts if p]


def _split_mssql(sql: str) -> List[str]:
    parts: List[str] = []
    buf: List[str] = []
    for line in sql.splitlines():
        if line.strip().upper() == "GO":
            chunk = "\n".join(buf).strip()
            if chunk:
                parts.append(chunk)
            buf = []
        else:
            buf.append(line)
    last = "\n".join(buf).strip()
    if last:
        parts.append(last)
    return parts


def _split_oracle(sql_text: str) -> list[str]:
    """
    Split Oracle SQL/PLSQL script into executable statements.

    Rules:
      - PL/SQL blocks (BEGIN...END; / or DECLARE...BEGIN...END; /) are terminated ONLY by a line containing just "/".
        We do not split on semicolons inside those blocks.
      - Standalone SQL statements (e.g., ALTER SESSION, CREATE USER, GRANT, etc.) are terminated by a trailing ";".
      - We keep lines verbatim; no stripping of internal semicolons.
    Heuristic to detect PL/SQL:
      - We consider ourselves "inside PL/SQL" after we see a line that starts with BEGIN or DECLARE (ignoring leading whitespace)
        and remain so until we hit a "/" on its own line.
    This is robust enough for typical SQL*Plus-style files.
    """
    statements: list[str] = []
    buf: list[str] = []
    in_plsql = False

    def flush_buffer():
        nonlocal buf
        stmt = "\n".join(buf).strip()
        if stmt:
            statements.append(stmt)
        buf = []

    for raw_line in sql_text.splitlines():
        line = raw_line.rstrip("\n")
        stripped = line.strip()

        # If we encounter a PL/SQL start keyword at the beginning of a line, enter PL/SQL mode
        if not in_plsql and (stripped.upper().startswith("BEGIN") or stripped.upper().startswith("DECLARE")):
            in_plsql = True

        if in_plsql:
            # In PL/SQL mode: only "/" on its own line ends the block
            if stripped == "/":
                flush_buffer()
                in_plsql = False
            else:
                buf.append(line)
            continue

        # Not in PL/SQL mode:
        buf.append(line)
        if stripped.endswith(";"):  # end of a simple SQL statement
            flush_buffer()

    # Flush any trailing content
    tail = "\n".join(buf).strip()
    if tail:
        statements.append(tail)

    return statements


def _strip_leading_sql_comments(s: str) -> str:
    """Remove leading '--' comment lines and blank lines from a SQL chunk."""
    lines = s.splitlines()
    i = 0
    # Drop leading comment lines
    while i < len(lines) and lines[i].lstrip().startswith("--"):
        i += 1
    # Drop any subsequent leading blank lines
    while i < len(lines) and lines[i].strip() == "":
        i += 1
    return "\n".join(lines[i:]).strip()


def _run_statements(engine: Engine, statements: Iterable[str], label: str) -> None:
    with engine.begin() as conn:
        for i, stmt in enumerate(statements, 1):
            s = _strip_leading_sql_comments(stmt.strip())
            if not s:
                continue

            # For Oracle: strip a single trailing ";" for standalone SQL (drivers reject it),
            # but keep semicolons inside PL/SQL blocks (BEGIN/DECLARE).
            if label == "oracle":
                head = s.lstrip().upper()
                is_plsql = head.startswith("BEGIN") or head.startswith("DECLARE")
                if not is_plsql and s.endswith(";"):
                    s = s[:-1]

            print(f"[{label}] ({i}) executing:\n{s[:240]}{'...' if len(s) > 240 else ''}\n")
            # Use raw string with exec_driver_sql (do NOT wrap in text())
            conn.exec_driver_sql(s)
            # Alternatively:
            # from sqlalchemy import text as sa_text
            # conn.execute(sa_text(s))


def hydrate_postgres(sql_root: Path) -> None:
    user = os.environ.get("POSTGRES_USER", "testuser")
    pwd = os.environ.get("POSTGRES_PASSWORD", "testpass")
    db = os.environ.get("POSTGRES_DB", "testdb")
    url = f"postgresql+psycopg://{user}:{pwd}@127.0.0.1:5432/{db}"
    sql_file = sql_root / "pg" / "001-sales-alt.sql"
    if not sql_file.exists():
        print(f"[pg] Skipping: {sql_file} not found")
        return
    from sqlalchemy import create_engine
    engine = create_engine(url, future=True)
    _run_statements(engine, _split_pg_mysql(_read(sql_file)), "pg")


def hydrate_mysql(sql_root: Path) -> None:
    # Use root to allow CREATE DATABASE/USE statements in the script.
    root_pwd = os.environ.get("MYSQL_ROOT_PASSWORD", "root")
    # Connect without selecting a default DB so CREATE DATABASE works cleanly.
    url = f"mysql+pymysql://root:{root_pwd}@127.0.0.1:3306/"
    sql_file = sql_root / "mysql" / "001-sales-alt.sql"
    if not sql_file.exists():
        print(f"[mysql] Skipping: {sql_file} not found")
        return
    engine = create_engine(url, future=True)
    _run_statements(engine, _split_pg_mysql(_read(sql_file)), "mysql")


def hydrate_mssql(sql_root: Path) -> None:
    sa_pwd = os.environ.get("MSSQL_SA_PASSWORD", "YourStrong!Passw0rd")
    params = "driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"
    url_master = f"mssql+pyodbc://sa:{sa_pwd}@127.0.0.1:1433/master?{params}"
    url_testdb = f"mssql+pyodbc://sa:{sa_pwd}@127.0.0.1:1433/testdb?{params}"
    sql_file = sql_root / "mssql" / "sales-alt.sql"
    if not sql_file.exists():
        print(f"[mssql] Skipping: {sql_file} not found")
        return

    try:
        # Ensure testdb exists OUTSIDE a transaction
        eng_master = create_engine(url_master, future=True)
        with eng_master.connect() as conn:
            conn = conn.execution_options(isolation_level="AUTOCOMMIT")
            conn.exec_driver_sql("IF DB_ID(N'testdb') IS NULL CREATE DATABASE [testdb];")

        # Now run the script against testdb (normal transactional begin is fine)
        eng_db = create_engine(url_testdb, future=True)
        _run_statements(eng_db, _split_mssql(_read(sql_file)), "mssql")

    except Exception as e:
        msg = str(e)
        if "pyodbc" in msg or "libodbc" in msg or "ODBC" in msg:
            print("[mssql] pyodbc/ODBC path not available. Falling back to Docker sqlcmd runner...")
            _hydrate_mssql_via_docker(sql_file, sa_pwd, db_name="testdb")
        else:
            raise


def _hydrate_mssql_via_docker(sql_file: Path, sa_pwd: str, db_name: str = "testdb") -> None:
    """
    Run MSSQL script with sqlcmd inside a throwaway container.
    Uses mssql-tools18 (has /opt/mssql-tools18/bin/sqlcmd).
    On Apple Silicon, force amd64.
    """
    if not sql_file.exists():
        print(f"[mssql] Skipping: {sql_file} not found")
        return

    image = "mcr.microsoft.com/mssql-tools18"
    sqlcmd = "/opt/mssql-tools18/bin/sqlcmd"
    base = [
        "docker", "run", "--rm",
        "--platform", "linux/amd64",
        "-e", "ACCEPT_EULA=Y",
    ]

    # 1) Ensure DB exists
    create_cmd = base + [
        image, sqlcmd,
        "-S", "host.docker.internal,1433",
        "-U", "sa",
        "-P", sa_pwd,
        "-C",
        "-l", "30",
        "-Q", f"IF DB_ID(N'{db_name}') IS NULL CREATE DATABASE [{db_name}];",
    ]
    print("[mssql] Ensuring DB exists via docker:", " ".join(create_cmd))
    res1 = subprocess.run(create_cmd, capture_output=True, text=True)
    if res1.returncode != 0:
        print(res1.stdout)
        print(res1.stderr, file=sys.stderr)
        raise RuntimeError(f"sqlcmd (create db) failed with code {res1.returncode}")

    # 2) Run script against db_name
    run_cmd = base + [
        "-v", f"{sql_file.parent}:/sql:ro",
        image, sqlcmd,
        "-S", "host.docker.internal,1433",
        "-U", "sa",
        "-P", sa_pwd,
        "-C",
        "-l", "30",
        "-d", db_name,
        "-i", f"/sql/{sql_file.name}",
    ]
    print("[mssql] Running script via docker:", " ".join(run_cmd))
    res2 = subprocess.run(run_cmd, capture_output=True, text=True)
    if res2.returncode != 0:
        print(res2.stdout)
        print(res2.stderr, file=sys.stderr)
        raise RuntimeError(f"sqlcmd (run file) failed with code {res2.returncode}")
    else:
        if res2.stdout.strip():
            print(res2.stdout)
        print("[mssql] Script applied successfully via docker.")


def hydrate_oracle(sql_root: Path) -> None:
    # Defaults tuned for Oracle Database Free 23ai image
    host = os.environ.get("ORACLE_HOST", "127.0.0.1")
    port = int(os.environ.get("ORACLE_PORT", "1521"))
    system_pwd = os.environ.get("ORACLE_PWD", "YourStrong!Passw0rd")
    pdb = os.environ.get("ORACLE_PDB", "FREEPDB1")  # ORCLPDB1 for EE/SE; FREEPDB1 for Oracle Free

    # SQLAlchemy URL (service name style) and thin-driver DSN for readiness check
    url = f"oracle+oracledb://system:{system_pwd}@{host}:{port}/?service_name={pdb}"
    dsn = f"{host}:{port}/{pdb}"

    sql_file = sql_root / "oracle" / "sales-alt.sql"
    if not sql_file.exists():
        print(f"[oracle] Skipping: {sql_file} not found")
        return

    # 1) Wait for TCP listener
    if not _wait_for_tcp(host, port, timeout=240):
        print(f"[oracle] Listener on {host}:{port} not accepting connections after wait; skipping.")
        return

    # 2) Wait for the PDB service to be open and registered
    print(f"[oracle] Waiting for PDB service {pdb} to be ready...")
    try:
        _wait_for_oracle(dsn, "SYSTEM", system_pwd, timeout=900, interval=5)
    except Exception as e:
        print(f"[oracle] Database did not become ready: {e}")
        raise

    # 3) Run hydration statements
    engine = create_engine(url, future=True)
    _run_statements(engine, _split_oracle(_read(sql_file)), "oracle")


def main() -> None:
    ap = argparse.ArgumentParser(description="Hydrate local DBs with test schemas/data.")
    ap.add_argument("--only", choices=["pg", "mysql", "mssql", "oracle"], help="Run only one engine")
    ap.add_argument("--sql-root", help="Override path to source-sql-ddl-and-data/")
    args = ap.parse_args()

    sql_root = find_sql_root(args.sql_root)
    print(f"[info] Using SQL root: {sql_root}")

    # if args.only in (None, "pg"):
    #     hydrate_postgres(sql_root)
    # if args.only in (None, "mysql"):
    #     hydrate_mysql(sql_root)
    if args.only in (None, "mssql"):
        hydrate_mssql(sql_root)
    # if args.only in (None, "oracle"):
    #     hydrate_oracle(sql_root)


if __name__ == "__main__":
    main()
