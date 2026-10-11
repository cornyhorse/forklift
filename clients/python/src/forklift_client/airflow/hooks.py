"""The Airflow hook: a :class:`~forklift_client.ForkliftClient` from a ``forklift`` connection.

A connection of type ``forklift`` has the gateway's URL as its host
(``https://forklift.example.org``; a host name alone means https, with the connection's port
if it has one), an API token as its password, and optionally the extras ``ca_file`` (a PEM
bundle) and ``timeout`` (seconds). Airflow masks the password in task logs; the client never
puts the token in a message.
"""

from __future__ import annotations

from typing import Any, Optional, Tuple

from airflow.sdk import BaseHook
from airflow.sdk import __version__ as sdk_version
from airflow.sdk.exceptions import AirflowException

from forklift_client import ForkliftClient, ForkliftError, RetryPolicy


def client_for(connection: Any, *, retry: Optional[RetryPolicy] = None) -> ForkliftClient:
    """A client for an Airflow connection of type ``forklift``."""
    name = connection.conn_id
    host = (connection.host or "").strip()
    if not host:
        raise AirflowException(
            f"Connection {name!r} has no host: set it to the forklift gateway's URL, such as "
            "https://forklift.example.org"
        )
    if not connection.password:
        raise AirflowException(
            f"Connection {name!r} has no password: set it to a forklift API token (fkl_...)"
        )
    if "://" not in host:
        host = f"https://{host}" + (f":{connection.port}" if connection.port else "")
    extra = connection.extra_dejson
    options: dict = {"retry": retry, "user_agent": f"apache-airflow-task-sdk/{sdk_version}"}
    if extra.get("ca_file"):
        options["ca_file"] = extra["ca_file"]
    if extra.get("timeout") not in (None, ""):
        try:
            options["timeout"] = float(extra["timeout"])
        except (TypeError, ValueError):
            raise AirflowException(
                f"Connection {name!r}: the extra 'timeout' must be a number of seconds"
            ) from None
    try:
        return ForkliftClient(host, connection.password, **options)
    except ValueError as error:
        raise AirflowException(f"Connection {name!r}: {error}") from None


class ForkliftHook(BaseHook):
    """A forklift gateway, from an Airflow connection of type ``forklift``.

    :param forklift_conn_id: the connection (default ``forklift_default``)
    :param retry: the client's :class:`~forklift_client.RetryPolicy` (default: its own)
    """

    conn_name_attr = "forklift_conn_id"
    default_conn_name = "forklift_default"
    conn_type = "forklift"
    hook_name = "Forklift"

    def __init__(
        self, forklift_conn_id: str = default_conn_name, *, retry: Optional[RetryPolicy] = None
    ):
        super().__init__()
        self.forklift_conn_id = forklift_conn_id
        self.retry = retry
        self._client: Optional[ForkliftClient] = None

    def get_conn(self) -> ForkliftClient:
        """The client (made once per hook)."""
        if self._client is None:
            connection = self.get_connection(self.forklift_conn_id)
            self._client = client_for(connection, retry=self.retry)
        return self._client

    async def aget_conn(self) -> ForkliftClient:
        """The client, for triggers: the connection is read without blocking the event loop."""
        connection = await self.aget_connection(self.forklift_conn_id)
        return client_for(connection, retry=self.retry)

    def test_connection(self) -> Tuple[bool, str]:
        """For the UI's Test button: whom the token signs in as."""
        try:
            client = self.get_conn()
            me = client.me()
        except (AirflowException, ForkliftError) as error:
            return False, str(error)
        return True, f"Connected to {client.base_url} as {me.user.username} ({me.user.role})"
