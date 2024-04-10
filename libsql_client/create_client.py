from __future__ import annotations

from typing import Optional

from .client import Client
from .client import LibsqlError
from .config import _expand_config
from .hrana import _create_hrana_client
from .http import _create_http_client
from .sqlite3_utils import _create_sqlite3_client


def create_client(
    url: str, *, auth_token: Optional[str] = None, tls: Optional[bool] = None
) -> Client:
    config = _expand_config(url, auth_token=auth_token, tls=tls)
    if config.scheme == "file":
        return _create_sqlite3_client(config)
    elif config.scheme in ("ws", "wss"):
        return _create_hrana_client(config)
    elif config.scheme in ("http", "https"):
        return _create_http_client(config)
    elif not url:
        raise LibsqlError(
            f"Database URL is {url}.", "URL_UNDEFINED"
        )
    else:
        raise LibsqlError(
            f"Unsupported URL scheme {config.scheme!r}", "URL_SCHEME_NOT_SUPPORTED"
        )
