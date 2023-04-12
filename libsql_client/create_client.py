from typing import Optional

from .client import Client, LibsqlError
from .config import _expand_config
from .sqlite3 import _create_sqlite3_client

def create_client(url: str, *, auth_token: Optional[str] = None) -> Client:
    config = _expand_config(url, auth_token=auth_token)
    if config.scheme == "file":
        return _create_sqlite3_client(config)
    else:
        raise LibsqlError(f"Unsupported URL scheme {config.scheme!r}", "URL_SCHEME_NOT_SUPPORTED")
