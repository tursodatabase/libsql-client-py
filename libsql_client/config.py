from __future__ import annotations

from typing import NamedTuple
from typing import Optional
import urllib.parse

from .client import LibsqlError


class _Config(NamedTuple):
    scheme: str
    authority: str
    path: str
    auth_token: Optional[str]
    tls: bool


def _expand_config(
    url: str, *, auth_token: Optional[str], tls: Optional[bool]
) -> _Config:
    url_parsed = urllib.parse.urlparse(url)
    scheme = url_parsed.scheme
    authority = url_parsed.netloc
    path = url_parsed.path

    qsl = urllib.parse.parse_qsl(url_parsed.query, keep_blank_values=True)
    for key, value in qsl:
        if key == "authToken":
            auth_token = value or None
        elif key == "tls":
            if value == "0":
                tls = False
            elif value == "1":
                tls = True
            else:
                raise LibsqlError(
                    f"Unknown value for the 'tls' query argument: {value!r}. "
                    "Supported values are '0' and '1'",
                    "URL_INVALID",
                )
        else:
            raise LibsqlError(
                f"Unknown URL query parameter {key!r}", "URL_PARAM_NOT_SUPPORTED"
            )

    if scheme == "libsql":
        if tls is False:
            if url_parsed.port is None:
                raise LibsqlError(
                    "A 'libsql:' URL with ?tls=0 must specify an explicit port",
                    "URL_INVALID",
                )
            scheme = "ws"
        else:
            scheme = "wss"
    elif scheme in ("http", "ws") and tls is None:
        tls = False

    if tls is None:
        tls = True

    if url_parsed.params:
        raise LibsqlError(
            f"Unsupported URL parameter: {url_parsed.params!r}", "URL_INVALID"
        )
    if url_parsed.fragment:
        raise LibsqlError(
            f"Unsupported URL fragment: {url_parsed.fragment!r}", "URL_INVALID"
        )

    return _Config(scheme, authority, path, auth_token, tls)
