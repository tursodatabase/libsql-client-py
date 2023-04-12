from typing import NamedTuple, Optional
import urllib.parse

from .client import LibsqlError

class _Config(NamedTuple):
    scheme: str
    authority: str
    path: str
    auth_token: Optional[str]

def _expand_config(url: str, *, auth_token: Optional[str]) -> _Config:
    url_parsed = urllib.parse.urlparse(url)
    scheme = url_parsed.scheme
    authority = url_parsed.netloc
    path = url_parsed.path

    qsl = urllib.parse.parse_qsl(url_parsed.query, keep_blank_values=True)
    for key, value in qsl:
        if key == "authToken":
            auth_token = value or None
        else:
            raise LibsqlError(f"Unknown URL query parameter {key!r}", "URL_PARAM_NOT_SUPPORTED")

    if url_parsed.params:
        raise LibsqlError(f"Unsupported URL parameter: {url_parsed.params!r}", "URL_INVALID")
    if url_parsed.fragment:
        raise LibsqlError(f"Unsupported URL fragment: {url_parsed.fragment!r}", "URL_INVALID")

    return _Config(scheme, authority, path, auth_token)
