"""ADP OAuth authentication handling."""

from __future__ import annotations

import os
import ssl
import sys
import tempfile
from functools import cached_property
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.helpers._util import utc_now

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


AUTH_ENDPOINT = "https://accounts.adp.com/auth/oauth/v2/token"


class _MTLSAdapter(HTTPAdapter):
    """Requests adapter that injects a pre-built SSL context for mTLS.

    Works around SSL context caching in requests >=2.32.5 (psf/requests#6767)
    that ignores the ``cert=`` parameter when a cached context already exists
    for the target host, causing mTLS authentication to silently drop the
    client certificate.
    """

    def __init__(self, ssl_context: ssl.SSLContext, **kwargs: Any) -> None:
        self._ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args: Any, **kwargs: Any) -> None:
        kwargs["ssl_context"] = self._ssl_context
        super().init_poolmanager(*args, **kwargs)  # type: ignore[no-untyped-call]


class ADPAuthenticator(OAuthAuthenticator):
    """Authenticator class for ADP."""

    @override
    def __init__(
        self,
        *args: Any,
        client_id: str,
        client_secret: str,
        cert_public: str,
        cert_private: str,
        auth_endpoint: str = AUTH_ENDPOINT,
        **kwargs: Any,
    ) -> None:
        super().__init__(
            *args,
            client_id=client_id,
            client_secret=client_secret,
            auth_endpoint=auth_endpoint,
            **kwargs,
        )
        self.cert_public = cert_public
        self.cert_private = cert_private

    @override
    @property
    def oauth_request_body(self) -> dict[str, Any]:
        """Define the OAuth request body for ADP."""
        return {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

    @cached_property
    def ssl_context(self) -> ssl.SSLContext:
        """Build an SSL context with the client certificate pre-loaded.

        Writes the PEM strings to temporary files, loads them into an
        ``ssl.SSLContext``, then deletes the files before returning.  The
        context holds the cert in memory so the files are not needed at
        request time.
        """
        with (
            tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".pem") as cert_file,
            tempfile.NamedTemporaryFile(mode="wb", delete=False, suffix=".pem") as key_file,
        ):
            cert_path = cert_file.name
            key_path = key_file.name
            cert_file.write(self.cert_public.encode("utf-8"))
            key_file.write(self.cert_private.encode("utf-8"))

        try:
            os.chmod(cert_path, 0o600)  # noqa: PTH101
            os.chmod(key_path, 0o600)  # noqa: PTH101
            ctx = ssl.create_default_context()
            ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
        finally:
            os.unlink(cert_path)  # noqa: PTH108
            os.unlink(key_path)  # noqa: PTH108

        return ctx

    @override
    def update_access_token(self) -> None:
        """Update `access_token` along with `last_refreshed` and `expires_in`."""
        request_time = utc_now()

        session = requests.Session()
        session.mount("https://", _MTLSAdapter(ssl_context=self.ssl_context))

        try:
            response = session.post(
                self.auth_endpoint,
                data=self.oauth_request_body,
                headers=self._oauth_headers,
                timeout=60,
            )
            response.raise_for_status()
        except requests.HTTPError:
            self.logger.warning(
                "Failed OAuth login, response was '%s'",
                response.text,
            )
            raise

        self.logger.info("OAuth authorization attempt was successful.")

        token_json = response.json()
        self.access_token = token_json["access_token"]
        # subtract 10 minutes from the expiration to allow additional time for
        # reauthentication to occur.
        expiration = token_json.get("expires_in", self._default_expiration) - 600
        self.expires_in = int(expiration) if expiration else None

        if self.expires_in is None:
            self.logger.debug(
                "No expires_in received in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires.",
            )

        self.last_refreshed = request_time
