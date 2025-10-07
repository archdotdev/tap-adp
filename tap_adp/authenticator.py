"""ADP OAuth authentication handling."""

from __future__ import annotations

import os
import sys
import tempfile
from typing import Any

import requests
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.helpers._util import utc_now

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override


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
        **kwargs: Any,
    ) -> None:
        super().__init__(
            *args,
            client_id=client_id,
            client_secret=client_secret,
            **kwargs,
        )
        self.cert_public = cert_public
        self.cert_private = cert_private

    @override
    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for ADP."""
        return {
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

    @override
    def update_access_token(self) -> None:
        """Update `access_token` along with `last_refreshed` and `expires_in`."""
        request_time = utc_now()

        # Create temporary files for the cert and key
        with (
            tempfile.NamedTemporaryFile(mode="wb+", delete=False) as cert_file,
            tempfile.NamedTemporaryFile(mode="wb+", delete=False) as key_file,
        ):
            # Write contents to the temporary files
            cert_file.write(self.cert_public.encode("utf-8"))
            cert_file.flush()

            key_file.write(self.cert_private.encode("utf-8"))
            key_file.flush()

            # Ensure the files are readable only by the owner (optional)
            os.chmod(cert_file.name, 0o600)  # noqa: PTH101
            os.chmod(key_file.name, 0o600)  # noqa: PTH101

            # Make the OAuth request
            try:
                response = requests.post(
                    self.auth_endpoint,
                    data=self.oauth_request_body,
                    headers=self._oauth_headers,
                    timeout=60,
                    cert=(cert_file.name, key_file.name),
                )
                response.raise_for_status()
            except requests.HTTPError:
                self.logger.warning(
                    "Failed OAuth login, response was '%s'",
                    response.text,
                )
                raise
            finally:
                # Clean up the temporary files
                cert_file.close()
                key_file.close()
                os.unlink(cert_file.name)  # noqa: PTH108
                os.unlink(key_file.name)  # noqa: PTH108

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
