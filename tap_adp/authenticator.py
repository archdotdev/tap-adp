"""ADP OAuth authentication handling."""

from __future__ import annotations

import os
import tempfile

import requests
from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.helpers._util import utc_now
from singer_sdk.streams import RESTStream


class ADPAuthenticator(OAuthAuthenticator):
    """Authenticator class for ADP."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for ADP."""
        return {
            "grant_type": "client_credentials",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
        }
    
    def update_access_token(self) -> None:
        """Update `access_token` along with `last_refreshed` and `expires_in`."""
        request_time = utc_now()

        # Create temporary files for the cert and key
        with tempfile.NamedTemporaryFile(mode='wb+', delete=False) as cert_file, \
             tempfile.NamedTemporaryFile(mode='wb+', delete=False) as key_file:

            # Write contents to the temporary files
            cert_file.write(self.config["cert_public"].encode('utf-8'))
            cert_file.flush()

            key_file.write(self.config["cert_private"].encode('utf-8'))
            key_file.flush()

            # Ensure the files are readable only by the owner (optional)
            breakpoint()
            os.chmod(cert_file.name, 0o600)
            os.chmod(key_file.name, 0o600)

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
            except requests.HTTPError as ex:
                self.logger.error(
                    f"Failed OAuth login, response was '{response.text}'. {ex}"
                )
                raise RuntimeError(
                    f"Failed OAuth login, response was '{response.text}'. {ex}"
                ) from ex
            finally:
                # Clean up the temporary files
                cert_file.close()
                key_file.close()
                os.unlink(cert_file.name)
                os.unlink(key_file.name)

        self.logger.info("OAuth authorization attempt was successful.")

        token_json = response.json()
        self.access_token = token_json["access_token"]
        expiration = token_json.get("expires_in", self._default_expiration)
        self.expires_in = int(expiration) if expiration else None

        if self.expires_in is None:
            self.logger.debug(
                "No expires_in received in OAuth response and no "
                "default_expiration set. Token will be treated as if it never "
                "expires.",
            )

        self.last_refreshed = request_time

    @classmethod
    def create_for_stream(
        cls: type[ADPAuthenticator],
        stream: RESTStream,
    ) -> ADPAuthenticator:
        """Create an Authenticator object specific to the Stream class."""
        return cls(
            stream=stream,
            auth_endpoint="https://accounts.adp.com/auth/oauth/v2/token",
            oauth_scopes="read",
            default_expiration=3600,
        )
