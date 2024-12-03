"""ADP tap class."""

from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th  # JSON schema typing helpers

# TODO: Import your custom stream types here:
from tap_adp import streams


class TapADP(Tap):
    """ADP tap class."""

    name = "tap-adp"

    # TODO: Update this section with the actual config values you expect:
    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
            secret=True,
            description="The OAuth client ID for ADP API",
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
            secret=True,
            description="The OAuth client secret for ADP API",
        ),
        th.Property(
            "cert_public",
            th.StringType,
            description="Client certificate for ADP API",
        ),
        th.Property(
            "cert_private",
            th.StringType,
            secret=True,
            description="Client private key for ADP API",
        ),
        th.Property(
            "user_agent",
            th.StringType,
            description=(
                "A custom User-Agent header to send with each request. Default is "
                "'<tap_name>/<tap_version>'"
            ),
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.ADPStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.WorkersStream(self),
        ]


if __name__ == "__main__":
    TapADP.cli()
