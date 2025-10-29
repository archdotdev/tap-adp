"""REST client handling, including ADPStream base class."""

from __future__ import annotations

import decimal
import sys
import typing as t
from functools import cached_property
from http import HTTPStatus

from singer_sdk import SchemaDirectory, StreamSchema
from singer_sdk.helpers._typing import TypeConformanceLevel
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator
from singer_sdk.streams import RESTStream

from tap_adp import schemas
from tap_adp.authenticator import ADPAuthenticator

if sys.version_info >= (3, 12):
    from typing import override
else:
    from typing_extensions import override

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


class ADPStream(RESTStream):
    """ADP stream class."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    replication_key: str | None = None
    schema = StreamSchema(SchemaDirectory(schemas))

    _LOG_REQUEST_METRIC_URLS: bool = True
    TYPE_CONFORMANCE_LEVEL = TypeConformanceLevel.ROOT_ONLY

    @override
    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.adp.com"

    @override
    @cached_property
    def authenticator(self) -> ADPAuthenticator:
        """Return a new authenticator object."""
        return ADPAuthenticator(
            client_id=self.config["client_id"],
            client_secret=self.config["client_secret"],
            cert_public=self.config["cert_public"],
            cert_private=self.config["cert_private"],
        )

    @override
    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: A raw `requests.Response`

        Yields:
            One item for every item found in the response.
        """
        if response.status_code == HTTPStatus.NO_CONTENT or not response.content:
            # No content to parse
            return
        else:
            yield from extract_jsonpath(
                self.records_jsonpath,
                input=response.json(parse_float=decimal.Decimal),
            )

    @override
    def response_error_message(self, response: requests.Response) -> str:
        """Additional info for debugging purposes.

        Authorization is included in Headers, but Body is safe to print out in logs.
        """
        truncated_request_body = f"{response.request.body!r}"[:10000]
        truncated_response_content = f"{response.content!r}"[:10000]
        return (
            f"{super().response_error_message(response)} "
            f"with request URL {response.request.url} "
            f"and with request body {truncated_request_body} "
            f"and with response content {truncated_response_content}"
        )


class PaginatedADPStream(ADPStream):
    """Paginated ADP stream class."""

    @override
    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new paginator for ADP API pagination."""
        return ADPPaginator(start_value=0, page_size=100)

    @override
    def get_url_params(
        self,
        context: Context | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        return {
            "$top": 100,  # Set the desired page size
            "$skip": next_page_token or 0,
        }


class ADPPaginator(BaseAPIPaginator[int]):
    """Paginator for ADP API that uses 'top' and 'skip' parameters and stops on 204 response."""  # noqa: E501

    @override
    def __init__(
        self,
        start_value: int,
        page_size: int,
        *args: t.Any,
        **kwargs: t.Any,
    ) -> None:
        """Initialize the paginator with a starting value and page size.

        Args:
            start_value: The initial skip value.
            page_size: The number of records to retrieve per page.
            args: Additional positional arguments.
            kwargs: Additional keyword arguments.
        """
        super().__init__(start_value, *args, **kwargs)
        self.page_size = page_size

    @override
    def get_next(self, response: requests.Response) -> int | None:
        """Calculate the next skip value.

        Args:
            response: The HTTP response received from the API.

        Returns:
            The next skip value or `None` if pagination should stop.
        """
        return self.current_value + self.page_size

    @override
    def has_more(self, response: requests.Response) -> bool:
        """Determine if there are more pages to fetch.

        Args:
            response: The HTTP response received from the API.

        Returns:
            `True` if pagination should continue, `False` if a 204 No Content is received.
        """  # noqa: E501
        return response.status_code != HTTPStatus.NO_CONTENT
