"""REST client handling, including ADPStream base class."""

from __future__ import annotations

import decimal
import typing as t
from importlib import resources

from singer_sdk.authenticators import OAuthAuthenticator
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import BaseAPIPaginator 
from singer_sdk.streams import RESTStream
from tap_adp.authenticator import ADPAuthenticator
from functools import cached_property

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context


SCHEMAS_DIR = resources.files(__package__) / "schemas"


class ADPStream(RESTStream):
    """ADP stream class."""

    records_jsonpath = "$[*]"
    next_page_token_jsonpath = None
    replication_key = None
    _LOG_REQUEST_METRIC_URLS: bool = True

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return "https://api.adp.com"

    @cached_property
    def authenticator(self) -> ADPAuthenticator:
        """Return a new authenticator object."""
        return ADPAuthenticator.create_for_stream(self)

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: A raw `requests.Response`

        Yields:
            One item for every item found in the response.
        """
        if response.status_code == 204 or not response.content:
            # No content to parse
            return iter([])
        else:
            yield from extract_jsonpath(
                self.records_jsonpath,
                input=response.json(parse_float=decimal.Decimal),
            )


class PaginatedADPStream(ADPStream):

    def get_new_paginator(self) -> BaseAPIPaginator:
        """Create a new paginator for ADP API pagination."""
        return ADPPaginator(start_value=0, page_size=100)

    def get_url_params(
        self,
        context: dict | None,
        next_page_token: int | None,
    ) -> dict[str, t.Any]:
        params = super().get_url_params(context, next_page_token)
        params = {
            "$top": 100,  # Set the desired page size
            "$skip": next_page_token or 0,
        }
        return params


class ADPPaginator(BaseAPIPaginator[int]):
    """Paginator for ADP API that uses 'top' and 'skip' parameters and stops on 204 response."""

    def __init__(self, start_value: int, page_size: int, *args: t.Any, **kwargs: t.Any) -> None:
        """Initialize the paginator with a starting value and page size.

        Args:
            start_value: The initial skip value.
            page_size: The number of records to retrieve per page.
            args: Additional positional arguments.
            kwargs: Additional keyword arguments.
        """
        super().__init__(start_value, *args, **kwargs)
        self.page_size = page_size

    def get_next(self, response: requests.Response) -> int | None:
        """Calculate the next skip value.

        Args:
            response: The HTTP response received from the API.

        Returns:
            The next skip value or `None` if pagination should stop.
        """
        return self.current_value + self.page_size

    def has_more(self, response: requests.Response) -> bool:
        """Determine if there are more pages to fetch.

        Args:
            response: The HTTP response received from the API.

        Returns:
            `True` if pagination should continue, `False` if a 204 No Content is received.
        """
        return response.status_code != 204
