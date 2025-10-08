"""Tests standard tap features using the built-in SDK tests library."""

import requests
from singer_sdk.testing import get_tap_test_class

from tap_adp.client import ADPPaginator
from tap_adp.tap import TapADP

SAMPLE_CONFIG = {
    "client_id": "test",
    "client_secret": "test",
    "cert_public": "test",
    "cert_private": "test",
}


TestTapADP = get_tap_test_class(
    TapADP,
    config=SAMPLE_CONFIG,
    include_tap_tests=False,
    include_stream_tests=False,
    include_stream_attribute_tests=False,
)


class TestADPPaginator:
    """Test ADP paginator."""

    def test_get_next(self) -> None:
        """Test get next."""
        paginator = ADPPaginator(start_value=0, page_size=100)
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"foo": "bar"}'  # noqa: SLF001

        paginator.advance(response)
        assert paginator.current_value == 100  # noqa: PLR2004

        paginator.advance(response)
        assert paginator.current_value == 200  # noqa: PLR2004

    def test_has_more(self) -> None:
        """Test has more."""
        paginator = ADPPaginator(start_value=0, page_size=100)
        response = requests.Response()
        response.status_code = 200
        response._content = b'{"next": 100}'  # noqa: SLF001
        assert paginator.has_more(response)

    def test_has_more_no_content(self) -> None:
        """Test has more no content."""
        paginator = ADPPaginator(start_value=0, page_size=100)
        response = requests.Response()
        response.status_code = 204
        assert not paginator.has_more(response)
