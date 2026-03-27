import unittest
import urllib.parse
from unittest.mock import patch

from bot.binance_client import BinanceClient, BinanceClientConfig


class _StubBinanceClient(BinanceClient):
    def __init__(self, config: BinanceClientConfig, responses: list[object]) -> None:
        super().__init__(config)
        self._responses = list(responses)
        self.requests = []

    def _execute_request(self, request):
        self.requests.append(request)
        if not self._responses:
            raise AssertionError("No stub response configured")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def _query_params(request) -> dict[str, list[str]]:
    parsed = urllib.parse.urlparse(request.full_url)
    return urllib.parse.parse_qs(parsed.query)


class BinanceClientTimestampTests(unittest.IsolatedAsyncioTestCase):
    def _client(self, responses: list[object]) -> _StubBinanceClient:
        return _StubBinanceClient(
            BinanceClientConfig(
                api_key="test_key",
                api_secret="test_secret",
                rest_base_url="https://api.binance.test",
                ws_base_url="wss://stream.binance.test",
            ),
            responses,
        )

    async def test_signed_request_uses_synced_server_time_offset(self) -> None:
        client = self._client(
            responses=[
                {"serverTime": 1_000_250},
                {"balances": [{"asset": "USDT", "free": "1", "locked": "0"}]},
            ]
        )

        with patch("bot.binance_client.time.time", side_effect=[1000.0, 1000.0]):
            balances = await client.get_balances()

        self.assertEqual(len(balances), 1)
        self.assertEqual(len(client.requests), 2)
        self.assertEqual(urllib.parse.urlparse(client.requests[0].full_url).path, "/api/v3/time")
        self.assertEqual(urllib.parse.urlparse(client.requests[1].full_url).path, "/api/v3/account")

        account_query = _query_params(client.requests[1])
        self.assertEqual(account_query["recvWindow"][0], "5000")
        self.assertEqual(account_query["timestamp"][0], "1000250")
        self.assertIn("signature", account_query)

    async def test_signed_request_resyncs_and_retries_once_on_1021(self) -> None:
        timestamp_error = RuntimeError(
            'Binance HTTP error 400: {"code":-1021,"msg":"Timestamp for this request is outside of the recvWindow."}'
        )
        client = self._client(
            responses=[
                {"serverTime": 2_000_000},
                timestamp_error,
                {"serverTime": 2_000_500},
                {"balances": [{"asset": "USDT", "free": "2", "locked": "0"}]},
            ]
        )

        with patch("bot.binance_client.time.time", side_effect=[2000.0, 2000.0, 2000.0, 2000.0]):
            balances = await client.get_balances()

        self.assertEqual(len(balances), 1)
        self.assertEqual(len(client.requests), 4)
        paths = [urllib.parse.urlparse(item.full_url).path for item in client.requests]
        self.assertEqual(paths, ["/api/v3/time", "/api/v3/account", "/api/v3/time", "/api/v3/account"])

        first_query = _query_params(client.requests[1])
        second_query = _query_params(client.requests[3])
        self.assertEqual(first_query["timestamp"][0], "2000000")
        self.assertEqual(second_query["timestamp"][0], "2000500")


if __name__ == "__main__":
    unittest.main()
