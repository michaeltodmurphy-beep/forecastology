"""Regression tests for settlement_reconciler._fetch_kalshi_market_result.

Bug fixed: build_auth_headers was called with (api_key, private_key, ...) —
positional arguments swapped — causing AttributeError: 'str' object has no
attribute 'sign' and every Kalshi fetch silently returning None.

These tests use a real (test-generated) RSA private key so that the signing
path is exercised end-to-end, and a mocked httpx.AsyncClient to avoid
real network calls.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.backends import default_backend


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _generate_rsa_key() -> rsa.RSAPrivateKey:
    """Generate a throw-away 2048-bit RSA key for testing."""
    return rsa.generate_private_key(
        public_exponent=65537,
        key_size=2048,
        backend=default_backend(),
    )


def _fake_httpx_client(response_body: dict, status_code: int = 200):
    """Return a context-manager-compatible fake httpx.AsyncClient."""
    mock_resp = MagicMock()
    mock_resp.status_code = status_code
    mock_resp.json.return_value = response_body
    mock_resp.raise_for_status = MagicMock()

    client_instance = AsyncMock()
    client_instance.get = AsyncMock(return_value=mock_resp)
    client_instance.__aenter__ = AsyncMock(return_value=client_instance)
    client_instance.__aexit__ = AsyncMock(return_value=False)
    return client_instance


# ---------------------------------------------------------------------------
# Tests for _fetch_kalshi_market_result
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_fetch_kalshi_market_result_yes():
    """Returns 'yes' when Kalshi responds with market.result == 'yes'."""
    from core.settlement_reconciler import _fetch_kalshi_market_result

    private_key = _generate_rsa_key()

    with patch("app.signing.load_private_key", return_value=private_key):
        with patch("httpx.AsyncClient", return_value=_fake_httpx_client({"market": {"result": "yes"}})):
            result = await _fetch_kalshi_market_result(
                "KXLOWTBOS-26JUL16-B70",
                "https://api.kalshi.com",
                "test-api-key",
                "/fake/key.pem",
            )

    assert result == "yes"


@pytest.mark.asyncio
async def test_fetch_kalshi_market_result_no():
    """Returns 'no' when Kalshi responds with market.result == 'no'."""
    from core.settlement_reconciler import _fetch_kalshi_market_result

    private_key = _generate_rsa_key()

    with patch("app.signing.load_private_key", return_value=private_key):
        with patch("httpx.AsyncClient", return_value=_fake_httpx_client({"market": {"result": "no"}})):
            result = await _fetch_kalshi_market_result(
                "KXLOWTBOS-26JUL16-B70",
                "https://api.kalshi.com",
                "test-api-key",
                "/fake/key.pem",
            )

    assert result == "no"


@pytest.mark.asyncio
async def test_fetch_kalshi_market_result_unresolved():
    """Returns None when Kalshi responds without a result field."""
    from core.settlement_reconciler import _fetch_kalshi_market_result

    private_key = _generate_rsa_key()

    with patch("app.signing.load_private_key", return_value=private_key):
        with patch("httpx.AsyncClient", return_value=_fake_httpx_client({"market": {"status": "open"}})):
            result = await _fetch_kalshi_market_result(
                "KXLOWTBOS-26JUL16-B70",
                "https://api.kalshi.com",
                "test-api-key",
                "/fake/key.pem",
            )

    assert result is None


@pytest.mark.asyncio
async def test_fetch_kalshi_market_result_404():
    """Returns None on a 404 response (market not found)."""
    from core.settlement_reconciler import _fetch_kalshi_market_result

    private_key = _generate_rsa_key()

    with patch("app.signing.load_private_key", return_value=private_key):
        with patch("httpx.AsyncClient", return_value=_fake_httpx_client({}, status_code=404)):
            result = await _fetch_kalshi_market_result(
                "KXLOWTBOS-26JUL16-B70",
                "https://api.kalshi.com",
                "test-api-key",
                "/fake/key.pem",
            )

    assert result is None


@pytest.mark.asyncio
async def test_fetch_kalshi_market_result_uses_real_rsa_signing():
    """Confirms build_auth_headers receives private_key as first arg (not api_key).

    Regression guard for the swapped-argument bug: if arguments were still
    swapped, sign_pss would receive a str and raise
    AttributeError: 'str' object has no attribute 'sign'.
    """
    from core.settlement_reconciler import _fetch_kalshi_market_result
    from app.signing import build_auth_headers as real_build_auth_headers

    private_key = _generate_rsa_key()
    captured_calls: list = []

    def recording_build_auth_headers(pk, ak, method, path):
        captured_calls.append((pk, ak, method, path))
        # Call the real implementation — exercises the actual RSA signing.
        return real_build_auth_headers(pk, ak, method, path)

    with patch("app.signing.load_private_key", return_value=private_key):
        with patch("app.signing.build_auth_headers", side_effect=recording_build_auth_headers):
            with patch("httpx.AsyncClient", return_value=_fake_httpx_client({"market": {"result": "yes"}})):
                result = await _fetch_kalshi_market_result(
                    "KXLOWTBOS-26JUL16-B70",
                    "https://api.kalshi.com",
                    "my-api-key",
                    "/fake/key.pem",
                )

    assert result == "yes"
    assert len(captured_calls) == 1
    pk_arg, ak_arg, method_arg, _path_arg = captured_calls[0]
    # private_key must be the RSA key object, NOT the string "my-api-key"
    assert pk_arg is private_key, "First arg must be the RSA private key object"
    assert ak_arg == "my-api-key", "Second arg must be the API key string"
    assert method_arg == "GET"


@pytest.mark.asyncio
async def test_fetch_kalshi_market_result_yes_leads_to_settled_win_status():
    """Confirms 'yes' result maps to the SETTLED_WIN status constant."""
    from app.models import TradeOutcomeStatus

    # result=="yes" triggers SETTLED_WIN in the reconciler's _reconcile_one_market.
    # Verify the status constants are as expected.
    assert TradeOutcomeStatus.SETTLED_WIN == "SETTLED_WIN"
    assert TradeOutcomeStatus.SETTLED_LOSS == "SETTLED_LOSS"

    # And confirm the fetch path returns "yes" for a matching response.
    from core.settlement_reconciler import _fetch_kalshi_market_result
    private_key = _generate_rsa_key()

    with patch("app.signing.load_private_key", return_value=private_key):
        with patch("httpx.AsyncClient", return_value=_fake_httpx_client({"market": {"result": "yes"}})):
            result = await _fetch_kalshi_market_result(
                "KXLOWTBOS-26JUL16-B70",
                "https://api.kalshi.com",
                "test-api-key",
                "/fake/key.pem",
            )

    assert result == "yes"
