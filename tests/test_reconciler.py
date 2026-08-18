"""Tests for settlement reconciler, including Bug 3 regression.

Bug 3: reconciler's _fetch_kalshi_market_result passed api_key and
private_key in the wrong positional order to build_auth_headers, causing
'str' object has no attribute 'sign'.
"""
import asyncio
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


# ---------------------------------------------------------------------------
# Bug 3 regression: reconciler auth header argument order
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_kalshi_market_result_passes_key_objects_correctly(monkeypatch):
    """Regression Bug 3: build_auth_headers must receive the RSA key object as
    the first argument, not the api_key string.  Previously the call was
    build_auth_headers(api_key, private_key, ...) which caused
    'str' object has no attribute 'sign'."""
    import core.settlement_reconciler as reconciler

    call_args: list[tuple] = []

    class _FakeKey:
        """Mimics a loaded RSA private key (has a .sign method)."""
        def sign(self, *_a, **_kw):
            return b"sig"

    fake_key = _FakeKey()

    def fake_load_private_key(path: str):
        return fake_key

    def fake_build_auth_headers(private_key, api_key, method: str, path: str) -> dict:
        call_args.append((private_key, api_key))
        return {"Authorization": "fake"}

    import httpx

    class _FakeResponse:
        status_code = 200

        def raise_for_status(self):
            pass

        def json(self):
            return {"market": {"result": "yes"}}

    class _FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

        async def get(self, url, headers=None):
            return _FakeResponse()

    monkeypatch.setattr(httpx, "AsyncClient", lambda **_kw: _FakeClient())

    # Patch the imports inside _fetch_kalshi_market_result
    import app.signing as signing_mod
    monkeypatch.setattr(signing_mod, "load_private_key", fake_load_private_key)
    monkeypatch.setattr(signing_mod, "build_auth_headers", fake_build_auth_headers)

    # Re-import to pick up patched module
    from core.settlement_reconciler import _fetch_kalshi_market_result

    result = await _fetch_kalshi_market_result(
        market_ticker="KXLOWTMIN-26AUG16-B67.5",
        rest_base_url="https://example.test",
        api_key="my-api-key",
        private_key_path="/path/to/key.pem",
    )

    assert result == "yes", f"Expected 'yes', got {result!r}"

    assert call_args, "build_auth_headers was never called"
    passed_private_key, passed_api_key = call_args[0]

    # The RSA key object (not a string) must be the first positional arg
    assert not isinstance(passed_private_key, str), (
        "Bug 3: private_key was passed as a string — fix the argument order in "
        "_fetch_kalshi_market_result"
    )
    assert isinstance(passed_private_key, _FakeKey), (
        "Bug 3: first arg to build_auth_headers must be the loaded key object"
    )
    assert passed_api_key == "my-api-key", (
        "Bug 3: api_key must be the second positional arg to build_auth_headers"
    )
