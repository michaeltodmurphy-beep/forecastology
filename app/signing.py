# app/signing.py
import base64
import time
from cryptography.hazmat.primitives import serialization, hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa
from cryptography.exceptions import InvalidSignature


def load_private_key(path: str) -> rsa.RSAPrivateKey:
    """Load an RSA private key from a PEM file."""
    with open(path, "rb") as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None,
        )
    return private_key  # type: ignore


def sign_pss(private_key: rsa.RSAPrivateKey, text: str) -> str:
    """Sign text using RSA-PSS with SHA256."""
    message = text.encode("utf-8")
    try:
        signature = private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")
    except InvalidSignature as e:
        raise ValueError("RSA sign PSS failed") from e


def build_auth_headers(
    private_key: rsa.RSAPrivateKey,
    api_key: str,
    method: str,
    path: str,
) -> dict:
    """
    Build Kalshi authentication headers for a REST request.
    The path should be without query params (e.g. /trade-api/v2/portfolio/orders).
    """
    timestamp_ms = int(time.time() * 1000)
    path_without_query = path.split("?")[0]
    msg_string = str(timestamp_ms) + method + path_without_query
    signature = sign_pss(private_key, msg_string)

    return {
        "KALSHI-ACCESS-KEY": api_key,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": str(timestamp_ms),
        "Content-Type": "application/json",
    }


def build_ws_headers(private_key: rsa.RSAPrivateKey, api_key: str) -> dict:
    """Build authentication headers for WebSocket handshake."""
    return build_auth_headers(private_key, api_key, "GET", "/trade-api/ws/v2")
