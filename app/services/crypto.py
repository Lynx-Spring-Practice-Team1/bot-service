"""Application-layer encryption for JWT tokens stored in the database.

Fernet (AES-128-CBC + HMAC-SHA256) is used so that a read-only breach of
the database does not expose usable bearer tokens.  The encryption key is
derived from JWT_SECRET via PBKDF2-SHA256; Fernet itself generates a random
IV per encryption so two encryptions of the same token produce different
ciphertext.

Key-rotation note: if JWT_SECRET changes, all stored tokens become
unreadable.  The restore logic in main.py handles this gracefully by
marking affected sessions as 'error' rather than crashing.
"""

from __future__ import annotations

import base64

from cryptography.fernet import Fernet, InvalidToken  # noqa: F401  (re-exported for callers)
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC

from app.config import settings

# Fixed application-level salt.  Security comes from the strength of
# JWT_SECRET, not the salt; Fernet's per-message randomness handles
# ciphertext uniqueness.
_SALT = b"bot-svc-jwt-enc-v1"
_ITERATIONS = 260_000  # OWASP recommended minimum for PBKDF2-SHA256


def _build_fernet() -> Fernet:
    kdf = PBKDF2HMAC(
        algorithm=hashes.SHA256(),
        length=32,
        salt=_SALT,
        iterations=_ITERATIONS,
    )
    key = base64.urlsafe_b64encode(kdf.derive(settings.JWT_SECRET.encode()))
    return Fernet(key)


# Derived once at module import time.  If the secret changes the service
# must be restarted for the new key to take effect.
_fernet: Fernet = _build_fernet()


def encrypt_token(plaintext: str) -> str:
    """Return Fernet-encrypted, base64url-encoded ciphertext."""
    return _fernet.encrypt(plaintext.encode()).decode()


def decrypt_token(ciphertext: str) -> str:
    """Decrypt a value produced by encrypt_token().

    Raises ``cryptography.fernet.InvalidToken`` if the ciphertext is
    corrupt or was produced with a different key (e.g., after secret
    rotation).  Callers should catch this and mark the session as error.
    """
    return _fernet.decrypt(ciphertext.encode()).decode()
