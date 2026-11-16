"""Password hashing (Argon2id), JWT access tokens, rotating refresh tokens, RBAC."""
from __future__ import annotations

import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from typing import Annotated, Any

import jwt
from argon2 import PasswordHasher
from argon2.exceptions import InvalidHashError, VerificationError, VerifyMismatchError
from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.config import get_settings
from app.database.session import get_db
from app.models.entities import RefreshToken, User

_hasher = PasswordHasher()
_bearer = HTTPBearer(auto_error=False)
ROLES = ("ADMIN", "ANALYST")
MIN_PASSWORD_LENGTH = 12
# Valid hash of a random secret: verifying against it equalises timing for unknown usernames
DUMMY_HASH = _hasher.hash(secrets.token_urlsafe(24))


def hash_password(password: str) -> str:
    validate_password_strength(password)
    return _hasher.hash(password)


def validate_password_strength(password: str) -> None:
    if len(password) < MIN_PASSWORD_LENGTH:
        raise ValueError(f"password must be at least {MIN_PASSWORD_LENGTH} characters")
    classes = sum([any(c.islower() for c in password), any(c.isupper() for c in password),
                   any(c.isdigit() for c in password), any(not c.isalnum() for c in password)])
    if classes < 3:
        raise ValueError("password must contain at least 3 of: lowercase, uppercase, digit, symbol")


def verify_password(password: str, hashed: str) -> bool:
    try:
        return _hasher.verify(hashed, password)
    except (VerifyMismatchError, VerificationError, InvalidHashError):
        return False


def create_access_token(user: User) -> tuple[str, int]:
    s = get_settings()
    now = datetime.now(timezone.utc)
    exp = now + timedelta(minutes=s.access_token_minutes)
    payload = {"sub": str(user.id), "username": user.username, "role": user.role.name, "type": "access",
               "iat": int(now.timestamp()), "exp": int(exp.timestamp()), "jti": secrets.token_hex(8)}
    return jwt.encode(payload, s.jwt_secret, algorithm=s.jwt_algorithm), s.access_token_minutes * 60


def decode_access_token(token: str) -> dict[str, Any]:
    s = get_settings()
    try:
        payload = jwt.decode(token, s.jwt_secret, algorithms=[s.jwt_algorithm], options={"require": ["exp", "sub"]})
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "token expired", {"WWW-Authenticate": "Bearer"}) from exc
    except jwt.PyJWTError as exc:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token", {"WWW-Authenticate": "Bearer"}) from exc
    if payload.get("type") != "access":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid token type")
    return payload


def _hash_refresh(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()


def issue_refresh_token(db: Session, user: User) -> str:
    token = secrets.token_urlsafe(48)
    db.add(RefreshToken(user_id=user.id, token_hash=_hash_refresh(token),
                        expires_at=datetime.now(timezone.utc) + timedelta(days=get_settings().refresh_token_days)))
    return token


def rotate_refresh_token(db: Session, token: str) -> tuple[User, str]:
    """Validate and revoke the presented refresh token, returning the user and a new token.
    Re-use of a revoked token revokes every token of that user (theft detection)."""
    rec = db.scalar(select(RefreshToken).where(RefreshToken.token_hash == _hash_refresh(token)))
    now = datetime.now(timezone.utc)
    if rec is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid refresh token")
    if rec.revoked_at is not None:
        for t in db.scalars(select(RefreshToken).where(RefreshToken.user_id == rec.user_id,
                                                        RefreshToken.revoked_at.is_(None))):
            t.revoked_at = now
        db.commit()
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "refresh token reuse detected")
    if rec.expires_at < now:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "refresh token expired")
    user = db.get(User, rec.user_id)
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "user inactive")
    rec.revoked_at = now
    new = issue_refresh_token(db, user)
    return user, new


def revoke_refresh_token(db: Session, token: str) -> None:
    rec = db.scalar(select(RefreshToken).where(RefreshToken.token_hash == _hash_refresh(token)))
    if rec and rec.revoked_at is None:
        rec.revoked_at = datetime.now(timezone.utc)


def get_current_user(request: Request, creds: Annotated[HTTPAuthorizationCredentials | None, Depends(_bearer)],
                     db: Annotated[Session, Depends(get_db)]) -> User:
    if creds is None or creds.scheme.lower() != "bearer":
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "not authenticated", {"WWW-Authenticate": "Bearer"})
    payload = decode_access_token(creds.credentials)
    user = db.get(User, int(payload["sub"]))
    if user is None or not user.is_active:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "user not found or inactive")
    request.state.user = user
    return user


def require_admin(user: Annotated[User, Depends(get_current_user)]) -> User:
    if user.role.name != "ADMIN":
        raise HTTPException(status.HTTP_403_FORBIDDEN, "administrator role required")
    return user


CurrentUser = Annotated[User, Depends(get_current_user)]
AdminUser = Annotated[User, Depends(require_admin)]
DB = Annotated[Session, Depends(get_db)]
