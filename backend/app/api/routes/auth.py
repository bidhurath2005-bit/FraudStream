from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Request, Response, status
from sqlalchemy import select

from app.api.params import IdPath
from app.core.config import get_settings
from app.core.runtime import get_runtime
from app.models.entities import Role, User
from app.schemas.api import LoginRequest, UserCreate, UserUpdate, user_out
from app.security.auth import (
    DB,
    DUMMY_HASH,
    AdminUser,
    CurrentUser,
    create_access_token,
    hash_password,
    issue_refresh_token,
    revoke_refresh_token,
    rotate_refresh_token,
    verify_password,
)
from app.services import audit

router = APIRouter(tags=["auth"])
REFRESH_COOKIE = "fs_refresh"


def _set_refresh_cookie(resp: Response, token: str) -> None:
    s = get_settings()
    resp.set_cookie(REFRESH_COOKIE, token, httponly=True, secure=s.secure_cookies, samesite="strict",
                    max_age=s.refresh_token_days * 86400, path="/api/v1/auth")


@router.post("/auth/login")
def login(body: LoginRequest, request: Request, response: Response, db: DB) -> dict:
    rt = get_runtime()
    ip = request.client.host if request.client else "unknown"
    allowed, retry = rt.limiter.hit(f"login:{ip}", rt.settings.login_rate_limit_per_minute)
    if not allowed:
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS, "too many login attempts",
                            headers={"Retry-After": str(retry)})
    user = db.scalar(select(User).where(User.username == body.username))
    ok = verify_password(body.password, user.password_hash if user else DUMMY_HASH)
    if not user or not ok or not user.is_active:
        audit.record(db, None, "LOGIN_FAILED", "user", body.username[:64], details={"ip": ip})
        db.commit()
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "invalid username or password")
    user.last_login_at = datetime.now(timezone.utc)
    token, expires = create_access_token(user)
    refresh = issue_refresh_token(db, user)
    audit.record(db, user, "LOGIN", "user", user.username, details={"ip": ip})
    db.commit()
    _set_refresh_cookie(response, refresh)
    return {"access_token": token, "token_type": "bearer", "expires_in": expires, "user": user_out(user)}


@router.post("/auth/refresh")
def refresh(request: Request, response: Response, db: DB) -> dict:
    token = request.cookies.get(REFRESH_COOKIE)
    if not token:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "no refresh token")
    user, new_refresh = rotate_refresh_token(db, token)
    access, expires = create_access_token(user)
    db.commit()
    _set_refresh_cookie(response, new_refresh)
    return {"access_token": access, "token_type": "bearer", "expires_in": expires, "user": user_out(user)}


@router.post("/auth/logout", status_code=204, response_model=None)
def logout(request: Request, response: Response, db: DB) -> Response:
    token = request.cookies.get(REFRESH_COOKIE)
    if token:
        revoke_refresh_token(db, token)
        db.commit()
    response.delete_cookie(REFRESH_COOKIE, path="/api/v1/auth")
    response.status_code = 204
    return response


@router.get("/auth/me")
def me(user: CurrentUser) -> dict:
    return user_out(user)


# ------------------------------------------------------------------ users (admin)
@router.get("/users", tags=["users"])
def list_users(user: CurrentUser, db: DB) -> list[dict]:
    # analysts may list users (needed for assignment display); only admins can modify
    return [user_out(u) for u in db.scalars(select(User).order_by(User.username))]


@router.post("/users", tags=["users"], status_code=201)
def create_user(body: UserCreate, admin: AdminUser, db: DB) -> dict:
    if db.scalar(select(User.id).where(User.username == body.username)):
        raise HTTPException(status.HTTP_409_CONFLICT, "username already exists")
    role = db.scalar(select(Role).where(Role.name == body.role))
    if role is None:
        raise HTTPException(status.HTTP_500_INTERNAL_SERVER_ERROR, "roles not seeded")
    try:
        pw = hash_password(body.password)
    except ValueError as exc:
        raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
    u = User(username=body.username, full_name=body.full_name, password_hash=pw, role_id=role.id)
    db.add(u)
    db.flush()
    audit.record(db, admin, "USER_CREATED", "user", u.username, field="role", new=body.role)
    db.commit()
    db.refresh(u)
    return user_out(u)


@router.patch("/users/{user_id}", tags=["users"])
def update_user(user_id: IdPath, body: UserUpdate, admin: AdminUser, db: DB) -> dict:
    u = db.get(User, user_id)
    if u is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, "user not found")
    if u.id == admin.id and (body.is_active is False or (body.role and body.role != "ADMIN")):
        raise HTTPException(status.HTTP_409_CONFLICT, "administrators cannot demote or deactivate themselves")
    if body.full_name is not None:
        u.full_name = body.full_name
    if body.role is not None and body.role != u.role.name:
        audit.record(db, admin, "USER_ROLE_CHANGED", "user", u.username, field="role", old=u.role.name, new=body.role)
        u.role_id = db.scalar(select(Role.id).where(Role.name == body.role))
    if body.is_active is not None and body.is_active != u.is_active:
        audit.record(db, admin, "USER_ACTIVE_CHANGED", "user", u.username, field="is_active",
                     old=u.is_active, new=body.is_active)
        u.is_active = body.is_active
    if body.password is not None:
        try:
            u.password_hash = hash_password(body.password)
        except ValueError as exc:
            raise HTTPException(status.HTTP_422_UNPROCESSABLE_CONTENT, str(exc)) from exc
        audit.record(db, admin, "USER_PASSWORD_RESET", "user", u.username)
    db.commit()
    db.expire(u)
    return user_out(db.get(User, user_id))  # type: ignore[arg-type]
