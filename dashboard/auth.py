"""
Dashboard 인증·세션·감사로그 레이어.

설계:
- ADMIN_PASSWORD_HASH (.env, bcrypt) 기반 단일 관리자
- 세션 토큰 (HMAC + expires) — 쿠키에 저장
- IP allowlist (.env ADMIN_IPS, comma-sep)
- 모든 컨트롤 액션 → audit_log 테이블

Public read endpoints (기존 GET들)는 인증 우회.
POST /api/control/* 만 require_admin 데코레이터로 보호.
"""
from __future__ import annotations
import hashlib
import hmac
import os
import secrets
import time
from typing import Optional

from fastapi import HTTPException, Request, Response
from core import db


_SESSION_DURATION_SEC = 3600 * 24      # 24시간 세션
_LIVE_TOGGLE_RECENT_AUTH_SEC = 300     # LIVE 모드 토글은 최근 5분 내 로그인 필요


def _get_secret() -> bytes:
    secret = os.getenv("DASHBOARD_SESSION_SECRET", "")
    if not secret:
        # 첫 실행 시 자동 생성 — .env에 저장 권장
        secret = secrets.token_hex(32)
    return secret.encode()


def _get_admin_hash() -> str:
    """bcrypt hash 또는 sha256 평문 비교 fallback."""
    return os.getenv("ADMIN_PASSWORD_HASH", "")


def _get_admin_ips() -> list[str]:
    raw = os.getenv("ADMIN_IPS", "")
    return [ip.strip() for ip in raw.split(",") if ip.strip()]


def _check_password(plaintext: str) -> bool:
    """비밀번호 검증. bcrypt가 설치돼있으면 bcrypt, 아니면 sha256."""
    stored = _get_admin_hash()
    if not stored:
        return False
    if stored.startswith("$2"):
        # bcrypt format
        try:
            import bcrypt
            return bcrypt.checkpw(plaintext.encode(), stored.encode())
        except ImportError:
            return False
    # sha256 fallback (sha256:hexdigest 형식)
    if stored.startswith("sha256:"):
        return hashlib.sha256(plaintext.encode()).hexdigest() == stored[7:]
    return False


def issue_session(ip: str = "", ua: str = "") -> str:
    """로그인 성공 시 세션 토큰 발급."""
    issued_at = int(time.time())
    expires_at = issued_at + _SESSION_DURATION_SEC
    payload = f"{issued_at}|{expires_at}|{ip}"
    sig = hmac.new(_get_secret(), payload.encode(), hashlib.sha256).hexdigest()
    token = f"{payload}|{sig}"
    db.insert_audit_log("admin", "login", None, {"ip": ip, "ua": ua}, ip, ua)
    return token


def _is_private_ip(ip: str) -> bool:
    """RFC1918 / CGNAT / Cloud internal proxy IPs."""
    if not ip:
        return False
    return (ip.startswith("10.") or ip.startswith("172.16.") or ip.startswith("172.17.")
            or ip.startswith("172.18.") or ip.startswith("172.19.") or ip.startswith("172.2")
            or ip.startswith("172.30.") or ip.startswith("172.31.")
            or ip.startswith("192.168.") or ip.startswith("100.")    # CGNAT incl Railway
            or ip == "127.0.0.1" or ip == "localhost")


def verify_session(token: str, ip: str = "") -> dict | None:
    """세션 토큰 검증. 유효하면 {issued_at, expires_at, ip} 반환."""
    if not token:
        return None
    parts = token.split("|")
    if len(parts) != 4:
        return None
    issued_at, expires_at, session_ip, sig = parts
    payload = f"{issued_at}|{expires_at}|{session_ip}"
    expected_sig = hmac.new(_get_secret(), payload.encode(), hashlib.sha256).hexdigest()
    if not hmac.compare_digest(sig, expected_sig):
        return None
    try:
        issued_at_i = int(issued_at)
        expires_at_i = int(expires_at)
    except ValueError:
        return None
    if time.time() > expires_at_i:
        return None
    # IP 검증 — 세션 발급 시 IP와 다르면 거부.
    # 단, Railway/CGNAT 같은 클라우드 프록시는 매 요청마다 internal IP 바뀜 →
    # 양쪽 모두 private면 IP check 스킵 (HMAC 시그니처가 변조 방지).
    if session_ip and ip and session_ip != ip:
        if not (_is_private_ip(session_ip) and _is_private_ip(ip)):
            return None
    return {
        "issued_at": issued_at_i,
        "expires_at": expires_at_i,
        "ip": session_ip,
    }


def is_ip_allowed(ip: str) -> bool:
    allowed = _get_admin_ips()
    if not allowed:
        return True  # ADMIN_IPS 미설정 시 모든 IP 허용 (개발 편의)
    return ip in allowed


def require_admin(request: Request) -> dict:
    """FastAPI dependency — 모든 control 엔드포인트 앞에 붙임."""
    ip = request.client.host if request.client else ""
    if not is_ip_allowed(ip):
        raise HTTPException(403, f"IP {ip} not in allowlist")
    token = request.cookies.get("admin_session", "")
    sess = verify_session(token, ip)
    if not sess:
        raise HTTPException(401, "Authentication required")
    return sess


def require_recent_auth(request: Request) -> dict:
    """LIVE 토글 등 위험 액션 — 최근 5분 내 로그인 요구."""
    sess = require_admin(request)
    if time.time() - sess["issued_at"] > _LIVE_TOGGLE_RECENT_AUTH_SEC:
        raise HTTPException(401, "Recent re-authentication required (within 5 min)")
    return sess


def login(password: str, ip: str = "", ua: str = "") -> Optional[str]:
    """비밀번호 검증 후 세션 토큰 발급. 실패 시 None."""
    if not _check_password(password):
        db.insert_audit_log("anonymous", "login_failed", None, {"ip": ip}, ip, ua)
        return None
    return issue_session(ip, ua)


def logout(actor: str, ip: str = "") -> None:
    db.insert_audit_log(actor, "logout", None, None, ip, "")


def hash_password_for_env(plaintext: str) -> str:
    """.env에 넣을 ADMIN_PASSWORD_HASH 생성 도우미."""
    try:
        import bcrypt
        return bcrypt.hashpw(plaintext.encode(), bcrypt.gensalt()).decode()
    except ImportError:
        return "sha256:" + hashlib.sha256(plaintext.encode()).hexdigest()
