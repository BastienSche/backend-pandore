from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Form, Request, Header, Response, Query
from fastapi.responses import StreamingResponse, RedirectResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket
import os
import logging
import json
import re
from pathlib import Path
from pydantic import AliasChoices, BaseModel, Field, EmailStr, ConfigDict
from typing import Any, List, Optional, Literal
import uuid
import secrets
import hashlib
import smtplib
import ssl
from email.message import EmailMessage
from email.utils import formataddr
from datetime import datetime, timezone, timedelta
import bcrypt
import jwt
import shutil
import asyncio
import io
import wave
import tempfile
import subprocess
from urllib.parse import quote, urlencode
from bson import ObjectId

import stripe
from pymongo import ReturnDocument
from stripe_payments import (
    compute_platform_fee_cents,
    create_account_login_link,
    create_account_onboarding_link,
    create_checkout_session,
    create_connect_transfer,
    create_express_connected_account,
    get_min_payout_cents,
    get_platform_fee_percent,
    parse_thin_event_notification,
    retrieve_checkout_session,
    retrieve_connect_account,
    retrieve_connect_balance,
    construct_webhook_event,
    stripe_configured,
    stripe_connect_enabled,
)

ROOT_DIR = Path(__file__).parent
# Load `.env.{APP_ENV}` then optional `.env` (local overrides). Railway/Render inject vars directly.
_app_env = os.getenv("APP_ENV", "development").lower().strip()


def _runtime_app_env() -> str:
    return str(os.getenv("APP_ENV") or _app_env or "").lower().strip()


def _is_dev_runtime() -> bool:
    return _runtime_app_env() in {"development", "dev", "local", "test"}


def _allow_dev_reset_link() -> bool:
    """Reset URL in the JSON body: local only. Unset/wrong APP_ENV in prod must not leak."""
    flag = str(os.getenv("ALLOW_DEV_RESET_LINK") or "").lower().strip()
    if flag in {"1", "true", "yes"}:
        return True
    env = str(os.getenv("APP_ENV") or "").lower().strip()
    return env in {"development", "dev", "local", "test"}
_primary = ROOT_DIR / f".env.{_app_env}"
if _primary.exists():
    load_dotenv(_primary)
_legacy = ROOT_DIR / ".env"
if _legacy.exists():
    load_dotenv(_legacy, override=False)

def _read_docker_secret(name: str) -> str | None:
    """
    Read a Docker "secret" mounted by docker compose under /run/secrets/<name>.
    Returns None if not present (e.g. local dev).
    """
    try:
        p = Path("/run/secrets") / name
        if p.exists():
            v = p.read_text().strip()
            return v or None
    except Exception:
        pass
    return None

# Docker compose secrets (prod): populate env so stripe_payments.py can keep using os.environ.
_stripe_sk = _read_docker_secret("stripe_sk")
if _stripe_sk and not (os.environ.get("STRIPE_API_KEY") or "").strip():
    os.environ["STRIPE_API_KEY"] = _stripe_sk

_stripe_whsec = _read_docker_secret("stripe_whsec")
if _stripe_whsec and not (os.environ.get("STRIPE_WEBHOOK_SECRET") or "").strip():
    os.environ["STRIPE_WEBHOOK_SECRET"] = _stripe_whsec

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]
fs = AsyncIOMotorGridFSBucket(db, bucket_name="uploads")

# JWT Configuration
JWT_SECRET = os.environ.get('JWT_SECRET', 'pandore_secret_key_change_in_production_2025')
JWT_ALGORITHM = os.environ.get('JWT_ALGORITHM', 'HS256')
ACCESS_TOKEN_TTL = timedelta(hours=1)
REFRESH_TOKEN_TTL = timedelta(days=30)
ACCESS_TOKEN_MAX_AGE = int(ACCESS_TOKEN_TTL.total_seconds())
REFRESH_TOKEN_MAX_AGE = int(REFRESH_TOKEN_TTL.total_seconds())

# Stripe Configuration
STRIPE_API_KEY = os.environ.get('STRIPE_API_KEY')
STRIPE_CURRENCY_ENV = os.environ.get('STRIPE_CURRENCY', 'eur').lower()

# Google OAuth Configuration
GOOGLE_OAUTH_CLIENT_ID = os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = os.environ.get("GOOGLE_OAUTH_REDIRECT_URI")
FRONTEND_OAUTH_REDIRECT_URL = os.environ.get("FRONTEND_OAUTH_REDIRECT_URL")
PUBLIC_FRONTEND_URL = os.environ.get("PUBLIC_FRONTEND_URL", "").rstrip("/")

HARDCODED_ADMIN_EMAILS = (
    "bastien.schektman@gmail.com",
    "merwan.snk@gmail.com",
    "merwane.snk@gmail.com",
)


def canonical_email(email: str) -> str:
    """Lowercase + trim. Does not collapse Gmail dots / +tags."""
    return str(email or "").strip().strip("\"'").lower()


def normalize_email(email: str) -> str:
    """Mailbox identity for uniqueness / login only. Gmail dots and +tags collapse."""
    raw = canonical_email(email)
    if "@" not in raw:
        return raw
    local, _, domain = raw.partition("@")
    domain = domain.strip()
    if domain in {"gmail.com", "googlemail.com"}:
        local = local.split("+", 1)[0].replace(".", "")
        domain = "gmail.com"
    return f"{local}@{domain}"


def admin_emails() -> set[str]:
    """Allowlist exacte (casse ignorée). Les variantes Gmail ne sont pas admin."""
    return {canonical_email(e) for e in HARDCODED_ADMIN_EMAILS}


def email_is_listed_admin(email: str) -> bool:
    return canonical_email(email) in admin_emails()


def is_unlisted_gmail_alias_of_admin(email: str) -> bool:
    """Same Gmail mailbox as an allowlist address, but not the exact listed address."""
    if email_is_listed_admin(email):
        return False
    mailbox = normalize_email(email)
    return any(normalize_email(listed) == mailbox for listed in admin_emails())

def _load_password_reset_config() -> dict:
    cfg = {}
    candidates = [
        ROOT_DIR / "password_reset.config.json",
        ROOT_DIR / "password_reset.config.local.json",
    ]
    for path in candidates:
        if not path.exists():
            continue
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                cfg.update(raw)
        except Exception:
            logging.getLogger(__name__).exception("Impossible de lire la config reset password: %s", path)
    return cfg

PASSWORD_RESET_CONFIG = _load_password_reset_config()
PASSWORD_RESET_SMTP = PASSWORD_RESET_CONFIG.get("smtp") if isinstance(PASSWORD_RESET_CONFIG.get("smtp"), dict) else {}

FRONTEND_RESET_PASSWORD_URL = str(
    os.environ.get("FRONTEND_RESET_PASSWORD_URL")
    or PASSWORD_RESET_CONFIG.get("frontend_reset_password_url")
    or ""
).strip().rstrip("/")
PASSWORD_RESET_TOKEN_TTL_MINUTES = int(
    os.environ.get("PASSWORD_RESET_TOKEN_TTL_MINUTES")
    or PASSWORD_RESET_CONFIG.get("token_ttl_minutes")
    or 30
)

# SMTP / email : env vars override password_reset.config*.json
SMTP_HOST = str(os.environ.get("SMTP_HOST") or PASSWORD_RESET_SMTP.get("host") or "").strip()
SMTP_PORT = int(os.environ.get("SMTP_PORT") or PASSWORD_RESET_SMTP.get("port") or 587)
SMTP_USERNAME = str(os.environ.get("SMTP_USERNAME") or PASSWORD_RESET_SMTP.get("username") or "").strip()
SMTP_PASSWORD = str(
    os.environ.get("RESEND_API_KEY")
    or os.environ.get("SMTP_PASSWORD")
    or PASSWORD_RESET_SMTP.get("password")
    or ""
)
SMTP_FROM_EMAIL = str(
    os.environ.get("SMTP_FROM_EMAIL") or PASSWORD_RESET_SMTP.get("from_email") or ""
).strip()
SMTP_FROM_NAME = str(
    os.environ.get("SMTP_FROM_NAME") or PASSWORD_RESET_SMTP.get("from_name") or "Kloud"
).strip()
SMTP_REPLY_TO = str(
    os.environ.get("SMTP_REPLY_TO") or PASSWORD_RESET_SMTP.get("reply_to") or ""
).strip()
SMTP_USE_TLS = str(
    os.environ.get("SMTP_USE_TLS")
    if os.environ.get("SMTP_USE_TLS") is not None
    else PASSWORD_RESET_SMTP.get("use_tls", True)
).strip().lower() in {"1", "true", "yes", "on"}

app = FastAPI()
api_router = APIRouter(prefix="/api")

logger = logging.getLogger(__name__)

async def log_transaction_event(*, kind: str, data: dict) -> None:
    """
    Journal des transactions (achats / ajouts) sans données sensibles.
    Ne stocke jamais de numéro de carte, IBAN, billing details, etc.
    """
    try:
        await db.transactions.insert_one(
            {
                "event_id": f"txe_{uuid.uuid4().hex[:12]}",
                "kind": kind,
                "data": data,
                "created_at": datetime.now(timezone.utc).isoformat(),
            }
        )
    except Exception:
        logger.exception("log_transaction_event kind=%s", kind)


def purchase_amount_cents(p: dict) -> float:
    """Montant en centimes (achats Stripe : price_paid ; ancien champ : amount)."""
    for key in ("amount", "price_paid"):
        v = p.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return 0.0


def purchase_seller_net_cents(p: dict) -> float:
    """Part vendeur après commission plateforme. Sinon fallback sur le prix payé (legacy)."""
    v = p.get("seller_net_cents")
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    return purchase_amount_cents(p)


def _as_int_cents(value, default: int = 0) -> int:
    try:
        return int(round(float(value)))
    except (TypeError, ValueError):
        return default


def _connect_account_can_receive(acct) -> bool:
    if not acct:
        return False
    if getattr(acct, "payouts_enabled", False) or getattr(acct, "charges_enabled", False):
        return True
    caps = getattr(acct, "capabilities", None)
    if caps is None:
        return False
    transfers = caps.get("transfers") if isinstance(caps, dict) else getattr(caps, "transfers", None)
    return str(transfers or "") == "active"


async def _credit_artist_wallet_from_paid_transaction(transaction: dict) -> bool:
    """Crédite la cagnotte artiste une seule fois (webhook + polling status)."""
    if not transaction or transaction.get("settlement") != "wallet":
        return False
    txn_id = transaction.get("transaction_id")
    artist_id = transaction.get("artist_id")
    net = _as_int_cents(transaction.get("seller_amount_cents"), 0)
    if not txn_id or not artist_id or net <= 0:
        return False
    now = datetime.now(timezone.utc).isoformat()
    claimed = await db.payment_transactions.find_one_and_update(
        {"transaction_id": txn_id, "wallet_credited": {"$ne": True}},
        {"$set": {"wallet_credited": True, "wallet_credited_at": now}},
        projection={"transaction_id": 1},
    )
    if not claimed:
        return False
    await db.users.update_one(
        {"user_id": artist_id},
        {"$inc": {"wallet_available_cents": net}},
    )
    await db.wallet_ledger.insert_one(
        {
            "entry_id": f"wle_{uuid.uuid4().hex[:12]}",
            "artist_id": artist_id,
            "kind": "sale_credit",
            "amount_cents": net,
            "transaction_id": txn_id,
            "created_at": now,
        }
    )
    return True


# ==================== PUBLIC ROUTES ====================

@api_router.get("/public/stats")
async def get_public_stats():
    """
    Public, no-auth stats for homepage.
    Returns total users, total artists, total published tracks.
    """
    try:
        users_count = await db.users.count_documents({})
    except Exception:
        users_count = 0
    try:
        artists_count = await db.users.count_documents({"role": "artist"})
    except Exception:
        artists_count = 0
    try:
        tracks_count = await db.tracks.count_documents({"status": "published"})
    except Exception:
        # backward compat (some envs may not have status)
        tracks_count = await db.tracks.count_documents({})

    return {
        "users_count": int(users_count or 0),
        "artists_count": int(artists_count or 0),
        "tracks_count": int(tracks_count or 0),
    }


def _parse_iso_dt(value) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        dt = value
    else:
        try:
            dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        except Exception:
            return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def contest_phase(contest: dict, now: Optional[datetime] = None) -> str:
    """draft | upcoming | live | ended"""
    if not contest.get("is_published"):
        return "draft"
    now = now or datetime.now(timezone.utc)
    start = _parse_iso_dt(contest.get("starts_at"))
    end = _parse_iso_dt(contest.get("ends_at"))
    if start and now < start:
        return "upcoming"
    if end and now > end:
        return "ended"
    return "live"


async def compute_contest_standings(
    starts_at: str,
    ends_at: str,
    *,
    limit: int = 50,
    series_top: int = 3,
) -> dict:
    """
    Classement revenus artistes (euros générés = part vendeur des achats payés)
    sur [starts_at, ends_at].
    """
    start = _parse_iso_dt(starts_at)
    end = _parse_iso_dt(ends_at)
    if not start or not end or end < start:
        return {"leaderboard": [], "series": [], "days": []}

    purchases = await db.purchases.find(
        {
            "purchased_at": {"$gte": start.isoformat(), "$lte": end.isoformat()},
            "artist_id": {"$exists": True, "$ne": None},
        },
        {
            "_id": 0,
            "artist_id": 1,
            "purchased_at": 1,
            "amount": 1,
            "price_paid": 1,
            "seller_net_cents": 1,
        },
    ).to_list(20000)

    # Fallback: paid Stripe transactions if purchases are sparse
    if not purchases:
        txns = await db.payment_transactions.find(
            {
                "payment_status": "paid",
                "created_at": {"$gte": start.isoformat(), "$lte": end.isoformat()},
                "artist_id": {"$exists": True, "$ne": None},
            },
            {
                "_id": 0,
                "artist_id": 1,
                "created_at": 1,
                "amount": 1,
                "seller_amount_cents": 1,
            },
        ).to_list(20000)
        purchases = [
            {
                "artist_id": t.get("artist_id"),
                "purchased_at": t.get("created_at"),
                "amount": t.get("amount"),
                "seller_net_cents": t.get("seller_amount_cents"),
            }
            for t in txns
        ]

    totals: dict[str, float] = {}
    daily_by_artist: dict[str, dict[str, float]] = {}

    for p in purchases:
        artist_id = str(p.get("artist_id") or "").strip()
        if not artist_id:
            continue
        cents = purchase_seller_net_cents(p)
        if cents <= 0:
            # ignore free library adds
            if purchase_amount_cents(p) <= 0:
                continue
            cents = purchase_amount_cents(p)
        if cents <= 0:
            continue
        totals[artist_id] = totals.get(artist_id, 0.0) + cents
        day = str(p.get("purchased_at") or "")[:10]
        if len(day) == 10:
            bucket = daily_by_artist.setdefault(artist_id, {})
            bucket[day] = bucket.get(day, 0.0) + cents

    ranked = sorted(totals.items(), key=lambda kv: kv[1], reverse=True)[: max(1, limit)]
    artist_ids = [aid for aid, _ in ranked]
    artists = await db.users.find(
        {"user_id": {"$in": artist_ids}},
        {"_id": 0, "user_id": 1, "artist_name": 1, "name": 1, "picture": 1},
    ).to_list(len(artist_ids))
    by_id = {a["user_id"]: a for a in artists}

    profiles = await db.artist_profiles.find(
        {"user_id": {"$in": artist_ids}},
        {"_id": 0, "user_id": 1, "genres": 1, "genre": 1, "avatar_url": 1},
    ).to_list(len(artist_ids))
    genres_by_id = {p["user_id"]: p for p in profiles}

    leaderboard = []
    for idx, (aid, cents) in enumerate(ranked, start=1):
        u = by_id.get(aid) or {"user_id": aid}
        u = await _ensure_default_picture(u)
        by_id[aid] = u
        gdoc = genres_by_id.get(aid) or {}
        genres = gdoc.get("genres") or gdoc.get("genre") or []
        if isinstance(genres, str):
            genre_label = genres
        elif isinstance(genres, list):
            genre_label = " · ".join(str(x) for x in genres[:2] if x)
        else:
            genre_label = ""
        leaderboard.append(
            {
                "rank": idx,
                "artist_id": aid,
                "artist_name": u.get("artist_name") or u.get("name") or "Artiste",
                "picture": _custom_avatar_url(gdoc.get("avatar_url"), u.get("picture")),
                "genre": genre_label or None,
                "revenue_cents": int(round(cents)),
            }
        )

    # Day labels across contest window (cap chart points for UX)
    days: list[str] = []
    cursor = start.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    end_day = end.astimezone(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
    while cursor <= end_day and len(days) < 62:
        days.append(cursor.date().isoformat())
        cursor += timedelta(days=1)

    top_for_series = leaderboard[: max(0, series_top)]
    series = []
    for entry in top_for_series:
        aid = entry["artist_id"]
        daily = daily_by_artist.get(aid) or {}
        cumulative = 0.0
        points = []
        for day in days:
            cumulative += float(daily.get(day) or 0.0)
            points.append({"day": day, "revenue_cents": int(round(cumulative))})
        series.append(
            {
                "artist_id": aid,
                "artist_name": entry["artist_name"],
                "picture": entry["picture"],
                "rank": entry["rank"],
                "points": points,
            }
        )

    return {"leaderboard": leaderboard, "series": series, "days": days}


def ensure_stripe_available():
    if not stripe_configured():
        raise HTTPException(
            status_code=503,
            detail=(
                "Stripe non configuré : définir STRIPE_API_KEY sur le serveur "
                "(Dashboard Stripe → Developers → API keys → Secret key)."
            ),
        )

def is_free_item_price(price) -> bool:
    """Prix stocké en centimes, aligné avec le frontend (`isFreePrice`)."""
    try:
        cents = float(price or 0)
    except (TypeError, ValueError):
        cents = 0.0
    euros = cents / 100.0
    return f"{euros:.2f}" == "0.00"

# ==================== MODELS ====================

class UserRegister(BaseModel):
    email: EmailStr
    password: str = Field(..., min_length=6)
    name: str
    artist_name: Optional[str] = None

class UserLogin(BaseModel):
    email: EmailStr
    password: str

class UserResponse(BaseModel):
    user_id: str
    email: str
    name: str
    picture: Optional[str] = None
    role: str
    artist_name: Optional[str] = None
    created_at: str
    is_admin: bool = False

class GoogleSessionRequest(BaseModel):
    session_id: str

class GoogleAuthCodeRequest(BaseModel):
    code: str
    state: Optional[str] = None

class GoogleSessionResponse(BaseModel):
    user_id: str
    email: str
    name: str
    picture: Optional[str]
    session_token: str

class ForgotPasswordRequest(BaseModel):
    email: EmailStr

class ResetPasswordRequest(BaseModel):
    token: str
    new_password: str = Field(..., min_length=6)

class TrackCreate(BaseModel):
    title: str
    price: float  # Prix en cents
    is_free_price: bool = False
    min_price: Optional[float] = None  # Minimum en cents (si prix libre)
    genre: str
    description: Optional[str] = None
    album_id: Optional[str] = None
    preview_start_time: int = 0
    preview_duration_sec: Optional[int] = 15
    duration_sec: Optional[int] = None
    mastering: Optional[dict] = None  # {"engineer": "name", "details": "info"}
    splits: Optional[List[dict]] = None  # [{"party": "name", "percent": 50}]
    bpm: Optional[int] = None
    key: Optional[str] = None
    isrc: Optional[str] = None
    release_date: Optional[str] = None
    mix_version: Optional[str] = None  # maquette|mixed
    availability: Optional[str] = None  # exclusive|all_platforms
    producer: Optional[str] = None
    beatmaker: Optional[str] = None
    status: str = "draft"  # draft|published

class TrackResponse(BaseModel):
    track_id: str
    title: str
    artist_id: str
    artist_name: str
    album_id: Optional[str]
    price: float
    is_free_price: bool = False
    min_price: Optional[float] = None
    duration: Optional[int]
    preview_url: str
    preview_start_time: int
    preview_duration: int
    file_url: Optional[str] = None
    cover_url: Optional[str]
    genre: str
    description: Optional[str]
    mastering: Optional[dict]
    splits: Optional[List[dict]]
    bpm: Optional[int] = None
    key: Optional[str] = None
    isrc: Optional[str] = None
    release_date: Optional[str] = None
    mix_version: Optional[str] = None
    availability: Optional[str] = None
    producer: Optional[str] = None
    beatmaker: Optional[str] = None
    status: str
    likes_count: int
    play_count: int = 0
    created_at: str

class AlbumCreate(BaseModel):
    title: str
    price: float  # Prix en cents
    is_free_price: bool = False
    min_price: Optional[float] = None  # Minimum en cents (si prix libre)
    description: Optional[str] = None
    mix_version: Optional[str] = None  # maquette|mixed
    availability: Optional[str] = None  # exclusive|all_platforms
    producer: Optional[str] = None
    beatmaker: Optional[str] = None
    status: str = "draft"  # draft|published

class AlbumResponse(BaseModel):
    album_id: str
    title: str
    artist_id: str
    artist_name: str
    price: float
    is_free_price: bool = False
    min_price: Optional[float] = None
    cover_url: Optional[str]
    description: Optional[str]
    mix_version: Optional[str] = None
    availability: Optional[str] = None
    producer: Optional[str] = None
    beatmaker: Optional[str] = None
    track_ids: List[str]
    status: str
    likes_count: int
    play_count: int = 0
    created_at: str

class AlbumTrackCreate(BaseModel):
    title: str
    price: float  # Prix en cents
    is_free_price: bool = False
    min_price: Optional[float] = None  # Minimum en cents (si prix libre)
    genre: str
    description: Optional[str] = None
    preview_start_time: int = 0
    preview_duration_sec: Optional[int] = 15
    duration_sec: Optional[int] = None
    mastering: Optional[dict] = None
    splits: Optional[List[dict]] = None
    bpm: Optional[int] = None
    key: Optional[str] = None
    isrc: Optional[str] = None
    release_date: Optional[str] = None
    mix_version: Optional[str] = None
    availability: Optional[str] = None
    producer: Optional[str] = None
    beatmaker: Optional[str] = None
    status: str = "draft"

class AlbumTrackResponse(BaseModel):
    track_id: str
    album_id: str
    title: str
    artist_id: str
    artist_name: str
    price: float
    is_free_price: bool = False
    min_price: Optional[float] = None
    duration: Optional[int]
    preview_url: str
    preview_start_time: int
    preview_duration: int
    file_url: Optional[str] = None
    cover_url: Optional[str]
    genre: str
    description: Optional[str]
    mastering: Optional[dict]
    splits: Optional[List[dict]]
    bpm: Optional[int] = None
    key: Optional[str] = None
    isrc: Optional[str] = None
    release_date: Optional[str] = None
    mix_version: Optional[str] = None
    availability: Optional[str] = None
    producer: Optional[str] = None
    beatmaker: Optional[str] = None
    status: str
    likes_count: int
    play_count: int = 0
    created_at: str

class PlaylistCreate(BaseModel):
    name: str
    description: Optional[str] = None

class PlaylistUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None

class PlaylistResponse(BaseModel):
    playlist_id: str
    user_id: str
    name: str
    description: Optional[str]
    track_ids: List[str]
    created_at: str
    updated_at: str

class LikeRequest(BaseModel):
    item_type: Literal["track", "album", "artist"]
    item_id: str

class LikesStateResponse(BaseModel):
    model_config = ConfigDict(extra="allow")

class ArtistProfileCreate(BaseModel):
    model_config = ConfigDict(populate_by_name=True)
    name: str = Field(..., validation_alias=AliasChoices("name", "artist_name"))
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    links: Optional[List[str]] = None

class ArtistProfileResponse(BaseModel):
    profile_id: str
    user_id: str
    name: str
    bio: Optional[str]
    avatar_url: Optional[str]
    links: Optional[List[str]]
    created_at: str
    updated_at: str

class CheckoutRequest(BaseModel):
    item_type: Literal["track", "album"]
    item_id: str = Field(..., min_length=1)
    origin_url: str
    amount_cents: Optional[float] = None  # Utilisé uniquement si prix libre

class AddToLibraryRequest(BaseModel):
    item_type: Literal["track", "album"]
    item_id: str = Field(..., min_length=1)

class FollowRequest(BaseModel):
    artist_id: str

class UserSettings(BaseModel):
    autoplay: bool = True
    normalize_volume: bool = True
    high_quality_streaming: bool = True
    notifications_new_releases: bool = True
    notifications_recommendations: bool = True
    notifications_purchases: bool = True
    notifications_likes: bool = True
    privacy_share_listening_activity: bool = False
    privacy_personalized_ads: bool = False

# ==================== ADMIN MODELS ====================

class AdminTrackUpdate(BaseModel):
    title: Optional[str] = None
    status: Optional[str] = None
    price: Optional[float] = None
    is_free_price: Optional[bool] = None
    min_price: Optional[float] = None


class AdminAlbumUpdate(BaseModel):
    title: Optional[str] = None
    status: Optional[str] = None
    price: Optional[float] = None

class FeedbackSubmissionResponse(BaseModel):
    feedback_id: str
    title: str
    description: str
    image_url: Optional[str] = None
    image_name: Optional[str] = None
    reporter_user_id: Optional[str] = None
    reporter_email: Optional[str] = None
    reporter_name: Optional[str] = None
    created_at: str
    status: str = "new"

class ContestCreate(BaseModel):
    title: str = "Course aux revenus"
    subtitle: Optional[str] = "Les artistes qui génèrent le plus d'euros"
    starts_at: str
    ends_at: str
    is_published: bool = False
    prize_label: Optional[str] = None

class ContestUpdate(BaseModel):
    title: Optional[str] = None
    subtitle: Optional[str] = None
    starts_at: Optional[str] = None
    ends_at: Optional[str] = None
    is_published: Optional[bool] = None
    prize_label: Optional[str] = None

# ==================== AUTH HELPERS ====================

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_jwt_token(user_id: str) -> str:
    """Access token court (compat: ancien nom create_jwt_token)."""
    payload = {
        "user_id": user_id,
        "type": "access",
        "exp": datetime.now(timezone.utc) + ACCESS_TOKEN_TTL,
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def hash_token(raw_token: str) -> str:
    return hashlib.sha256(raw_token.encode("utf-8")).hexdigest()

def hash_reset_token(raw_token: str) -> str:
    return hash_token(raw_token)

async def create_refresh_token(user_id: str) -> str:
    raw = secrets.token_urlsafe(48)
    now = datetime.now(timezone.utc)
    await db.refresh_tokens.insert_one(
        {
            "token_hash": hash_token(raw),
            "user_id": user_id,
            "created_at": now.isoformat(),
            "expires_at": (now + REFRESH_TOKEN_TTL).isoformat(),
            "revoked_at": None,
        }
    )
    return raw

def set_auth_cookies(response: Response, access_token: str, refresh_token: str) -> None:
    response.set_cookie(
        key="session_token",
        value=access_token,
        httponly=False,
        secure=False,
        samesite="lax",
        path="/",
        max_age=ACCESS_TOKEN_MAX_AGE,
    )
    response.set_cookie(
        key="refresh_token",
        value=refresh_token,
        httponly=False,
        secure=False,
        samesite="lax",
        path="/",
        max_age=REFRESH_TOKEN_MAX_AGE,
    )

def clear_auth_cookies(response: Response) -> None:
    response.delete_cookie(key="session_token", path="/")
    response.delete_cookie(key="refresh_token", path="/")

async def issue_auth_tokens(user_id: str, response: Optional[Response] = None) -> dict:
    access_token = create_jwt_token(user_id)
    refresh_token = await create_refresh_token(user_id)
    if response is not None:
        set_auth_cookies(response, access_token, refresh_token)
    return {
        "token": access_token,
        "access_token": access_token,
        "refresh_token": refresh_token,
        "expires_in": ACCESS_TOKEN_MAX_AGE,
    }

async def revoke_refresh_token(raw_token: Optional[str]) -> None:
    if not raw_token:
        return
    await db.refresh_tokens.update_one(
        {"token_hash": hash_token(raw_token), "revoked_at": None},
        {"$set": {"revoked_at": datetime.now(timezone.utc).isoformat()}},
    )

async def revoke_user_refresh_tokens(user_id: str) -> None:
    await db.refresh_tokens.update_many(
        {"user_id": user_id, "revoked_at": None},
        {"$set": {"revoked_at": datetime.now(timezone.utc).isoformat()}},
    )

def _parse_iso_dt(value) -> datetime:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt

async def rotate_refresh_token(raw_token: str, response: Optional[Response] = None) -> dict:
    token_hash = hash_token(raw_token)
    doc = await db.refresh_tokens.find_one({"token_hash": token_hash}, {"_id": 0})
    if not doc or doc.get("revoked_at"):
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    expires_at = _parse_iso_dt(doc["expires_at"])
    if expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=401, detail="Refresh token expired")

    user_id = doc["user_id"]
    user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0})
    if not user_doc:
        raise HTTPException(status_code=401, detail="User not found")

    # Rotation : invalide l'ancien refresh avant d'en émettre un nouveau
    await db.refresh_tokens.update_one(
        {"token_hash": token_hash},
        {"$set": {"revoked_at": datetime.now(timezone.utc).isoformat()}},
    )
    tokens = await issue_auth_tokens(user_id, response)
    return {
        **tokens,
        "user": user_response_from_doc(await ensure_admin_flag(user_doc)),
    }

def _request_frontend_origin(request: Optional[Request]) -> str:
    if request is None:
        return ""
    raw = str(request.headers.get("origin") or request.headers.get("referer") or "").strip()
    if not raw:
        return ""
    try:
        from urllib.parse import urlparse

        parsed = urlparse(raw)
        if parsed.scheme in {"http", "https"} and parsed.netloc:
            return f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        return ""
    return ""


def build_reset_password_url(raw_token: str, request: Optional[Request] = None) -> str:
    env_override = str(os.environ.get("FRONTEND_RESET_PASSWORD_URL") or "").strip().rstrip("/")
    origin = _request_frontend_origin(request)
    if _is_dev_runtime() and origin and ("localhost" in origin or "127.0.0.1" in origin):
        base = f"{origin}/reset-password"
    elif env_override:
        base = env_override
    elif _is_dev_runtime():
        base = "http://localhost:3001/reset-password"
    elif FRONTEND_RESET_PASSWORD_URL:
        base = FRONTEND_RESET_PASSWORD_URL
    elif PUBLIC_FRONTEND_URL:
        base = f"{PUBLIC_FRONTEND_URL.rstrip('/')}/reset-password"
    else:
        base = "http://localhost:3000/reset-password"
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}token={raw_token}"

def smtp_configured() -> bool:
    return bool(SMTP_FROM_EMAIL and SMTP_PASSWORD and (SMTP_HOST or SMTP_PASSWORD.startswith("re_")))

def _password_reset_email_bodies(*, to_name: Optional[str], reset_url: str) -> tuple[str, str, str]:
    subject = "Réinitialisation de votre mot de passe Kloud"
    recipient_name = (to_name or "").strip() or "there"
    plain_text = (
        f"Bonjour {recipient_name},\n\n"
        "Nous avons reçu une demande de réinitialisation de mot de passe.\n"
        f"Ouvrir ce lien (valide {PASSWORD_RESET_TOKEN_TTL_MINUTES} minutes) :\n{reset_url}\n\n"
        "Si vous n'êtes pas à l'origine de cette demande, ignorez cet email.\n\n"
        "— Kloud"
    )
    html = (
        f"<p>Bonjour {recipient_name},</p>"
        "<p>Nous avons reçu une demande de réinitialisation de mot de passe.</p>"
        f"<p><a href=\"{reset_url}\">Réinitialiser mon mot de passe</a></p>"
        f"<p>Ce lien est valide <strong>{PASSWORD_RESET_TOKEN_TTL_MINUTES} minutes</strong>.</p>"
        "<p>Si vous n'êtes pas à l'origine de cette demande, ignorez cet email.</p>"
        "<p>— Kloud</p>"
    )
    return subject, plain_text, html

def send_password_reset_email_sync(*, to_email: str, to_name: Optional[str], reset_url: str) -> None:
    subject, plain_text, html = _password_reset_email_bodies(to_name=to_name, reset_url=reset_url)

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = formataddr((SMTP_FROM_NAME, SMTP_FROM_EMAIL))
    msg["To"] = to_email
    if SMTP_REPLY_TO:
        msg["Reply-To"] = SMTP_REPLY_TO
    msg.set_content(plain_text)
    msg.add_alternative(html, subtype="html")

    host = SMTP_HOST or "smtp.resend.com"
    port = SMTP_PORT or 587

    # Port 465 = SMTPS implicite ; 587 = STARTTLS
    if port in (465, 2465):
        with smtplib.SMTP_SSL(host, port, timeout=20, context=ssl.create_default_context()) as smtp:
            if SMTP_USERNAME:
                smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
            smtp.send_message(msg)
        return

    with smtplib.SMTP(host, port, timeout=20) as smtp:
        smtp.ehlo()
        if SMTP_USE_TLS:
            smtp.starttls(context=ssl.create_default_context())
            smtp.ehlo()
        if SMTP_USERNAME:
            smtp.login(SMTP_USERNAME, SMTP_PASSWORD)
        smtp.send_message(msg)

async def send_password_reset_email_resend_api(*, to_email: str, to_name: Optional[str], reset_url: str) -> None:
    """Envoi via l'API HTTP Resend (plus fiable que SMTP derrière certains hébergeurs)."""
    import aiohttp

    subject, plain_text, html = _password_reset_email_bodies(to_name=to_name, reset_url=reset_url)
    payload = {
        "from": f"{SMTP_FROM_NAME} <{SMTP_FROM_EMAIL}>",
        "to": [to_email],
        "subject": subject,
        "html": html,
        "text": plain_text,
    }
    if SMTP_REPLY_TO:
        payload["reply_to"] = SMTP_REPLY_TO

    headers = {
        "Authorization": f"Bearer {SMTP_PASSWORD}",
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
    }
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://api.resend.com/emails",
            json=payload,
            headers=headers,
            timeout=aiohttp.ClientTimeout(total=20),
        ) as resp:
            body = await resp.text()
            if resp.status >= 400:
                raise RuntimeError(f"Resend API {resp.status}: {body[:500]}")

def _resend_key_rejected(exc: BaseException) -> bool:
    text = str(exc).lower()
    return any(
        needle in text
        for needle in (
            "suspended",
            "api key is invalid",
            "invalid api key",
            "invalid_api_key",
            "suspended_api_key",
        )
    )


async def send_password_reset_email(*, to_email: str, to_name: Optional[str], reset_url: str) -> None:
    use_resend_api = SMTP_PASSWORD.startswith("re_") or (SMTP_HOST or "").endswith("resend.com")
    if use_resend_api:
        try:
            await send_password_reset_email_resend_api(
                to_email=to_email, to_name=to_name, reset_url=reset_url
            )
            return
        except Exception as exc:
            if _resend_key_rejected(exc):
                logger.error(
                    "Resend key rejected (%s). Create a new key at resend.com "
                    "and set RESEND_API_KEY or password_reset.config.local.json",
                    exc,
                )
                raise
            logger.exception("Resend API failed, falling back to SMTP to=%s", to_email)
    await asyncio.to_thread(
        send_password_reset_email_sync,
        to_email=to_email,
        to_name=to_name,
        reset_url=reset_url,
    )

def _env_text(value: Optional[str]) -> str:
    return str(value or "").strip()


def google_oauth_is_configured() -> bool:
    return bool(
        _env_text(GOOGLE_OAUTH_CLIENT_ID)
        and _env_text(GOOGLE_OAUTH_CLIENT_SECRET)
        and _env_text(GOOGLE_OAUTH_REDIRECT_URI)
    )


def ensure_google_oauth_configured():
    if not google_oauth_is_configured():
        raise HTTPException(status_code=503, detail="Connexion Google non configurée")


def frontend_oauth_callback_url(tokens: Optional[dict] = None, error: Optional[str] = None) -> Optional[str]:
    base = _env_text(FRONTEND_OAUTH_REDIRECT_URL)
    if not base and PUBLIC_FRONTEND_URL:
        base = f"{PUBLIC_FRONTEND_URL}/auth/callback"
    if not base:
        return None
    params = {}
    if error:
        params["error"] = error
    if tokens:
        access = tokens.get("access_token") or tokens.get("token") or ""
        refresh = tokens.get("refresh_token") or ""
        if access:
            params["access_token"] = access
        if refresh:
            params["refresh_token"] = refresh
    if not params:
        return base
    sep = "&" if "?" in base else "?"
    return f"{base}{sep}{urlencode(params)}"

def validate_oauth_state(request: Request, state: Optional[str]):
    expected_state = request.cookies.get("oauth_state") if request else None
    if expected_state:
        if not state or state != expected_state:
            raise HTTPException(status_code=400, detail="Invalid OAuth state")

DEFAULT_MASK_AVATARS = [
    "/avatars/mask-01.jpg",
    "/avatars/mask-02.jpg",
    "/avatars/mask-03.jpg",
    "/avatars/mask-04.jpg",
    "/avatars/mask-05.jpg",
    "/avatars/mask-06.jpg",
    "/avatars/mask-07.jpg",
    "/avatars/mask-08.jpg",
]


def _fnv1a_32(text: str) -> int:
    h = 2166136261
    for ch in str(text or ""):
        h ^= ord(ch)
        h = (h * 16777619) & 0xFFFFFFFF
    return h


def _is_mask_avatar(url: Optional[str]) -> bool:
    return "/avatars/mask-" in str(url or "")


def _default_mask_avatar(user_id: Optional[str]) -> str:
    key = str(user_id or "artist")
    return DEFAULT_MASK_AVATARS[_fnv1a_32(key) % len(DEFAULT_MASK_AVATARS)]


async def _ensure_default_picture(user: Optional[dict]) -> Optional[dict]:
    """Assign a stable default mask once; never overwrite a custom photo."""
    if not user or not user.get("user_id"):
        return user
    picture = str(user.get("picture") or "").strip()
    if picture:
        return user
    url = _default_mask_avatar(user["user_id"])
    await db.users.update_one({"user_id": user["user_id"]}, {"$set": {"picture": url}})
    user["picture"] = url
    return user


async def get_current_user(authorization: Optional[str] = Header(None), request: Request = None) -> dict:
    """Get current user from JWT token in Authorization header or session_token cookie"""
    token = None
    
    # Try cookie first
    if request:
        token = request.cookies.get("session_token")
    
    # Fallback to Authorization header
    if not token and authorization:
        if authorization.startswith("Bearer "):
            token = authorization.replace("Bearer ", "")
        else:
            token = authorization
    
    if not token:
        raise HTTPException(status_code=401, detail="Connecte-toi pour continuer")
    
    # Check if it's a JWT token (custom auth)
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        token_type = payload.get("type")
        # Accepte les access tokens + les JWT legacy (sans champ type)
        if token_type and token_type != "access":
            raise HTTPException(status_code=401, detail="Session invalide")
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Session invalide")
        
        user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0})
        if not user_doc:
            raise HTTPException(status_code=401, detail="Compte introuvable")
        return await ensure_admin_flag(user_doc)
    except HTTPException:
        raise
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Session expirée, reconnecte-toi")
    except jwt.InvalidTokenError:
        pass
    
    # Check if it's a Google OAuth session token
    session_doc = await db.user_sessions.find_one({"session_token": token}, {"_id": 0})
    if not session_doc:
        raise HTTPException(status_code=401, detail="Session invalide")
    
    # Check expiry
    expires_at = _parse_iso_dt(session_doc["expires_at"])
    if expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=401, detail="Session expirée, reconnecte-toi")
    
    user_doc = await db.users.find_one({"user_id": session_doc["user_id"]}, {"_id": 0})
    if not user_doc:
        raise HTTPException(status_code=401, detail="Compte introuvable")

    return await ensure_admin_flag(user_doc)

def user_is_admin(user: Optional[dict]) -> bool:
    if not user:
        return False
    email = user.get("email") or ""
    if email_is_listed_admin(email):
        return True
    # Gmail +tag / dots of a listed address must never inherit admin, even if is_admin was persisted.
    if is_unlisted_gmail_alias_of_admin(email):
        return False
    return bool(user.get("is_admin"))


async def ensure_admin_flag(user: dict) -> dict:
    """Keep is_admin aligned with the exact allowlist; strip alias-based grants."""
    if not user:
        return user
    email = user.get("email") or ""
    if is_unlisted_gmail_alias_of_admin(email):
        if user.get("is_admin"):
            await db.users.update_one({"user_id": user["user_id"]}, {"$set": {"is_admin": False}})
            user["is_admin"] = False
        return await _ensure_default_picture(user)
    if email_is_listed_admin(email) and not user.get("is_admin"):
        await db.users.update_one({"user_id": user["user_id"]}, {"$set": {"is_admin": True}})
        user["is_admin"] = True
    return await _ensure_default_picture(user)

def user_response_from_doc(user: dict) -> UserResponse:
    return UserResponse(
        user_id=user["user_id"],
        email=user["email"],
        name=user.get("name", ""),
        picture=user.get("picture"),
        role=user.get("role", "user"),
        artist_name=user.get("artist_name"),
        created_at=user.get("created_at", ""),
        is_admin=user_is_admin(user),
    )

def require_admin(user: dict) -> None:
    if not user_is_admin(user):
        raise HTTPException(status_code=403, detail="Admin only")

# ==================== AUTH ROUTES ====================

@api_router.post("/auth/register", response_model=UserResponse)
async def register(user_data: UserRegister):
    email = canonical_email(user_data.email)
    if await find_user_by_email(email):
        raise HTTPException(status_code=400, detail="Email already registered")
    
    user_id = f"user_{uuid.uuid4().hex[:12]}"
    hashed_pwd = hash_password(user_data.password)
    
    user_doc = {
        "user_id": user_id,
        "email": email,
        "password_hash": hashed_pwd,
        "name": user_data.name,
        "picture": _default_mask_avatar(user_id),
        "role": "artist" if user_data.artist_name else "user",
        "artist_name": user_data.artist_name,
        "is_admin": email_is_listed_admin(email),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.users.insert_one(user_doc)
    return user_response_from_doc(user_doc)

async def find_user_by_email(email: str) -> Optional[dict]:
    raw = str(email or "").strip()
    if not raw:
        return None
    doc = await db.users.find_one(
        {"email": {"$regex": f"^{re.escape(raw)}$", "$options": "i"}},
        {"_id": 0},
    )
    if doc:
        return doc
    target = normalize_email(raw)
    cursor = db.users.find({}, {"_id": 0, "password_hash": 0})
    async for user in cursor:
        if normalize_email(user.get("email") or "") == target:
            full = await db.users.find_one({"user_id": user["user_id"]}, {"_id": 0})
            return full
    return None


@api_router.post("/auth/login")
async def login(credentials: UserLogin, response: Response):
    user_doc = await find_user_by_email(str(credentials.email))
    if not user_doc or not user_doc.get("password_hash") or not verify_password(credentials.password, user_doc["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")

    user_doc = await ensure_admin_flag(user_doc)
    tokens = await issue_auth_tokens(user_doc["user_id"], response)
    return {
        **tokens,
        "user": user_response_from_doc(user_doc),
    }

async def _read_refresh_token_from_request(request: Request) -> Optional[str]:
    raw = request.cookies.get("refresh_token") if request else None
    try:
        body = await request.json()
        if isinstance(body, dict) and body.get("refresh_token"):
            raw = body.get("refresh_token")
    except Exception:
        pass
    return raw

@api_router.post("/auth/refresh")
async def refresh_auth(request: Request, response: Response):
    """Échange un refresh token valide contre une nouvelle paire access/refresh."""
    raw = await _read_refresh_token_from_request(request)
    if not raw:
        raise HTTPException(status_code=401, detail="Missing refresh token")
    return await rotate_refresh_token(raw, response)

@api_router.post("/auth/forgot-password")
async def forgot_password(payload: ForgotPasswordRequest, request: Request):
    """
    Toujours renvoyer un message générique pour éviter l'énumération des emails.
    En local/dev sans SMTP (ou si l'envoi échoue), on renvoie aussi le token/lien pour faciliter les tests.
    """
    generic_message = "Si cet email existe, un lien de réinitialisation a été envoyé."
    # Même lookup souple que le login (casse / Gmail dots)
    user_doc = await find_user_by_email(str(payload.email))
    if not user_doc:
        return {"message": generic_message}

    if not user_doc.get("password_hash"):
        # Compte OAuth-only : pas de reset mot de passe email
        logger.info("password-reset skipped (no password) user_id=%s", user_doc.get("user_id"))
        return {"message": generic_message}

    raw_token = secrets.token_urlsafe(32)
    token_hash = hash_reset_token(raw_token)
    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(minutes=PASSWORD_RESET_TOKEN_TTL_MINUTES)

    # Invalide les anciens tokens non utilisés pour ce compte
    await db.password_reset_tokens.delete_many({"user_id": user_doc["user_id"], "used_at": None})
    await db.password_reset_tokens.insert_one(
        {
            "token_hash": token_hash,
            "user_id": user_doc["user_id"],
            "email": user_doc.get("email"),
            "created_at": now.isoformat(),
            "expires_at": expires_at.isoformat(),
            "used_at": None,
        }
    )

    reset_url = build_reset_password_url(raw_token, request)
    email_sent = False
    to_email = str(user_doc.get("email") or "").strip()
    if smtp_configured() and to_email:
        try:
            await send_password_reset_email(
                to_email=to_email,
                to_name=user_doc.get("name"),
                reset_url=reset_url,
            )
            email_sent = True
            logger.info("password-reset-email-sent user_id=%s to=%s", user_doc["user_id"], to_email)
        except Exception:
            logger.exception("password-reset-email-send-failed user_id=%s to=%s", user_doc["user_id"], to_email)
    elif not smtp_configured():
        logger.warning(
            "SMTP non configuré : reset link en logs uniquement user_id=%s url=%s",
            user_doc["user_id"],
            reset_url,
        )
    else:
        logger.warning("password-reset skipped (empty email) user_id=%s", user_doc["user_id"])

    # Uniquement en local/dev explicite — jamais si APP_ENV est oublié en prod.
    if _allow_dev_reset_link() and not email_sent:
        return {
            "message": generic_message,
            "reset_token": raw_token,
            "reset_url": reset_url,
            "expires_in_minutes": PASSWORD_RESET_TOKEN_TTL_MINUTES,
            "email_sent": False,
        }
    return {"message": generic_message}

@api_router.post("/auth/reset-password")
async def reset_password(payload: ResetPasswordRequest):
    token_hash = hash_reset_token(payload.token)
    token_doc = await db.password_reset_tokens.find_one({"token_hash": token_hash, "used_at": None}, {"_id": 0})
    if not token_doc:
        raise HTTPException(status_code=400, detail="Token invalide ou expiré")

    expires_at = token_doc.get("expires_at")
    if isinstance(expires_at, str):
        expires_at = datetime.fromisoformat(expires_at)
    if not expires_at:
        raise HTTPException(status_code=400, detail="Token invalide ou expiré")
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at < datetime.now(timezone.utc):
        await db.password_reset_tokens.update_one(
            {"token_hash": token_hash},
            {"$set": {"used_at": datetime.now(timezone.utc).isoformat()}},
        )
        raise HTTPException(status_code=400, detail="Token invalide ou expiré")

    user_doc = await db.users.find_one({"user_id": token_doc["user_id"]}, {"_id": 0})
    if not user_doc:
        raise HTTPException(status_code=400, detail="Token invalide ou expiré")

    new_hash = hash_password(payload.new_password)
    await db.users.update_one(
        {"user_id": user_doc["user_id"]},
        {"$set": {"password_hash": new_hash}},
    )
    await db.password_reset_tokens.update_one(
        {"token_hash": token_hash},
        {"$set": {"used_at": datetime.now(timezone.utc).isoformat()}},
    )
    await db.user_sessions.delete_many({"user_id": user_doc["user_id"]})
    await revoke_user_refresh_tokens(user_doc["user_id"])

    return {"message": "Mot de passe réinitialisé avec succès"}

@api_router.get("/auth/google/status")
async def google_oauth_status():
    return {"configured": google_oauth_is_configured()}


@api_router.get("/auth/google/login")
async def google_login():
    """Redirect user to Google OAuth consent screen"""
    ensure_google_oauth_configured()
    state = uuid.uuid4().hex
    params = {
        "client_id": GOOGLE_OAUTH_CLIENT_ID,
        "redirect_uri": GOOGLE_OAUTH_REDIRECT_URI,
        "response_type": "code",
        "scope": "openid email profile",
        "state": state,
        "access_type": "online",
        "prompt": "consent",
    }
    url = f"https://accounts.google.com/o/oauth2/v2/auth?{urlencode(params)}"
    response = RedirectResponse(url=url)
    response.set_cookie(
        key="oauth_state",
        value=state,
        httponly=True,
        secure=False,
        samesite="lax",
        path="/",
        max_age=10 * 60,
    )
    return response

async def exchange_google_code_for_user(code: str) -> dict:
    """Exchange authorization code for userinfo"""
    ensure_google_oauth_configured()
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.post(
            "https://oauth2.googleapis.com/token",
            data={
                "client_id": GOOGLE_OAUTH_CLIENT_ID,
                "client_secret": GOOGLE_OAUTH_CLIENT_SECRET,
                "code": code,
                "grant_type": "authorization_code",
                "redirect_uri": GOOGLE_OAUTH_REDIRECT_URI,
            },
        ) as resp:
            if resp.status != 200:
                raise HTTPException(status_code=401, detail="Token exchange failed")
            token_data = await resp.json()

        access_token = token_data.get("access_token")
        if not access_token:
            raise HTTPException(status_code=401, detail="Missing access token")

        async with session.get(
            "https://www.googleapis.com/oauth2/v3/userinfo",
            headers={"Authorization": f"Bearer {access_token}"},
        ) as resp:
            if resp.status != 200:
                raise HTTPException(status_code=401, detail="Failed to fetch user info")
            return await resp.json()

async def upsert_google_user(user_info: dict) -> dict:
    """Create or update a user from Google profile data"""
    email = canonical_email(user_info["email"])
    user_doc = await find_user_by_email(email)
    if not user_doc:
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        user_doc = {
            "user_id": user_id,
            "email": email,
            "password_hash": None,
            "name": user_info.get("name") or user_info.get("given_name") or "",
            "picture": str(user_info.get("picture") or "").strip() or _default_mask_avatar(user_id),
            "role": "user",
            "artist_name": None,
            "is_admin": email_is_listed_admin(email),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await db.users.insert_one(user_doc)
    else:
        user_id = user_doc["user_id"]
        update_fields = {
            "name": user_info.get("name") or user_info.get("given_name") or user_doc.get("name"),
        }
        existing_picture = str(user_doc.get("picture") or "").strip()
        if not existing_picture:
            update_fields["picture"] = str(user_info.get("picture") or "").strip() or _default_mask_avatar(user_id)
        # Never promote a Gmail-alias account just because Google returned the primary address.
        if email_is_listed_admin(user_doc.get("email") or ""):
            update_fields["is_admin"] = True
        elif is_unlisted_gmail_alias_of_admin(user_doc.get("email") or ""):
            update_fields["is_admin"] = False
        await db.users.update_one(
            {"user_id": user_id},
            {"$set": update_fields},
        )
        user_doc.update(update_fields)
    user_doc["user_id"] = user_id
    return user_doc

async def create_google_session(user_id: str, response: Optional[Response] = None) -> dict:
    """Émet JWT access + refresh (cookies optionnels). Conserve une session opaque pour compat."""
    tokens = await issue_auth_tokens(user_id, response)
    session_doc = {
        "user_id": user_id,
        "session_token": tokens["access_token"],
        "expires_at": datetime.now(timezone.utc) + ACCESS_TOKEN_TTL,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.user_sessions.delete_many({"user_id": user_id})
    await db.user_sessions.insert_one(session_doc)
    return tokens

@api_router.get("/auth/google/callback")
async def google_callback_redirect(
    request: Request,
    response: Response,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):
    """Handle Google redirect and then redirect back to frontend"""
    fail_url = frontend_oauth_callback_url(error=error or ("code manquant" if not code else None))
    if error:
        if fail_url:
            return RedirectResponse(url=fail_url)
        raise HTTPException(status_code=401, detail=error)
    if not code:
        if fail_url:
            return RedirectResponse(url=fail_url)
        raise HTTPException(status_code=400, detail="Missing code")

    validate_oauth_state(request, state)
    user_info = await exchange_google_code_for_user(code)
    user_doc = await upsert_google_user(user_info)

    tokens = await create_google_session(user_doc["user_id"], None)
    redirect_url = frontend_oauth_callback_url(tokens)
    if redirect_url:
        response = RedirectResponse(url=redirect_url)
        set_auth_cookies(response, tokens["access_token"], tokens["refresh_token"])
        response.delete_cookie(key="oauth_state", path="/")
        return response

    response.delete_cookie(key="oauth_state", path="/")
    return GoogleSessionResponse(
        user_id=user_doc["user_id"],
        email=user_doc["email"],
        name=user_doc["name"],
        picture=user_doc.get("picture"),
        session_token=tokens["access_token"],
    )

@api_router.post("/auth/google/callback", response_model=GoogleSessionResponse)
async def google_callback_api(
    request: Request,
    response: Response,
    payload: GoogleAuthCodeRequest,
):
    """Exchange authorization code for user data and create/update user"""
    validate_oauth_state(request, payload.state)
    user_info = await exchange_google_code_for_user(payload.code)
    user_doc = await upsert_google_user(user_info)
    tokens = await create_google_session(user_doc["user_id"], response)
    
    response.delete_cookie(key="oauth_state", path="/")
    return GoogleSessionResponse(
        user_id=user_doc["user_id"],
        email=user_doc["email"],
        name=user_doc["name"],
        picture=user_doc.get("picture"),
        session_token=tokens["access_token"],
    )

@api_router.get("/auth/me", response_model=UserResponse)
async def get_me(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    return user_response_from_doc(user)

# ==================== USER SETTINGS ROUTES ====================

@api_router.get("/users/me/settings", response_model=UserSettings)
async def get_user_settings(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    doc = await db.user_settings.find_one({"user_id": user["user_id"]}, {"_id": 0})
    if not doc:
        return UserSettings()
    # Filter to known fields
    allowed = UserSettings().model_dump().keys()
    data = {k: doc.get(k) for k in allowed if k in doc}
    return UserSettings(**data)

@api_router.put("/users/me/settings", response_model=UserSettings)
async def update_user_settings(payload: UserSettings, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    data = payload.model_dump()
    data["user_id"] = user["user_id"]
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    await db.user_settings.update_one({"user_id": user["user_id"]}, {"$set": data}, upsert=True)
    return payload


class UserMeUpdate(BaseModel):
    picture: Optional[str] = None
    name: Optional[str] = None


@api_router.put("/users/me", response_model=UserResponse)
async def update_me(payload: UserMeUpdate, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    updates = {}
    if payload.picture is not None:
        picture = str(payload.picture).strip()
        updates["picture"] = picture or None
    if payload.name is not None:
        name = str(payload.name).strip()
        if name:
            updates["name"] = name
    if updates:
        await db.users.update_one({"user_id": user["user_id"]}, {"$set": updates})
        user.update(updates)
    user = await _ensure_default_picture(user)
    return user_response_from_doc(user)

@api_router.post("/auth/logout")
async def logout(
    response: Response,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    refresh_raw = await _read_refresh_token_from_request(request) if request else None

    try:
        user = await get_current_user(authorization, request)
        await db.user_sessions.delete_many({"user_id": user["user_id"]})
        await revoke_user_refresh_tokens(user["user_id"])
    except HTTPException:
        # Access expiré : on révoque quand même le refresh fourni
        await revoke_refresh_token(refresh_raw)

    clear_auth_cookies(response)
    return {"message": "Logged out successfully"}

@api_router.put("/auth/role")
async def update_role(new_role: str, artist_name: Optional[str] = None, authorization: Optional[str] = Header(None), request: Request = None):
    """Toggle between user and artist role"""
    user = await get_current_user(authorization, request)
    
    # Prevent privilege escalation through this endpoint.
    allowed = {"user", "artist"}
    if new_role not in allowed:
        raise HTTPException(status_code=400, detail="Invalid role")
    
    # Si on passe en mode artist et qu'on a déjà un artist_name, le garder
    if new_role == "artist":
        # Si pas de nouveau nom fourni, garder l'ancien s'il existe
        if not artist_name and user.get("artist_name"):
            artist_name = user["artist_name"]
        elif not artist_name:
            raise HTTPException(status_code=400, detail="Artist name required for artist role")
    
    await db.users.update_one(
        {"user_id": user["user_id"]},
        {"$set": {
            "role": new_role,
            "artist_name": artist_name if new_role == "artist" else user.get("artist_name"),
            "is_admin": user_is_admin(user),
        }}
    )
    
    return {"role": new_role, "artist_name": artist_name, "is_admin": user_is_admin(user)}

# ==================== ADMIN ROUTES ====================

@api_router.get("/admin/overview")
async def admin_overview(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    require_admin(user)

    users_count = await db.users.count_documents({})
    artists_count = await db.users.count_documents({"role": "artist"})
    tracks_count = await db.tracks.count_documents({})
    albums_count = await db.albums.count_documents({})
    purchases_count = await db.purchases.count_documents({})
    payment_txn_count = await db.payment_transactions.count_documents({})

    paid_cursor = db.payment_transactions.find(
        {"payment_status": "paid"},
        {"_id": 0, "amount": 1, "platform_fee_cents": 1, "seller_amount_cents": 1, "created_at": 1},
    )
    paid = await paid_cursor.to_list(5000)

    now = datetime.now(timezone.utc)
    cutoff_day = (now - timedelta(days=29)).date().isoformat()

    gross_cents = 0
    fee_cents = 0
    seller_net_cents = 0
    gross_30d = 0
    fee_30d = 0
    seller_30d = 0
    daily: dict[str, dict[str, int]] = {}

    for t in paid:
        try:
            amount = int(round(float(t.get("amount") or 0)))
        except (TypeError, ValueError):
            amount = 0
        try:
            fee = int(t.get("platform_fee_cents") or 0)
        except (TypeError, ValueError):
            fee = 0
        try:
            seller = int(t.get("seller_amount_cents") or 0)
        except (TypeError, ValueError):
            seller = 0

        gross_cents += amount
        fee_cents += fee
        seller_net_cents += seller

        iso = t.get("created_at") or ""
        day = str(iso)[:10] if iso else ""
        if day:
            d = daily.setdefault(day, {"gross_cents": 0, "fee_cents": 0})
            d["gross_cents"] += amount
            d["fee_cents"] += fee
        if day and day >= cutoff_day:
            gross_30d += amount
            fee_30d += fee
            seller_30d += seller

    series = []
    for i in range(29, -1, -1):
        day = (now - timedelta(days=i)).date().isoformat()
        bucket = daily.get(day) or {"gross_cents": 0, "fee_cents": 0}
        series.append({"day": day, "gross_cents": bucket["gross_cents"], "fee_cents": bucket["fee_cents"]})

    return {
        "counts": {
            "users": int(users_count),
            "artists": int(artists_count),
            "tracks": int(tracks_count),
            "albums": int(albums_count),
            "purchases": int(purchases_count),
            "payment_transactions": int(payment_txn_count),
            "paid_transactions": int(len(paid)),
        },
        "money": {
            "gross_cents": int(gross_cents),
            "platform_fees_cents": int(fee_cents),
            "seller_net_cents": int(seller_net_cents),
            "gross_cents_30d": int(gross_30d),
            "platform_fees_cents_30d": int(fee_30d),
            "seller_net_cents_30d": int(seller_30d),
            "currency": os.environ.get("STRIPE_CURRENCY", "eur"),
            "platform_fee_percent": float(get_platform_fee_percent()),
        },
        "series_30d": series,
    }

@api_router.get("/admin/storage")
async def admin_storage(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    require_admin(user)

    # Container filesystem stats (useful to detect low disk scenarios)
    disk_total = None
    disk_used = None
    disk_free = None
    try:
        du = shutil.disk_usage("/")
        disk_total = int(du.total)
        disk_used = int(du.used)
        disk_free = int(du.free)
    except Exception:
        pass

    # MongoDB stats
    mongo_data_size = None
    mongo_storage_size = None
    mongo_index_size = None
    try:
        db_stats = await db.command("dbStats")
        mongo_data_size = int(db_stats.get("dataSize") or 0)
        mongo_storage_size = int(db_stats.get("storageSize") or 0)
        mongo_index_size = int(db_stats.get("indexSize") or 0)
    except Exception:
        pass

    # GridFS ("uploads" bucket) footprint
    gridfs_files_count = 0
    gridfs_total_bytes = 0
    try:
        gridfs_files_count = int(await db["uploads.files"].count_documents({}))
        agg = await db["uploads.files"].aggregate(
            [{"$group": {"_id": None, "total": {"$sum": "$length"}}}]
        ).to_list(1)
        if agg:
            gridfs_total_bytes = int(agg[0].get("total") or 0)
    except Exception:
        pass

    return {
        "disk_total_bytes": disk_total,
        "disk_used_bytes": disk_used,
        "disk_free_bytes": disk_free,
        "mongo_data_size_bytes": mongo_data_size,
        "mongo_storage_size_bytes": mongo_storage_size,
        "mongo_index_size_bytes": mongo_index_size,
        "gridfs_files_count": gridfs_files_count,
        "gridfs_total_bytes": gridfs_total_bytes,
    }


@api_router.get("/admin/payment-transactions")
async def admin_payment_transactions(
    limit: int = Query(200, ge=1, le=2000),
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    cursor = (
        db.payment_transactions.find(
            {},
            {
                "_id": 0,
                "transaction_id": 1,
                "session_id": 1,
                "user_id": 1,
                "artist_id": 1,
                "amount": 1,
                "currency": 1,
                "status": 1,
                "payment_status": 1,
                "metadata": 1,
                "stripe_connect_account_id": 1,
                "platform_fee_cents": 1,
                "seller_amount_cents": 1,
                "created_at": 1,
            },
        )
        .sort("created_at", -1)
        .limit(limit)
    )
    items = await cursor.to_list(limit)
    return {"items": items}


@api_router.get("/admin/logs")
async def admin_logs(
    limit: int = Query(200, ge=1, le=2000),
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    cursor = (
        db.transactions.find({}, {"_id": 0})
        .sort("created_at", -1)
        .limit(limit)
    )
    items = await cursor.to_list(limit)
    return {"items": items}

@api_router.post("/feedback")
async def submit_feedback(
    title: str = Form(...),
    description: str = Form(...),
    image: Optional[UploadFile] = File(None),
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    title_clean = str(title or "").strip()
    description_clean = str(description or "").strip()
    if not title_clean:
        raise HTTPException(status_code=400, detail="Le titre est requis")
    if not description_clean:
        raise HTTPException(status_code=400, detail="La description est requise")
    if len(title_clean) > 180:
        raise HTTPException(status_code=400, detail="Le titre est trop long (max 180)")
    if len(description_clean) > 6000:
        raise HTTPException(status_code=400, detail="La description est trop longue (max 6000)")

    image_url = None
    image_name = None
    if image is not None:
        content = await image.read()
        if not content:
            raise HTTPException(status_code=400, detail="Image vide")
        if len(content) > 8 * 1024 * 1024:
            raise HTTPException(status_code=400, detail="Image trop lourde (max 8MB)")
        content_type = str(image.content_type or "").lower().strip()
        if not content_type.startswith("image/"):
            raise HTTPException(status_code=400, detail="Le fichier doit être une image")
        file_id = await fs.upload_from_stream(
            filename=image.filename or f"{uuid.uuid4().hex}.feedback",
            source=content,
            metadata={
                "kind": "feedback_image",
                "content_type": content_type or "application/octet-stream",
                "uploaded_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        image_url = f"/api/files/covers/{str(file_id)}"
        image_name = image.filename

    reporter_user_id = None
    reporter_email = None
    reporter_name = None
    try:
        current_user = await get_current_user(authorization, request)
        reporter_user_id = current_user.get("user_id")
        reporter_email = current_user.get("email")
        reporter_name = current_user.get("name") or current_user.get("artist_name")
    except Exception:
        pass

    feedback_id = f"feedback_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()
    forwarded_for = request.headers.get("x-forwarded-for") if request else None
    client_ip = (forwarded_for.split(",")[0].strip() if forwarded_for else None) or (
        request.client.host if request and request.client else None
    )

    feedback_doc = {
        "feedback_id": feedback_id,
        "title": title_clean,
        "description": description_clean,
        "image_url": image_url,
        "image_name": image_name,
        "reporter_user_id": reporter_user_id,
        "reporter_email": reporter_email,
        "reporter_name": reporter_name,
        "client_ip": client_ip,
        "user_agent": request.headers.get("user-agent") if request else None,
        "created_at": now,
        "status": "new",
    }
    await db.feedback_submissions.insert_one(feedback_doc)
    return {"submitted": True, "feedback_id": feedback_id}

@api_router.get("/admin/feedback")
async def admin_feedback(
    limit: int = Query(300, ge=1, le=2000),
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    cursor = (
        db.feedback_submissions.find({}, {"_id": 0})
        .sort("created_at", -1)
        .limit(limit)
    )
    items = await cursor.to_list(limit)
    return {"items": items}


@api_router.get("/admin/tracks")
async def admin_tracks(
    q: Optional[str] = None,
    status: Optional[str] = None,
    artist_id: Optional[str] = None,
    limit: int = Query(300, ge=1, le=2000),
    skip: int = Query(0, ge=0, le=200000),
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    query: dict = {}
    if status:
        query["status"] = str(status)
    if artist_id:
        query["artist_id"] = str(artist_id)
    if q:
        qs = str(q).strip()
        if qs:
            query["$or"] = [
                {"track_id": {"$regex": qs, "$options": "i"}},
                {"title": {"$regex": qs, "$options": "i"}},
                {"artist_name": {"$regex": qs, "$options": "i"}},
            ]
    cursor = (
        db.tracks.find(query, {"_id": 0})
        .sort("created_at", -1)
        .skip(skip)
        .limit(limit)
    )
    items = await cursor.to_list(limit)
    return {"items": items}


@api_router.get("/admin/albums")
async def admin_albums(
    q: Optional[str] = None,
    status: Optional[str] = None,
    artist_id: Optional[str] = None,
    limit: int = Query(300, ge=1, le=2000),
    skip: int = Query(0, ge=0, le=200000),
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    query: dict = {}
    if status:
        query["status"] = str(status)
    if artist_id:
        query["artist_id"] = str(artist_id)
    if q:
        qs = str(q).strip()
        if qs:
            query["$or"] = [
                {"album_id": {"$regex": qs, "$options": "i"}},
                {"title": {"$regex": qs, "$options": "i"}},
                {"artist_name": {"$regex": qs, "$options": "i"}},
            ]
    cursor = (
        db.albums.find(query, {"_id": 0})
        .sort("created_at", -1)
        .skip(skip)
        .limit(limit)
    )
    items = await cursor.to_list(limit)
    return {"items": items}


@api_router.get("/admin/tracks.csv")
async def admin_tracks_csv(
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    cursor = db.tracks.find({}, {"_id": 0}).sort("created_at", -1).limit(5000)
    items = await cursor.to_list(5000)

    import csv
    import io

    output = io.StringIO()
    fieldnames = [
        "track_id",
        "title",
        "artist_id",
        "artist_name",
        "status",
        "price",
        "created_at",
    ]
    w = csv.DictWriter(output, fieldnames=fieldnames)
    w.writeheader()
    for it in items:
        w.writerow({k: it.get(k, "") for k in fieldnames})
    return Response(content=output.getvalue(), media_type="text/csv")


@api_router.get("/admin/albums.csv")
async def admin_albums_csv(
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    cursor = db.albums.find({}, {"_id": 0}).sort("created_at", -1).limit(5000)
    items = await cursor.to_list(5000)

    import csv
    import io

    output = io.StringIO()
    fieldnames = [
        "album_id",
        "title",
        "artist_id",
        "artist_name",
        "status",
        "price",
        "created_at",
    ]
    w = csv.DictWriter(output, fieldnames=fieldnames)
    w.writeheader()
    for it in items:
        w.writerow({k: it.get(k, "") for k in fieldnames})
    return Response(content=output.getvalue(), media_type="text/csv")


@api_router.patch("/admin/tracks/{track_id}")
async def admin_update_track(
    track_id: str,
    payload: AdminTrackUpdate,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    data = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not data:
        raise HTTPException(status_code=400, detail="No fields to update")
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    await db.tracks.update_one({"track_id": track_id}, {"$set": data})
    doc = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Track not found")
    return doc


@api_router.delete("/admin/tracks/{track_id}")
async def admin_delete_track(
    track_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    res = await db.tracks.delete_one({"track_id": track_id})
    if not res.deleted_count:
        raise HTTPException(status_code=404, detail="Track not found")
    return {"deleted": True}


@api_router.patch("/admin/albums/{album_id}")
async def admin_update_album(
    album_id: str,
    payload: AdminAlbumUpdate,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    data = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not data:
        raise HTTPException(status_code=400, detail="No fields to update")
    data["updated_at"] = datetime.now(timezone.utc).isoformat()
    await db.albums.update_one({"album_id": album_id}, {"$set": data})
    doc = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if not doc:
        raise HTTPException(status_code=404, detail="Album not found")
    return doc


@api_router.delete("/admin/albums/{album_id}")
async def admin_delete_album(
    album_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    res = await db.albums.delete_one({"album_id": album_id})
    if not res.deleted_count:
        raise HTTPException(status_code=404, detail="Album not found")
    return {"deleted": True}


@api_router.get("/admin/contests")
async def admin_list_contests(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    require_admin(user)
    items = await db.contests.find({}, {"_id": 0}).sort("starts_at", -1).to_list(100)
    now = datetime.now(timezone.utc)
    for c in items:
        c["phase"] = contest_phase(c, now)
    return {"items": items}


@api_router.post("/admin/contests")
async def admin_create_contest(
    payload: ContestCreate,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    start = _parse_iso_dt(payload.starts_at)
    end = _parse_iso_dt(payload.ends_at)
    if not start or not end or end <= start:
        raise HTTPException(status_code=400, detail="Période invalide (starts_at < ends_at)")
    now = datetime.now(timezone.utc).isoformat()
    doc = {
        "contest_id": f"contest_{uuid.uuid4().hex[:12]}",
        "title": (payload.title or "Course aux revenus").strip(),
        "subtitle": (payload.subtitle or "").strip() or None,
        "starts_at": start.isoformat(),
        "ends_at": end.isoformat(),
        "is_published": bool(payload.is_published),
        "prize_label": (payload.prize_label or "").strip() or None,
        "created_at": now,
        "updated_at": now,
        "created_by": user.get("user_id"),
    }
    if doc["is_published"]:
        # Un seul concours publié à la fois
        await db.contests.update_many(
            {"is_published": True},
            {"$set": {"is_published": False, "updated_at": now}},
        )
    await db.contests.insert_one(doc)
    out = {k: v for k, v in doc.items() if k != "_id"}
    out["phase"] = contest_phase(out)
    return out


@api_router.patch("/admin/contests/{contest_id}")
async def admin_update_contest(
    contest_id: str,
    payload: ContestUpdate,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    existing = await db.contests.find_one({"contest_id": contest_id}, {"_id": 0})
    if not existing:
        raise HTTPException(status_code=404, detail="Contest not found")
    data = {k: v for k, v in payload.model_dump().items() if v is not None}
    if "title" in data:
        data["title"] = str(data["title"]).strip() or existing.get("title")
    if "subtitle" in data:
        data["subtitle"] = str(data["subtitle"]).strip() or None
    if "prize_label" in data:
        data["prize_label"] = str(data["prize_label"]).strip() or None
    if "starts_at" in data:
        start = _parse_iso_dt(data["starts_at"])
        if not start:
            raise HTTPException(status_code=400, detail="starts_at invalide")
        data["starts_at"] = start.isoformat()
    if "ends_at" in data:
        end = _parse_iso_dt(data["ends_at"])
        if not end:
            raise HTTPException(status_code=400, detail="ends_at invalide")
        data["ends_at"] = end.isoformat()
    start = _parse_iso_dt(data.get("starts_at") or existing.get("starts_at"))
    end = _parse_iso_dt(data.get("ends_at") or existing.get("ends_at"))
    if not start or not end or end <= start:
        raise HTTPException(status_code=400, detail="Période invalide")
    now = datetime.now(timezone.utc).isoformat()
    data["updated_at"] = now
    if data.get("is_published") is True:
        await db.contests.update_many(
            {"contest_id": {"$ne": contest_id}, "is_published": True},
            {"$set": {"is_published": False, "updated_at": now}},
        )
    await db.contests.update_one({"contest_id": contest_id}, {"$set": data})
    doc = await db.contests.find_one({"contest_id": contest_id}, {"_id": 0})
    doc["phase"] = contest_phase(doc)
    return doc


@api_router.delete("/admin/contests/{contest_id}")
async def admin_delete_contest(
    contest_id: str,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await get_current_user(authorization, request)
    require_admin(user)
    res = await db.contests.delete_one({"contest_id": contest_id})
    if not res.deleted_count:
        raise HTTPException(status_code=404, detail="Contest not found")
    return {"deleted": True}


@api_router.get("/contests/current")
async def get_current_contest():
    """Concours publié en cours (ou à venir) + top classement + courbes."""
    now = datetime.now(timezone.utc)
    published = await db.contests.find({"is_published": True}, {"_id": 0}).sort("starts_at", -1).to_list(20)
    contest = None
    for c in published:
        phase = contest_phase(c, now)
        c["phase"] = phase
        if phase in {"live", "upcoming"}:
            contest = c
            break
    if not contest and published:
        # Dernier terminé encore affiché brièvement
        contest = published[0]
        contest["phase"] = contest_phase(contest, now)
    if not contest:
        return {"contest": None, "leaderboard": [], "series": [], "days": []}

    standings = await compute_contest_standings(
        contest["starts_at"],
        contest["ends_at"],
        limit=10,
        series_top=3,
    )
    return {
        "contest": contest,
        "leaderboard": standings["leaderboard"],
        "series": standings["series"],
        "days": standings["days"],
    }


@api_router.get("/contests/{contest_id}")
async def get_contest_detail(contest_id: str, limit: int = Query(50, ge=1, le=200)):
    contest = await db.contests.find_one({"contest_id": contest_id}, {"_id": 0})
    if not contest or not contest.get("is_published"):
        raise HTTPException(status_code=404, detail="Contest not found")
    contest["phase"] = contest_phase(contest)
    standings = await compute_contest_standings(
        contest["starts_at"],
        contest["ends_at"],
        limit=limit,
        series_top=5,
    )
    return {
        "contest": contest,
        "leaderboard": standings["leaderboard"],
        "series": standings["series"],
        "days": standings["days"],
    }


# ==================== ARTIST PROFILE ROUTES ====================

@api_router.post("/artist/profile", response_model=ArtistProfileResponse)
async def create_artist_profile(profile_data: ArtistProfileCreate, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can create profiles")
    
    # Check if profile already exists
    existing = await db.artist_profiles.find_one({"user_id": user["user_id"]}, {"_id": 0})
    if existing:
        raise HTTPException(status_code=400, detail="Profile already exists")
    
    profile_id = f"profile_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()
    
    profile_doc = {
        "profile_id": profile_id,
        "user_id": user["user_id"],
        "name": profile_data.name,
        "bio": profile_data.bio,
        "avatar_url": profile_data.avatar_url,
        "links": profile_data.links or [],
        "created_at": now,
        "updated_at": now
    }
    
    await db.artist_profiles.insert_one(profile_doc)
    return ArtistProfileResponse(**profile_doc)

@api_router.get("/artist/profile", response_model=ArtistProfileResponse)
async def get_my_artist_profile(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    profile = await db.artist_profiles.find_one({"user_id": user["user_id"]}, {"_id": 0})
    if not profile:
        raise HTTPException(status_code=404, detail="Profile not found")
    
    return ArtistProfileResponse(**profile)

@api_router.put("/artist/profile", response_model=ArtistProfileResponse)
async def update_artist_profile(profile_data: ArtistProfileCreate, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can update profiles")
    
    now = datetime.now(timezone.utc).isoformat()
    update_data = profile_data.model_dump()
    update_data["updated_at"] = now

    existing = await db.artist_profiles.find_one({"user_id": user["user_id"]}, {"_id": 0})
    if not existing:
        profile_doc = {
            "profile_id": f"profile_{uuid.uuid4().hex[:12]}",
            "user_id": user["user_id"],
            "created_at": now,
            **update_data,
        }
        await db.artist_profiles.insert_one(profile_doc)
        return ArtistProfileResponse(**profile_doc)

    await db.artist_profiles.update_one(
        {"user_id": user["user_id"]},
        {"$set": update_data},
    )
    profile = await db.artist_profiles.find_one({"user_id": user["user_id"]}, {"_id": 0})
    if not profile:
        raise HTTPException(status_code=500, detail="Profil artiste introuvable")
    return ArtistProfileResponse(**profile)

# ==================== UPLOAD ROUTES ====================

_AUDIO_CHUNK_SIZE = 256 * 1024
_RANGE_RE = re.compile(r"bytes\s*=\s*(\d*)\s*-\s*(\d*)", re.IGNORECASE)
_WAV_CONTENT_TYPES = {
    "audio/wav",
    "audio/wave",
    "audio/x-wav",
    "audio/vnd.wave",
    "audio/x-pn-wav",
}


def _sniff_audio_content_type(header: bytes) -> Optional[str]:
    if len(header) >= 12 and header[:4] == b"RIFF" and header[8:12] == b"WAVE":
        return "audio/wav"
    if header[:3] == b"ID3":
        return "audio/mpeg"
    if len(header) >= 2 and header[0] == 0xFF and (header[1] & 0xE0) == 0xE0:
        return "audio/mpeg"
    if header[:4] == b"fLaC":
        return "audio/flac"
    if header[:4] == b"OggS":
        return "audio/ogg"
    if len(header) >= 12 and header[4:8] == b"ftyp":
        return "audio/mp4"
    return None


def _normalize_audio_content_type(
    content_type: Optional[str],
    filename: Optional[str] = None,
    header: Optional[bytes] = None,
) -> str:
    sniffed = _sniff_audio_content_type(header) if header else None
    if sniffed:
        return sniffed

    ct = (content_type or "").split(";")[0].strip().lower()
    fn = (filename or "").lower()

    if ct in _WAV_CONTENT_TYPES or fn.endswith((".wav", ".wave")):
        return "audio/wav"
    if ct in {"audio/mpeg", "audio/mp3", "audio/x-mpeg", "audio/x-mp3"} or fn.endswith((".mp3", ".mpga")):
        return "audio/mpeg"
    if ct in {"audio/flac", "audio/x-flac"} or fn.endswith(".flac"):
        return "audio/flac"
    if ct in {"audio/ogg", "audio/vorbis", "application/ogg"} or fn.endswith((".ogg", ".oga")):
        return "audio/ogg"
    if ct in {"audio/mp4", "audio/aac", "audio/x-m4a", "audio/m4a"} or fn.endswith((".m4a", ".aac", ".mp4")):
        return "audio/mp4"
    if ct.startswith("audio/"):
        return ct
    return ct or "application/octet-stream"


def _safe_audio_filename(filename: Optional[str], content_type: str) -> str:
    name = (filename or "track").replace("\\", "/").split("/")[-1].replace('"', "").strip()
    if name.lower().endswith(".wave"):
        name = name[:-5] + ".wav"
    if "." not in name:
        ext = {
            "audio/wav": ".wav",
            "audio/mpeg": ".mp3",
            "audio/flac": ".flac",
            "audio/ogg": ".ogg",
            "audio/mp4": ".m4a",
        }.get(content_type, "")
        name = f"{name or 'track'}{ext}"
    return name or "track"


def _parse_byte_range(range_header: str, file_size: int):
    """Parse a single RFC 7233 range. Returns (start, end) inclusive, 'unsatisfiable', or None."""
    if not range_header or file_size <= 0:
        return None
    first = range_header.split(",", 1)[0]
    match = _RANGE_RE.search(first) or _RANGE_RE.search(range_header)
    if not match:
        return None
    start_s, end_s = match.group(1), match.group(2)
    if start_s == "" and end_s == "":
        return None
    try:
        if start_s == "":
            suffix = int(end_s)
            if suffix <= 0:
                return "unsatisfiable"
            start = max(0, file_size - suffix)
            return start, file_size - 1
        start = int(start_s)
        end = int(end_s) if end_s else file_size - 1
    except ValueError:
        return None
    if start >= file_size or end < start:
        return "unsatisfiable"
    return start, min(end, file_size - 1)


async def _seek_grid_out(grid_out, position: int) -> bool:
    seek = getattr(grid_out, "seek", None)
    if seek is None:
        return False
    try:
        result = seek(position)
        if hasattr(result, "__await__"):
            await result
        return True
    except Exception:
        return False


async def _seek_grid_out_or_skip(grid_out, position: int) -> None:
    if position <= 0:
        return
    if await _seek_grid_out(grid_out, position):
        return
    remaining = position
    while remaining > 0:
        chunk = await grid_out.read(min(_AUDIO_CHUNK_SIZE, remaining))
        if not chunk:
            break
        remaining -= len(chunk)


def _latin1_filename(name: str) -> str:
    """HTTP header values are latin-1. Anything else (typographic quotes, emoji) crashes the response."""
    out = []
    for ch in name:
        o = ord(ch)
        if ch in '"\\' or o < 32 or o == 127:
            out.append("_")
        elif o <= 255:
            out.append(ch)
        else:
            out.append("_")
    cleaned = "".join(out).strip()
    return cleaned or "track"


def _content_disposition_inline(filename: Optional[str], content_type: str) -> str:
    name = _safe_audio_filename(filename, content_type).replace("\r", " ").replace("\n", " ").replace('"', "")
    # filename= stays latin-1 for old clients; filename* keeps the real UTF-8 name (RFC 5987).
    fallback = _latin1_filename(name)
    encoded = quote(name, safe="")
    disposition = f"inline; filename=\"{fallback}\"; filename*=UTF-8''{encoded}"
    try:
        disposition.encode("latin-1")
        return disposition
    except Exception:
        logger.exception("Unsafe Content-Disposition for audio filename")
        return 'inline; filename="track"'


def _audio_response_headers(
    content_type: str,
    filename: Optional[str],
    extra: Optional[dict] = None,
    *,
    private: bool = False,
) -> dict:
    safe_type = content_type if isinstance(content_type, str) and content_type else "application/octet-stream"
    try:
        safe_type.encode("latin-1")
    except Exception:
        safe_type = "application/octet-stream"
    headers = {
        "Accept-Ranges": "bytes",
        "Content-Type": safe_type,
        # no-transform: proxies (Caddy gzip) must not recode the stream — that breaks Range + WAV.
        "Cache-Control": "private, max-age=0, no-transform" if private else "public, max-age=86400, no-transform",
        "X-Content-Type-Options": "nosniff",
        "Content-Disposition": _content_disposition_inline(filename, safe_type),
    }
    if extra:
        for key, value in extra.items():
            text = "" if value is None else str(value)
            try:
                str(key).encode("latin-1")
                text.encode("latin-1")
            except Exception:
                logger.exception("Dropping non-latin1 audio header %s", key)
                continue
            headers[str(key)] = text
    return headers


def _audio_file_id_from_url(url: Optional[str]) -> Optional[str]:
    if not url:
        return None
    match = re.search(r"/api/files/audio/([a-fA-F0-9]{24})", str(url))
    return match.group(1) if match else None


def _clip_wav_bytes(content: bytes, start_sec: int, duration_sec: int) -> Optional[bytes]:
    try:
        with wave.open(io.BytesIO(content), "rb") as reader:
            rate = reader.getframerate()
            channels = reader.getnchannels()
            sample_width = reader.getsampwidth()
            start_frame = max(0, int(start_sec)) * rate
            count = max(1, int(duration_sec)) * rate
            reader.setpos(min(start_frame, reader.getnframes()))
            frames = reader.readframes(count)
        out = io.BytesIO()
        with wave.open(out, "wb") as writer:
            writer.setnchannels(channels)
            writer.setsampwidth(sample_width)
            writer.setframerate(rate)
            writer.writeframes(frames)
        return out.getvalue()
    except Exception:
        return None


def _clip_with_ffmpeg(content: bytes, start_sec: int, duration_sec: int) -> Optional[bytes]:
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        return None
    try:
        with tempfile.TemporaryDirectory() as tmp:
            src = Path(tmp) / "src.bin"
            dst = Path(tmp) / "preview.mp3"
            src.write_bytes(content)
            cmd = [
                ffmpeg,
                "-y",
                "-ss",
                str(max(0, int(start_sec))),
                "-t",
                str(max(1, int(duration_sec))),
                "-i",
                str(src),
                "-vn",
                "-c:a",
                "libmp3lame",
                "-b:a",
                "128k",
                str(dst),
            ]
            subprocess.run(cmd, check=True, capture_output=True, timeout=60)
            if dst.exists() and dst.stat().st_size > 0:
                return dst.read_bytes()
    except Exception:
        logger.exception("ffmpeg preview generation failed")
    return None


async def _store_preview_bytes(content: bytes, *, user_id: str, filename: str, content_type: str) -> Optional[str]:
    if not content:
        return None
    file_id = await fs.upload_from_stream(
        filename=filename,
        source=content,
        metadata={
            "kind": "audio_preview",
            "content_type": content_type,
            "uploader_user_id": user_id,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    return f"/api/files/audio/{str(file_id)}"


async def _preview_url_from_bytes(
    content: bytes,
    *,
    user_id: str,
    filename: str,
    content_type: str,
    start_sec: int = 0,
    duration_sec: int = 15,
) -> Optional[str]:
    clip = await asyncio.to_thread(_clip_with_ffmpeg, content, start_sec, duration_sec)
    stored_type = "audio/mpeg"
    stored_name = "preview.mp3"
    if not clip:
        clip = await asyncio.to_thread(_clip_wav_bytes, content, start_sec, duration_sec)
        stored_type = "audio/wav"
        stored_name = "preview.wav"
    if not clip:
        return None
    return await _store_preview_bytes(
        clip,
        user_id=user_id,
        filename=stored_name,
        content_type=stored_type,
    )


async def _read_gridfs_bytes(file_id: str) -> Optional[bytes]:
    try:
        grid_out = await fs.open_download_stream(ObjectId(file_id))
        return await grid_out.read()
    except Exception:
        return None


async def _preview_url_from_file_url(
    file_url: str,
    *,
    user_id: str,
    start_sec: int = 0,
    duration_sec: int = 15,
) -> Optional[str]:
    file_id = _audio_file_id_from_url(file_url)
    if not file_id:
        return None
    content = await _read_gridfs_bytes(file_id)
    if not content:
        return None
    return await _preview_url_from_bytes(
        content,
        user_id=user_id,
        filename="source.audio",
        content_type="application/octet-stream",
        start_sec=start_sec,
        duration_sec=duration_sec,
    )


async def _prepare_track_audio_fields(payload: dict, *, existing: dict, user_id: str) -> dict:
    out = dict(payload or {})
    if "duration_sec" in out:
        if "duration" not in out:
            out["duration"] = out.get("duration_sec")
        out.pop("duration_sec", None)
    if "preview_duration_sec" in out:
        if "preview_duration" not in out:
            try:
                out["preview_duration"] = max(1, int(out.get("preview_duration_sec") or 15))
            except (TypeError, ValueError):
                out["preview_duration"] = 15
        out.pop("preview_duration_sec", None)

    file_url = out.get("file_url") if "file_url" in out else existing.get("file_url")
    preview_url = out.get("preview_url") if "preview_url" in out else existing.get("preview_url")
    start = int(out.get("preview_start_time", existing.get("preview_start_time") or 0) or 0)
    duration = int(out.get("preview_duration", existing.get("preview_duration") or 15) or 15)
    old_start = int(existing.get("preview_start_time") or 0)
    old_duration = int(existing.get("preview_duration") or 15)
    file_changed = "file_url" in out and out.get("file_url") != existing.get("file_url")
    window_changed = start != old_start or duration != old_duration
    needs_preview = bool(file_url) and (
        file_changed or window_changed or not preview_url or preview_url == file_url
    )
    if needs_preview:
        generated = await _preview_url_from_file_url(
            file_url,
            user_id=user_id,
            start_sec=start,
            duration_sec=max(1, duration),
        )
        out["preview_url"] = generated or ""
    return out


def _track_duration_sec(track: Optional[dict]) -> Optional[int]:
    raw = (track or {}).get("duration")
    if raw is None or raw == "":
        raw = (track or {}).get("duration_sec")
    try:
        n = int(raw)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _merge_album_track_ids(album: Optional[dict], tracks: Optional[list]) -> List[str]:
    album = album or {}
    linked = [tid for tid in (album.get("track_ids") or []) if tid]
    aid = album.get("album_id")
    linked_set = set(linked)
    extra = []
    for track in tracks or []:
        tid = track.get("track_id")
        if not tid:
            continue
        if track.get("album_id") == aid or tid in linked_set:
            extra.append(tid)
    return list(dict.fromkeys([*linked, *extra]))


async def _hydrate_album_track_ids(albums: List[dict]) -> List[dict]:
    if not albums:
        return []
    album_ids = [a.get("album_id") for a in albums if a.get("album_id")]
    stored_ids = [tid for a in albums for tid in (a.get("track_ids") or []) if tid]
    clauses = []
    if album_ids:
        clauses.append({"album_id": {"$in": album_ids}})
    if stored_ids:
        clauses.append({"track_id": {"$in": stored_ids}})
    tracks: List[dict] = []
    if clauses:
        tracks = await db.tracks.find(
            {"$or": clauses},
            {"_id": 0, "track_id": 1, "album_id": 1},
        ).to_list(10000)
    hydrated = []
    for album in albums:
        doc = dict(album)
        doc["track_ids"] = _merge_album_track_ids(album, tracks)
        hydrated.append(doc)
    return hydrated


def _has_audio_file(track: Optional[dict]) -> bool:
    return bool(str((track or {}).get("file_url") or "").strip())


def _ensure_published_has_audio(track: Optional[dict]) -> None:
    if str((track or {}).get("status") or "") == "published" and not _has_audio_file(track):
        raise HTTPException(status_code=400, detail="Un titre publié doit avoir un fichier audio")


async def _album_member_tracks(album: Optional[dict]) -> List[dict]:
    album = album or {}
    aid = album.get("album_id")
    ids = [tid for tid in (album.get("track_ids") or []) if tid]
    clauses = []
    if aid:
        clauses.append({"album_id": aid})
    if ids:
        clauses.append({"track_id": {"$in": ids}})
    if not clauses:
        return []
    return await db.tracks.find({"$or": clauses}, {"_id": 0}).to_list(1000)


async def _sync_album_tracks_status(album: Optional[dict], *, status: str) -> List[dict]:
    """Publie / dépublie les pistes de l'album. Publication : seulement celles avec audio.
    Dépublier : uniquement les titres créés dans cet album (`album_id`)."""
    album = album or {}
    aid = album.get("album_id")
    tracks = await _album_member_tracks(album)
    now = datetime.now(timezone.utc).isoformat()
    changed = []
    for track in tracks:
        previous = track.get("status")
        if previous == status:
            continue
        if status == "published" and not _has_audio_file(track):
            continue
        if status == "draft" and track.get("album_id") != aid:
            continue
        await db.tracks.update_one(
            {"track_id": track["track_id"]},
            {"$set": {"status": status, "updated_at": now}},
        )
        updated = {**track, "status": status}
        await _safe_notify(maybe_notify_published("track", previous, updated))
        changed.append(updated)
    return changed


async def _ensure_album_can_publish(album: Optional[dict]) -> None:
    members = await _album_member_tracks(album)
    if not any(_has_audio_file(t) for t in members):
        raise HTTPException(
            status_code=400,
            detail="Un album publié doit contenir au moins un titre avec audio",
        )


def _playable_preview_url(track: Optional[dict]) -> str:
    """Play the uploaded file when possible (player keeps a 15s window).

    Prefer `file_url` over a generated clip: a stale/wrong preview clip was
    making catalog playback launch a different song than the clicked track.
    """
    data = track or {}
    return str(data.get("file_url") or data.get("preview_url") or "").strip()


def _track_response(track: dict, *, include_file: bool = False) -> TrackResponse:
    data = dict(track or {})
    file_url = str(data.get("file_url") or "").strip()
    preview_url = _playable_preview_url(data)
    data["preview_url"] = preview_url
    data["file_url"] = file_url if include_file else None
    return TrackResponse(**data)


def _public_track_doc(track: dict) -> dict:
    data = dict(track or {})
    data["preview_url"] = _playable_preview_url(data)
    data.pop("file_url", None)
    return data


async def _optional_user(authorization: Optional[str], request: Optional[Request]) -> Optional[dict]:
    try:
        return await get_current_user(authorization, request)
    except Exception:
        return None


async def _user_from_audio_request(request: Request, authorization: Optional[str]) -> Optional[dict]:
    token = None
    if request:
        token = request.query_params.get("token") or request.query_params.get("access_token")
    if token:
        fake_auth = f"Bearer {token}"
        try:
            return await get_current_user(fake_auth, None)
        except Exception:
            pass
    return await _optional_user(authorization, request)


def _parse_track_ids(raw: Optional[str], *, limit: int = 100) -> List[str]:
    seen: List[str] = []
    for part in str(raw or "").split(","):
        tid = part.strip()
        if not tid or tid in seen:
            continue
        seen.append(tid)
        if len(seen) >= limit:
            break
    return seen


async def _batch_owned_track_ids(user: Optional[dict], tracks: List[dict]) -> set:
    if not user or not tracks:
        return set()
    if user_is_admin(user):
        return {t["track_id"] for t in tracks if t.get("track_id")}
    owned = {
        t["track_id"]
        for t in tracks
        if t.get("track_id") and t.get("artist_id") == user.get("user_id")
    }
    tids = [t["track_id"] for t in tracks if t.get("track_id")]
    album_ids = {t.get("album_id") for t in tracks if t.get("album_id")}
    extra = []
    if tids:
        extra = await db.albums.find(
            {"track_ids": {"$in": tids}},
            {"_id": 0, "album_id": 1, "track_ids": 1},
        ).to_list(500)
    album_track_map = {}
    for album in extra:
        aid = album.get("album_id")
        if aid:
            album_ids.add(aid)
        for tid in album.get("track_ids") or []:
            album_track_map.setdefault(tid, set()).add(aid)
    conds = []
    if tids:
        conds.append({"item_type": "track", "item_id": {"$in": tids}})
    clean_albums = [aid for aid in album_ids if aid]
    if clean_albums:
        conds.append({"item_type": "album", "item_id": {"$in": clean_albums}})
    if not conds:
        return owned
    purchases = await db.purchases.find(
        {"user_id": user["user_id"], "$or": conds},
        {"_id": 0, "item_type": 1, "item_id": 1},
    ).to_list(4000)
    bought_tracks = {p["item_id"] for p in purchases if p.get("item_type") == "track"}
    bought_albums = {p["item_id"] for p in purchases if p.get("item_type") == "album"}
    for track in tracks:
        tid = track.get("track_id")
        if not tid or tid in owned:
            continue
        if tid in bought_tracks:
            owned.add(tid)
            continue
        aids = set(album_track_map.get(tid) or [])
        if track.get("album_id"):
            aids.add(track["album_id"])
        if aids & bought_albums:
            owned.add(tid)
    return owned


def _is_catalog_published(doc: Optional[dict]) -> bool:
    return str((doc or {}).get("status") or "published").lower() in {"published", "public"}


async def _catalog_visible_track_ids(tracks: List[dict]) -> set:
    """Published tracks, plus draft members of a published album (legacy data)."""
    visible = {t["track_id"] for t in tracks if t.get("track_id") and _is_catalog_published(t)}
    pending = [t for t in tracks if t.get("track_id") and t["track_id"] not in visible]
    if not pending:
        return visible
    tids = [t["track_id"] for t in pending]
    aids = [t.get("album_id") for t in pending if t.get("album_id")]
    clauses = []
    if tids:
        clauses.append({"track_ids": {"$in": tids}})
    if aids:
        clauses.append({"album_id": {"$in": aids}})
    albums = await db.albums.find(
        {"$or": clauses},
        {"_id": 0, "album_id": 1, "track_ids": 1, "status": 1},
    ).to_list(1000)
    pub_album_ids = set()
    pub_member_ids = set()
    for album in albums:
        if not _is_catalog_published(album):
            continue
        if album.get("album_id"):
            pub_album_ids.add(album["album_id"])
        for tid in album.get("track_ids") or []:
            if tid:
                pub_member_ids.add(tid)
    for track in pending:
        tid = track["track_id"]
        if tid in pub_member_ids or track.get("album_id") in pub_album_ids:
            visible.add(tid)
    return visible


def _catalog_status_query(status: Optional[str], user: Optional[dict]) -> Optional[dict]:
    """Mongo fragment for public lists. None = no access (empty result)."""
    wanted = (status or "").strip().lower()
    if not wanted or wanted in {"published", "public"}:
        return {"status": {"$nin": ["draft"]}}
    if wanted == "draft":
        if not user:
            return None
        if user_is_admin(user):
            return {"status": "draft"}
        return {"status": "draft", "artist_id": user.get("user_id")}
    if user and user_is_admin(user):
        return {"status": status}
    return None


async def _published_album_id_set() -> set:
    albums = await db.albums.find(
        {"status": {"$nin": ["draft"]}},
        {"_id": 0, "album_id": 1},
    ).to_list(5000)
    return {a["album_id"] for a in albums if a.get("album_id")}


async def _catalog_track_list_query(extra: Optional[dict] = None) -> dict:
    """Published tracks, plus draft members of a published album."""
    pub_albums = await _published_album_id_set()
    status_ok = {"status": {"$nin": ["draft"]}}
    visible = {"$or": [status_ok, {"album_id": {"$in": list(pub_albums)}}]} if pub_albums else status_ok
    if extra:
        return {"$and": [visible, extra]}
    return visible


async def _user_owns_album(user: Optional[dict], album: Optional[dict]) -> bool:
    if not user or not album:
        return False
    if user_is_admin(user):
        return True
    if album.get("artist_id") == user.get("user_id"):
        return True
    purchased = await db.purchases.find_one(
        {
            "user_id": user["user_id"],
            "item_type": "album",
            "item_id": album.get("album_id"),
        },
        {"_id": 1},
    )
    return bool(purchased)


async def _user_owns_track(user: Optional[dict], track: Optional[dict]) -> bool:
    if not user or not track:
        return False
    if user_is_admin(user):
        return True
    if track.get("artist_id") == user.get("user_id"):
        return True
    conditions = [{"item_type": "track", "item_id": track.get("track_id")}]
    album_ids = []
    if track.get("album_id"):
        album_ids.append(track["album_id"])
    extra = await db.albums.find({"track_ids": track.get("track_id")}, {"_id": 0, "album_id": 1}).to_list(20)
    album_ids.extend(a.get("album_id") for a in extra if a.get("album_id"))
    for album_id in {aid for aid in album_ids if aid}:
        conditions.append({"item_type": "album", "item_id": album_id})
    purchased = await db.purchases.find_one({"user_id": user["user_id"], "$or": conditions}, {"_id": 1})
    return bool(purchased)


async def _audio_access_level(file_id: str, request: Request, authorization: Optional[str]) -> Optional[str]:
    try:
        oid = ObjectId(file_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file id")
    try:
        grid_out = await fs.open_download_stream(oid)
    except Exception:
        raise HTTPException(status_code=404, detail="File not found")
    meta = getattr(grid_out, "metadata", None) or {}
    kind = str(meta.get("kind") or "")
    file_url = f"/api/files/audio/{file_id}"
    matches = await db.tracks.find(
        {"$or": [{"file_url": file_url}, {"preview_url": file_url}]},
        {"_id": 0},
    ).to_list(50)

    def _pick(field: str) -> Optional[dict]:
        matched = [t for t in matches if str(t.get(field) or "") == file_url]
        if not matched:
            return None
        published = [t for t in matched if _is_catalog_published(t)]
        return (published or matched)[0]

    as_full = _pick("file_url")
    as_preview = _pick("preview_url")
    if kind == "audio_preview" and not as_full:
        return "preview"
    user = await _user_from_audio_request(request, authorization)
    if user and (user_is_admin(user) or meta.get("uploader_user_id") == user.get("user_id")):
        return "full"
    if as_full and await _user_owns_track(user, as_full):
        return "full"
    if as_full and (
        _is_catalog_published(as_full)
        or as_full.get("track_id") in await _catalog_visible_track_ids([as_full])
    ):
        return "preview"
    if as_preview and (
        kind == "audio_preview"
        or _is_catalog_published(as_preview)
        or as_preview.get("track_id") in await _catalog_visible_track_ids([as_preview])
    ):
        return "preview"
    return None


@api_router.post("/upload/audio")
async def upload_audio(file: UploadFile = File(...), authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can upload audio")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    filename = file.filename or f"{uuid.uuid4().hex}.audio"
    content_type = _normalize_audio_content_type(file.content_type, filename, content[:16])

    file_id = await fs.upload_from_stream(
        filename=filename,
        source=content,
        metadata={
            "kind": "audio",
            "content_type": content_type,
            "uploader_user_id": user["user_id"],
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        },
    )
    file_url = f"/api/files/audio/{str(file_id)}"
    preview_url = await _preview_url_from_bytes(
        content,
        user_id=user["user_id"],
        filename=filename,
        content_type=content_type,
        start_sec=0,
        duration_sec=15,
    )

    return {"file_url": file_url, "preview_url": preview_url or ""}

def _looks_like_image(content: bytes, content_type: Optional[str]) -> bool:
    ct = str(content_type or "").lower()
    if ct.startswith("image/"):
        return True
    return (
        content[:3] == b"\xff\xd8\xff"
        or content[:8] == b"\x89PNG\r\n\x1a\n"
        or content[:6] in {b"GIF87a", b"GIF89a"}
        or content[:4] == b"RIFF"
        or content[:4] == b"\x00\x00\x01\x00"
    )


@api_router.post("/upload/cover")
async def upload_cover(file: UploadFile = File(...), authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(content) > 8 * 1024 * 1024:
        raise HTTPException(status_code=400, detail="Image trop lourde (8 Mo max)")
    if not _looks_like_image(content, file.content_type):
        raise HTTPException(status_code=400, detail="Fichier image requis")

    file_id = await fs.upload_from_stream(
        filename=file.filename or f"{uuid.uuid4().hex}.cover",
        source=content,
        metadata={
            "kind": "cover",
            "content_type": file.content_type or "application/octet-stream",
            "uploader_user_id": user["user_id"],
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    return {"cover_url": f"/api/files/covers/{str(file_id)}"}

async def _stream_audio_file(file_id: str, request: Request, *, private: bool = False):
    try:
        oid = ObjectId(file_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file id")

    try:
        grid_out = await fs.open_download_stream(oid)
    except Exception:
        raise HTTPException(status_code=404, detail="File not found")

    file_size = int(getattr(grid_out, "length", 0) or 0)
    filename = getattr(grid_out, "filename", None) or ""
    stored_type = (
        (getattr(grid_out, "metadata", None) or {}).get("content_type")
        or getattr(grid_out, "content_type", None)
        or ""
    )
    content_type = _normalize_audio_content_type(stored_type, filename)

    # Existing .wave uploads often stored audio/wave or octet-stream — sniff if still generic.
    if content_type in {"application/octet-stream", "audio/wave", ""} or not str(content_type).startswith("audio/"):
        if await _seek_grid_out(grid_out, 0):
            try:
                header = await grid_out.read(16)
                content_type = _normalize_audio_content_type(stored_type, filename, header)
            finally:
                await _seek_grid_out(grid_out, 0)

    parsed = _parse_byte_range(request.headers.get("range") or "", file_size)
    if parsed == "unsatisfiable":
        return Response(
            status_code=416,
            headers=_audio_response_headers(
                content_type,
                filename,
                {"Content-Range": f"bytes */{file_size}"},
                private=private,
            ),
        )

    if parsed:
        start, end = parsed

        async def iterfile():
            remaining = end - start + 1
            await _seek_grid_out_or_skip(grid_out, start)
            while remaining > 0:
                data = await grid_out.read(min(_AUDIO_CHUNK_SIZE, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data

        return StreamingResponse(
            iterfile(),
            status_code=206,
            media_type=content_type,
            headers=_audio_response_headers(
                content_type,
                filename,
                {
                    "Content-Range": f"bytes {start}-{end}/{file_size}",
                    "Content-Length": str(end - start + 1),
                },
                private=private,
            ),
        )

    async def iterfile_full():
        while True:
            data = await grid_out.read(_AUDIO_CHUNK_SIZE)
            if not data:
                break
            yield data

    return StreamingResponse(
        iterfile_full(),
        media_type=content_type,
        headers=_audio_response_headers(
            content_type,
            filename,
            {"Content-Length": str(file_size)},
            private=private,
        ),
    )


@api_router.get("/files/audio/{file_id}")
async def get_audio_file(file_id: str, request: Request, authorization: Optional[str] = Header(None)):
    try:
        access = await _audio_access_level(file_id, request, authorization)
        if not access:
            raise HTTPException(status_code=403, detail="Achat requis pour le fichier complet")
        return await _stream_audio_file(file_id, request, private=access == "full")
    except HTTPException:
        raise
    except Exception:
        logger.exception("Failed to serve audio file_id=%s", file_id)
        raise HTTPException(status_code=500, detail="Audio file could not be served")

@api_router.get("/files/covers/{file_id}")
async def get_cover_file(file_id: str):
    try:
        oid = ObjectId(file_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid file id")

    try:
        grid_out = await fs.open_download_stream(oid)
    except Exception:
        raise HTTPException(status_code=404, detail="File not found")

    file_size = int(getattr(grid_out, "length", 0) or 0)
    content_type = (
        (getattr(grid_out, "metadata", None) or {}).get("content_type")
        or getattr(grid_out, "content_type", None)
        or "application/octet-stream"
    )

    async def iterfile():
        while True:
            data = await grid_out.read(8192)
            if not data:
                break
            yield data

    return StreamingResponse(
        iterfile(),
        headers={
            "Content-Length": str(file_size),
            "Content-Type": content_type,
        },
    )

# ==================== TRACK ROUTES ====================

@api_router.post("/tracks", response_model=TrackResponse)
async def create_track(track_data: TrackCreate, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can create tracks")
    
    track_id = f"track_{uuid.uuid4().hex[:12]}"
    
    track_doc = {
        "track_id": track_id,
        "title": track_data.title,
        "artist_id": user["user_id"],
        "artist_name": user["artist_name"],
        "album_id": track_data.album_id,
        "price": track_data.price,
        "is_free_price": bool(track_data.is_free_price),
        "min_price": track_data.min_price if track_data.is_free_price else None,
        "duration": track_data.duration_sec,
        "preview_url": "",
        "preview_start_time": track_data.preview_start_time,
        "preview_duration": int(track_data.preview_duration_sec or 15),
        "file_url": "",
        "cover_url": None,
        "genre": track_data.genre,
        "description": track_data.description,
        "mastering": track_data.mastering,
        "splits": track_data.splits or [],
        "bpm": track_data.bpm,
        "key": track_data.key,
        "isrc": track_data.isrc,
        "release_date": track_data.release_date,
        "mix_version": track_data.mix_version,
        "availability": track_data.availability,
        "producer": track_data.producer,
        "beatmaker": track_data.beatmaker,
        "status": "draft",
        "likes_count": 0,
        "play_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.tracks.insert_one(track_doc)
    await _safe_notify(maybe_notify_published("track", None, track_doc))
    return TrackResponse(**track_doc)

@api_router.get("/tracks", response_model=List[TrackResponse])
async def get_tracks(
    limit: int = 50,
    skip: int = 0,
    genre: Optional[str] = None,
    status: Optional[str] = None,
    ids: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    id_list = _parse_track_ids(ids)
    if id_list:
        user = await _optional_user(authorization, request)
        found = await db.tracks.find({"track_id": {"$in": id_list}}, {"_id": 0}).to_list(len(id_list))
        owned_ids = await _batch_owned_track_ids(user, found)
        listed_ids = await _catalog_visible_track_ids(found)
        visible = []
        for track in found:
            tid = track.get("track_id")
            if tid in listed_ids or tid in owned_ids:
                visible.append(track)
        by_id = {t["track_id"]: t for t in visible if t.get("track_id")}
        ordered = [by_id[tid] for tid in id_list if tid in by_id]
        return [
            _track_response(track, include_file=track.get("track_id") in owned_ids)
            for track in ordered
        ]

    user = await _optional_user(authorization, request)
    status_q = _catalog_status_query(status, user)
    if status_q is None:
        return []

    wanted = (status or "").strip().lower()
    extra = {"genre": genre} if genre else None
    if not wanted or wanted in {"published", "public"}:
        query = await _catalog_track_list_query(extra)
    else:
        query = dict(status_q)
        if extra:
            query.update(extra)

    tracks = await db.tracks.find(query, {"_id": 0}).skip(skip).limit(limit).to_list(limit)
    return [_track_response(track, include_file=False) for track in tracks]

@api_router.get("/tracks/{track_id}", response_model=TrackResponse)
async def get_track(track_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")
    user = await _optional_user(authorization, request)
    owned = await _user_owns_track(user, track)
    listed = track.get("track_id") in await _catalog_visible_track_ids([track])
    if not listed and not owned:
        raise HTTPException(status_code=404, detail="Track not found")
    return _track_response(track, include_file=owned)

@api_router.put("/tracks/{track_id}")
async def update_track(track_id: str, track_data: dict, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")
    
    if track["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    previous_status = track.get("status")
    prepared = await _prepare_track_audio_fields(track_data, existing=track, user_id=user["user_id"])
    _ensure_published_has_audio({
        "status": prepared.get("status", track.get("status")),
        "file_url": prepared["file_url"] if "file_url" in prepared else track.get("file_url"),
    })
    await db.tracks.update_one({"track_id": track_id}, {"$set": prepared})
    
    updated_track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    await _safe_notify(maybe_notify_published("track", previous_status, updated_track))
    return _track_response(updated_track, include_file=True)

@api_router.delete("/tracks/{track_id}")
async def delete_track(track_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")
    
    if track["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.tracks.delete_one({"track_id": track_id})
    return {"message": "Track deleted"}

# ==================== ALBUM ROUTES ====================

@api_router.post("/albums", response_model=AlbumResponse)
async def create_album(album_data: AlbumCreate, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can create albums")
    
    album_id = f"album_{uuid.uuid4().hex[:12]}"
    
    album_doc = {
        "album_id": album_id,
        "title": album_data.title,
        "artist_id": user["user_id"],
        "artist_name": user["artist_name"],
        "price": album_data.price,
        "is_free_price": bool(album_data.is_free_price),
        "min_price": album_data.min_price if album_data.is_free_price else None,
        "cover_url": None,
        "description": album_data.description,
        "mix_version": album_data.mix_version,
        "availability": album_data.availability,
        "producer": album_data.producer,
        "beatmaker": album_data.beatmaker,
        "track_ids": [],
        "status": album_data.status,
        "likes_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.albums.insert_one(album_doc)
    await _safe_notify(maybe_notify_published("album", None, album_doc))
    return AlbumResponse(**album_doc)

@api_router.get("/albums", response_model=List[AlbumResponse])
async def get_albums(
    limit: int = 50,
    skip: int = 0,
    status: Optional[str] = None,
    authorization: Optional[str] = Header(None),
    request: Request = None,
):
    user = await _optional_user(authorization, request)
    status_q = _catalog_status_query(status, user)
    if status_q is None:
        return []
    albums = await db.albums.find(status_q, {"_id": 0}).skip(skip).limit(limit).to_list(limit)
    hydrated = await _hydrate_album_track_ids(albums)
    return [AlbumResponse(**album) for album in hydrated]

@api_router.get("/albums/{album_id}", response_model=AlbumResponse)
async def get_album(album_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if not album:
        raise HTTPException(status_code=404, detail="Album not found")
    user = await _optional_user(authorization, request)
    if not _is_catalog_published(album) and not await _user_owns_album(user, album):
        raise HTTPException(status_code=404, detail="Album not found")
    hydrated = await _hydrate_album_track_ids([album])
    return AlbumResponse(**hydrated[0])

@api_router.put("/albums/{album_id}")
async def update_album(album_id: str, album_data: dict, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if not album:
        raise HTTPException(status_code=404, detail="Album not found")
    
    if album["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    previous_status = album.get("status")
    payload = dict(album_data or {})
    track_ids = payload.get("track_ids")
    if "track_ids" in payload and not (isinstance(track_ids, list) and len(track_ids) > 0):
        payload.pop("track_ids", None)
    next_status = payload.get("status", previous_status)
    preview_album = {**album, **payload}
    if next_status == "published":
        await _ensure_album_can_publish(preview_album)
    await db.albums.update_one({"album_id": album_id}, {"$set": payload})
    
    updated_album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if next_status == "published":
        await _sync_album_tracks_status(updated_album, status="published")
    elif next_status == "draft" and previous_status == "published":
        await _sync_album_tracks_status(updated_album, status="draft")
    await _safe_notify(maybe_notify_published("album", previous_status, updated_album))
    hydrated = await _hydrate_album_track_ids([updated_album])
    return AlbumResponse(**hydrated[0])

@api_router.delete("/albums/{album_id}")
async def delete_album(album_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)

    album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if not album:
        raise HTTPException(status_code=404, detail="Album not found")

    if album["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")

    track_ids = list(album.get("track_ids") or [])
    # Supprime les titres rattachés à cet album (création album + liens track_ids)
    delete_filter = {"$or": [{"album_id": album_id}]}
    if track_ids:
        delete_filter["$or"].append({"track_id": {"$in": track_ids}})
    await db.tracks.delete_many(delete_filter)

    await db.albums.delete_one({"album_id": album_id})
    return {"message": "Album deleted"}

@api_router.post("/albums/{album_id}/tracks", response_model=AlbumTrackResponse)
async def create_album_track(album_id: str, track_data: AlbumTrackCreate, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)

    album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if not album:
        raise HTTPException(status_code=404, detail="Album not found")

    if album["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")

    track_id = f"track_{uuid.uuid4().hex[:12]}"

    track_doc = {
        "track_id": track_id,
        "title": track_data.title,
        "artist_id": user["user_id"],
        "artist_name": user["artist_name"],
        "album_id": album_id,
        "price": track_data.price,
        "is_free_price": bool(track_data.is_free_price),
        "min_price": track_data.min_price if track_data.is_free_price else None,
        "duration": track_data.duration_sec,
        "preview_url": "",
        "preview_start_time": track_data.preview_start_time,
        "preview_duration": int(track_data.preview_duration_sec or 15),
        "file_url": "",
        "cover_url": None,
        "genre": track_data.genre,
        "description": track_data.description,
        "mastering": track_data.mastering,
        "splits": track_data.splits or [],
        "bpm": track_data.bpm,
        "key": track_data.key,
        "isrc": track_data.isrc,
        "release_date": track_data.release_date,
        "mix_version": track_data.mix_version,
        "availability": track_data.availability,
        "producer": track_data.producer,
        "beatmaker": track_data.beatmaker,
        "status": "draft",
        "likes_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    await db.tracks.insert_one(track_doc)
    await _safe_notify(maybe_notify_published("track", None, track_doc))

    # add to album.track_ids
    await db.albums.update_one(
        {"album_id": album_id},
        {"$addToSet": {"track_ids": track_id}}
    )

    return AlbumTrackResponse(**track_doc)

@api_router.put("/albums/{album_id}/tracks/{track_id}", response_model=AlbumTrackResponse)
async def update_album_track(album_id: str, track_id: str, payload: dict, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)

    album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if not album:
        raise HTTPException(status_code=404, detail="Album not found")
    if album["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")

    track = await db.tracks.find_one({"track_id": track_id, "album_id": album_id}, {"_id": 0})
    if not track:
        raise HTTPException(status_code=404, detail="Track not found in this album")

    previous_status = track.get("status")
    prepared = await _prepare_track_audio_fields(payload, existing=track, user_id=user["user_id"])
    _ensure_published_has_audio({
        "status": prepared.get("status", track.get("status")),
        "file_url": prepared["file_url"] if "file_url" in prepared else track.get("file_url"),
    })
    await db.tracks.update_one({"track_id": track_id}, {"$set": prepared})
    updated = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    await _safe_notify(maybe_notify_published("track", previous_status, updated))
    return AlbumTrackResponse(**updated)

# ==================== ARTIST ROUTES ====================

def _custom_avatar_url(*candidates) -> Optional[str]:
    """Photo perso en priorité, sinon le masque déjà stocké."""
    fallback = None
    for raw in candidates:
        value = str(raw or "").strip()
        if not value:
            continue
        if not _is_mask_avatar(value):
            return value
        if fallback is None:
            fallback = value
    return fallback


async def _apply_custom_avatars(artists: list) -> None:
    ids = [a.get("user_id") for a in artists if a.get("user_id")]
    if not ids:
        return
    profiles = await db.artist_profiles.find(
        {"user_id": {"$in": ids}},
        {"_id": 0, "user_id": 1, "avatar_url": 1},
    ).to_list(len(ids))
    by_id = {p["user_id"]: p for p in profiles}
    for artist in artists:
        profile = by_id.get(artist.get("user_id")) or {}
        artist["avatar_url"] = str(profile.get("avatar_url") or "").strip() or None
        chosen = _custom_avatar_url(profile.get("avatar_url"), artist.get("picture"))
        if not chosen:
            await _ensure_default_picture(artist)
            chosen = artist.get("picture")
        artist["picture"] = chosen

@api_router.get("/artists")
async def get_artists(limit: int = 50):
    artists = await db.users.find({"role": "artist"}, {"_id": 0, "password_hash": 0}).limit(limit).to_list(limit)
    await _apply_custom_avatars(artists)
    return artists

@api_router.get("/artists/{artist_id}")
async def get_artist(artist_id: str):
    artist = await db.users.find_one({"user_id": artist_id, "role": "artist"}, {"_id": 0, "password_hash": 0})
    if not artist:
        raise HTTPException(status_code=404, detail="Artist not found")

    await _apply_custom_avatars([artist])
    
    tracks = await db.tracks.find({"artist_id": artist_id}, {"_id": 0}).to_list(200)
    visible_ids = await _catalog_visible_track_ids(tracks)
    artist["tracks"] = [
        _public_track_doc(track) for track in tracks if track.get("track_id") in visible_ids
    ]

    albums = await db.albums.find(
        {"artist_id": artist_id, "status": {"$nin": ["draft"]}},
        {"_id": 0},
    ).to_list(100)
    artist["albums"] = await _hydrate_album_track_ids(albums)
    
    return artist

# ==================== PLAYLIST ROUTES ====================

@api_router.post("/playlists", response_model=PlaylistResponse)
async def create_playlist(playlist_data: PlaylistCreate, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    playlist_id = f"playlist_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()
    
    playlist_doc = {
        "playlist_id": playlist_id,
        "user_id": user["user_id"],
        "name": playlist_data.name,
        "description": playlist_data.description,
        "track_ids": [],
        "created_at": now,
        "updated_at": now
    }
    
    await db.playlists.insert_one(playlist_doc)
    return PlaylistResponse(**playlist_doc)

@api_router.get("/playlists", response_model=List[PlaylistResponse])
async def get_playlists(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    playlists = await db.playlists.find({"user_id": user["user_id"]}, {"_id": 0}).to_list(100)
    return [PlaylistResponse(**playlist) for playlist in playlists]

@api_router.get("/playlists/{playlist_id}", response_model=PlaylistResponse)
async def get_playlist(playlist_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)

    playlist = await db.playlists.find_one({"playlist_id": playlist_id}, {"_id": 0})
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist introuvable")
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Action non autorisée")

    return PlaylistResponse(**playlist)

@api_router.put("/playlists/{playlist_id}", response_model=PlaylistResponse)
async def update_playlist(playlist_id: str, payload: PlaylistUpdate, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)

    playlist = await db.playlists.find_one({"playlist_id": playlist_id}, {"_id": 0})
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist introuvable")
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Action non autorisée")

    update_data = {k: v for k, v in payload.model_dump().items() if v is not None}
    if not update_data:
        return PlaylistResponse(**playlist)

    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    await db.playlists.update_one({"playlist_id": playlist_id}, {"$set": update_data})

    updated = await db.playlists.find_one({"playlist_id": playlist_id}, {"_id": 0})
    return PlaylistResponse(**updated)

@api_router.put("/playlists/{playlist_id}/tracks")
async def update_playlist_tracks(playlist_id: str, track_ids: List[str], authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    playlist = await db.playlists.find_one({"playlist_id": playlist_id}, {"_id": 0})
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist introuvable")
    
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Action non autorisée")
    
    await db.playlists.update_one(
        {"playlist_id": playlist_id},
        {"$set": {
            "track_ids": track_ids,
            "updated_at": datetime.now(timezone.utc).isoformat()
        }}
    )
    
    return {"message": "Playlist updated"}

class PlaylistTrackAdd(BaseModel):
    track_id: str

@api_router.post("/playlists/{playlist_id}/tracks")
async def add_track_to_playlist(playlist_id: str, payload: PlaylistTrackAdd, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)

    playlist = await db.playlists.find_one({"playlist_id": playlist_id}, {"_id": 0})
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist introuvable")
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Action non autorisée")

    track_id = payload.track_id
    exists = await db.tracks.find_one({"track_id": track_id}, {"_id": 1})
    if not exists:
        raise HTTPException(status_code=404, detail="Track not found")

    if track_id in (playlist.get("track_ids") or []):
        return {"message": "Already in playlist"}

    await db.playlists.update_one(
        {"playlist_id": playlist_id},
        {"$set": {"updated_at": datetime.now(timezone.utc).isoformat()}, "$push": {"track_ids": track_id}},
    )
    return {"message": "Added"}

@api_router.delete("/playlists/{playlist_id}/tracks/{track_id}")
async def remove_track_from_playlist(playlist_id: str, track_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)

    playlist = await db.playlists.find_one({"playlist_id": playlist_id}, {"_id": 0})
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist introuvable")
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Action non autorisée")

    await db.playlists.update_one(
        {"playlist_id": playlist_id},
        {"$set": {"updated_at": datetime.now(timezone.utc).isoformat()}, "$pull": {"track_ids": track_id}},
    )
    return {"message": "Removed"}

@api_router.delete("/playlists/{playlist_id}")
async def delete_playlist(playlist_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    playlist = await db.playlists.find_one({"playlist_id": playlist_id}, {"_id": 0})
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist introuvable")
    
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Action non autorisée")
    
    await db.playlists.delete_one({"playlist_id": playlist_id})
    return {"message": "Playlist deleted"}

# ==================== NOTIFICATIONS ====================

def _format_eur_from_cents(cents) -> str:
    try:
        euros = float(cents or 0) / 100
    except (TypeError, ValueError):
        euros = 0.0
    return f"{euros:.2f}".replace(".", ",") + " €"


def _notification_doc(
    *,
    user_id: str,
    kind: str,
    title: str,
    body: str,
    href: str,
    actor_user_id: Optional[str] = None,
    actor_name: Optional[str] = None,
    item_type: Optional[str] = None,
    item_id: Optional[str] = None,
    item_title: Optional[str] = None,
    amount_cents: Optional[float] = None,
    dedupe_key: Optional[str] = None,
) -> dict:
    return {
        "notification_id": f"notif_{uuid.uuid4().hex[:12]}",
        "user_id": user_id,
        "type": kind,
        "title": title,
        "body": body,
        "href": href,
        "actor_user_id": actor_user_id,
        "actor_name": actor_name,
        "item_type": item_type,
        "item_id": item_id,
        "item_title": item_title,
        "amount_cents": amount_cents,
        "dedupe_key": dedupe_key,
        "read": False,
        "created_at": datetime.now(timezone.utc).isoformat(),
    }


async def _safe_notify(coro) -> None:
    try:
        await coro
    except Exception:
        logger.exception("Notification dispatch failed")


async def _actor_label(user_id: Optional[str]) -> str:
    if not user_id:
        return "Quelqu'un"
    user = await db.users.find_one({"user_id": user_id}, {"_id": 0, "name": 1, "artist_name": 1})
    if not user:
        return "Quelqu'un"
    name = str(user.get("artist_name") or user.get("name") or "").strip()
    return name or "Quelqu'un"


async def _filter_by_setting(user_ids: List[str], field: str) -> List[str]:
    if not user_ids:
        return []
    docs = await db.user_settings.find(
        {"user_id": {"$in": user_ids}, field: False},
        {"_id": 0, "user_id": 1},
    ).to_list(len(user_ids))
    blocked = {d.get("user_id") for d in docs}
    return [uid for uid in user_ids if uid not in blocked]


async def _fan_ids_for_artist(artist_id: str) -> List[str]:
    follows = await db.follows.find({"artist_id": artist_id}, {"_id": 0, "user_id": 1}).to_list(5000)
    likes = await db.likes.find(
        {"item_type": "artist", "item_id": artist_id},
        {"_id": 0, "user_id": 1},
    ).to_list(5000)
    seen = set()
    ids: List[str] = []
    for doc in follows + likes:
        uid = doc.get("user_id")
        if not uid or uid == artist_id or uid in seen:
            continue
        seen.add(uid)
        ids.append(uid)
    return ids


async def notify_item_liked(*, actor_id: str, item_type: str, item_id: str) -> None:
    href = None
    artist_id = None
    item_title = None
    body = "Like"
    if item_type == "track":
        item = await db.tracks.find_one({"track_id": item_id}, {"_id": 0, "title": 1, "artist_id": 1})
        if not item:
            return
        artist_id = item.get("artist_id")
        item_title = item.get("title") or "un titre"
        href = f"/track/{item_id}"
        body = "Like sur un titre"
    elif item_type == "album":
        item = await db.albums.find_one({"album_id": item_id}, {"_id": 0, "title": 1, "artist_id": 1})
        if not item:
            return
        artist_id = item.get("artist_id")
        item_title = item.get("title") or "un album"
        href = f"/album/{item_id}"
        body = "Like sur un album"
    else:
        artist_id = item_id
        artist = await db.users.find_one({"user_id": artist_id}, {"_id": 0, "artist_name": 1, "name": 1})
        if not artist:
            return
        item_title = artist.get("artist_name") or artist.get("name") or "ton profil"
        href = f"/artist/{artist_id}"
        body = "Like sur ton profil"

    if not artist_id or artist_id == actor_id:
        return
    allowed = await _filter_by_setting([artist_id], "notifications_likes")
    if not allowed:
        return
    actor = await _actor_label(actor_id)
    await db.notifications.insert_one(
        _notification_doc(
            user_id=artist_id,
            kind="like",
            title=f"{actor} a aimé {item_title}",
            body=body,
            href=href,
            actor_user_id=actor_id,
            actor_name=actor,
            item_type=item_type,
            item_id=item_id,
            item_title=item_title,
        )
    )


async def notify_item_acquired(*, buyer_id: str, item_type: str, item_id: str, amount_cents: float) -> None:
    dedupe_key = f"purchase:{buyer_id}:{item_type}:{item_id}"
    if await db.notifications.find_one({"dedupe_key": dedupe_key}, {"_id": 1}):
        return
    if item_type == "track":
        item = await db.tracks.find_one({"track_id": item_id}, {"_id": 0, "title": 1, "artist_id": 1})
        href = f"/track/{item_id}"
        kind_label = "titre"
    else:
        item = await db.albums.find_one({"album_id": item_id}, {"_id": 0, "title": 1, "artist_id": 1})
        href = f"/album/{item_id}"
        kind_label = "album"
    if not item:
        return
    artist_id = item.get("artist_id")
    if not artist_id or artist_id == buyer_id:
        return
    allowed = await _filter_by_setting([artist_id], "notifications_purchases")
    if not allowed:
        return
    actor = await _actor_label(buyer_id)
    item_title = item.get("title") or kind_label
    try:
        cents = float(amount_cents or 0)
    except (TypeError, ValueError):
        cents = 0.0
    if cents <= 0:
        title = f"{actor} a ajouté {item_title}"
        body = f"Ajout gratuit · {kind_label}"
    else:
        title = f"{actor} a acheté {item_title}"
        body = f"{_format_eur_from_cents(cents)} · {kind_label}"
    await db.notifications.insert_one(
        _notification_doc(
            user_id=artist_id,
            kind="purchase",
            title=title,
            body=body,
            href=href,
            actor_user_id=buyer_id,
            actor_name=actor,
            item_type=item_type,
            item_id=item_id,
            item_title=item_title,
            amount_cents=cents,
            dedupe_key=dedupe_key,
        )
    )


async def notify_new_release(item: dict, item_type: str) -> None:
    if not item or item.get("status") != "published":
        return
    artist_id = item.get("artist_id")
    if not artist_id:
        return
    if item_type == "track":
        item_id = item.get("track_id")
        href = f"/track/{item_id}"
        kind_label = "un titre"
    else:
        item_id = item.get("album_id")
        href = f"/album/{item_id}"
        kind_label = "un album"
    if not item_id:
        return
    fans = await _filter_by_setting(await _fan_ids_for_artist(artist_id), "notifications_new_releases")
    if not fans:
        return
    artist_name = str(item.get("artist_name") or "").strip() or await _actor_label(artist_id)
    item_title = item.get("title") or kind_label
    now = datetime.now(timezone.utc).isoformat()
    docs = []
    for uid in fans:
        doc = _notification_doc(
            user_id=uid,
            kind="release",
            title=f"{artist_name} a sorti {item_title}",
            body=f"Nouvelle sortie · {kind_label}",
            href=href,
            actor_user_id=artist_id,
            actor_name=artist_name,
            item_type=item_type,
            item_id=item_id,
            item_title=item_title,
        )
        doc["created_at"] = now
        docs.append(doc)
    if docs:
        await db.notifications.insert_many(docs)


async def maybe_notify_published(item_type: str, previous_status: Optional[str], item: Optional[dict]) -> None:
    if not item or item.get("status") != "published":
        return
    if previous_status == "published":
        return
    await notify_new_release(item, item_type)


@api_router.get("/notifications")
async def list_notifications(limit: int = 40, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    limit = max(1, min(int(limit or 40), 100))
    items = await db.notifications.find(
        {"user_id": user["user_id"]},
        {"_id": 0},
    ).sort("created_at", -1).limit(limit).to_list(limit)
    unread_count = await db.notifications.count_documents({"user_id": user["user_id"], "read": False})
    return {"items": items, "unread_count": unread_count}


@api_router.post("/notifications/read-all")
async def mark_all_notifications_read(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    await db.notifications.update_many(
        {"user_id": user["user_id"], "read": False},
        {"$set": {"read": True}},
    )
    return {"ok": True}


@api_router.post("/notifications/{notification_id}/read")
async def mark_notification_read(notification_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    await db.notifications.update_one(
        {"notification_id": notification_id, "user_id": user["user_id"]},
        {"$set": {"read": True}},
    )
    return {"ok": True}

# ==================== LIKES ROUTES ====================

@api_router.post("/likes")
async def add_like(like_data: LikeRequest, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    # Check if already liked
    existing = await db.likes.find_one({
        "user_id": user["user_id"],
        "item_type": like_data.item_type,
        "item_id": like_data.item_id
    })
    
    if existing:
        return {"message": "Already liked"}
    
    # Add like
    await db.likes.insert_one({
        "user_id": user["user_id"],
        "item_type": like_data.item_type,
        "item_id": like_data.item_id,
        "created_at": datetime.now(timezone.utc).isoformat()
    })
    
    # Update likes count
    if like_data.item_type == "track":
        await db.tracks.update_one({"track_id": like_data.item_id}, {"$inc": {"likes_count": 1}})
    elif like_data.item_type == "album":
        await db.albums.update_one({"album_id": like_data.item_id}, {"$inc": {"likes_count": 1}})
    else:
        await db.users.update_one({"user_id": like_data.item_id, "role": "artist"}, {"$inc": {"likes_count": 1}})

    await _safe_notify(notify_item_liked(actor_id=user["user_id"], item_type=like_data.item_type, item_id=like_data.item_id))
    
    return {"message": "Liked"}

@api_router.delete("/likes")
async def remove_like(item_type: str, item_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    result = await db.likes.delete_one({
        "user_id": user["user_id"],
        "item_type": item_type,
        "item_id": item_id
    })
    
    if result.deleted_count > 0:
        # Update likes count
        if item_type == "track":
            await db.tracks.update_one({"track_id": item_id}, {"$inc": {"likes_count": -1}})
        elif item_type == "album":
            await db.albums.update_one({"album_id": item_id}, {"$inc": {"likes_count": -1}})
        else:
            await db.users.update_one({"user_id": item_id, "role": "artist"}, {"$inc": {"likes_count": -1}})
    
    return {"message": "Unliked"}

@api_router.get("/likes")
async def get_likes(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    likes = await db.likes.find({"user_id": user["user_id"]}, {"_id": 0}).to_list(1000)
    return likes

@api_router.get("/likes/state")
async def get_likes_state(item_type: str, ids: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Return a map {id: boolean} for the given item_type and comma-separated ids."""
    user = await _optional_user(authorization, request)
    if not user:
        return {}

    id_list = [i.strip() for i in (ids or "").split(",") if i.strip()]
    if not id_list:
        return {}

    cursor = db.likes.find(
        {"user_id": user["user_id"], "item_type": item_type, "item_id": {"$in": id_list}},
        {"_id": 0, "item_id": 1},
    )
    liked_docs = await cursor.to_list(len(id_list))
    liked_set = {d["item_id"] for d in liked_docs}
    return {i: (i in liked_set) for i in id_list}

@api_router.get("/likes/summary")
async def get_likes_summary(limit: int = 200, authorization: Optional[str] = Header(None), request: Request = None):
    """Return expanded liked items grouped by type."""
    user = await get_current_user(authorization, request)
    likes = await db.likes.find({"user_id": user["user_id"]}, {"_id": 0}).to_list(5000)

    track_ids = [l["item_id"] for l in likes if l["item_type"] == "track"][:limit]
    album_ids = [l["item_id"] for l in likes if l["item_type"] == "album"][:limit]
    artist_ids = [l["item_id"] for l in likes if l["item_type"] == "artist"][:limit]

    tracks = await db.tracks.find({"track_id": {"$in": track_ids}}, {"_id": 0}).to_list(len(track_ids))
    albums = await db.albums.find({"album_id": {"$in": album_ids}}, {"_id": 0}).to_list(len(album_ids))
    artists = await db.users.find({"user_id": {"$in": artist_ids}, "role": "artist"}, {"_id": 0, "password_hash": 0}).to_list(len(artist_ids))

    # preserve order roughly by ids list
    track_by_id = {t["track_id"]: t for t in tracks}
    album_by_id = {a["album_id"]: a for a in albums}
    artist_by_id = {a["user_id"]: a for a in artists}
    liked_artists = [artist_by_id[i] for i in artist_ids if i in artist_by_id]
    await _apply_custom_avatars(liked_artists)

    return {
        "tracks": [_public_track_doc(track_by_id[i]) for i in track_ids if i in track_by_id],
        "albums": [album_by_id[i] for i in album_ids if i in album_by_id],
        "artists": liked_artists,
    }

# ==================== PURCHASE ROUTES ====================

@api_router.post("/purchases/checkout")
async def create_checkout(checkout_data: CheckoutRequest, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    item_id = checkout_data.item_id.strip()
    if not item_id:
        raise HTTPException(status_code=400, detail="item_id invalide")

    # Get item details
    if checkout_data.item_type == "track":
        item = await db.tracks.find_one({"track_id": item_id}, {"_id": 0})
        if not item:
            raise HTTPException(status_code=404, detail="Track not found")
    else:
        item = await db.albums.find_one({"album_id": item_id}, {"_id": 0})
        if not item:
            raise HTTPException(status_code=404, detail="Album not found")

    artist_id = item.get("artist_id")
    if not artist_id:
        raise HTTPException(status_code=400, detail="Article sans vendeur (artist_id manquant)")
    if user["user_id"] == artist_id:
        raise HTTPException(status_code=400, detail="Tu ne peux pas acheter ton propre contenu")

    seller = await db.users.find_one({"user_id": artist_id, "role": "artist"}, {"_id": 0})
    if not seller:
        raise HTTPException(status_code=400, detail="Vendeur introuvable")

    is_pay_what_you_want = bool(item.get("is_free_price"))
    if is_pay_what_you_want:
        if checkout_data.amount_cents is None:
            raise HTTPException(status_code=400, detail="amount_cents requis pour un prix libre")
        try:
            amount_cents = float(checkout_data.amount_cents)
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="amount_cents invalide")
        if amount_cents <= 0:
            raise HTTPException(status_code=400, detail="amount_cents doit être > 0 (utilise /purchases/library pour 0€)")
        min_price = item.get("min_price")
        if min_price is not None:
            try:
                min_cents = float(min_price)
            except (TypeError, ValueError):
                min_cents = None
            if min_cents is not None and amount_cents < min_cents:
                raise HTTPException(status_code=400, detail=f"Minimum: {min_cents} cents")
    else:
        if is_free_item_price(item.get("price")):
            raise HTTPException(
                status_code=400,
                detail="Les contenus gratuits s'ajoutent à la bibliothèque sans passer par le paiement.",
            )

    ensure_stripe_available()

    # Check if already purchased (même item exact — id normalisé)
    existing_purchase = await db.purchases.find_one({
        "user_id": user["user_id"],
        "item_type": checkout_data.item_type,
        "item_id": item_id,
    })

    if existing_purchase:
        raise HTTPException(status_code=400, detail="Already purchased")

    if is_pay_what_you_want:
        amount_total_cents = int(round(amount_cents))
    else:
        raw_price = item.get("price")
        if raw_price is None:
            raise HTTPException(status_code=400, detail="Prix manquant pour cet article")
        try:
            amount_total_cents = int(round(float(raw_price)))
        except (TypeError, ValueError):
            raise HTTPException(status_code=400, detail="Prix invalide pour cet article")

    product_title = (item.get("title") or "").strip() or (
        "Album" if checkout_data.item_type == "album" else "Track"
    )

    fee_cents = compute_platform_fee_cents(amount_total_cents)
    seller_amount_cents = max(0, int(amount_total_cents) - int(fee_cents))

    try:
        session_result = await create_checkout_session(
            amount_cents=amount_total_cents,
            product_name=product_title,
            user_id=user["user_id"],
            item_type=checkout_data.item_type,
            item_id=item_id,
            origin_url=checkout_data.origin_url,
            artist_id=artist_id,
        )
    except stripe.error.StripeError as e:
        # Clé invalide, réseau, paramètres Stripe, etc. — éviter une 500 sans message.
        logger.warning("Stripe checkout session: %s", e, exc_info=True)
        msg = getattr(e, "user_message", None) or getattr(e, "message", None) or str(e)
        raise HTTPException(status_code=502, detail=f"Stripe: {msg}")

    transaction_doc = {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "session_id": session_result["session_id"],
        "user_id": user["user_id"],
        "amount": float(amount_total_cents),
        "currency": os.environ.get("STRIPE_CURRENCY", "eur"),
        "status": "pending",
        "payment_status": "pending",
        "settlement": "wallet",
        "wallet_credited": False,
        "metadata": {
            "item_type": checkout_data.item_type,
            "item_id": item_id,
        },
        "artist_id": artist_id,
        "stripe_connect_account_id": None,
        "platform_fee_cents": int(fee_cents),
        "seller_amount_cents": int(seller_amount_cents),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }

    await db.payment_transactions.insert_one(transaction_doc)

    await log_transaction_event(
        kind="checkout_created",
        data={
            "session_id": session_result["session_id"],
            "transaction_id": transaction_doc["transaction_id"],
            "user_id": user["user_id"],
            "item_type": checkout_data.item_type,
            "item_id": item_id,
            "artist_id": artist_id,
            "amount_cents": int(amount_total_cents),
            "currency": os.environ.get("STRIPE_CURRENCY", "eur"),
            "connect_used": False,
            "settlement": "wallet",
            "platform_fee_cents": int(fee_cents),
            "seller_net_cents": int(seller_amount_cents),
        },
    )

    return {"url": session_result["url"], "session_id": session_result["session_id"]}

@api_router.get("/purchases/status/{session_id}")
async def get_checkout_status(session_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    ensure_stripe_available()
    user = await get_current_user(authorization, request)

    sk = (os.environ.get("STRIPE_API_KEY") or "").strip()
    if session_id.startswith("cs_test_") and sk.startswith("sk_live_"):
        raise HTTPException(
            status_code=400,
            detail=(
                "Incohérence Stripe : session de test (cs_test_…) mais STRIPE_API_KEY est en live. "
                "Utilise une clé sk_test_… côté serveur pour le développement, ou refais un paiement en mode live."
            ),
        )
    if session_id.startswith("cs_live_") and sk.startswith("sk_test_"):
        raise HTTPException(
            status_code=400,
            detail=(
                "Incohérence Stripe : session live (cs_live_…) mais STRIPE_API_KEY est en test. "
                "Mets une clé sk_live_… sur le serveur qui a créé la session, ou repasse un paiement en mode test."
            ),
        )
    
    # Get transaction (may not exist in some edge cases; keep a minimal placeholder)
    transaction = await db.payment_transactions.find_one({"session_id": session_id}, {"_id": 0})
    if not transaction:
        transaction = {
            "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
            "session_id": session_id,
            "user_id": user["user_id"],
            "amount": 0.0,
            "currency": os.environ.get("STRIPE_CURRENCY", "eur"),
            "status": "unknown",
            "payment_status": "unknown",
            "metadata": {},
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await db.payment_transactions.insert_one(transaction)
    
    if transaction["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")

    try:
        checkout_status = await retrieve_checkout_session(session_id)
    except stripe.error.StripeError as e:
        logger.warning("retrieve_checkout_session %s: %s", session_id, e, exc_info=True)
        msg = getattr(e, "user_message", None) or getattr(e, "message", None) or str(e)
        raise HTTPException(status_code=502, detail=f"Stripe: {msg}")
    except Exception as e:
        logger.exception("retrieve_checkout_session inattendu %s", session_id)
        raise HTTPException(
            status_code=502,
            detail=f"Lecture session Stripe impossible: {e!s}",
        )

    await db.payment_transactions.update_one(
        {"session_id": session_id},
        {
            "$set": {
                "status": checkout_status.get("status"),
                "payment_status": checkout_status.get("payment_status"),
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        },
    )

    # Métadonnées : Mongo + repli sur la session Stripe (évite KeyError / docs incomplets)
    md_db = transaction.get("metadata") or {}
    md_stripe = checkout_status.get("metadata") or {}
    if not isinstance(md_db, dict):
        md_db = {}
    if not isinstance(md_stripe, dict):
        md_stripe = {}
    item_type = md_db.get("item_type") or md_stripe.get("item_type")
    item_id_raw = md_db.get("item_id") or md_stripe.get("item_id")
    item_id = str(item_id_raw).strip() if item_id_raw is not None else ""

    if checkout_status.get("payment_status") == "paid":
        if not item_type or not item_id:
            logger.error(
                "Session %s payée mais metadata item_type/item_id manquants (db=%s stripe=%s)",
                session_id,
                md_db,
                md_stripe,
            )
            raise HTTPException(
                status_code=400,
                detail=(
                    "Métadonnées d'achat incomplètes (item_type / item_id). "
                    "Vérifie que le checkout a bien été créé par cette API."
                ),
            )
        existing_purchase = await db.purchases.find_one(
            {
                "user_id": transaction["user_id"],
                "item_type": item_type,
                "item_id": item_id,
            }
        )
        if not existing_purchase:
            try:
                amt = transaction.get("amount")
                price_paid = float(amt) if amt is not None else 0.0
            except (TypeError, ValueError):
                price_paid = 0.0
            purchase_doc = {
                "purchase_id": f"purchase_{uuid.uuid4().hex[:12]}",
                "user_id": transaction["user_id"],
                "item_type": item_type,
                "item_id": item_id,
                "price_paid": price_paid,
                "purchased_at": datetime.now(timezone.utc).isoformat(),
                "artist_id": transaction.get("artist_id") or md_stripe.get("artist_id"),
                "platform_fee_cents": transaction.get("platform_fee_cents"),
                "seller_net_cents": transaction.get("seller_amount_cents"),
            }
            try:
                await db.purchases.insert_one(purchase_doc)
            except Exception as e:
                logger.exception("insert purchase session=%s", session_id)
                raise HTTPException(
                    status_code=500,
                    detail=f"Enregistrement de l'achat impossible: {e!s}",
                )
            await _safe_notify(notify_item_acquired(
                buyer_id=transaction["user_id"],
                item_type=item_type,
                item_id=item_id,
                amount_cents=price_paid,
            ))

        await _credit_artist_wallet_from_paid_transaction(transaction)

        await log_transaction_event(
            kind="checkout_paid",
            data={
                "session_id": session_id,
                "user_id": transaction["user_id"],
                "item_type": item_type,
                "item_id": item_id,
                "artist_id": transaction.get("artist_id") or md_stripe.get("artist_id"),
                "amount_cents": int(round(float(transaction.get("amount") or 0))),
                "currency": os.environ.get("STRIPE_CURRENCY", "eur"),
                "source": "status_endpoint",
            },
        )

    await log_transaction_event(
        kind="checkout_status_checked",
        data={
            "session_id": session_id,
            "user_id": transaction["user_id"],
            "status": checkout_status.get("status"),
            "payment_status": checkout_status.get("payment_status"),
            "source": "status_endpoint",
        },
    )

    return {
        "status": checkout_status.get("status"),
        "payment_status": checkout_status.get("payment_status"),
        "amount_total": checkout_status.get("amount_total"),
        "currency": checkout_status.get("currency"),
    }

@api_router.post("/purchases/library")
async def add_to_library(payload: AddToLibraryRequest, authorization: Optional[str] = Header(None), request: Request = None):
    """Ajoute un titre ou album gratuit à la bibliothèque (sans Stripe)."""
    user = await get_current_user(authorization, request)

    if payload.item_type == "track":
        item = await db.tracks.find_one({"track_id": payload.item_id}, {"_id": 0})
        if not item:
            raise HTTPException(status_code=404, detail="Track not found")
    else:
        item = await db.albums.find_one({"album_id": payload.item_id}, {"_id": 0})
        if not item:
            raise HTTPException(status_code=404, detail="Album not found")

    if bool(item.get("is_free_price")):
        min_price = item.get("min_price")
        if min_price is not None:
            try:
                min_cents = float(min_price)
            except (TypeError, ValueError):
                min_cents = None
            if min_cents is not None and min_cents > 0:
                raise HTTPException(status_code=400, detail="Ce prix libre a un minimum : utilisez le checkout.")
    else:
        if not is_free_item_price(item.get("price")):
            raise HTTPException(
                status_code=400,
                detail="Ce contenu est payant : utilisez le checkout.",
            )

    existing = await db.purchases.find_one({
        "user_id": user["user_id"],
        "item_type": payload.item_type,
        "item_id": payload.item_id,
    })
    if existing:
        return {"message": "Déjà dans la bibliothèque", "purchase_id": existing.get("purchase_id")}

    purchase_doc = {
        "purchase_id": f"purchase_{uuid.uuid4().hex[:12]}",
        "user_id": user["user_id"],
        "item_type": payload.item_type,
        "item_id": payload.item_id,
        "price_paid": 0,
        "purchased_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.purchases.insert_one(purchase_doc)
    await _safe_notify(notify_item_acquired(
        buyer_id=user["user_id"],
        item_type=payload.item_type,
        item_id=payload.item_id,
        amount_cents=0,
    ))
    await log_transaction_event(
        kind="library_add_free",
        data={
            "purchase_id": purchase_doc["purchase_id"],
            "user_id": user["user_id"],
            "item_type": payload.item_type,
            "item_id": payload.item_id,
            "amount_cents": 0,
            "currency": os.environ.get("STRIPE_CURRENCY", "eur"),
            "source": "library_endpoint",
        },
    )
    return {"message": "Ajouté à la bibliothèque", "purchase_id": purchase_doc["purchase_id"]}

@api_router.get("/purchases/library")
async def get_library(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    purchases = await db.purchases.find({"user_id": user["user_id"]}, {"_id": 0}).to_list(1000)
    
    # Get track and album details
    library = {"tracks": [], "albums": []}
    
    for purchase in purchases:
        if purchase["item_type"] == "track":
            track = await db.tracks.find_one({"track_id": purchase["item_id"]}, {"_id": 0})
            if track:
                try:
                    library["tracks"].append(_track_response(track, include_file=True).model_dump())
                except Exception:
                    library["tracks"].append(track)
        else:
            album = await db.albums.find_one({"album_id": purchase["item_id"]}, {"_id": 0})
            if album:
                library["albums"].append(album)
    
    return library

# ==================== FOLLOW ROUTES ====================

@api_router.post("/follows")
async def follow_artist(payload: FollowRequest, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    artist_id = payload.artist_id

    artist = await db.users.find_one({"user_id": artist_id, "role": "artist"}, {"_id": 1})
    if not artist:
        raise HTTPException(status_code=404, detail="Artist not found")

    existing = await db.follows.find_one({"user_id": user["user_id"], "artist_id": artist_id})
    if existing:
        return {"message": "Already following"}

    await db.follows.insert_one({
        "user_id": user["user_id"],
        "artist_id": artist_id,
        "created_at": datetime.now(timezone.utc).isoformat(),
    })

    await db.users.update_one({"user_id": artist_id, "role": "artist"}, {"$inc": {"followers_count": 1}})
    return {"message": "Following"}

@api_router.delete("/follows")
async def unfollow_artist(artist_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    result = await db.follows.delete_one({"user_id": user["user_id"], "artist_id": artist_id})
    if result.deleted_count:
        await db.users.update_one({"user_id": artist_id, "role": "artist"}, {"$inc": {"followers_count": -1}})
    return {"message": "Unfollowed"}

@api_router.get("/follows")
async def get_my_follows(limit: int = 200, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    follows = await db.follows.find({"user_id": user["user_id"]}, {"_id": 0}).limit(limit).to_list(limit)
    artist_ids = [f["artist_id"] for f in follows]
    artists = await db.users.find({"user_id": {"$in": artist_ids}, "role": "artist"}, {"_id": 0, "password_hash": 0}).to_list(len(artist_ids))
    by_id = {a["user_id"]: a for a in artists}
    ordered = [by_id[i] for i in artist_ids if i in by_id]
    await _apply_custom_avatars(ordered)
    return ordered

@api_router.get("/follows/state")
async def get_follow_state(artist_ids: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await _optional_user(authorization, request)
    if not user:
        return {}
    ids_list = [i.strip() for i in (artist_ids or "").split(",") if i.strip()]
    if not ids_list:
        return {}
    docs = await db.follows.find({"user_id": user["user_id"], "artist_id": {"$in": ids_list}}, {"_id": 0, "artist_id": 1}).to_list(len(ids_list))
    followed = {d["artist_id"] for d in docs}
    return {i: (i in followed) for i in ids_list}

# ==================== WEBHOOK ROUTES ====================

# Thin (Dashboard) : type `v1.checkout.session.completed` — pas l’objet complet dans le POST,
# on récupère la session via `fetch_related_object()` après vérification du secret thin.
THIN_CHECKOUT_SESSION_COMPLETED = "v1.checkout.session.completed"


async def fulfill_checkout_session_completed(sess) -> None:
    """Met à jour la transaction et crée l’achat si payé (snapshot ou session récupérée en thin)."""
    session_id = sess["id"]
    payment_status = sess.get("payment_status") or ""
    md = sess.get("metadata") or {}
    if md and not isinstance(md, dict):
        md = dict(md)

    base_update = {
        "payment_status": payment_status,
        "status": "complete" if payment_status == "paid" else sess.get("status", "open"),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }
    at = sess.get("amount_total")
    if at is not None:
        try:
            base_update["amount"] = float(at)
        except (TypeError, ValueError):
            pass

    # Ensure a row exists even if created elsewhere.
    await db.payment_transactions.update_one(
        {"session_id": session_id},
        {
            "$setOnInsert": {
                "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
                "session_id": session_id,
                "created_at": datetime.now(timezone.utc).isoformat(),
                "currency": os.environ.get("STRIPE_CURRENCY", "eur"),
                "metadata": {},
            },
            "$set": base_update,
        },
        upsert=True,
    )

    if payment_status == "paid":
        transaction = await db.payment_transactions.find_one({"session_id": session_id}, {"_id": 0})
        user_id = None
        item_type = None
        item_id = None
        price_paid = None
        artist_id_pub = None
        platform_fee_pub = None
        seller_net_pub = None
        if transaction:
            user_id = transaction["user_id"]
            item_type = transaction["metadata"]["item_type"]
            item_id = str(transaction["metadata"]["item_id"]).strip()
            price_paid = transaction["amount"]
            artist_id_pub = transaction.get("artist_id")
            platform_fee_pub = transaction.get("platform_fee_cents")
            seller_net_pub = transaction.get("seller_amount_cents")
        else:
            user_id = md.get("user_id")
            item_type = md.get("item_type")
            item_id = str(md.get("item_id") or "").strip()
            at = sess.get("amount_total")
            if at is not None:
                price_paid = float(at)
            else:
                price_paid = 0.0
            artist_id_pub = md.get("artist_id")

        if user_id and item_type and item_id:
            existing_purchase = await db.purchases.find_one(
                {
                    "user_id": user_id,
                    "item_type": item_type,
                    "item_id": item_id,
                }
            )
            if not existing_purchase:
                purchase_doc = {
                    "purchase_id": f"purchase_{uuid.uuid4().hex[:12]}",
                    "user_id": user_id,
                    "item_type": item_type,
                    "item_id": item_id,
                    "price_paid": price_paid,
                    "purchased_at": datetime.now(timezone.utc).isoformat(),
                    "artist_id": artist_id_pub,
                    "platform_fee_cents": platform_fee_pub,
                    "seller_net_cents": seller_net_pub,
                }
                await db.purchases.insert_one(purchase_doc)
                await _safe_notify(notify_item_acquired(
                    buyer_id=user_id,
                    item_type=item_type,
                    item_id=item_id,
                    amount_cents=price_paid or 0,
                ))

        if transaction:
            await _credit_artist_wallet_from_paid_transaction(transaction)

        await log_transaction_event(
            kind="checkout_completed",
            data={
                "session_id": session_id,
                "user_id": user_id,
                "item_type": item_type,
                "item_id": item_id,
                "artist_id": artist_id_pub,
                "amount_cents": int(round(float(price_paid or 0))),
                "currency": os.environ.get("STRIPE_CURRENCY", "eur"),
                "source": "webhook",
            },
        )


@api_router.post("/webhook/stripe")
async def stripe_webhook(request: Request):
    ensure_stripe_available()
    wh_secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
    if not wh_secret:
        raise HTTPException(
            status_code=503,
            detail="STRIPE_WEBHOOK_SECRET manquant — ajoute le secret du webhook (Dashboard Stripe → Webhooks).",
        )

    body_bytes = await request.body()
    signature = request.headers.get("Stripe-Signature")

    try:
        event = construct_webhook_event(body_bytes, signature)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except stripe.error.SignatureVerificationError as e:
        logger.warning("Stripe webhook signature: %s", e)
        raise HTTPException(status_code=400, detail="Signature Stripe invalide")

    if event["type"] == "checkout.session.completed":
        await fulfill_checkout_session_completed(event["data"]["object"])

    return {"received": True}


@api_router.post("/webhook/stripe/thin")
async def stripe_webhook_thin(request: Request):
    """
    Webhooks « thin » : notification légère signée avec STRIPE_WEBHOOK_SECRET_THIN.
    Pour un paiement Checkout, on récupère la session complète côté API puis même logique que le snapshot.
    """
    ensure_stripe_available()
    thin_secret = os.environ.get("STRIPE_WEBHOOK_SECRET_THIN", "").strip()
    if not thin_secret:
        raise HTTPException(
            status_code=503,
            detail="STRIPE_WEBHOOK_SECRET_THIN manquant — secret de la destination *thin* (Dashboard Stripe → Webhooks).",
        )

    body_bytes = await request.body()
    signature = request.headers.get("Stripe-Signature")

    try:
        event_notif = parse_thin_event_notification(body_bytes, signature)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except stripe.error.SignatureVerificationError as e:
        logger.warning("Stripe thin webhook signature: %s", e)
        raise HTTPException(status_code=400, detail="Signature Stripe invalide (thin)")

    if event_notif.type != THIN_CHECKOUT_SESSION_COMPLETED:
        logger.info("Stripe thin webhook ignoré (type=%s)", event_notif.type)
        return {"received": True, "handled": False}

    try:
        sess = await asyncio.to_thread(event_notif.fetch_related_object)
    except Exception as e:
        logger.exception("Stripe thin fetch_related_object: %s", e)
        raise HTTPException(status_code=500, detail="Impossible de récupérer la session Checkout")

    if sess is None:
        logger.warning("Stripe thin checkout.session.completed sans related_object")
        return {"received": True, "handled": False}

    await fulfill_checkout_session_completed(sess)
    return {"received": True, "handled": True}

# ==================== STRIPE CONNECT (ARTIST) ====================


def _balance_amount_for_currency(bal: Any, kind: str, currency: str) -> int:
    total = 0
    lst = getattr(bal, kind, None) or []
    cur = (currency or "").lower()
    for m in lst:
        c = getattr(m, "currency", None) or ""
        if str(c).lower() == cur:
            total += int(getattr(m, "amount", 0) or 0)
    return total


@api_router.get("/artist/stripe/connect/status")
async def artist_stripe_connect_status(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    if user.get("role") != "artist":
        raise HTTPException(status_code=403, detail="Réservé aux artistes")
    ensure_stripe_available()
    acct_id = user.get("stripe_connect_account_id")
    if not acct_id:
        return {
            "has_account": False,
            "charges_enabled": False,
            "details_submitted": False,
            "payouts_enabled": False,
            "platform_fee_percent": get_platform_fee_percent(),
            "connect_enabled": stripe_connect_enabled(),
        }
    try:
        acct = await asyncio.to_thread(retrieve_connect_account, acct_id)
    except stripe.error.StripeError as e:
        logger.warning("Stripe retrieve account: %s", e)
        raise HTTPException(status_code=502, detail=str(e))
    await db.users.update_one(
        {"user_id": user["user_id"]},
        {
            "$set": {
                "stripe_connect_charges_enabled": getattr(acct, "charges_enabled", False),
                "stripe_connect_details_submitted": getattr(acct, "details_submitted", False),
            }
        },
    )
    return {
        "has_account": True,
        "account_id": acct_id,
        "charges_enabled": getattr(acct, "charges_enabled", False),
        "details_submitted": getattr(acct, "details_submitted", False),
        "payouts_enabled": getattr(acct, "payouts_enabled", False),
        "platform_fee_percent": get_platform_fee_percent(),
        "connect_enabled": stripe_connect_enabled(),
    }


@api_router.post("/artist/stripe/connect/onboard")
async def artist_stripe_connect_onboard(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    if user.get("role") != "artist":
        raise HTTPException(status_code=403, detail="Réservé aux artistes")
    ensure_stripe_available()
    base = os.environ.get("PUBLIC_FRONTEND_URL", "https://localhost:3000").rstrip("/")
    refresh_url = f"{base}/artist-dashboard?connect=refresh"
    return_url = f"{base}/artist-dashboard?connect=return"

    acct_id = user.get("stripe_connect_account_id")
    try:
        if not acct_id:

            def _mk_acct():
                return create_express_connected_account(email=user.get("email"), country="FR")

            acct = await asyncio.to_thread(_mk_acct)
            acct_id = acct.id
            await db.users.update_one(
                {"user_id": user["user_id"]},
                {"$set": {"stripe_connect_account_id": acct_id}},
            )

        def _mk_link():
            return create_account_onboarding_link(
                account_id=acct_id,
                refresh_url=refresh_url,
                return_url=return_url,
            )

        link_url = await asyncio.to_thread(_mk_link)
    except stripe.error.StripeError as e:
        msg = getattr(e, "user_message", None) or str(e)
        raise HTTPException(status_code=502, detail=f"Stripe: {msg}")
    return {"url": link_url}


@api_router.post("/artist/stripe/connect/login-link")
async def artist_stripe_connect_login_link(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    if user.get("role") != "artist":
        raise HTTPException(status_code=403, detail="Réservé aux artistes")
    ensure_stripe_available()
    acct_id = user.get("stripe_connect_account_id")
    if not acct_id:
        raise HTTPException(status_code=400, detail="Compte Stripe Connect non créé")
    try:
        url = await asyncio.to_thread(create_account_login_link, acct_id)
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    return {"url": url}


@api_router.get("/artist/stripe/connect/balance")
async def artist_stripe_connect_balance(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    if user.get("role") != "artist":
        raise HTTPException(status_code=403, detail="Réservé aux artistes")
    ensure_stripe_available()
    acct_id = user.get("stripe_connect_account_id")
    if not acct_id:
        raise HTTPException(status_code=400, detail="Compte Stripe Connect non créé")
    try:
        bal = await asyncio.to_thread(retrieve_connect_balance, acct_id)
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    cur = STRIPE_CURRENCY_ENV
    available = _balance_amount_for_currency(bal, "available", cur)
    pending = _balance_amount_for_currency(bal, "pending", cur)
    min_cents = get_min_payout_cents()
    return {
        "currency": cur,
        "available_cents": available,
        "pending_cents": pending,
        "min_payout_hint_euros": round(min_cents / 100.0, 2),
    }


@api_router.get("/artist/wallet")
async def artist_wallet(authorization: Optional[str] = Header(None), request: Request = None):
    """Cagnotte Kloud : ventes encaissées sur la plateforme, à retirer via Stripe Connect."""
    user = await get_current_user(authorization, request)
    if user.get("role") != "artist":
        raise HTTPException(status_code=403, detail="Réservé aux artistes")

    available = _as_int_cents(user.get("wallet_available_cents"), 0)
    paid_out = _as_int_cents(user.get("wallet_paid_out_cents"), 0)
    min_cents = get_min_payout_cents()
    acct_id = user.get("stripe_connect_account_id")
    has_account = bool(acct_id)
    charges_enabled = False
    payouts_enabled = False
    details_submitted = False
    connect_ready = False

    if has_account and stripe_configured():
        try:
            acct = await asyncio.to_thread(retrieve_connect_account, acct_id)
            charges_enabled = bool(getattr(acct, "charges_enabled", False))
            payouts_enabled = bool(getattr(acct, "payouts_enabled", False))
            details_submitted = bool(getattr(acct, "details_submitted", False))
            connect_ready = _connect_account_can_receive(acct)
        except stripe.error.StripeError as e:
            logger.warning("wallet connect status: %s", e)

    blocked = None
    can_withdraw = False
    if available < min_cents:
        blocked = "below_minimum"
    elif not has_account or not connect_ready:
        blocked = "connect_required"
    else:
        can_withdraw = True

    return {
        "currency": STRIPE_CURRENCY_ENV,
        "available_cents": available,
        "paid_out_cents": paid_out,
        "min_payout_cents": min_cents,
        "min_payout_euros": round(min_cents / 100.0, 2),
        "platform_fee_percent": get_platform_fee_percent(),
        "can_withdraw": can_withdraw,
        "withdraw_blocked_reason": blocked,
        "has_account": has_account,
        "charges_enabled": charges_enabled,
        "payouts_enabled": payouts_enabled,
        "details_submitted": details_submitted,
        "connect_ready": connect_ready,
        "connect_enabled": stripe_connect_enabled(),
    }


@api_router.post("/artist/wallet/payout")
async def artist_wallet_payout(authorization: Optional[str] = Header(None), request: Request = None):
    """Retire toute la cagnotte vers Stripe Connect (seuil minimum)."""
    user = await get_current_user(authorization, request)
    if user.get("role") != "artist":
        raise HTTPException(status_code=403, detail="Réservé aux artistes")
    ensure_stripe_available()
    if not stripe_connect_enabled():
        raise HTTPException(status_code=400, detail="Stripe Connect est désactivé")

    min_cents = get_min_payout_cents()
    available = _as_int_cents(user.get("wallet_available_cents"), 0)
    if available < min_cents:
        raise HTTPException(
            status_code=400,
            detail=f"Retrait possible à partir de {min_cents / 100:.0f} € (cagnotte actuelle : {available / 100:.2f} €).",
        )

    acct_id = user.get("stripe_connect_account_id")
    if not acct_id:
        raise HTTPException(
            status_code=400,
            detail="Active Stripe Connect pour retirer ta cagnotte.",
        )
    try:
        acct = await asyncio.to_thread(retrieve_connect_account, acct_id)
    except stripe.error.StripeError as e:
        raise HTTPException(status_code=502, detail=str(e))
    if not _connect_account_can_receive(acct):
        raise HTTPException(
            status_code=400,
            detail="Termine l'activation Stripe Connect pour retirer ta cagnotte.",
        )

    amount = available
    payout_id = f"payout_{uuid.uuid4().hex[:12]}"
    now = datetime.now(timezone.utc).isoformat()
    claimed = await db.users.find_one_and_update(
        {"user_id": user["user_id"], "wallet_available_cents": amount},
        {
            "$inc": {
                "wallet_available_cents": -amount,
                "wallet_paid_out_cents": amount,
            }
        },
        return_document=ReturnDocument.AFTER,
    )
    if not claimed:
        raise HTTPException(status_code=409, detail="Cagnotte mise à jour pendant le retrait — réessaie.")

    await db.artist_payouts.insert_one(
        {
            "payout_id": payout_id,
            "artist_id": user["user_id"],
            "amount_cents": amount,
            "currency": STRIPE_CURRENCY_ENV,
            "status": "pending",
            "created_at": now,
        }
    )

    try:
        transfer = await asyncio.to_thread(
            create_connect_transfer,
            amount_cents=amount,
            currency=STRIPE_CURRENCY_ENV,
            destination=acct_id,
            transfer_id=payout_id,
            metadata={
                "artist_id": str(user["user_id"]),
                "payout_id": payout_id,
            },
        )
    except stripe.error.StripeError as e:
        await db.users.update_one(
            {"user_id": user["user_id"]},
            {
                "$inc": {
                    "wallet_available_cents": amount,
                    "wallet_paid_out_cents": -amount,
                }
            },
        )
        await db.artist_payouts.update_one(
            {"payout_id": payout_id},
            {
                "$set": {
                    "status": "failed",
                    "error": str(e),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            },
        )
        msg = getattr(e, "user_message", None) or getattr(e, "message", None) or str(e)
        raise HTTPException(status_code=502, detail=f"Retrait Stripe impossible : {msg}")

    transfer_id = getattr(transfer, "id", None)
    await db.artist_payouts.update_one(
        {"payout_id": payout_id},
        {
            "$set": {
                "status": "transferred",
                "stripe_transfer_id": transfer_id,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
        },
    )
    await db.wallet_ledger.insert_one(
        {
            "entry_id": f"wle_{uuid.uuid4().hex[:12]}",
            "artist_id": user["user_id"],
            "kind": "payout_debit",
            "amount_cents": -amount,
            "payout_id": payout_id,
            "stripe_transfer_id": transfer_id,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    await log_transaction_event(
        kind="artist_wallet_payout",
        data={
            "payout_id": payout_id,
            "artist_id": user["user_id"],
            "amount_cents": amount,
            "currency": STRIPE_CURRENCY_ENV,
            "stripe_transfer_id": transfer_id,
        },
    )
    return {
        "payout_id": payout_id,
        "amount_cents": amount,
        "currency": STRIPE_CURRENCY_ENV,
        "stripe_transfer_id": transfer_id,
        "available_cents": _as_int_cents(claimed.get("wallet_available_cents"), 0),
        "paid_out_cents": _as_int_cents(claimed.get("wallet_paid_out_cents"), 0),
    }


# ==================== ARTIST STATS ROUTES ====================

@api_router.get("/artist/stats")
async def get_artist_stats(authorization: Optional[str] = Header(None), request: Request = None):
    """Get comprehensive stats for an artist (écoutes, ventes payantes, ajouts biblio, likes)."""
    user = await get_current_user(authorization, request)

    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can view stats")

    artist_id = user["user_id"]

    tracks = await db.tracks.find({"artist_id": artist_id}, {"_id": 0}).to_list(1000)
    albums = await db.albums.find({"artist_id": artist_id}, {"_id": 0}).to_list(500)
    track_ids = [t["track_id"] for t in tracks]
    album_ids = [a["album_id"] for a in albums]

    purchase_conds = []
    if track_ids:
        purchase_conds.append({"item_type": "track", "item_id": {"$in": track_ids}})
    if album_ids:
        purchase_conds.append({"item_type": "album", "item_id": {"$in": album_ids}})
    purchases: List[dict] = []
    if purchase_conds:
        purchases = await db.purchases.find({"$or": purchase_conds}, {"_id": 0}).to_list(10000)

    plays_collection: List[dict] = []
    if track_ids:
        plays_collection = await db.plays.find({"track_id": {"$in": track_ids}}, {"_id": 0}).to_list(100000)

    total_tracks = len(tracks)
    published_tracks = len([t for t in tracks if t.get("status") == "published"])
    draft_tracks = total_tracks - published_tracks

    paid_purchases = [p for p in purchases if purchase_amount_cents(p) > 0]
    library_purchases = [p for p in purchases if purchase_amount_cents(p) == 0]
    total_sales = len(paid_purchases)
    total_library_adds = len(library_purchases)
    total_revenue = sum(purchase_seller_net_cents(p) for p in paid_purchases)
    total_likes_tracks = sum(int(t.get("likes_count") or 0) for t in tracks)
    total_likes_albums = sum(int(a.get("likes_count") or 0) for a in albums)

    track_stats = []
    for track in tracks:
        tid = track["track_id"]
        t_purchases = [p for p in purchases if p.get("item_type") == "track" and p["item_id"] == tid]
        paid_tp = [p for p in t_purchases if purchase_amount_cents(p) > 0]
        free_tp = [p for p in t_purchases if purchase_amount_cents(p) == 0]
        track_plays = [p for p in plays_collection if p.get("track_id") == tid]
        play_n = max(len(track_plays), int(track.get("play_count") or 0))
        duration_sec = _track_duration_sec(track)
        track_stats.append({
            "track_id": tid,
            "title": track["title"],
            "cover_url": track.get("cover_url"),
            "genre": track.get("genre"),
            "price": track.get("price") if track.get("price") is not None else 0,
            "is_free_price": bool(track.get("is_free_price")),
            "min_price": track.get("min_price") if track.get("is_free_price") else None,
            "status": track.get("status", "draft"),
            "duration": duration_sec,
            "duration_sec": duration_sec,
            "file_url": track.get("file_url") or "",
            "preview_url": track.get("preview_url") or "",
            "sales_count": len(paid_tp),
            "library_adds_count": len(free_tp),
            "revenue": sum(purchase_seller_net_cents(p) for p in paid_tp),
            "play_count": play_n,
            "play_duration_sec": sum(p.get("duration_sec", 15) for p in track_plays),
            "likes_count": int(track.get("likes_count") or 0),
            "created_at": track.get("created_at"),
        })

    track_stats.sort(key=lambda x: x["play_count"], reverse=True)
    top_tracks = track_stats[:5]

    album_stats = []
    for album in albums:
        aid = album["album_id"]
        a_purchases = [p for p in purchases if p.get("item_type") == "album" and p["item_id"] == aid]
        paid_ap = [p for p in a_purchases if purchase_amount_cents(p) > 0]
        free_ap = [p for p in a_purchases if purchase_amount_cents(p) == 0]
        linked_ids = [tid for tid in (album.get("track_ids") or []) if tid]
        tr_in_album = [
            t for t in tracks
            if t.get("album_id") == aid or t.get("track_id") in set(linked_ids)
        ]
        play_sum = 0
        for t in tr_in_album:
            play_sum += len([p for p in plays_collection if p.get("track_id") == t["track_id"]])
        duration_sec = sum(_track_duration_sec(t) or 0 for t in tr_in_album) or None
        track_ids = _merge_album_track_ids(album, tracks)
        album_stats.append({
            "album_id": aid,
            "title": album["title"],
            "cover_url": album.get("cover_url"),
            "price": album.get("price") if album.get("price") is not None else 0,
            "is_free_price": bool(album.get("is_free_price")),
            "min_price": album.get("min_price") if album.get("is_free_price") else None,
            "track_ids": track_ids,
            "track_count": len(track_ids),
            "duration": duration_sec,
            "duration_sec": duration_sec,
            "sales_count": len(paid_ap),
            "library_adds_count": len(free_ap),
            "revenue": sum(purchase_seller_net_cents(p) for p in paid_ap),
            "play_count": play_sum,
            "likes_count": int(album.get("likes_count") or 0),
            "status": album.get("status", "draft"),
            "created_at": album.get("created_at"),
        })

    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    def parse_date(date_str):
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        except Exception:
            return None

    recent_purchases = [
        p for p in purchases
        if parse_date(p.get("purchased_at")) and parse_date(p.get("purchased_at")) > week_ago
    ]
    monthly_purchases = [
        p for p in purchases
        if parse_date(p.get("purchased_at")) and parse_date(p.get("purchased_at")) > month_ago
    ]
    recent_paid = [p for p in recent_purchases if purchase_amount_cents(p) > 0]
    monthly_paid = [p for p in monthly_purchases if purchase_amount_cents(p) > 0]

    total_play_duration = sum(p.get("duration_sec", 15) for p in plays_collection)

    return {
        "overview": {
            "total_tracks": total_tracks,
            "published_tracks": published_tracks,
            "draft_tracks": draft_tracks,
            "total_sales": total_sales,
            "total_library_adds": total_library_adds,
            "total_revenue": total_revenue,
            "total_play_count": len(plays_collection),
            "total_play_duration_sec": total_play_duration,
            "total_play_duration_hours": round(total_play_duration / 3600, 1),
            "total_likes_tracks": total_likes_tracks,
            "total_likes_albums": total_likes_albums,
        },
        "period_stats": {
            "last_7_days": {
                "sales": len(recent_paid),
                "library_adds": len([p for p in recent_purchases if purchase_amount_cents(p) == 0]),
                "revenue": sum(purchase_seller_net_cents(p) for p in recent_paid),
            },
            "last_30_days": {
                "sales": len(monthly_paid),
                "library_adds": len([p for p in monthly_purchases if purchase_amount_cents(p) == 0]),
                "revenue": sum(purchase_seller_net_cents(p) for p in monthly_paid),
            },
        },
        "track_stats": track_stats,
        "album_stats": album_stats,
        "top_tracks": top_tracks,
    }

@api_router.get("/artist/tracks")
async def get_artist_tracks(authorization: Optional[str] = Header(None), request: Request = None):
    """Get all tracks for the logged-in artist"""
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can view their tracks")
    
    tracks = await db.tracks.find({"artist_id": user["user_id"]}, {"_id": 0}).to_list(1000)
    return tracks

@api_router.put("/artist/tracks/{track_id}/publish")
async def publish_track(track_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Toggle track publish status"""
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can publish tracks")
    
    track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")
    
    if track["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    new_status = "published" if track.get("status") == "draft" else "draft"
    if new_status == "published":
        _ensure_published_has_audio({**track, "status": "published"})
    
    await db.tracks.update_one(
        {"track_id": track_id},
        {"$set": {"status": new_status, "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    await _safe_notify(maybe_notify_published("track", track.get("status"), {**track, "status": new_status}))
    return {"status": new_status, "message": f"Track {'published' if new_status == 'published' else 'unpublished'} successfully"}

@api_router.put("/artist/albums/{album_id}/publish")
async def publish_album(album_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    """Toggle album publish status"""
    user = await get_current_user(authorization, request)

    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can publish albums")

    album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if not album:
        raise HTTPException(status_code=404, detail="Album not found")

    if album["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")

    new_status = "published" if album.get("status") == "draft" else "draft"
    if new_status == "published":
        await _ensure_album_can_publish(album)

    await db.albums.update_one(
        {"album_id": album_id},
        {"$set": {"status": new_status, "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    await _sync_album_tracks_status(album, status=new_status)
    await _safe_notify(maybe_notify_published("album", album.get("status"), {**album, "status": new_status}))
    return {"status": new_status, "message": f"Album {'published' if new_status == 'published' else 'unpublished'} successfully"}

@api_router.post("/plays")
async def record_play(
    request: Request,
    track_id: str = Query(..., min_length=3),
    duration_sec: int = Query(15, ge=1, le=7200),
    authorization: Optional[str] = Header(None),
):
    """Enregistre une écoute (aperçu ou morceau complet) pour les stats artiste."""
    track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0, "track_id": 1})
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")

    play_doc = {
        "play_id": f"play_{uuid.uuid4().hex[:12]}",
        "track_id": track_id,
        "duration_sec": duration_sec,
        "played_at": datetime.now(timezone.utc).isoformat(),
        "user_id": None,
    }

    try:
        user = await get_current_user(authorization, request)
        play_doc["user_id"] = user["user_id"]
    except Exception:
        pass

    await db.plays.insert_one(play_doc)
    await db.tracks.update_one({"track_id": track_id}, {"$inc": {"play_count": 1}})
    return {"status": "recorded"}

# ==================== SEARCH ROUTES ====================

@api_router.get("/search")
async def search(q: str, types: str = "tracks,albums,artists", limit: int = 20):
    """Simple search across published tracks, albums, and artists."""
    query = (q or "").strip()
    if not query:
        return {"tracks": [], "albums": [], "artists": []}

    type_set = {t.strip().lower() for t in types.split(",") if t.strip()}
    regex = {"$regex": query, "$options": "i"}

    results = {"tracks": [], "albums": [], "artists": []}

    published = {"status": {"$nin": ["draft"]}}

    if "tracks" in type_set:
        track_query = await _catalog_track_list_query(
            {"$or": [{"title": regex}, {"artist_name": regex}, {"genre": regex}]}
        )
        tracks = await db.tracks.find(track_query, {"_id": 0}).limit(limit).to_list(limit)
        results["tracks"] = [_public_track_doc(track) for track in tracks]

    if "albums" in type_set:
        albums = await db.albums.find(
            {**published, "$or": [{"title": regex}, {"artist_name": regex}, {"genre": regex}]},
            {"_id": 0},
        ).limit(limit).to_list(limit)
        results["albums"] = albums

    if "artists" in type_set:
        artists = await db.users.find(
            {"role": "artist", "$or": [{"name": regex}, {"artist_name": regex}]},
            {"_id": 0, "password_hash": 0},
        ).limit(limit).to_list(limit)
        results["artists"] = artists
        await _apply_custom_avatars(results["artists"])

    return results

@app.get("/health")
async def health_check():
    """Ping public (prod / navigateur), sans JWT."""
    return {
        "status": "ok",
        "mail_configured": smtp_configured(),
        "mail_host": SMTP_HOST or "",
        "mail_from": SMTP_FROM_EMAIL or "",
        "mail_port": SMTP_PORT,
    }

# Include router
app.include_router(api_router)

# Dynamic CORS setup for credentials
async def add_cors_headers(request: Request, call_next):
    response = await call_next(request)
    origin = request.headers.get('origin')
    if origin:
        response.headers['Access-Control-Allow-Origin'] = origin
        response.headers['Access-Control-Allow-Credentials'] = 'true'
        response.headers['Access-Control-Allow-Methods'] = '*'
        response.headers['Access-Control-Allow-Headers'] = '*'
        response.headers['Access-Control-Expose-Headers'] = '*'
    return response

app.middleware('http')(add_cors_headers)

app.add_middleware(
    CORSMiddleware,
    allow_credentials=True,
    allow_origin_regex=r".*",
    allow_methods=["*"],
    allow_headers=["*"],
    expose_headers=["*"]
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def _backfill_audio_previews():
    try:
        cursor = db.tracks.find(
            {},
            {"_id": 0, "track_id": 1, "file_url": 1, "preview_url": 1, "artist_id": 1, "preview_start_time": 1, "preview_duration": 1},
        )
        fixed = 0
        async for track in cursor:
            file_url = track.get("file_url") or ""
            preview_url = track.get("preview_url") or ""
            if not file_url or (preview_url and preview_url != file_url):
                continue
            generated = await _preview_url_from_file_url(
                file_url,
                user_id=track.get("artist_id") or "",
                start_sec=int(track.get("preview_start_time") or 0),
                duration_sec=int(track.get("preview_duration") or 15),
            )
            if generated:
                await db.tracks.update_one({"track_id": track["track_id"]}, {"$set": {"preview_url": generated}})
                fixed += 1
        if fixed:
            logging.getLogger(__name__).info("Generated distinct previews for %s existing track(s)", fixed)
    except Exception:
        logging.getLogger(__name__).exception("Audio preview backfill skipped")


@app.on_event("startup")
async def sync_admin_emails_on_startup():
    wanted = admin_emails()
    logging.getLogger(__name__).info("ADMIN_EMAILS: %s", ", ".join(sorted(wanted)) or "(none)")
    logging.getLogger(__name__).info(
        "password-reset mail configured=%s from=%s host=%s",
        smtp_configured(),
        SMTP_FROM_EMAIL or "-",
        SMTP_HOST or "-",
    )
    asyncio.create_task(_backfill_audio_previews())
    if not wanted:
        return
    try:
        granted = 0
        revoked = 0
        cursor = db.users.find({}, {"_id": 0, "user_id": 1, "email": 1, "is_admin": 1})
        async for user in cursor:
            email = user.get("email") or ""
            if is_unlisted_gmail_alias_of_admin(email) and user.get("is_admin"):
                await db.users.update_one({"user_id": user["user_id"]}, {"$set": {"is_admin": False}})
                revoked += 1
                continue
            if email_is_listed_admin(email) and not user.get("is_admin"):
                await db.users.update_one({"user_id": user["user_id"]}, {"$set": {"is_admin": True}})
                granted += 1
        if granted:
            logging.getLogger(__name__).info("Granted is_admin to %s existing user(s)", granted)
        if revoked:
            logging.getLogger(__name__).info("Revoked is_admin from %s Gmail-alias user(s)", revoked)
    except Exception:
        logging.getLogger(__name__).exception(
            "Admin email sync skipped (Mongo unavailable). App still starts."
        )


@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
