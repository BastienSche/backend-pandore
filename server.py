from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Request, Header, Response, Query
from fastapi.responses import StreamingResponse, RedirectResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorGridFSBucket
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr, ConfigDict
from typing import Any, List, Optional, Literal
import uuid
from datetime import datetime, timezone, timedelta
import bcrypt
import jwt
import shutil
import asyncio
from urllib.parse import urlencode
from bson import ObjectId

import stripe
from stripe_payments import (
    compute_platform_fee_cents,
    create_account_login_link,
    create_account_onboarding_link,
    create_checkout_session,
    create_express_connected_account,
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
_primary = ROOT_DIR / f".env.{_app_env}"
if _primary.exists():
    load_dotenv(_primary)
_legacy = ROOT_DIR / ".env"
if _legacy.exists():
    load_dotenv(_legacy, override=True)

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]
fs = AsyncIOMotorGridFSBucket(db, bucket_name="uploads")

# JWT Configuration
JWT_SECRET = os.environ.get('JWT_SECRET', 'pandore_secret_key_change_in_production_2025')
JWT_ALGORITHM = os.environ.get('JWT_ALGORITHM', 'HS256')

# Stripe Configuration
STRIPE_API_KEY = os.environ.get('STRIPE_API_KEY')
STRIPE_CURRENCY_ENV = os.environ.get('STRIPE_CURRENCY', 'eur').lower()

# Google OAuth Configuration
GOOGLE_OAUTH_CLIENT_ID = os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = os.environ.get("GOOGLE_OAUTH_REDIRECT_URI")
FRONTEND_OAUTH_REDIRECT_URL = os.environ.get("FRONTEND_OAUTH_REDIRECT_URL")

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
    """Part vendeur après commission plateforme (Connect). Sinon fallback sur le prix payé (legacy)."""
    v = p.get("seller_net_cents")
    if v is not None:
        try:
            return float(v)
        except (TypeError, ValueError):
            pass
    return purchase_amount_cents(p)


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
    password: str
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

class TrackCreate(BaseModel):
    title: str
    price: float  # Prix en cents
    is_free_price: bool = False
    min_price: Optional[float] = None  # Minimum en cents (si prix libre)
    genre: str
    description: Optional[str] = None
    album_id: Optional[str] = None
    preview_start_time: int = 0
    duration_sec: Optional[int] = None
    mastering: Optional[dict] = None  # {"engineer": "name", "details": "info"}
    splits: Optional[List[dict]] = None  # [{"party": "name", "percent": 50}]
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
    cover_url: Optional[str]
    genre: str
    description: Optional[str]
    mastering: Optional[dict]
    splits: Optional[List[dict]]
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
    duration_sec: Optional[int] = None
    mastering: Optional[dict] = None
    splits: Optional[List[dict]] = None
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
    cover_url: Optional[str]
    genre: str
    description: Optional[str]
    mastering: Optional[dict]
    splits: Optional[List[dict]]
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
    name: str
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
    privacy_share_listening_activity: bool = False
    privacy_personalized_ads: bool = False

# ==================== AUTH HELPERS ====================

def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

def verify_password(password: str, hashed: str) -> bool:
    return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))

def create_jwt_token(user_id: str) -> str:
    payload = {
        "user_id": user_id,
        "exp": datetime.now(timezone.utc) + timedelta(days=7)
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGORITHM)

def ensure_google_oauth_configured():
    if not GOOGLE_OAUTH_CLIENT_ID or not GOOGLE_OAUTH_CLIENT_SECRET or not GOOGLE_OAUTH_REDIRECT_URI:
        raise HTTPException(status_code=500, detail="Google OAuth is not configured")

def validate_oauth_state(request: Request, state: Optional[str]):
    expected_state = request.cookies.get("oauth_state") if request else None
    if expected_state:
        if not state or state != expected_state:
            raise HTTPException(status_code=400, detail="Invalid OAuth state")

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
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    # Check if it's a JWT token (custom auth)
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        user_id = payload.get("user_id")
        if not user_id:
            raise HTTPException(status_code=401, detail="Invalid token")
        
        user_doc = await db.users.find_one({"user_id": user_id}, {"_id": 0})
        if not user_doc:
            raise HTTPException(status_code=401, detail="User not found")
        return user_doc
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expired")
    except jwt.InvalidTokenError:
        pass
    
    # Check if it's a Google OAuth session token
    session_doc = await db.user_sessions.find_one({"session_token": token}, {"_id": 0})
    if not session_doc:
        raise HTTPException(status_code=401, detail="Invalid session")
    
    # Check expiry
    expires_at = session_doc["expires_at"]
    if isinstance(expires_at, str):
        expires_at = datetime.fromisoformat(expires_at)
    if expires_at.tzinfo is None:
        expires_at = expires_at.replace(tzinfo=timezone.utc)
    if expires_at < datetime.now(timezone.utc):
        raise HTTPException(status_code=401, detail="Session expired")
    
    user_doc = await db.users.find_one({"user_id": session_doc["user_id"]}, {"_id": 0})
    if not user_doc:
        raise HTTPException(status_code=401, detail="User not found")
    
    return user_doc

# ==================== AUTH ROUTES ====================

@api_router.post("/auth/register", response_model=UserResponse)
async def register(user_data: UserRegister):
    # Check if user exists
    existing = await db.users.find_one({"email": user_data.email}, {"_id": 0})
    if existing:
        raise HTTPException(status_code=400, detail="Email already registered")
    
    user_id = f"user_{uuid.uuid4().hex[:12]}"
    hashed_pwd = hash_password(user_data.password)
    
    user_doc = {
        "user_id": user_id,
        "email": user_data.email,
        "password_hash": hashed_pwd,
        "name": user_data.name,
        "picture": None,
        "role": "artist" if user_data.artist_name else "user",
        "artist_name": user_data.artist_name,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.users.insert_one(user_doc)
    return UserResponse(**user_doc)

@api_router.post("/auth/login")
async def login(credentials: UserLogin, response: Response):
    user_doc = await db.users.find_one({"email": credentials.email}, {"_id": 0})
    if not user_doc or not verify_password(credentials.password, user_doc["password_hash"]):
        raise HTTPException(status_code=401, detail="Invalid credentials")
    
    token = create_jwt_token(user_doc["user_id"])
    
    # Set cookie
    response.set_cookie(
        key="session_token",
        value=token,
        httponly=False,
        secure=False,
        samesite="lax",
        path="/",
        max_age=7*24*60*60
    )
    
    return {
        "token": token,
        "user": UserResponse(**user_doc)
    }

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
    user_doc = await db.users.find_one({"email": user_info["email"]}, {"_id": 0})
    if not user_doc:
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        user_doc = {
            "user_id": user_id,
            "email": user_info["email"],
            "password_hash": None,
            "name": user_info.get("name") or user_info.get("given_name") or "",
            "picture": user_info.get("picture"),
            "role": "user",
            "artist_name": None,
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        await db.users.insert_one(user_doc)
    else:
        user_id = user_doc["user_id"]
        await db.users.update_one(
            {"user_id": user_id},
            {"$set": {
                "name": user_info.get("name") or user_info.get("given_name") or user_doc.get("name"),
                "picture": user_info.get("picture"),
            }},
        )
    user_doc["user_id"] = user_id
    return user_doc

async def create_google_session(user_id: str) -> str:
    session_token = uuid.uuid4().hex
    session_doc = {
        "user_id": user_id,
        "session_token": session_token,
        "expires_at": datetime.now(timezone.utc) + timedelta(days=7),
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    await db.user_sessions.delete_many({"user_id": user_id})
    await db.user_sessions.insert_one(session_doc)
    return session_token

@api_router.get("/auth/google/callback")
async def google_callback_redirect(
    request: Request,
    response: Response,
    code: Optional[str] = None,
    state: Optional[str] = None,
    error: Optional[str] = None,
):
    """Handle Google redirect and then redirect back to frontend"""
    if error:
        raise HTTPException(status_code=401, detail=error)
    if not code:
        raise HTTPException(status_code=400, detail="Missing code")

    validate_oauth_state(request, state)
    user_info = await exchange_google_code_for_user(code)
    user_doc = await upsert_google_user(user_info)
    session_token = await create_google_session(user_doc["user_id"])

    redirect_url = FRONTEND_OAUTH_REDIRECT_URL
    if redirect_url:
        response = RedirectResponse(url=redirect_url)
        response.set_cookie(
            key="session_token",
            value=session_token,
            httponly=False,
            secure=False,
            samesite="lax",
            path="/",
            max_age=7 * 24 * 60 * 60,
        )
        response.delete_cookie(key="oauth_state", path="/")
        return response

    response.set_cookie(
        key="session_token",
        value=session_token,
        httponly=False,
        secure=False,
        samesite="lax",
        path="/",
        max_age=7 * 24 * 60 * 60,
    )
    response.delete_cookie(key="oauth_state", path="/")
    return GoogleSessionResponse(
        user_id=user_doc["user_id"],
        email=user_doc["email"],
        name=user_doc["name"],
        picture=user_doc.get("picture"),
        session_token=session_token,
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
    session_token = await create_google_session(user_doc["user_id"])
    
    # Set cookie
    response.set_cookie(
        key="session_token",
        value=session_token,
        httponly=False,
        secure=False,
        samesite="lax",
        path="/",
        max_age=7*24*60*60
    )
    
    response.delete_cookie(key="oauth_state", path="/")
    return GoogleSessionResponse(
        user_id=user_doc["user_id"],
        email=user_doc["email"],
        name=user_doc["name"],
        picture=user_doc.get("picture"),
        session_token=session_token,
    )

@api_router.get("/auth/me", response_model=UserResponse)
async def get_me(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    return UserResponse(**user)

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

@api_router.post("/auth/logout")
async def logout(response: Response, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    # Delete all sessions for this user
    await db.user_sessions.delete_many({"user_id": user["user_id"]})
    
    # Clear cookie
    response.delete_cookie(key="session_token", path="/")
    
    return {"message": "Logged out successfully"}

@api_router.put("/auth/role")
async def update_role(new_role: str, artist_name: Optional[str] = None, authorization: Optional[str] = Header(None), request: Request = None):
    """Toggle between user and artist role"""
    user = await get_current_user(authorization, request)
    
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
            "artist_name": artist_name if new_role == "artist" else user.get("artist_name")
        }}
    )
    
    return {"role": new_role, "artist_name": artist_name}

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
    
    update_data = profile_data.model_dump()
    update_data["updated_at"] = datetime.now(timezone.utc).isoformat()
    
    await db.artist_profiles.update_one(
        {"user_id": user["user_id"]},
        {"$set": update_data}
    )
    
    profile = await db.artist_profiles.find_one({"user_id": user["user_id"]}, {"_id": 0})
    return ArtistProfileResponse(**profile)

# ==================== UPLOAD ROUTES ====================

@api_router.post("/upload/audio")
async def upload_audio(file: UploadFile = File(...), authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can upload audio")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

    file_id = await fs.upload_from_stream(
        filename=file.filename or f"{uuid.uuid4().hex}.audio",
        source=content,
        metadata={
            "kind": "audio",
            "content_type": file.content_type or "application/octet-stream",
            "uploader_user_id": user["user_id"],
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        },
    )

    return {"file_url": f"/api/files/audio/{str(file_id)}"}

@api_router.post("/upload/cover")
async def upload_cover(file: UploadFile = File(...), authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can upload covers")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Empty file")

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

@api_router.get("/files/audio/{file_id}")
async def get_audio_file(file_id: str, request: Request):
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

    # Support range requests for audio preview/streaming
    range_header = request.headers.get("range")
    if range_header:
        range_match = range_header.replace("bytes=", "").split("-")
        start = int(range_match[0])
        end = int(range_match[1]) if range_match[1] else file_size - 1

        if file_size <= 0:
            raise HTTPException(status_code=416, detail="Invalid range")
        if start < 0 or end < start or end >= file_size:
            raise HTTPException(status_code=416, detail="Requested Range Not Satisfiable")

        async def iterfile():
            # Try to seek efficiently if supported; otherwise discard bytes.
            remaining = end - start + 1
            try:
                await grid_out.seek(start)  # type: ignore[attr-defined]
            except Exception:
                to_discard = start
                while to_discard > 0:
                    chunk = await grid_out.read(min(8192, to_discard))
                    if not chunk:
                        break
                    to_discard -= len(chunk)

            while remaining > 0:
                data = await grid_out.read(min(8192, remaining))
                if not data:
                    break
                remaining -= len(data)
                yield data

        return StreamingResponse(
            iterfile(),
            status_code=206,
            headers={
                "Content-Range": f"bytes {start}-{end}/{file_size}",
                "Accept-Ranges": "bytes",
                "Content-Length": str(end - start + 1),
                "Content-Type": content_type,
            }
        )

    async def iterfile_full():
        while True:
            data = await grid_out.read(8192)
            if not data:
                break
            yield data

    return StreamingResponse(
        iterfile_full(),
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(file_size),
            "Content-Type": content_type,
        },
    )

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
        "preview_duration": 15,
        "file_url": "",
        "cover_url": None,
        "genre": track_data.genre,
        "description": track_data.description,
        "mastering": track_data.mastering,
        "splits": track_data.splits or [],
        "status": track_data.status,
        "likes_count": 0,
        "play_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.tracks.insert_one(track_doc)
    return TrackResponse(**track_doc)

@api_router.get("/tracks", response_model=List[TrackResponse])
async def get_tracks(limit: int = 50, skip: int = 0, genre: Optional[str] = None, status: Optional[str] = None, authorization: Optional[str] = Header(None), request: Request = None):
    # Get current user if authenticated
    try:
        user = await get_current_user(authorization, request) if authorization else None
    except:
        user = None
    
    query = {}
    if genre:
        query["genre"] = genre
    
    # Only show published tracks unless user is viewing their own tracks
    if status:
        query["status"] = status
    elif not user:
        query["status"] = "published"
    
    tracks = await db.tracks.find(query, {"_id": 0}).skip(skip).limit(limit).to_list(limit)
    return [TrackResponse(**track) for track in tracks]

@api_router.get("/tracks/{track_id}", response_model=TrackResponse)
async def get_track(track_id: str):
    track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")
    return TrackResponse(**track)

@api_router.put("/tracks/{track_id}")
async def update_track(track_id: str, track_data: dict, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    if not track:
        raise HTTPException(status_code=404, detail="Track not found")
    
    if track["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.tracks.update_one({"track_id": track_id}, {"$set": track_data})
    
    updated_track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    return TrackResponse(**updated_track)

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
        "track_ids": [],
        "status": album_data.status,
        "likes_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.albums.insert_one(album_doc)
    return AlbumResponse(**album_doc)

@api_router.get("/albums", response_model=List[AlbumResponse])
async def get_albums(limit: int = 50, skip: int = 0):
    albums = await db.albums.find({}, {"_id": 0}).skip(skip).limit(limit).to_list(limit)
    return [AlbumResponse(**album) for album in albums]

@api_router.get("/albums/{album_id}", response_model=AlbumResponse)
async def get_album(album_id: str):
    album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if not album:
        raise HTTPException(status_code=404, detail="Album not found")
    return AlbumResponse(**album)

@api_router.put("/albums/{album_id}")
async def update_album(album_id: str, album_data: dict, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    if not album:
        raise HTTPException(status_code=404, detail="Album not found")
    
    if album["artist_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.albums.update_one({"album_id": album_id}, {"$set": album_data})
    
    updated_album = await db.albums.find_one({"album_id": album_id}, {"_id": 0})
    return AlbumResponse(**updated_album)

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
        "preview_duration": 15,
        "file_url": "",
        "cover_url": None,
        "genre": track_data.genre,
        "description": track_data.description,
        "mastering": track_data.mastering,
        "splits": track_data.splits or [],
        "status": track_data.status,
        "likes_count": 0,
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    await db.tracks.insert_one(track_doc)

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

    await db.tracks.update_one({"track_id": track_id}, {"$set": payload})
    updated = await db.tracks.find_one({"track_id": track_id}, {"_id": 0})
    return AlbumTrackResponse(**updated)

# ==================== ARTIST ROUTES ====================

@api_router.get("/artists")
async def get_artists(limit: int = 50):
    artists = await db.users.find({"role": "artist"}, {"_id": 0, "password_hash": 0}).limit(limit).to_list(limit)
    return artists

@api_router.get("/artists/{artist_id}")
async def get_artist(artist_id: str):
    artist = await db.users.find_one({"user_id": artist_id, "role": "artist"}, {"_id": 0, "password_hash": 0})
    if not artist:
        raise HTTPException(status_code=404, detail="Artist not found")
    
    # Get artist's tracks
    tracks = await db.tracks.find({"artist_id": artist_id}, {"_id": 0}).to_list(100)
    artist["tracks"] = tracks
    
    # Get artist's albums
    albums = await db.albums.find({"artist_id": artist_id}, {"_id": 0}).to_list(100)
    artist["albums"] = albums
    
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
        raise HTTPException(status_code=404, detail="Playlist not found")
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")

    return PlaylistResponse(**playlist)

@api_router.put("/playlists/{playlist_id}", response_model=PlaylistResponse)
async def update_playlist(playlist_id: str, payload: PlaylistUpdate, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)

    playlist = await db.playlists.find_one({"playlist_id": playlist_id}, {"_id": 0})
    if not playlist:
        raise HTTPException(status_code=404, detail="Playlist not found")
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")

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
        raise HTTPException(status_code=404, detail="Playlist not found")
    
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
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
        raise HTTPException(status_code=404, detail="Playlist not found")
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")

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
        raise HTTPException(status_code=404, detail="Playlist not found")
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")

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
        raise HTTPException(status_code=404, detail="Playlist not found")
    
    if playlist["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    await db.playlists.delete_one({"playlist_id": playlist_id})
    return {"message": "Playlist deleted"}

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
    user = await get_current_user(authorization, request)

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

    return {
        "tracks": [track_by_id[i] for i in track_ids if i in track_by_id],
        "albums": [album_by_id[i] for i in album_ids if i in album_by_id],
        "artists": [artist_by_id[i] for i in artist_ids if i in artist_by_id],
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

    connect_account_id = seller.get("stripe_connect_account_id")
    fee_cents = compute_platform_fee_cents(amount_total_cents)
    use_connect = (
        stripe_connect_enabled()
        and amount_total_cents > 0
        and bool(connect_account_id)
    )

    if stripe_connect_enabled() and amount_total_cents > 0:
        if not connect_account_id:
            raise HTTPException(
                status_code=400,
                detail="Ce vendeur n'a pas encore activé les paiements (Stripe Connect). Réessaie plus tard.",
            )
        try:
            acct = await asyncio.to_thread(retrieve_connect_account, connect_account_id)
            if not getattr(acct, "charges_enabled", False):
                raise HTTPException(
                    status_code=400,
                    detail="Le vendeur n'a pas terminé l'activation des paiements Stripe.",
                )
        except HTTPException:
            raise
        except stripe.error.StripeError as e:
            logger.warning("Stripe Connect account: %s", e, exc_info=True)
            msg = getattr(e, "user_message", None) or getattr(e, "message", None) or str(e)
            raise HTTPException(status_code=502, detail=f"Stripe Connect: {msg}")

    try:
        session_result = await create_checkout_session(
            amount_cents=amount_total_cents,
            product_name=product_title,
            user_id=user["user_id"],
            item_type=checkout_data.item_type,
            item_id=item_id,
            origin_url=checkout_data.origin_url,
            artist_id=artist_id,
            connect_account_id=connect_account_id if use_connect else None,
            platform_fee_cents=fee_cents if use_connect else None,
        )
    except stripe.error.StripeError as e:
        # Clé invalide, réseau, paramètres Stripe, etc. — éviter une 500 sans message.
        logger.warning("Stripe checkout session: %s", e, exc_info=True)
        msg = getattr(e, "user_message", None) or getattr(e, "message", None) or str(e)
        raise HTTPException(status_code=502, detail=f"Stripe: {msg}")

    seller_amount = float(amount_total_cents - fee_cents) if use_connect else float(amount_total_cents)

    transaction_doc = {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "session_id": session_result["session_id"],
        "user_id": user["user_id"],
        "amount": float(amount_total_cents),
        "currency": os.environ.get("STRIPE_CURRENCY", "eur"),
        "status": "pending",
        "payment_status": "pending",
        "metadata": {
            "item_type": checkout_data.item_type,
            "item_id": item_id,
        },
        "artist_id": artist_id,
        "stripe_connect_account_id": connect_account_id if use_connect else None,
        "platform_fee_cents": int(fee_cents) if use_connect else 0,
        "seller_amount_cents": int(round(seller_amount)) if use_connect else int(amount_total_cents),
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
            "connect_used": bool(use_connect),
            "platform_fee_cents": int(fee_cents) if use_connect else 0,
            "seller_net_cents": int(round(seller_amount)) if use_connect else int(amount_total_cents),
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
    return [by_id[i] for i in artist_ids if i in by_id]

@api_router.get("/follows/state")
async def get_follow_state(artist_ids: str, authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
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
    try:
        hint = float(os.environ.get("STRIPE_MIN_PAYOUT_HINT_EUROS", "20"))
    except (TypeError, ValueError):
        hint = 20.0
    return {
        "currency": cur,
        "available_cents": available,
        "pending_cents": pending,
        "min_payout_hint_euros": hint,
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
        track_stats.append({
            "track_id": tid,
            "title": track["title"],
            "cover_url": track.get("cover_url"),
            "genre": track.get("genre"),
            "price": track.get("price", 0),
            "status": track.get("status", "draft"),
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
        tr_in_album = [t for t in tracks if t.get("album_id") == aid]
        play_sum = 0
        for t in tr_in_album:
            play_sum += len([p for p in plays_collection if p.get("track_id") == t["track_id"]])
        album_stats.append({
            "album_id": aid,
            "title": album["title"],
            "cover_url": album.get("cover_url"),
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
    
    await db.tracks.update_one(
        {"track_id": track_id},
        {"$set": {"status": new_status, "updated_at": datetime.now(timezone.utc).isoformat()}}
    )
    
    return {"status": new_status, "message": f"Track {'published' if new_status == 'published' else 'unpublished'} successfully"}

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

    if "tracks" in type_set:
        tracks = await db.tracks.find(
            {"status": "published", "$or": [{"title": regex}, {"artist_name": regex}, {"genre": regex}]},
            {"_id": 0},
        ).limit(limit).to_list(limit)
        results["tracks"] = tracks

    if "albums" in type_set:
        albums = await db.albums.find(
            {"$or": [{"title": regex}, {"artist_name": regex}]},
            {"_id": 0},
        ).limit(limit).to_list(limit)
        results["albums"] = albums

    if "artists" in type_set:
        artists = await db.users.find(
            {"role": "artist", "$or": [{"name": regex}, {"artist_name": regex}]},
            {"_id": 0, "password_hash": 0},
        ).limit(limit).to_list(limit)
        results["artists"] = artists

    return results

@app.get("/health")
async def health_check():
    """Ping public (prod / navigateur), sans JWT."""
    return {"status": "ok"}

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

@app.on_event("shutdown")
async def shutdown_db_client():
    client.close()
