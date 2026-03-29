from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Request, Header, Response
from fastapi.responses import FileResponse, StreamingResponse, RedirectResponse
from dotenv import load_dotenv
from starlette.middleware.cors import CORSMiddleware
from motor.motor_asyncio import AsyncIOMotorClient
import os
import logging
from pathlib import Path
from pydantic import BaseModel, Field, EmailStr, ConfigDict
from typing import List, Optional, Literal
import uuid
from datetime import datetime, timezone, timedelta
import bcrypt
import jwt
try:
    from emergentintegrations.payments.stripe.checkout import (
        StripeCheckout,
        CheckoutSessionResponse,
        CheckoutStatusResponse,
        CheckoutSessionRequest,
    )
except ImportError:
    StripeCheckout = None
    CheckoutSessionResponse = None
    CheckoutStatusResponse = None
    CheckoutSessionRequest = None
import aiofiles
import shutil
from urllib.parse import urlencode

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

# JWT Configuration
JWT_SECRET = os.environ.get('JWT_SECRET', 'pandore_secret_key_change_in_production_2025')
JWT_ALGORITHM = os.environ.get('JWT_ALGORITHM', 'HS256')

# Stripe Configuration
STRIPE_API_KEY = os.environ.get('STRIPE_API_KEY')

# Google OAuth Configuration
GOOGLE_OAUTH_CLIENT_ID = os.environ.get("GOOGLE_OAUTH_CLIENT_ID")
GOOGLE_OAUTH_CLIENT_SECRET = os.environ.get("GOOGLE_OAUTH_CLIENT_SECRET")
GOOGLE_OAUTH_REDIRECT_URI = os.environ.get("GOOGLE_OAUTH_REDIRECT_URI")
FRONTEND_OAUTH_REDIRECT_URL = os.environ.get("FRONTEND_OAUTH_REDIRECT_URL")

# Create uploads directory
UPLOADS_DIR = ROOT_DIR / "uploads"
UPLOADS_DIR.mkdir(exist_ok=True)
(UPLOADS_DIR / "audio").mkdir(exist_ok=True)
(UPLOADS_DIR / "covers").mkdir(exist_ok=True)

app = FastAPI()
api_router = APIRouter(prefix="/api")

logger = logging.getLogger(__name__)

def ensure_stripe_available():
    if StripeCheckout is None:
        raise HTTPException(
            status_code=503,
            detail="Stripe integration unavailable. Install emergentintegrations to enable payments.",
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
    created_at: str

class AlbumCreate(BaseModel):
    title: str
    price: float  # Prix en cents
    description: Optional[str] = None
    status: str = "draft"  # draft|published

class AlbumResponse(BaseModel):
    album_id: str
    title: str
    artist_id: str
    artist_name: str
    price: float
    cover_url: Optional[str]
    description: Optional[str]
    track_ids: List[str]
    status: str
    likes_count: int
    created_at: str

class AlbumTrackCreate(BaseModel):
    title: str
    price: float  # Prix en cents
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
    item_id: str
    origin_url: str

class AddToLibraryRequest(BaseModel):
    item_type: Literal["track", "album"]
    item_id: str

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
    
    # Generate unique filename
    file_ext = Path(file.filename).suffix
    filename = f"{uuid.uuid4().hex}{file_ext}"
    file_path = UPLOADS_DIR / "audio" / filename
    
    # Save file
    async with aiofiles.open(file_path, 'wb') as f:
        content = await file.read()
        await f.write(content)
    
    return {"file_url": f"/api/files/audio/{filename}"}

@api_router.post("/upload/cover")
async def upload_cover(file: UploadFile = File(...), authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can upload covers")
    
    # Generate unique filename
    file_ext = Path(file.filename).suffix
    filename = f"{uuid.uuid4().hex}{file_ext}"
    file_path = UPLOADS_DIR / "covers" / filename
    
    # Save file
    async with aiofiles.open(file_path, 'wb') as f:
        content = await file.read()
        await f.write(content)
    
    return {"cover_url": f"/api/files/covers/{filename}"}

@api_router.get("/files/audio/{filename}")
async def get_audio_file(filename: str, request: Request):
    file_path = UPLOADS_DIR / "audio" / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    
    # Support range requests for audio preview
    range_header = request.headers.get("range")
    if range_header:
        range_match = range_header.replace("bytes=", "").split("-")
        start = int(range_match[0])
        file_size = file_path.stat().st_size
        end = int(range_match[1]) if range_match[1] else file_size - 1
        
        async def iterfile():
            async with aiofiles.open(file_path, 'rb') as f:
                await f.seek(start)
                remaining = end - start + 1
                while remaining > 0:
                    chunk_size = min(8192, remaining)
                    data = await f.read(chunk_size)
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
                "Content-Type": "audio/mpeg"
            }
        )
    
    return FileResponse(file_path, media_type="audio/mpeg")

@api_router.get("/files/covers/{filename}")
async def get_cover_file(filename: str):
    file_path = UPLOADS_DIR / "covers" / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
    return FileResponse(file_path)

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

    # Get item details
    if checkout_data.item_type == "track":
        item = await db.tracks.find_one({"track_id": checkout_data.item_id}, {"_id": 0})
        if not item:
            raise HTTPException(status_code=404, detail="Track not found")
    else:
        item = await db.albums.find_one({"album_id": checkout_data.item_id}, {"_id": 0})
        if not item:
            raise HTTPException(status_code=404, detail="Album not found")

    if is_free_item_price(item.get("price")):
        raise HTTPException(
            status_code=400,
            detail="Les contenus gratuits s'ajoutent à la bibliothèque sans passer par le paiement.",
        )

    ensure_stripe_available()

    # Check if already purchased
    existing_purchase = await db.purchases.find_one({
        "user_id": user["user_id"],
        "item_type": checkout_data.item_type,
        "item_id": checkout_data.item_id
    })

    if existing_purchase:
        raise HTTPException(status_code=400, detail="Already purchased")

    # Create Stripe checkout session
    host_url = checkout_data.origin_url
    webhook_url = f"{str(request.base_url).rstrip('/')}/api/webhook/stripe"
    stripe_checkout = StripeCheckout(api_key=STRIPE_API_KEY, webhook_url=webhook_url)
    
    success_url = f"{host_url}/library?session_id={{{{CHECKOUT_SESSION_ID}}}}"
    cancel_url = f"{host_url}/browse"
    
    checkout_request = CheckoutSessionRequest(
        amount=float(item["price"]),
        currency="usd",
        success_url=success_url,
        cancel_url=cancel_url,
        metadata={
            "user_id": user["user_id"],
            "item_type": checkout_data.item_type,
            "item_id": checkout_data.item_id
        }
    )
    
    session: CheckoutSessionResponse = await stripe_checkout.create_checkout_session(checkout_request)
    
    # Create payment transaction record
    transaction_doc = {
        "transaction_id": f"txn_{uuid.uuid4().hex[:12]}",
        "session_id": session.session_id,
        "user_id": user["user_id"],
        "amount": float(item["price"]),
        "currency": "usd",
        "status": "pending",
        "payment_status": "pending",
        "metadata": {
            "item_type": checkout_data.item_type,
            "item_id": checkout_data.item_id
        },
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    await db.payment_transactions.insert_one(transaction_doc)
    
    return {"url": session.url, "session_id": session.session_id}

@api_router.get("/purchases/status/{session_id}")
async def get_checkout_status(session_id: str, authorization: Optional[str] = Header(None), request: Request = None):
    ensure_stripe_available()
    user = await get_current_user(authorization, request)
    
    # Get transaction
    transaction = await db.payment_transactions.find_one({"session_id": session_id}, {"_id": 0})
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")
    
    if transaction["user_id"] != user["user_id"]:
        raise HTTPException(status_code=403, detail="Not authorized")
    
    # Check Stripe status
    host_url = str(request.base_url).rstrip('/')
    webhook_url = f"{host_url}/api/webhook/stripe"
    stripe_checkout = StripeCheckout(api_key=STRIPE_API_KEY, webhook_url=webhook_url)
    
    checkout_status: CheckoutStatusResponse = await stripe_checkout.get_checkout_status(session_id)
    
    # Update transaction
    await db.payment_transactions.update_one(
        {"session_id": session_id},
        {"$set": {
            "status": checkout_status.status,
            "payment_status": checkout_status.payment_status
        }}
    )
    
    # If paid, create purchase record
    if checkout_status.payment_status == "paid":
        # Check if purchase already exists
        existing_purchase = await db.purchases.find_one({
            "user_id": transaction["user_id"],
            "item_type": transaction["metadata"]["item_type"],
            "item_id": transaction["metadata"]["item_id"]
        })
        
        if not existing_purchase:
            purchase_doc = {
                "purchase_id": f"purchase_{uuid.uuid4().hex[:12]}",
                "user_id": transaction["user_id"],
                "item_type": transaction["metadata"]["item_type"],
                "item_id": transaction["metadata"]["item_id"],
                "price_paid": transaction["amount"],
                "purchased_at": datetime.now(timezone.utc).isoformat()
            }
            await db.purchases.insert_one(purchase_doc)
    
    return {
        "status": checkout_status.status,
        "payment_status": checkout_status.payment_status,
        "amount_total": checkout_status.amount_total,
        "currency": checkout_status.currency
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

@api_router.post("/webhook/stripe")
async def stripe_webhook(request: Request):
    ensure_stripe_available()
    body_bytes = await request.body()
    signature = request.headers.get("Stripe-Signature")
    
    webhook_url = f"{str(request.base_url).rstrip('/')}/api/webhook/stripe"
    stripe_checkout = StripeCheckout(api_key=STRIPE_API_KEY, webhook_url=webhook_url)
    
    try:
        webhook_response = await stripe_checkout.handle_webhook(body_bytes, signature)
        
        # Update transaction
        await db.payment_transactions.update_one(
            {"session_id": webhook_response.session_id},
            {"$set": {
                "payment_status": webhook_response.payment_status,
                "status": "complete" if webhook_response.payment_status == "paid" else "failed"
            }}
        )
        
        # If paid, create purchase
        if webhook_response.payment_status == "paid" and webhook_response.metadata:
            transaction = await db.payment_transactions.find_one({"session_id": webhook_response.session_id}, {"_id": 0})
            if transaction:
                # Check if purchase already exists
                existing_purchase = await db.purchases.find_one({
                    "user_id": transaction["user_id"],
                    "item_type": transaction["metadata"]["item_type"],
                    "item_id": transaction["metadata"]["item_id"]
                })
                
                if not existing_purchase:
                    purchase_doc = {
                        "purchase_id": f"purchase_{uuid.uuid4().hex[:12]}",
                        "user_id": transaction["user_id"],
                        "item_type": transaction["metadata"]["item_type"],
                        "item_id": transaction["metadata"]["item_id"],
                        "price_paid": transaction["amount"],
                        "purchased_at": datetime.now(timezone.utc).isoformat()
                    }
                    await db.purchases.insert_one(purchase_doc)
        
        return {"status": "success"}
    except Exception as e:
        logger.error(f"Webhook error: {e}")
        raise HTTPException(status_code=400, detail=str(e))

# ==================== ARTIST STATS ROUTES ====================

@api_router.get("/artist/stats")
async def get_artist_stats(authorization: Optional[str] = Header(None), request: Request = None):
    """Get comprehensive stats for an artist"""
    user = await get_current_user(authorization, request)
    
    if user["role"] != "artist":
        raise HTTPException(status_code=403, detail="Only artists can view stats")
    
    artist_id = user["user_id"]
    
    # Get all artist's tracks
    tracks = await db.tracks.find({"artist_id": artist_id}, {"_id": 0}).to_list(1000)
    
    # Get all purchases for artist's tracks
    track_ids = [t["track_id"] for t in tracks]
    purchases = await db.purchases.find({"item_id": {"$in": track_ids}}, {"_id": 0}).to_list(10000)
    
    # Get play counts (from a plays collection if exists, otherwise simulate)
    plays_collection = await db.plays.find({"track_id": {"$in": track_ids}}, {"_id": 0}).to_list(100000)
    
    # Calculate stats
    total_tracks = len(tracks)
    published_tracks = len([t for t in tracks if t.get("status") == "published"])
    draft_tracks = total_tracks - published_tracks
    
    total_sales = len(purchases)
    total_revenue = sum(p.get("amount", 0) for p in purchases)
    
    # Calculate per-track stats
    track_stats = []
    for track in tracks:
        track_purchases = [p for p in purchases if p["item_id"] == track["track_id"]]
        track_plays = [p for p in plays_collection if p.get("track_id") == track["track_id"]]
        
        track_stats.append({
            "track_id": track["track_id"],
            "title": track["title"],
            "cover_url": track.get("cover_url"),
            "genre": track.get("genre"),
            "price": track.get("price", 0),
            "status": track.get("status", "draft"),
            "sales_count": len(track_purchases),
            "revenue": sum(p.get("amount", 0) for p in track_purchases),
            "play_count": len(track_plays),
            "play_duration_sec": sum(p.get("duration_sec", 15) for p in track_plays),
            "likes_count": track.get("likes_count", 0),
            "created_at": track.get("created_at")
        })
    
    # Sort by revenue
    track_stats.sort(key=lambda x: x["revenue"], reverse=True)
    
    # Calculate time-based stats (last 7 days, 30 days)
    from datetime import timedelta
    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)
    
    def parse_date(date_str):
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.replace('Z', '+00:00'))
        except:
            return None
    
    recent_purchases = [p for p in purchases if parse_date(p.get("purchased_at")) and parse_date(p.get("purchased_at")) > week_ago]
    monthly_purchases = [p for p in purchases if parse_date(p.get("purchased_at")) and parse_date(p.get("purchased_at")) > month_ago]
    
    # Total play time
    total_play_duration = sum(p.get("duration_sec", 15) for p in plays_collection)
    
    return {
        "overview": {
            "total_tracks": total_tracks,
            "published_tracks": published_tracks,
            "draft_tracks": draft_tracks,
            "total_sales": total_sales,
            "total_revenue": total_revenue,
            "total_play_count": len(plays_collection),
            "total_play_duration_sec": total_play_duration,
            "total_play_duration_hours": round(total_play_duration / 3600, 1)
        },
        "period_stats": {
            "last_7_days": {
                "sales": len(recent_purchases),
                "revenue": sum(p.get("amount", 0) for p in recent_purchases)
            },
            "last_30_days": {
                "sales": len(monthly_purchases),
                "revenue": sum(p.get("amount", 0) for p in monthly_purchases)
            }
        },
        "track_stats": track_stats,
        "top_tracks": track_stats[:5]
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
async def record_play(track_id: str, duration_sec: int = 15, authorization: Optional[str] = Header(None), request: Request = None):
    """Record a track play for analytics"""
    play_doc = {
        "play_id": f"play_{uuid.uuid4().hex[:12]}",
        "track_id": track_id,
        "duration_sec": duration_sec,
        "played_at": datetime.now(timezone.utc).isoformat(),
        "user_id": None
    }
    
    # Try to get user if authenticated
    try:
        user = await get_current_user(authorization, request)
        play_doc["user_id"] = user["user_id"]
    except:
        pass
    
    await db.plays.insert_one(play_doc)
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
