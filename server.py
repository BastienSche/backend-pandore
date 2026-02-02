from fastapi import FastAPI, APIRouter, HTTPException, UploadFile, File, Request, Header, Response
from fastapi.responses import FileResponse, StreamingResponse
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

ROOT_DIR = Path(__file__).parent
load_dotenv(ROOT_DIR / '.env')

# MongoDB connection
mongo_url = os.environ['MONGO_URL']
client = AsyncIOMotorClient(mongo_url)
db = client[os.environ['DB_NAME']]

# JWT Configuration
JWT_SECRET = os.environ.get('JWT_SECRET', 'pandore_secret_key_change_in_production_2025')
JWT_ALGORITHM = os.environ.get('JWT_ALGORITHM', 'HS256')

# Stripe Configuration
STRIPE_API_KEY = os.environ.get('STRIPE_API_KEY')

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

class PlaylistCreate(BaseModel):
    name: str
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

@api_router.post("/auth/google/callback", response_model=GoogleSessionResponse)
async def google_callback(session_request: GoogleSessionRequest, response: Response):
    """Exchange session_id for user data and create/update user"""
    # REMINDER: DO NOT HARDCODE THE URL, OR ADD ANY FALLBACKS OR REDIRECT URLS, THIS BREAKS THE AUTH
    import aiohttp
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://demobackend.emergentagent.com/auth/v1/env/oauth/session-data",
            headers={"X-Session-ID": session_request.session_id}
        ) as resp:
            if resp.status != 200:
                raise HTTPException(status_code=401, detail="Invalid session_id")
            data = await resp.json()
    
    # Check if user exists
    user_doc = await db.users.find_one({"email": data["email"]}, {"_id": 0})
    
    if not user_doc:
        # Create new user
        user_id = f"user_{uuid.uuid4().hex[:12]}"
        user_doc = {
            "user_id": user_id,
            "email": data["email"],
            "password_hash": None,
            "name": data["name"],
            "picture": data.get("picture"),
            "role": "user",
            "artist_name": None,
            "created_at": datetime.now(timezone.utc).isoformat()
        }
        await db.users.insert_one(user_doc)
    else:
        # Update user info
        user_id = user_doc["user_id"]
        await db.users.update_one(
            {"user_id": user_id},
            {"$set": {
                "name": data["name"],
                "picture": data.get("picture")
            }}
        )
    
    # Store session
    session_token = data["session_token"]
    session_doc = {
        "user_id": user_id,
        "session_token": session_token,
        "expires_at": datetime.now(timezone.utc) + timedelta(days=7),
        "created_at": datetime.now(timezone.utc).isoformat()
    }
    
    # Delete old sessions for this user
    await db.user_sessions.delete_many({"user_id": user_id})
    await db.user_sessions.insert_one(session_doc)
    
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
    
    return GoogleSessionResponse(
        user_id=user_id,
        email=data["email"],
        name=data["name"],
        picture=data.get("picture"),
        session_token=session_token
    )

@api_router.get("/auth/me", response_model=UserResponse)
async def get_me(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    return UserResponse(**user)

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
    collection = db.tracks if like_data.item_type == "track" else db.albums
    id_field = "track_id" if like_data.item_type == "track" else "album_id"
    
    await collection.update_one(
        {id_field: like_data.item_id},
        {"$inc": {"likes_count": 1}}
    )
    
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
        collection = db.tracks if item_type == "track" else db.albums
        id_field = "track_id" if item_type == "track" else "album_id"
        
        await collection.update_one(
            {id_field: item_id},
            {"$inc": {"likes_count": -1}}
        )
    
    return {"message": "Unliked"}

@api_router.get("/likes")
async def get_likes(authorization: Optional[str] = Header(None), request: Request = None):
    user = await get_current_user(authorization, request)
    likes = await db.likes.find({"user_id": user["user_id"]}, {"_id": 0}).to_list(1000)
    return likes

# ==================== PURCHASE ROUTES ====================

@api_router.post("/purchases/checkout")
async def create_checkout(checkout_data: CheckoutRequest, authorization: Optional[str] = Header(None), request: Request = None):
    ensure_stripe_available()
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
