"""Analytics artiste : présence, sessions, funnel, séries quotidiennes."""
from __future__ import annotations

from collections import Counter
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from pydantic import BaseModel, Field

LIVE_WINDOW_SEC = 90
MAX_DAYS = 90
MIN_DAYS = 7


class AnalyticsHeartbeat(BaseModel):
    session_id: str = Field(..., min_length=8, max_length=80)
    path: Optional[str] = Field(None, max_length=240)
    referrer: Optional[str] = Field(None, max_length=400)
    timezone: Optional[str] = Field(None, max_length=64)
    locale: Optional[str] = Field(None, max_length=32)
    country: Optional[str] = Field(None, max_length=8)
    artist_id: Optional[str] = Field(None, max_length=80)
    track_id: Optional[str] = Field(None, max_length=80)
    album_id: Optional[str] = Field(None, max_length=80)
    dwell_sec: int = Field(15, ge=0, le=300)
    local_hour: Optional[int] = Field(None, ge=0, le=23)
    visible: bool = True


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now().isoformat()


def _parse_dt(value) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except Exception:
        return None


def _day_key(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).date().isoformat()


def _date_range(days: int) -> list[str]:
    today = _now().date()
    start = today - timedelta(days=days - 1)
    return [(start + timedelta(days=i)).isoformat() for i in range(days)]


def _median(values: list[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    n = len(ordered)
    mid = n // 2
    if n % 2:
        return float(ordered[mid])
    return (float(ordered[mid - 1]) + float(ordered[mid])) / 2.0


def _pct(num: float, den: float) -> float:
    if den <= 0:
        return 0.0
    return round(100.0 * num / den, 1)


def _region_label(country: Optional[str], tz: Optional[str]) -> str:
    if country:
        return str(country).upper()
    tz = (tz or "").strip()
    if not tz:
        return "Inconnu"
    if "/" in tz:
        area = tz.split("/", 1)[0]
        mapping = {
            "Europe": "Europe",
            "America": "Amériques",
            "Africa": "Afrique",
            "Asia": "Asie",
            "Australia": "Océanie",
            "Pacific": "Pacifique",
            "Atlantic": "Atlantique",
            "Indian": "Océan Indien",
        }
        return mapping.get(area, area)
    return tz


def _header_country(request) -> Optional[str]:
    if request is None:
        return None
    headers = request.headers
    for key in ("cf-ipcountry", "x-vercel-ip-country", "cloudfront-viewer-country"):
        raw = headers.get(key) or headers.get(key.title())
        if raw and raw.strip().lower() not in {"xx", "t1", "unknown"}:
            return raw.strip()[:8].upper()
    return None


def _clean_path(path: Optional[str]) -> str:
    raw = (path or "/").strip() or "/"
    if len(raw) > 240:
        raw = raw[:240]
    if "?" in raw:
        raw = raw.split("?", 1)[0]
    return raw or "/"


async def _resolve_artist_id(db, *, artist_id: Optional[str], track_id: Optional[str], album_id: Optional[str], path: str) -> Optional[str]:
    if artist_id:
        return artist_id
    if track_id:
        track = await db.tracks.find_one({"track_id": track_id}, {"_id": 0, "artist_id": 1})
        if track and track.get("artist_id"):
            return track["artist_id"]
    if album_id:
        album = await db.albums.find_one({"album_id": album_id}, {"_id": 0, "artist_id": 1})
        if album and album.get("artist_id"):
            return album["artist_id"]
    parts = [p for p in path.split("/") if p]
    if len(parts) >= 2 and parts[0] == "artist":
        return parts[1]
    if len(parts) >= 2 and parts[0] == "track":
        track = await db.tracks.find_one({"track_id": parts[1]}, {"_id": 0, "artist_id": 1})
        return (track or {}).get("artist_id")
    if len(parts) >= 2 and parts[0] == "album":
        album = await db.albums.find_one({"album_id": parts[1]}, {"_id": 0, "artist_id": 1})
        return (album or {}).get("artist_id")
    return None


async def ensure_analytics_indexes(db) -> None:
    try:
        await db.analytics_sessions.create_index("session_id", unique=True)
        await db.analytics_sessions.create_index("last_seen")
        await db.analytics_sessions.create_index("artist_ids")
        await db.analytics_hours.create_index(
            [("artist_id", 1), ("day", 1), ("hour", 1)],
            unique=True,
        )
    except Exception:
        pass


async def record_heartbeat(db, payload: AnalyticsHeartbeat, request, user: Optional[dict]) -> None:
    now = _now()
    now_iso = now.isoformat()
    path = _clean_path(payload.path)
    country = (payload.country or _header_country(request) or "").strip().upper()[:8] or None
    artist_id = await _resolve_artist_id(
        db,
        artist_id=payload.artist_id,
        track_id=payload.track_id,
        album_id=payload.album_id,
        path=path,
    )
    user_id = (user or {}).get("user_id")
    dwell = int(payload.dwell_sec or 0)

    existing = await db.analytics_sessions.find_one({"session_id": payload.session_id}, {"_id": 0, "started_at": 1})
    update: dict[str, Any] = {
        "last_seen": now_iso,
        "path": path,
        "timezone": payload.timezone,
        "locale": payload.locale,
        "country": country,
        "last_artist_id": artist_id,
        "last_track_id": payload.track_id,
        "local_hour": payload.local_hour,
        "visible": bool(payload.visible),
        "user_id": user_id,
    }
    if payload.referrer:
        update["referrer"] = payload.referrer
    ops: dict[str, Any] = {"$set": update, "$inc": {"dwell_sec": dwell}}
    if artist_id:
        ops["$addToSet"] = {"artist_ids": artist_id}
    if not existing:
        ops["$setOnInsert"] = {
            "session_id": payload.session_id,
            "started_at": now_iso,
        }
    await db.analytics_sessions.update_one({"session_id": payload.session_id}, ops, upsert=True)

    if artist_id:
        hour = payload.local_hour if payload.local_hour is not None else now.hour
        await db.analytics_hours.update_one(
            {"artist_id": artist_id, "day": _day_key(now), "hour": int(hour)},
            {"$inc": {"visits": 1, "dwell_sec": dwell}, "$set": {"updated_at": now_iso}},
            upsert=True,
        )


async def collect_artist_analytics(db, artist_id: str, days: int = 30) -> dict:
    days = max(MIN_DAYS, min(MAX_DAYS, int(days or 30)))
    day_keys = _date_range(days)
    start_iso = f"{day_keys[0]}T00:00:00+00:00"
    live_after = (_now() - timedelta(seconds=LIVE_WINDOW_SEC)).isoformat()

    tracks = await db.tracks.find({"artist_id": artist_id}, {"_id": 0, "track_id": 1, "title": 1}).to_list(2000)
    albums = await db.albums.find({"artist_id": artist_id}, {"_id": 0, "album_id": 1, "title": 1}).to_list(1000)
    track_ids = [t["track_id"] for t in tracks if t.get("track_id")]
    album_ids = [a["album_id"] for a in albums if a.get("album_id")]
    track_title = {t["track_id"]: t.get("title") or t["track_id"] for t in tracks}

    purchase_or = []
    if track_ids:
        purchase_or.append({"item_type": "track", "item_id": {"$in": track_ids}})
    if album_ids:
        purchase_or.append({"item_type": "album", "item_id": {"$in": album_ids}})

    like_or = [{"item_type": "artist", "item_id": artist_id}]
    if track_ids:
        like_or.append({"item_type": "track", "item_id": {"$in": track_ids}})
    if album_ids:
        like_or.append({"item_type": "album", "item_id": {"$in": album_ids}})

    plays = await db.plays.find(
        {"track_id": {"$in": track_ids}} if track_ids else {"track_id": "__none__"},
        {"_id": 0, "track_id": 1, "user_id": 1, "played_at": 1, "duration_sec": 1},
    ).to_list(80000) if track_ids else []
    likes = await db.likes.find({"$or": like_or}, {"_id": 0, "user_id": 1, "item_type": 1, "item_id": 1, "created_at": 1}).to_list(20000)
    follows = await db.follows.find({"artist_id": artist_id}, {"_id": 0, "user_id": 1, "created_at": 1}).to_list(20000)
    purchases = await db.purchases.find({"$or": purchase_or}, {"_id": 0}).to_list(20000) if purchase_or else []

    sessions = await db.analytics_sessions.find(
        {"artist_ids": artist_id, "last_seen": {"$gte": start_iso}},
        {"_id": 0},
    ).to_list(20000)
    live_sessions = await db.analytics_sessions.find(
        {"last_artist_id": artist_id, "last_seen": {"$gte": live_after}},
        {"_id": 0},
    ).to_list(2000)
    hour_docs = await db.analytics_hours.find(
        {"artist_id": artist_id, "day": {"$gte": day_keys[0]}},
        {"_id": 0},
    ).to_list(4000)

    paid = [p for p in purchases if _purchase_cents(p) > 0]

    daily = {
        d: {"plays": 0, "likes": 0, "follows": 0, "purchases": 0, "revenue_cents": 0, "visitors": 0, "dwell_sec": 0}
        for d in day_keys
    }

    def add_daily(field_docs, date_field, key, amount_fn=None):
        for doc in field_docs:
            day = str(doc.get(date_field) or "")[:10]
            slot = daily.get(day)
            if not slot:
                continue
            slot[key] += 1
            if amount_fn:
                slot["revenue_cents"] += amount_fn(doc)

    add_daily(plays, "played_at", "plays")
    add_daily(likes, "created_at", "likes")
    add_daily(follows, "created_at", "follows")
    add_daily(paid, "purchased_at", "purchases", lambda p: int(round(_purchase_cents(p))))

    for sess in sessions:
        day = str(sess.get("started_at") or sess.get("last_seen") or "")[:10]
        slot = daily.get(day)
        if not slot:
            continue
        slot["visitors"] += 1
        slot["dwell_sec"] += int(sess.get("dwell_sec") or 0)

    live_filtered = [s for s in live_sessions if s.get("user_id") != artist_id]
    live_logged = sum(1 for s in live_filtered if s.get("user_id"))
    path_counts = Counter(_clean_path(s.get("path")) for s in live_filtered)
    region_live = Counter(_region_label(s.get("country"), s.get("timezone")) for s in live_filtered)

    dwells = [float(s.get("dwell_sec") or 0) for s in sessions]
    logged_sessions = sum(1 for s in sessions if s.get("user_id"))
    guest_sessions = len(sessions) - logged_sessions

    hours = {h: 0 for h in range(24)}
    hours_dwell = {h: 0 for h in range(24)}
    for doc in hour_docs:
        h = int(doc.get("hour") or 0)
        if 0 <= h <= 23:
            hours[h] += int(doc.get("visits") or 0)
            hours_dwell[h] += int(doc.get("dwell_sec") or 0)
    if not hour_docs:
        for play in plays:
            dt = _parse_dt(play.get("played_at"))
            if dt:
                hours[dt.hour] += 1

    weekdays = {i: 0 for i in range(7)}
    for play in plays:
        dt = _parse_dt(play.get("played_at"))
        if dt:
            weekdays[dt.weekday()] += 1

    region_sessions = Counter(_region_label(s.get("country"), s.get("timezone")) for s in sessions)
    locale_sessions = Counter((s.get("locale") or "inconnue")[:12] for s in sessions)

    listeners = {p.get("user_id") for p in plays if p.get("user_id")}
    likers = {lk.get("user_id") for lk in likes if lk.get("user_id")}
    followers = {f.get("user_id") for f in follows if f.get("user_id")}
    buyers = {p.get("user_id") for p in paid if p.get("user_id")}
    buyer_counts = Counter(p.get("user_id") for p in paid if p.get("user_id"))
    repeat_buyers = sum(1 for _, n in buyer_counts.items() if n >= 2)

    first_play: dict[str, datetime] = {}
    for play in plays:
        uid = play.get("user_id")
        dt = _parse_dt(play.get("played_at"))
        if not uid or not dt:
            continue
        prev = first_play.get(uid)
        if prev is None or dt < prev:
            first_play[uid] = dt
    first_buy: dict[str, datetime] = {}
    for p in paid:
        uid = p.get("user_id")
        dt = _parse_dt(p.get("purchased_at"))
        if not uid or not dt:
            continue
        prev = first_buy.get(uid)
        if prev is None or dt < prev:
            first_buy[uid] = dt
    delays = []
    for uid, buy_at in first_buy.items():
        play_at = first_play.get(uid)
        if play_at and buy_at >= play_at:
            delays.append((buy_at - play_at).total_seconds() / 86400.0)

    play_track_counts = Counter(p.get("track_id") for p in plays if p.get("track_id"))
    top_tracks = [
        {"track_id": tid, "title": track_title.get(tid, tid), "plays": n}
        for tid, n in play_track_counts.most_common(6)
    ]

    recent = sorted(sessions, key=lambda s: s.get("last_seen") or "", reverse=True)[:18]
    recent_visitors = [
        {
            "when": s.get("last_seen"),
            "started_at": s.get("started_at"),
            "path": s.get("path") or "/",
            "region": _region_label(s.get("country"), s.get("timezone")),
            "timezone": s.get("timezone"),
            "logged_in": bool(s.get("user_id")),
            "dwell_sec": int(s.get("dwell_sec") or 0),
        }
        for s in recent
        if s.get("user_id") != artist_id
    ]

    weekday_labels = ["Lun", "Mar", "Mer", "Jeu", "Ven", "Sam", "Dim"]

    return {
        "range_days": days,
        "generated_at": _now_iso(),
        "live": {
            "visitors": len(live_filtered),
            "logged_in": live_logged,
            "guests": len(live_filtered) - live_logged,
            "by_path": [{"name": k or "/", "value": v} for k, v in path_counts.most_common(8)],
            "by_region": [{"name": k, "value": v} for k, v in region_live.most_common(8)],
        },
        "engagement": {
            "sessions": len(sessions),
            "logged_in_sessions": logged_sessions,
            "guest_sessions": guest_sessions,
            "avg_session_sec": round(sum(dwells) / len(dwells), 1) if dwells else 0,
            "median_session_sec": round(_median(dwells), 1),
            "total_dwell_sec": int(sum(dwells)),
        },
        "daily": [
            {
                "day": d,
                "label": d[5:],
                **daily[d],
            }
            for d in day_keys
        ],
        "hours": [{"hour": h, "label": f"{h:02d}h", "visits": hours[h], "dwell_sec": hours_dwell[h]} for h in range(24)],
        "weekdays": [{"weekday": i, "label": weekday_labels[i], "plays": weekdays[i]} for i in range(7)],
        "regions": [{"name": k, "value": v} for k, v in region_sessions.most_common(10)],
        "locales": [{"name": k, "value": v} for k, v in locale_sessions.most_common(8)],
        "funnel": [
            {"step": "Auditeurs connectés", "n": len(listeners)},
            {"step": "Likes", "n": len(likers)},
            {"step": "Follows", "n": len(followers)},
            {"step": "Acheteurs", "n": len(buyers)},
        ],
        "retention": {
            "unique_listeners": len(listeners),
            "unique_likers": len(likers),
            "unique_followers": len(followers),
            "unique_buyers": len(buyers),
            "guest_plays": sum(1 for p in plays if not p.get("user_id")),
            "play_to_like_pct": _pct(len(listeners & likers), len(listeners)),
            "play_to_follow_pct": _pct(len(listeners & followers), len(listeners)),
            "play_to_buy_pct": _pct(len(listeners & buyers), len(listeners)),
            "like_to_buy_pct": _pct(len(likers & buyers), len(likers)),
            "repeat_buyer_pct": _pct(repeat_buyers, len(buyers)),
            "repeat_buyers": repeat_buyers,
            "avg_days_to_purchase": round(sum(delays) / len(delays), 1) if delays else None,
        },
        "totals": {
            "plays": len(plays),
            "likes": len(likes),
            "follows": len(follows),
            "purchases": len(paid),
            "revenue_cents": int(round(sum(_purchase_cents(p) for p in paid))),
        },
        "top_tracks": top_tracks,
        "recent_visitors": recent_visitors,
    }


def _purchase_cents(p: dict) -> float:
    for key in ("seller_net_cents", "seller_amount_cents", "amount", "price_paid"):
        v = p.get(key)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                continue
    return 0.0
