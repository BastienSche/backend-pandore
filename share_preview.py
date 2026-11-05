"""Open Graph HTML for crawlers + QR SVG for artist smart links."""
from __future__ import annotations

import html
import io
import os
import re
from typing import Optional
from urllib.parse import urlparse

from fastapi import HTTPException, Request
from fastapi.responses import HTMLResponse, Response

PUBLIC_FRONTEND_URL = str(os.environ.get("PUBLIC_FRONTEND_URL") or "").rstrip("/")

_CRAWLER_SAFE_PATH = re.compile(r"^/(track|artist|album)/[A-Za-z0-9_-]+/?$")

SMART_LINK_HOSTS = (
    ("instagram.com", "instagram"),
    ("tiktok.com", "tiktok"),
    ("youtube.com", "youtube"),
    ("youtu.be", "youtube"),
    ("open.spotify.com", "spotify"),
    ("music.apple.com", "apple"),
    ("soundcloud.com", "soundcloud"),
    ("ffm.to", "presave"),
    ("feature.fm", "presave"),
    ("distrokid.com", "presave"),
    ("hyperfollow.com", "presave"),
    ("presave.io", "presave"),
)


def public_origin(request: Optional[Request] = None) -> str:
    if PUBLIC_FRONTEND_URL:
        return PUBLIC_FRONTEND_URL
    if request is not None:
        host = request.headers.get("x-forwarded-host") or request.headers.get("host") or ""
        proto = request.headers.get("x-forwarded-proto") or (request.url.scheme if request.url else "https")
        if host:
            return f"{proto}://{host.split(',')[0].strip()}".rstrip("/")
    return "https://kloud-music.fr"


def absolute_url(path_or_url: Optional[str], request: Optional[Request] = None) -> str:
    raw = str(path_or_url or "").strip()
    origin = public_origin(request)
    if not raw:
        return f"{origin}/kloud-logo.png"
    if raw.startswith("http://") or raw.startswith("https://"):
        return raw
    if not raw.startswith("/"):
        raw = f"/{raw}"
    return f"{origin}{raw}"


def infer_link_kind(url: str) -> str:
    try:
        host = (urlparse(url).hostname or "").lower().removeprefix("www.")
    except Exception:
        return "website"
    for suffix, kind in SMART_LINK_HOSTS:
        if host == suffix or host.endswith(f".{suffix}"):
            return kind
    return "website"


def normalize_smart_links(raw) -> list[dict]:
    out: list[dict] = []
    if not isinstance(raw, list):
        return out
    for item in raw:
        if isinstance(item, str):
            url = item.strip()
            if not url:
                continue
            out.append({"kind": infer_link_kind(url), "url": url, "label": ""})
            continue
        if not isinstance(item, dict):
            continue
        url = str(item.get("url") or "").strip()
        if not url:
            continue
        kind = str(item.get("kind") or infer_link_kind(url) or "website").strip() or "website"
        label = str(item.get("label") or "").strip()
        out.append({"kind": kind, "url": url, "label": label})
    return out


def og_html(*, title: str, description: str, url: str, image: str, og_type: str = "website") -> HTMLResponse:
    t = html.escape(title or "Kloud")
    d = html.escape((description or "Écoute et achète sur Kloud.")[:280])
    u = html.escape(url)
    img = html.escape(image)
    ogt = html.escape(og_type)
    body = f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8" />
  <title>{t}</title>
  <link rel="canonical" href="{u}" />
  <meta name="description" content="{d}" />
  <meta property="og:site_name" content="Kloud" />
  <meta property="og:title" content="{t}" />
  <meta property="og:description" content="{d}" />
  <meta property="og:type" content="{ogt}" />
  <meta property="og:url" content="{u}" />
  <meta property="og:image" content="{img}" />
  <meta property="og:image:alt" content="{t}" />
  <meta name="twitter:card" content="summary_large_image" />
  <meta name="twitter:title" content="{t}" />
  <meta name="twitter:description" content="{d}" />
  <meta name="twitter:image" content="{img}" />
  <meta http-equiv="refresh" content="0;url={u}" />
</head>
<body>
  <p><a href="{u}">{t} — ouvrir sur Kloud</a></p>
</body>
</html>"""
    return HTMLResponse(content=body, headers={"Cache-Control": "public, max-age=300"})


def allowed_share_target(raw: str, request: Optional[Request] = None) -> str:
    text = str(raw or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="URL manquante")
    origin = public_origin(request)
    if text.startswith("/"):
        if not _CRAWLER_SAFE_PATH.match(text):
            raise HTTPException(status_code=400, detail="Chemin non autorisé")
        return f"{origin}{text}"
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        raise HTTPException(status_code=400, detail="URL invalide")
    host = (parsed.hostname or "").lower()
    allowed = {
        urlparse(origin).hostname or "",
        "kloud-music.fr",
        "www.kloud-music.fr",
        "dev.kloud-music.fr",
        "localhost",
        "127.0.0.1",
    }
    if host not in allowed:
        raise HTTPException(status_code=400, detail="Domaine non autorisé")
    return text


def qr_svg_response(payload: str) -> Response:
    try:
        import segno
    except ImportError as exc:
        raise HTTPException(status_code=503, detail="QR indisponible") from exc
    qr = segno.make(payload, error="m")
    buf = io.BytesIO()
    qr.save(buf, kind="svg", xmldecl=False, scale=8, dark="#020617", light="#ffffff")
    return Response(
        content=buf.getvalue(),
        media_type="image/svg+xml; charset=utf-8",
        headers={"Cache-Control": "public, max-age=3600"},
    )
