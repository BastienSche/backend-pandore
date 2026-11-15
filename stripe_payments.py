"""
Paiements Stripe (Checkout Session) + Stripe Connect (Express).
Variables : STRIPE_API_KEY, STRIPE_WEBHOOK_SECRET, STRIPE_WEBHOOK_SECRET_THIN (optionnel),
STRIPE_CURRENCY, STRIPE_PLATFORM_FEE_PERCENT (défaut 5), STRIPE_CONNECT_ENABLED (défaut true).
"""
from __future__ import annotations

import asyncio
import os
from typing import Any, Dict, Optional

import stripe

STRIPE_CURRENCY = os.environ.get("STRIPE_CURRENCY", "eur").lower()


def _frontend_origin_from_env_or_request(origin_url: str) -> str:
    """
    Prefer a server-side configured frontend URL to avoid mismatched origins (http/https, port)
    and fragile client-provided values.
    """
    env_origin = (os.environ.get("PUBLIC_FRONTEND_URL") or "").strip()
    origin = (env_origin or origin_url or "").strip()
    origin = origin.rstrip("/")
    if not origin:
        return ""
    if not origin.startswith(("http://", "https://")):
        # Last-resort normalization (prevents Stripe rejecting invalid URLs)
        origin = "https://" + origin
    return origin


def configure_stripe() -> None:
    key = os.environ.get("STRIPE_API_KEY") or ""
    stripe.api_key = key or None


def stripe_configured() -> bool:
    return bool(os.environ.get("STRIPE_API_KEY", "").strip())


def stripe_connect_enabled() -> bool:
    v = os.environ.get("STRIPE_CONNECT_ENABLED", "true").strip().lower()
    return v in ("1", "true", "yes", "on")


def get_platform_fee_percent() -> float:
    raw = os.environ.get("STRIPE_PLATFORM_FEE_PERCENT", "5").strip()
    try:
        p = float(raw)
    except (TypeError, ValueError):
        p = 5.0
    return max(0.0, min(p, 99.0))


def compute_platform_fee_cents(amount_cents: int, fee_percent: Optional[float] = None) -> int:
    """Commission plateforme en centimes. Laisse au moins 1 centime au vendeur si montant > 1."""
    if amount_cents <= 0:
        return 0
    pct = get_platform_fee_percent() if fee_percent is None else fee_percent
    raw = int(round(amount_cents * pct / 100.0))
    raw = max(0, min(raw, amount_cents - 1))
    if amount_cents == 1:
        return 0
    return raw


def get_min_payout_cents() -> int:
    """Seuil de retrait cagnotte → Stripe Connect (défaut 10 €)."""
    raw = (
        os.environ.get("STRIPE_MIN_PAYOUT_EUROS")
        or os.environ.get("STRIPE_MIN_PAYOUT_HINT_EUROS")
        or "10"
    ).strip()
    try:
        euros = float(raw)
    except (TypeError, ValueError):
        euros = 10.0
    return max(100, int(round(max(0.0, euros) * 100)))


async def create_checkout_session(
    *,
    amount_cents: int,
    product_name: str,
    user_id: str,
    item_type: str,
    item_id: str,
    origin_url: str,
    artist_id: Optional[str] = None,
    connect_account_id: Optional[str] = None,
    platform_fee_cents: Optional[int] = None,
) -> Dict[str, Any]:
    """Crée une session Stripe Checkout (hosted). L'argent reste sur le compte plateforme (cagnotte)."""
    configure_stripe()
    origin = _frontend_origin_from_env_or_request(origin_url)
    if not origin:
        raise ValueError("origin_url / PUBLIC_FRONTEND_URL manquant")
    success_url = f"{origin}/library?session_id={{CHECKOUT_SESSION_ID}}"
    cancel_url = f"{origin}/browse"

    name = (product_name or "Achat Kloud").strip()[:120]

    meta: Dict[str, str] = {
        "user_id": str(user_id),
        "item_type": item_type,
        "item_id": str(item_id),
    }
    if artist_id:
        meta["artist_id"] = str(artist_id)

    def _create() -> stripe.checkout.Session:
        params: Dict[str, Any] = {
            "mode": "payment",
            "line_items": [
                {
                    "price_data": {
                        "currency": STRIPE_CURRENCY,
                        "unit_amount": max(1, int(amount_cents)),
                        "product_data": {"name": name},
                    },
                    "quantity": 1,
                }
            ],
            "success_url": success_url,
            "cancel_url": cancel_url,
            "metadata": meta,
        }
        # Les ventes restent sur le compte Kloud. Le versement artiste se fait plus tard
        # (cagnotte → Transfer Connect), même si un compte Express existe déjà.
        return stripe.checkout.Session.create(**params)

    session = await asyncio.to_thread(_create)
    return {"url": session.url, "session_id": session.id}


def _checkout_metadata_to_dict(sess: Any) -> Dict[str, Any]:
    """Stripe metadata peut être un dict ou un objet dict-like ; évite crash sur dict(...)."""
    m = getattr(sess, "metadata", None)
    if m is None:
        return {}
    if isinstance(m, dict):
        return {str(k): m[k] for k in m}
    try:
        return dict(m)
    except Exception:
        out: Dict[str, Any] = {}
        try:
            keys = m.keys() if hasattr(m, "keys") else []
            for k in keys:
                out[str(k)] = m[k]
        except Exception:
            pass
        return out


async def retrieve_checkout_session(session_id: str) -> Dict[str, Any]:
    configure_stripe()
    s: stripe.checkout.Session = await asyncio.to_thread(
        stripe.checkout.Session.retrieve, session_id
    )
    return {
        "status": getattr(s, "status", None),
        "payment_status": getattr(s, "payment_status", None),
        "amount_total": getattr(s, "amount_total", None),
        "currency": getattr(s, "currency", None),
        "metadata": _checkout_metadata_to_dict(s),
    }


def construct_webhook_event(payload: bytes, sig_header: Optional[str]) -> stripe.Event:
    configure_stripe()
    secret = os.environ.get("STRIPE_WEBHOOK_SECRET", "").strip()
    if not secret:
        raise ValueError("STRIPE_WEBHOOK_SECRET manquant")
    if not sig_header:
        raise ValueError("Header Stripe-Signature manquant")
    return stripe.Webhook.construct_event(payload, sig_header, secret)


def parse_thin_event_notification(payload: bytes, sig_header: Optional[str]):
    """
    Webhook « thin » (Event Notifications v2) — même principe que construct_event
    mais retourne une EventNotification (souvent UnknownEventNotification pour v1.*).
    Secret : STRIPE_WEBHOOK_SECRET_THIN (celui affiché sur la destination *thin* du Dashboard).
    """
    from stripe import StripeClient

    configure_stripe()
    secret = os.environ.get("STRIPE_WEBHOOK_SECRET_THIN", "").strip()
    if not secret:
        raise ValueError("STRIPE_WEBHOOK_SECRET_THIN manquant")
    if not sig_header:
        raise ValueError("Header Stripe-Signature manquant")
    key = os.environ.get("STRIPE_API_KEY", "").strip()
    if not key:
        raise ValueError("STRIPE_API_KEY manquant")
    client = StripeClient(key)
    return client.parse_event_notification(payload, sig_header, secret)


def create_express_connected_account(*, email: Optional[str], country: str = "FR") -> Any:
    configure_stripe()
    kwargs: Dict[str, Any] = {
        "type": "express",
        "country": country,
        "capabilities": {
            "card_payments": {"requested": True},
            "transfers": {"requested": True},
        },
    }
    if email:
        kwargs["email"] = email
    return stripe.Account.create(**kwargs)


def create_account_onboarding_link(*, account_id: str, refresh_url: str, return_url: str) -> str:
    configure_stripe()
    link = stripe.AccountLink.create(
        account=account_id,
        refresh_url=refresh_url,
        return_url=return_url,
        type="account_onboarding",
    )
    return link.url


def create_account_login_link(account_id: str) -> str:
    configure_stripe()
    link = stripe.Account.create_login_link(account_id)
    return link.url


def retrieve_connect_account(account_id: str) -> Any:
    configure_stripe()
    return stripe.Account.retrieve(account_id)


def retrieve_connect_balance(account_id: str) -> Any:
    configure_stripe()
    return stripe.Balance.retrieve(stripe_account=account_id)


def create_connect_transfer(
    *,
    amount_cents: int,
    currency: str,
    destination: str,
    transfer_id: str,
    metadata: Optional[Dict[str, str]] = None,
) -> Any:
    """Virement interne plateforme → compte Connect Express."""
    configure_stripe()
    return stripe.Transfer.create(
        amount=max(1, int(amount_cents)),
        currency=(currency or STRIPE_CURRENCY).lower(),
        destination=destination,
        metadata=metadata or {},
        idempotency_key=f"kloud_payout_{transfer_id}",
    )
