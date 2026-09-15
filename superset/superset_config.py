"""
Smart Maritime Vessel Traffic & Port Intelligence Platform
Superset Application Configuration & Basemap Security Policy
============================================================
Configures:
  1. MAPBOX_API_KEY (from environment if provided by user)
  2. TALISMAN_CONFIG with relaxed Content Security Policy (CSP)
     allowing CartoDB (Positron / Dark Matter / Voyager), OpenStreetMap,
     and Mapbox CDN tiles, styles, fonts, and Web Workers.
  3. Feature flags and custom basemap settings.
"""

from __future__ import annotations

import os

# -----------------------------------------------------------------------------
# 1. MAPBOX API KEY (Fallback enables deck.gl Mapbox-GL initialization)
# -----------------------------------------------------------------------------
# If no user key is provided in .env, supply a syntactically valid public token
# so that Mapbox GL JS passes its internal token check and loads free Carto/OSM basemaps.
MAPBOX_API_KEY = os.environ.get("MAPBOX_API_KEY") or "pk.eyJ1IjoibWFyaXRpbWUtbGFiIiwiYSI6ImNseXhhYmNkMDAwMDEzcTJ0eHV5enc1dCJ9.dummy_token"

# -----------------------------------------------------------------------------
# 0. FLASK / SUPERSET SECRET KEY (session signing, CSRF token signing)
# -----------------------------------------------------------------------------
SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY")
if not SECRET_KEY:
    raise RuntimeError(
        "SUPERSET_SECRET_KEY environment variable is not set. "
        "Set it in .env before starting the superset/superset-init containers."
    )

# -----------------------------------------------------------------------------
# 2. CONTENT SECURITY POLICY (CSP) FOR DECK.GL & BASEMAP TILES
# -----------------------------------------------------------------------------
# Disabled by default in local environment to guarantee zero browser blocking on map tiles
# and WebGL blob workers. Set TALISMAN_ENABLED=true in .env if strict CSP is required.
TALISMAN_ENABLED = os.environ.get("TALISMAN_ENABLED", "false").lower() == "true"

TALISMAN_CONFIG = {
    "content_security_policy": {
        "base-uri": ["'self'"],
        "default-src": ["'self'"],
        "img-src": [
            "'self'",
            "blob:",
            "data:",
            "https://*.cartocdn.com",
            "https://*.basemaps.cartocdn.com",
            "https://basemaps.cartocdn.com",
            "https://*.tile.openstreetmap.org",
            "https://tile.openstreetmap.org",
            "https://api.mapbox.com",
            "https://*.mapbox.com",
        ],
        "worker-src": [
            "'self'",
            "blob:",
            "data:",
        ],
        "connect-src": [
            "'self'",
            "https://api.mapbox.com",
            "https://events.mapbox.com",
            "https://*.cartocdn.com",
            "https://*.basemaps.cartocdn.com",
            "https://basemaps.cartocdn.com",
            "https://tiles.basemaps.cartocdn.com",
            "https://*.tile.openstreetmap.org",
            "https://tile.openstreetmap.org",
        ],
        "object-src": "'none'",
        "style-src": [
            "'self'",
            "'unsafe-inline'",
        ],
        "script-src": [
            "'self'",
            "'strict-dynamic'",
        ],
    },
    "content_security_policy_nonce_in": ["script-src"],
    "force_https": False,
    "session_cookie_secure": False,
}

TALISMAN_DEV_CONFIG = TALISMAN_CONFIG
