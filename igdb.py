"""
IGDB API client for querying game localization data.

Uses the Twitch OAuth flow to authenticate with IGDB API v4,
then queries alternative_names and game_localizations to extract
official Chinese and Japanese game names.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass, field

import requests

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TWITCH_TOKEN_URL = "https://id.twitch.tv/oauth2/token"
IGDB_BASE_URL = "https://api.igdb.com/v4"

# IGDB rate limit: ~4 requests per second
MIN_REQUEST_INTERVAL = 0.26  # seconds between requests

# alternative_names comment patterns (case-insensitive matching)
_COMMENT_CHINESE_SIMPLIFIED = "chinese title - simplified"
_COMMENT_CHINESE_TRADITIONAL = "chinese title - traditional"
_COMMENT_JAPANESE = "japanese title"

# game_localizations region IDs
_REGION_JAPAN = 3
_REGION_KOREA = 2

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass
class GameTranslation:
    """Holds the localized names for a single game."""

    english: str
    chinese_simplified: str = ""
    chinese_traditional: str = ""
    japanese: str = ""
    igdb_id: int | None = None
    matched: bool = False

    def to_dict(self) -> dict:
        d: dict = {"english": self.english}
        if self.chinese_simplified:
            d["chinese_simplified"] = self.chinese_simplified
        if self.chinese_traditional:
            d["chinese_traditional"] = self.chinese_traditional
        if self.japanese:
            d["japanese"] = self.japanese
        if self.igdb_id is not None:
            d["igdb_id"] = self.igdb_id
        d["matched"] = self.matched
        return d


# ---------------------------------------------------------------------------
# IGDB Client
# ---------------------------------------------------------------------------


class IGDBClient:
    """Thin wrapper around the IGDB API v4."""

    def __init__(self, client_id: str | None = None, client_secret: str | None = None):
        self.client_id = client_id or os.getenv("IGDB_CLIENT_ID", "")
        self.client_secret = client_secret or os.getenv("IGDB_CLIENT_SECRET", "")
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "IGDB credentials not found. Set IGDB_CLIENT_ID and "
                "IGDB_CLIENT_SECRET in .env or environment variables."
            )
        self._access_token: str = ""
        self._last_request_time: float = 0.0

    # -- Authentication ----------------------------------------------------

    def authenticate(self) -> None:
        """Obtain an OAuth2 access token from Twitch."""
        resp = requests.post(
            TWITCH_TOKEN_URL,
            params={
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "client_credentials",
            },
            timeout=15,
        )
        resp.raise_for_status()
        self._access_token = resp.json()["access_token"]

    # -- Low-level query ---------------------------------------------------

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)

    def query(self, endpoint: str, body: str) -> list[dict]:
        """Execute an Apicalypse query against an IGDB endpoint."""
        if not self._access_token:
            self.authenticate()

        self._throttle()
        resp = requests.post(
            f"{IGDB_BASE_URL}/{endpoint}",
            headers={
                "Client-ID": self.client_id,
                "Authorization": f"Bearer {self._access_token}",
            },
            data=body,
            timeout=15,
        )
        self._last_request_time = time.monotonic()
        resp.raise_for_status()
        return resp.json()

    # -- High-level helpers ------------------------------------------------

    def search_game_translations(self, game_name: str) -> GameTranslation:
        """Search IGDB for *game_name* and return its localized names.

        Strategy:
        1. Search ``/games`` with expanded ``alternative_names`` and
           ``game_localizations``.
        2. Extract Chinese simplified / traditional from
           ``alternative_names`` where ``comment`` matches.
        3. Extract Japanese from ``game_localizations`` (region 3) or
           ``alternative_names``.
        """
        result = GameTranslation(english=game_name)

        # Escape double-quotes in game name for Apicalypse query
        safe_name = game_name.replace('"', '\\"')

        games = self.query(
            "games",
            (
                f'search "{safe_name}"; '
                f"fields name, alternative_names.name, alternative_names.comment, "
                f"game_localizations.name, game_localizations.region; "
                f"limit 5;"
            ),
        )

        if not games:
            return result

        # Pick the best match (prefer exact name match)
        best = None
        for g in games:
            if g.get("name", "").lower() == game_name.lower():
                best = g
                break
        if best is None:
            best = games[0]

        result.igdb_id = best.get("id")
        result.matched = True

        # -- Extract from alternative_names --------------------------------
        for alt in best.get("alternative_names", []):
            comment = (alt.get("comment") or "").lower()
            name = alt.get("name", "")
            if _COMMENT_CHINESE_SIMPLIFIED in comment and not result.chinese_simplified:
                result.chinese_simplified = name
            elif _COMMENT_CHINESE_TRADITIONAL in comment and not result.chinese_traditional:
                result.chinese_traditional = name
            elif _COMMENT_JAPANESE in comment and not result.japanese:
                result.japanese = name

        # -- Extract from game_localizations (fallback for Japanese) -------
        if not result.japanese:
            for loc in best.get("game_localizations", []):
                if loc.get("region") == _REGION_JAPAN:
                    result.japanese = loc.get("name", "")
                    break

        return result
