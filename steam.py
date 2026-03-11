"""
Steam store client for querying localized game titles.

Uses the public Steam store endpoints to:
1. Search apps by their English name.
2. Fetch the localized store title in Simplified Chinese / Japanese.
"""

from __future__ import annotations

import html
import re
import time
import unicodedata
from dataclasses import dataclass

import requests

STORE_SEARCH_URL = "https://store.steampowered.com/api/storesearch/"
APP_DETAILS_URL = "https://store.steampowered.com/api/appdetails"
STORE_APP_URL = "https://store.steampowered.com/app/{appid}/"

MIN_REQUEST_INTERVAL = 0.45
MAX_RETRIES = 5
RETRY_BACKOFF_SECONDS = 2.0

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json,text/plain,*/*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://store.steampowered.com/",
}

_TRADEMARK_CHARS = {"\u2122", "\u00ae", "\u00a9"}
_PUNCT_TRANSLATION = str.maketrans({
    ":": " ",
    "：": " ",
    "-": " ",
    "–": " ",
    "—": " ",
    "−": " ",
    "_": " ",
    "/": " ",
    "\\": " ",
    ",": " ",
    "，": " ",
    ".": " ",
    "'": "",
    "’": "",
    "‘": "",
    '"': "",
    "“": "",
    "”": "",
    "!": " ",
    "！": " ",
    "?": " ",
    "？": " ",
    "(": " ",
    ")": " ",
    "[": " ",
    "]": " ",
    "{": " ",
    "}": " ",
    "&": " and ",
    "+": " ",
})


@dataclass
class SteamTranslation:
    """Holds the Steam localization data for a single game."""

    english: str
    chinese_simplified: str = ""
    japanese: str = ""
    steam_appid: int | None = None
    steam_english: str = ""
    steam_url: str = ""
    matched: bool = False

    def to_dict(self) -> dict:
        result: dict = {"english": self.english, "matched": self.matched}
        if self.chinese_simplified:
            result["chinese_simplified"] = self.chinese_simplified
        if self.japanese:
            result["japanese"] = self.japanese
        if self.steam_appid is not None:
            result["steam_appid"] = self.steam_appid
        if self.steam_english:
            result["steam_english"] = self.steam_english
        if self.steam_url:
            result["steam_url"] = self.steam_url
        return result


class SteamClient:
    """Thin wrapper around the public Steam store endpoints."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        self._last_request_time = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)

    def _get(self, url: str, *, params: dict[str, object]) -> requests.Response:
        for attempt in range(1, MAX_RETRIES + 1):
            self._throttle()
            response = self.session.get(url, params=params, timeout=20)
            self._last_request_time = time.monotonic()

            if response.status_code != 429:
                response.raise_for_status()
                return response

            if attempt == MAX_RETRIES:
                response.raise_for_status()

            retry_after = _parse_retry_after(response.headers.get("Retry-After"))
            sleep_seconds = retry_after or (RETRY_BACKOFF_SECONDS * attempt)
            time.sleep(sleep_seconds)

        raise RuntimeError("Steam request retry loop exhausted unexpectedly")

    def _search_apps(self, game_name: str) -> list[dict]:
        response = self._get(
            STORE_SEARCH_URL,
            params={
                "term": game_name,
                "l": "english",
                "cc": "US",
            },
        )
        payload = response.json()
        items = payload.get("items", [])
        return items if isinstance(items, list) else []

    def _get_localized_name(self, appid: int, language: str) -> str:
        response = self._get(
            APP_DETAILS_URL,
            params={
                "appids": appid,
                "l": language,
                "cc": "US",
            },
        )
        payload = response.json()
        app_data = payload.get(str(appid), {})
        if not app_data.get("success"):
            return ""
        data = app_data.get("data", {})
        name = data.get("name", "")
        return html.unescape(name).strip() if isinstance(name, str) else ""

    def search_game_translations(self, game_name: str) -> SteamTranslation:
        """Search Steam and return localized Simplified Chinese / Japanese titles."""
        result = SteamTranslation(english=game_name)
        items = self._search_apps(game_name)
        if not items:
            return result

        matched = self._pick_best_match(game_name, items)
        if matched is None:
            return result

        appid = matched["id"]
        steam_english = matched["name"]

        result.matched = True
        result.steam_appid = appid
        result.steam_english = steam_english
        result.steam_url = STORE_APP_URL.format(appid=appid)

        zh_name = self._get_localized_name(appid, "schinese")
        if zh_name:
            result.chinese_simplified = zh_name

        ja_name = self._get_localized_name(appid, "japanese")
        if ja_name:
            result.japanese = ja_name

        return result

    def _pick_best_match(self, game_name: str, items: list[dict]) -> dict | None:
        exact_matches = self._collect_matches(game_name, items, normalize=False)
        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            return None

        normalized_matches = self._collect_matches(game_name, items, normalize=True)
        if len(normalized_matches) == 1:
            return normalized_matches[0]
        return None

    def _collect_matches(
        self, game_name: str, items: list[dict], *, normalize: bool
    ) -> list[dict]:
        target = (
            _normalize_name(game_name)
            if normalize
            else html.unescape(game_name).casefold().strip()
        )
        matches: list[dict] = []
        seen_appids: set[int] = set()

        for item in items:
            name = item.get("name")
            raw_appid = item.get("id", item.get("appid"))
            appid = _coerce_appid(raw_appid)
            if not isinstance(name, str) or appid is None:
                continue

            candidate = (
                _normalize_name(name)
                if normalize
                else html.unescape(name).casefold().strip()
            )
            if candidate != target or appid in seen_appids:
                continue

            matches.append({"id": appid, "name": html.unescape(name).strip()})
            seen_appids.add(appid)

        return matches


def _normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKC", html.unescape(value)).casefold().strip()
    value = "".join(ch for ch in value if ch not in _TRADEMARK_CHARS)
    value = value.translate(_PUNCT_TRANSLATION)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _coerce_appid(value: object) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str) and value.isdigit():
        return int(value)
    return None


def _parse_retry_after(value: str | None) -> float | None:
    if value is None:
        return None
    try:
        seconds = float(value)
    except ValueError:
        return None
    return seconds if seconds > 0 else None
