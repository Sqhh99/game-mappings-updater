"""
Wikidata client for querying localized game titles.

Uses the public Wikidata API to:
1. Search entities by their English game name.
2. Fetch Chinese/Japanese labels for the matched entity.
"""

from __future__ import annotations

import html
import re
import time
import unicodedata
from dataclasses import dataclass

import requests

WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/{qid}"

MIN_REQUEST_INTERVAL = 0.10

REQUEST_HEADERS = {
    "User-Agent": "FLiNG-Downloader/0.1 (game-mappings-updater; Wikidata lookup)",
    "Accept": "application/json,text/plain,*/*",
}

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
_GAME_DESCRIPTION_KEYWORDS = (
    "video game",
    "computer game",
    "action role-playing game",
    "role-playing video game",
    "simulation video game",
    "adventure game",
    "strategy video game",
    "racing video game",
    "sports video game",
    "fighting game",
    "shooter game",
)


@dataclass
class WikidataTranslation:
    """Holds the Wikidata localization data for a single game."""

    english: str
    chinese_simplified: str = ""
    japanese: str = ""
    wikidata_id: str = ""
    wikidata_english: str = ""
    wikidata_url: str = ""
    matched: bool = False

    def to_dict(self) -> dict:
        result: dict = {"english": self.english, "matched": self.matched}
        if self.chinese_simplified:
            result["chinese_simplified"] = self.chinese_simplified
        if self.japanese:
            result["japanese"] = self.japanese
        if self.wikidata_id:
            result["wikidata_id"] = self.wikidata_id
        if self.wikidata_english:
            result["wikidata_english"] = self.wikidata_english
        if self.wikidata_url:
            result["wikidata_url"] = self.wikidata_url
        return result


class WikidataClient:
    """Thin wrapper around the public Wikidata API."""

    def __init__(self) -> None:
        self.session = requests.Session()
        self.session.headers.update(REQUEST_HEADERS)
        self._last_request_time = 0.0

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep(MIN_REQUEST_INTERVAL - elapsed)

    def _get(self, params: dict[str, object]) -> dict:
        self._throttle()
        response = self.session.get(WIKIDATA_API_URL, params=params, timeout=20)
        self._last_request_time = time.monotonic()
        response.raise_for_status()
        return response.json()

    def _search_entities(self, game_name: str) -> list[dict]:
        payload = self._get({
            "action": "wbsearchentities",
            "format": "json",
            "language": "en",
            "uselang": "en",
            "type": "item",
            "limit": 10,
            "search": game_name,
        })
        results = payload.get("search", [])
        return results if isinstance(results, list) else []

    def _get_entity(self, qid: str) -> dict:
        payload = self._get({
            "action": "wbgetentities",
            "format": "json",
            "ids": qid,
            "props": "labels",
            "languages": "en|zh-cn|zh-hans|zh|ja",
        })
        entities = payload.get("entities", {})
        entity = entities.get(qid, {})
        return entity if isinstance(entity, dict) else {}

    def search_game_translations(self, game_name: str) -> WikidataTranslation:
        """Search Wikidata and return Chinese/Japanese labels."""
        result = WikidataTranslation(english=game_name)
        candidates = self._search_entities(game_name)
        if not candidates:
            return result

        matched = self._pick_best_match(game_name, candidates)
        if matched is None:
            return result

        qid = matched["id"]
        entity = self._get_entity(qid)
        labels = entity.get("labels", {})

        result.matched = True
        result.wikidata_id = qid
        result.wikidata_english = _extract_label(labels, "en") or matched["label"]
        result.wikidata_url = WIKIDATA_ENTITY_URL.format(qid=qid)
        result.chinese_simplified = (
            _extract_label(labels, "zh-cn")
            or _extract_label(labels, "zh-hans")
            or _extract_label(labels, "zh")
        )
        result.japanese = _extract_label(labels, "ja")
        return result

    def _pick_best_match(self, game_name: str, candidates: list[dict]) -> dict | None:
        exact_matches = self._collect_matches(game_name, candidates, normalize=False)
        exact_matches = self._prefer_game_entities(exact_matches)
        if len(exact_matches) == 1:
            return exact_matches[0]
        if len(exact_matches) > 1:
            return None

        normalized_matches = self._collect_matches(game_name, candidates, normalize=True)
        normalized_matches = self._prefer_game_entities(normalized_matches)
        if len(normalized_matches) == 1:
            return normalized_matches[0]
        return None

    def _collect_matches(
        self, game_name: str, candidates: list[dict], *, normalize: bool
    ) -> list[dict]:
        target = _normalize_name(game_name) if normalize else _exact_key(game_name)
        matches: list[dict] = []
        seen_ids: set[str] = set()

        for candidate in candidates:
            qid = candidate.get("id")
            label = candidate.get("label")
            if not isinstance(qid, str) or not isinstance(label, str):
                continue

            texts = [label]
            match_info = candidate.get("match", {})
            match_text = match_info.get("text") if isinstance(match_info, dict) else None
            if isinstance(match_text, str):
                texts.append(match_text)

            if not any(
                (_normalize_name(text) if normalize else _exact_key(text)) == target
                for text in texts
            ):
                continue

            if qid in seen_ids:
                continue

            matches.append({
                "id": qid,
                "label": html.unescape(label).strip(),
                "description": _clean_text(candidate.get("description")),
            })
            seen_ids.add(qid)

        return matches

    def _prefer_game_entities(self, matches: list[dict]) -> list[dict]:
        if len(matches) <= 1:
            return matches

        preferred = [
            match
            for match in matches
            if any(keyword in match["description"] for keyword in _GAME_DESCRIPTION_KEYWORDS)
        ]
        return preferred or matches


def _normalize_name(value: str) -> str:
    value = unicodedata.normalize("NFKC", html.unescape(value)).casefold().strip()
    value = value.translate(_PUNCT_TRANSLATION)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _exact_key(value: str) -> str:
    return unicodedata.normalize("NFKC", html.unescape(value)).casefold().strip()


def _extract_label(labels: object, language: str) -> str:
    if not isinstance(labels, dict):
        return ""
    label_info = labels.get(language, {})
    if not isinstance(label_info, dict):
        return ""
    value = label_info.get("value")
    return html.unescape(value).strip() if isinstance(value, str) else ""


def _clean_text(value: object) -> str:
    return html.unescape(value).casefold().strip() if isinstance(value, str) else ""
