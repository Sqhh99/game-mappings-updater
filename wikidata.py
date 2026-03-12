"""
Wikidata client for querying localized game titles.

Uses the public Wikidata API to:
1. Search entities by their English game name.
2. Fetch Chinese/Japanese labels for the matched entity.
"""

from __future__ import annotations

import html
import random
import re
import time
import unicodedata
from dataclasses import dataclass

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
WIKIDATA_ENTITY_URL = "https://www.wikidata.org/wiki/{qid}"

# Be conservative with the public Wikidata API.
MIN_REQUEST_INTERVAL = 1.0

REQUEST_HEADERS = {
    "User-Agent": "FLiNG-Downloader/0.2 (game-mappings-updater; Wikidata lookup)",
    "Accept": "application/json,text/plain,*/*",
}

REQUEST_TIMEOUT = (10, 30)
MAX_RETRIES = 5

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
    "action-adventure game",
    "survival horror",
    "visual novel",
    "beat 'em up",
    "hack and slash",
    "platform game",
    "tactical role-playing game",
    "real-time strategy",
    "turn-based strategy",
    "sandbox game",
    "indie game",
)

_TRADITIONAL_ONLY_CHARS = set(
    "萬與專業東絲兩嚴喪個豐為麗舉麼義烏樂喬習鄉書買亂爭於虧雲亞產畝親褻嚳"
    "團園圍國圖圓聖場塊壞壩壇壓壘墊墜墮壯聲壺壹備夠夢奧奪奮婦媽嫵嬌學寶實寧"
    "對導屆屬岡島峽嶺嶼巔幣幹廣庫廁廄廈廚廝廟廠廳張強彈彌彎彙後徑從復徵恆悅"
    "惡惱愛愜愴愷愾慄態慚慣慮戲戶拋挾捨掃掛採揀揚換損搖擔據擠擴擺擾攝攜敵數"
    "斂斃斕鬥斬斷於時晉晝暈曉曖曠會東條來楊極樓樣樞標樂樹橋機檔歐歡歷歸歲殘"
    "殤殼毀畢毆氣漢湧滅滯漁澀濃濕濟濤濫瀆瀕瀨瀾灑燈爐爭爲牆獄獨獅獸獻現產甕"
    "畫當疇癡發皺盜盡監盤矚礙禮禍禦稅穀穩窩竄競筆築籃籠糧糾紀紅紋納紙級紛絕"
    "結給絡統絲經綁綜綠綴緊線締編緣縣縫縮總績繩繪繫續纏纖纜缺罰罷羅羥義習翹"
    "聞聯聲聰肅脅脈脫腦腳臺與葉號艦艙藝節芻華莊著葷蒐蒙蓋蓮蔣蕭薩藍蘇處號虛"
    "蟲術衆衛衝補裝複襲覺覽觀觸計訂訃討訐訓記訛訝訟訪設許訴診註詁詐詔評詞詠"
    "詢試詩話該詳誅誇誌認誑誕誘誠說課誼調請諒論諭諮諱諷諸謀謁謂謄謊謎謙講謝"
    "謠謹譁證譜識譚讀變讓豈豎豐貓貝貞負財貢貧貨販貪貫責貯貴買貸費貼貿賀資賈"
    "賊賑賓賜賞賠賢賣質賴賺賽贈贊贏趙趕趨跡踐踴蹟蹤躉躍躋躑車軌軍農運過達違"
    "遙遞選遺遼還邁鄉鄒鄧鄭醜醫醬釀釁釋釐鐘鐵鑑鑒鑣長門開關闖闡隊階際陣陰陳"
    "陽險雜雞離難雲電靈靜頁頂頃項順須頌預頑頒領頜頡頤頸頹頻顆題額顏願顛類顧"
    "顯風飛飢飯飲飼飾餅餓館饋饒馬馮馭駐駛駝驅驚驗體髮鬆鬥鯉鯊鰻鱷鳥鳳鳴鴨鴻"
    "鵬麥黃黨點黴齊齒龍龜"
)


@dataclass
class WikidataTranslation:
    """Holds the Wikidata localization data for a single game."""

    english: str
    chinese_simplified: str = ""
    chinese_traditional: str = ""
    japanese: str = ""
    wikidata_id: str = ""
    wikidata_english: str = ""
    wikidata_url: str = ""
    matched: bool = False

    def to_dict(self) -> dict:
        result: dict = {"english": self.english, "matched": self.matched}
        if self.chinese_simplified:
            result["chinese_simplified"] = self.chinese_simplified
        if self.chinese_traditional:
            result["chinese_traditional"] = self.chinese_traditional
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
        self.session = self._build_session()
        self._last_request_time = 0.0

    def _build_session(self) -> requests.Session:
        session = requests.Session()
        session.headers.update(REQUEST_HEADERS)

        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=1.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=frozenset(["GET"]),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(
            max_retries=retry,
            pool_connections=10,
            pool_maxsize=10,
        )
        session.mount("https://", adapter)
        session.mount("http://", adapter)
        return session

    def _rebuild_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = self._build_session()

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self._last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            time.sleep((MIN_REQUEST_INTERVAL - elapsed) + random.uniform(0.0, 0.25))

    def _get(self, params: dict[str, object]) -> dict:
        delay = 2.0

        for attempt in range(MAX_RETRIES):
            try:
                self._throttle()
                response = self.session.get(
                    WIKIDATA_API_URL,
                    params=params,
                    timeout=REQUEST_TIMEOUT,
                )
                self._last_request_time = time.monotonic()
                response.raise_for_status()
                return response.json()

            except requests.exceptions.SSLError:
                if attempt == MAX_RETRIES - 1:
                    raise
                self._rebuild_session()
                time.sleep(delay + random.uniform(0.0, 1.0))
                delay = min(delay * 2, 30.0)

            except requests.exceptions.RequestException:
                if attempt == MAX_RETRIES - 1:
                    raise
                time.sleep(delay + random.uniform(0.0, 1.0))
                delay = min(delay * 2, 30.0)

        raise RuntimeError("Unexpected retry flow in WikidataClient._get")

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
            "languages": "en|zh-cn|zh-hans|zh-sg|zh-my|zh-tw|zh-hant|zh-hk|zh-mo|zh|ja",
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

        simplified = (
            _extract_label(labels, "zh-cn")
            or _extract_label(labels, "zh-hans")
            or _extract_label(labels, "zh-sg")
            or _extract_label(labels, "zh-my")
        )
        traditional = (
            _extract_label(labels, "zh-tw")
            or _extract_label(labels, "zh-hant")
            or _extract_label(labels, "zh-hk")
            or _extract_label(labels, "zh-mo")
        )
        generic = _extract_label(labels, "zh")

        if simplified:
            result.chinese_simplified = simplified
        elif generic:
            # If only generic zh exists, prefer keeping it as simplified output
            # rather than leaving Chinese empty.
            result.chinese_simplified = generic

        if traditional:
            result.chinese_traditional = traditional
        elif generic and _looks_traditional_chinese(generic):
            result.chinese_traditional = generic

        result.japanese = _extract_label(labels, "ja")
        return result

    def _pick_best_match(self, game_name: str, candidates: list[dict]) -> dict | None:
        target_exact = _exact_key(game_name)
        target_norm = _normalize_name(game_name)

        scored: list[dict] = []

        for candidate in candidates:
            qid = candidate.get("id")
            label = candidate.get("label")

            if not isinstance(qid, str) or not isinstance(label, str):
                continue

            description = _clean_text(candidate.get("description"))

            texts = [label]
            match_info = candidate.get("match", {})
            if isinstance(match_info, dict):
                match_text = match_info.get("text")
                if isinstance(match_text, str) and match_text.strip():
                    texts.append(match_text)

            score = 0

            for text in texts:
                if _exact_key(text) == target_exact:
                    score += 100
                if _normalize_name(text) == target_norm:
                    score += 60

            if any(keyword in description for keyword in _GAME_DESCRIPTION_KEYWORDS):
                score += 25

            # Small preference for candidates whose label is not extremely short/noisy.
            if len(label.strip()) >= 3:
                score += 1

            if score <= 0:
                continue

            scored.append({
                "id": qid,
                "label": html.unescape(label).strip(),
                "description": description,
                "score": score,
            })

        if not scored:
            return None

        scored.sort(key=lambda item: item["score"], reverse=True)

        if len(scored) >= 2 and scored[0]["score"] == scored[1]["score"]:
            # Ambiguous best result; safer to skip.
            return None

        return scored[0]


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


def _looks_traditional_chinese(value: str) -> bool:
    return any(ch in _TRADITIONAL_ONLY_CHARS for ch in value)