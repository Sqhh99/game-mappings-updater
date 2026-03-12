"""
Build a SQLite database from the generated FLiNG translation JSON files.
"""

from __future__ import annotations

import html
import json
import re
import sqlite3
import unicodedata
from pathlib import Path

SOURCE_QUALITY_BASE = {
    "igdb": 70,
    "steam": 55,
    "wikidata": 80,
}

BEST_ZH_PRIORITY = {
    "wikidata": 300,
    "steam": 200,
    "igdb": 100,
}

BEST_JA_PRIORITY = {
    "igdb": 300,
    "wikidata": 200,
    "steam": 100,
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
    "（": " ",
    "）": " ",
    "[": " ",
    "]": " ",
    "{": " ",
    "}": " ",
    "&": " and ",
    "+": " ",
})

_LEADING_TRAILING_SEPARATORS = " -–—_:：|/\\,.;·・()（）[]{}"
_LATIN_RE = re.compile(r"[A-Za-z]")
_LATIN_TOKEN_RE = re.compile(r"[A-Za-z][A-Za-z0-9]*")
_CJK_RE = re.compile(r"[\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")
_JAPANESE_RE = re.compile(r"[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uf900-\ufaff]")


def build_sqlite_database(output_dir: Path) -> Path:
    """Build the SQLite database from JSON outputs in ``output_dir``."""
    output_dir.mkdir(parents=True, exist_ok=True)

    trainers = _load_json(output_dir / "fling_all_trainers.json", required=True)
    games = _build_games(trainers)

    source_records = {
        "igdb": _load_records(
            output_dir / "fling_translations_igdb.json",
            required=True,
        ),
        "steam": _load_records(
            output_dir / "fling_translations_steam.json",
            required=True,
        ),
        "wikidata": _load_records(
            output_dir / "fling_translations_wikidata.json",
            required=True,
        ),
    }

    db_path = output_dir / "fling_translations.db"
    temp_path = output_dir / "fling_translations.db.tmp"
    if temp_path.exists():
        temp_path.unlink()

    conn = sqlite3.connect(temp_path)
    try:
        _initialize_schema(conn)
        _insert_metadata(conn, trainers, games, source_records)
        _insert_games(conn, games)
        _insert_source_records(conn, games, source_records)
        _create_best_translations_view(conn)
        _insert_aliases(conn, games)
        conn.commit()
    finally:
        conn.close()

    temp_path.replace(db_path)
    return db_path


def _load_json(path: Path, *, required: bool) -> list[dict]:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required file not found: {path}")
        return []
    data = json.loads(path.read_text(encoding="utf-8"))
    return data if isinstance(data, list) else []


def _load_records(path: Path, *, required: bool) -> dict[str, dict]:
    records = _load_json(path, required=required)
    result: dict[str, dict] = {}
    for entry in records:
        english = entry.get("english")
        if isinstance(english, str) and english:
            result[english] = entry
    return result


def _build_games(trainers: list[dict]) -> dict[str, dict]:
    games: dict[str, dict] = {}
    for trainer in trainers:
        trainer_name = trainer.get("name")
        trainer_url = trainer.get("url")
        trainer_source = trainer.get("source")
        if not isinstance(trainer_name, str) or not isinstance(trainer_url, str):
            continue

        english = _extract_game_name(trainer_name)
        if not english or english in games:
            continue

        games[english] = {
            "english": english,
            "trainer_name": trainer_name,
            "trainer_url": trainer_url,
            "trainer_source": trainer_source if isinstance(trainer_source, str) else "",
        }
    return games


def _initialize_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode = DELETE;
        PRAGMA foreign_keys = ON;

        CREATE TABLE metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );

        CREATE TABLE games (
            english TEXT PRIMARY KEY,
            trainer_name TEXT NOT NULL,
            trainer_url TEXT NOT NULL,
            trainer_source TEXT NOT NULL
        );

        CREATE TABLE source_records (
            english TEXT NOT NULL,
            source TEXT NOT NULL,
            matched INTEGER NOT NULL,
            chinese_simplified_raw TEXT NOT NULL DEFAULT '',
            chinese_simplified_clean TEXT NOT NULL DEFAULT '',
            chinese_traditional_raw TEXT NOT NULL DEFAULT '',
            chinese_traditional_clean TEXT NOT NULL DEFAULT '',
            japanese_raw TEXT NOT NULL DEFAULT '',
            japanese_clean TEXT NOT NULL DEFAULT '',
            has_english_leak INTEGER NOT NULL DEFAULT 0,
            quality_score INTEGER NOT NULL DEFAULT 0,
            external_id TEXT NOT NULL DEFAULT '',
            external_name TEXT NOT NULL DEFAULT '',
            external_url TEXT NOT NULL DEFAULT '',
            payload_json TEXT NOT NULL,
            PRIMARY KEY (english, source),
            FOREIGN KEY (english) REFERENCES games(english) ON DELETE CASCADE
        );

        CREATE TABLE game_aliases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            english TEXT NOT NULL,
            alias TEXT NOT NULL,
            normalized_alias TEXT NOT NULL,
            language TEXT NOT NULL,
            source TEXT NOT NULL,
            is_primary INTEGER NOT NULL DEFAULT 0,
            FOREIGN KEY (english) REFERENCES games(english) ON DELETE CASCADE,
            UNIQUE (english, alias, language)
        );

        CREATE INDEX idx_source_records_lookup
            ON source_records(english, source);
        CREATE INDEX idx_game_aliases_lookup
            ON game_aliases(normalized_alias, language);
        CREATE INDEX idx_game_aliases_english
            ON game_aliases(english, language);
        """
    )


def _insert_metadata(
    conn: sqlite3.Connection,
    trainers: list[dict],
    games: dict[str, dict],
    source_records: dict[str, dict[str, dict]],
) -> None:
    rows = [
        ("trainers_count", str(len(trainers))),
        ("games_count", str(len(games))),
        ("igdb_records", str(len(source_records["igdb"]))),
        ("steam_records", str(len(source_records["steam"]))),
        ("wikidata_records", str(len(source_records["wikidata"]))),
    ]
    conn.executemany("INSERT INTO metadata(key, value) VALUES(?, ?)", rows)


def _insert_games(conn: sqlite3.Connection, games: dict[str, dict]) -> None:
    conn.executemany(
        """
        INSERT INTO games(english, trainer_name, trainer_url, trainer_source)
        VALUES(:english, :trainer_name, :trainer_url, :trainer_source)
        """,
        games.values(),
    )


def _insert_source_records(
    conn: sqlite3.Connection,
    games: dict[str, dict],
    source_records: dict[str, dict[str, dict]],
) -> None:
    rows: list[dict] = []
    for source, records in source_records.items():
        for english in games:
            entry = records.get(english)
            if entry is None:
                continue

            localized = _extract_localized_fields(source, english, entry)
            rows.append({
                "english": english,
                "source": source,
                "matched": 1 if entry.get("matched") else 0,
                "chinese_simplified_raw": localized["chinese_simplified_raw"],
                "chinese_simplified_clean": localized["chinese_simplified_clean"],
                "chinese_traditional_raw": localized["chinese_traditional_raw"],
                "chinese_traditional_clean": localized["chinese_traditional_clean"],
                "japanese_raw": localized["japanese_raw"],
                "japanese_clean": localized["japanese_clean"],
                "has_english_leak": 1 if localized["has_english_leak"] else 0,
                "quality_score": localized["quality_score"],
                "external_id": _extract_external_id(source, entry),
                "external_name": _extract_external_name(source, entry),
                "external_url": _extract_external_url(source, entry),
                "payload_json": json.dumps(entry, ensure_ascii=False, sort_keys=True),
            })

    conn.executemany(
        """
        INSERT INTO source_records(
            english, source, matched,
            chinese_simplified_raw, chinese_simplified_clean,
            chinese_traditional_raw, chinese_traditional_clean,
            japanese_raw, japanese_clean,
            has_english_leak, quality_score,
            external_id, external_name, external_url, payload_json
        )
        VALUES(
            :english, :source, :matched,
            :chinese_simplified_raw, :chinese_simplified_clean,
            :chinese_traditional_raw, :chinese_traditional_clean,
            :japanese_raw, :japanese_clean,
            :has_english_leak, :quality_score,
            :external_id, :external_name, :external_url, :payload_json
        )
        """,
        rows,
    )


def _create_best_translations_view(conn: sqlite3.Connection) -> None:
    conn.executescript(
        f"""
        CREATE VIEW best_translations AS
        WITH zh_candidates AS (
            SELECT
                english,
                source,
                chinese_simplified_clean AS value,
                CASE source
                    WHEN 'wikidata' THEN {BEST_ZH_PRIORITY['wikidata']}
                    WHEN 'steam' THEN {BEST_ZH_PRIORITY['steam']}
                    WHEN 'igdb' THEN {BEST_ZH_PRIORITY['igdb']}
                    ELSE 0
                END AS source_rank,
                quality_score
            FROM source_records
            WHERE chinese_simplified_clean != ''
        ),
        ja_candidates AS (
            SELECT
                english,
                source,
                japanese_clean AS value,
                CASE source
                    WHEN 'igdb' THEN {BEST_JA_PRIORITY['igdb']}
                    WHEN 'wikidata' THEN {BEST_JA_PRIORITY['wikidata']}
                    WHEN 'steam' THEN {BEST_JA_PRIORITY['steam']}
                    ELSE 0
                END AS source_rank,
                quality_score
            FROM source_records
            WHERE japanese_clean != ''
        )
        SELECT
            g.english,
            (
                SELECT value
                FROM zh_candidates z
                WHERE z.english = g.english
                ORDER BY z.source_rank DESC, z.quality_score DESC, z.source ASC
                LIMIT 1
            ) AS best_zh,
            (
                SELECT source
                FROM zh_candidates z
                WHERE z.english = g.english
                ORDER BY z.source_rank DESC, z.quality_score DESC, z.source ASC
                LIMIT 1
            ) AS best_zh_source,
            (
                SELECT value
                FROM ja_candidates j
                WHERE j.english = g.english
                ORDER BY j.source_rank DESC, j.quality_score DESC, j.source ASC
                LIMIT 1
            ) AS best_ja,
            (
                SELECT source
                FROM ja_candidates j
                WHERE j.english = g.english
                ORDER BY j.source_rank DESC, j.quality_score DESC, j.source ASC
                LIMIT 1
            ) AS best_ja_source
        FROM games g;
        """
    )


def _insert_aliases(conn: sqlite3.Connection, games: dict[str, dict]) -> None:
    aliases: list[dict] = []
    seen: set[tuple[str, str, str]] = set()

    for english in games:
        _append_alias(
            aliases,
            seen,
            english=english,
            alias=english,
            language="en",
            source="fling",
        )

    for row in conn.execute(
        "SELECT english, best_zh, best_zh_source, best_ja, best_ja_source FROM best_translations"
    ):
        english, best_zh, best_zh_source, best_ja, best_ja_source = row
        _append_alias(
            aliases,
            seen,
            english=english,
            alias=best_zh,
            language="zh",
            source=best_zh_source or "",
        )
        _append_alias(
            aliases,
            seen,
            english=english,
            alias=best_ja,
            language="ja",
            source=best_ja_source or "",
        )

    conn.executemany(
        """
        INSERT INTO game_aliases(
            english, alias, normalized_alias, language, source, is_primary
        )
        VALUES(
            :english, :alias, :normalized_alias, :language, :source, :is_primary
        )
        """,
        aliases,
    )


def _append_alias(
    aliases: list[dict],
    seen: set[tuple[str, str, str]],
    *,
    english: str,
    alias: object,
    language: str,
    source: str,
) -> None:
    clean_alias = _clean_text(alias)
    if not clean_alias:
        return

    key = (english, clean_alias, language)
    if key in seen:
        return

    seen.add(key)
    aliases.append({
        "english": english,
        "alias": clean_alias,
        "normalized_alias": _normalize_text(clean_alias),
        "language": language,
        "source": source,
        "is_primary": 1,
    })


def _extract_localized_fields(source: str, english: str, entry: dict) -> dict:
    zh_raw = _clean_text(entry.get("chinese_simplified"))
    zht_raw = _clean_text(entry.get("chinese_traditional"))
    ja_raw = _clean_text(entry.get("japanese"))

    zh_clean, zh_leak = _clean_localized_value(zh_raw, english, "zh")
    zht_clean, zht_leak = _clean_localized_value(zht_raw, english, "zh")
    ja_clean, ja_leak = _clean_localized_value(ja_raw, english, "ja")

    has_english_leak = zh_leak or zht_leak or ja_leak
    quality_score = _compute_quality_score(
        source=source,
        matched=bool(entry.get("matched")),
        zh_clean=zh_clean,
        zht_clean=zht_clean,
        ja_clean=ja_clean,
        has_english_leak=has_english_leak,
    )

    return {
        "chinese_simplified_raw": zh_raw,
        "chinese_simplified_clean": zh_clean,
        "chinese_traditional_raw": zht_raw,
        "chinese_traditional_clean": zht_clean,
        "japanese_raw": ja_raw,
        "japanese_clean": ja_clean,
        "has_english_leak": has_english_leak,
        "quality_score": quality_score,
    }


def _clean_localized_value(text: str, english: str, language: str) -> tuple[str, bool]:
    if not text:
        return "", False

    cleaned = _strip_embedded_english(text, english)
    cleaned = _trim_localized_text(cleaned)

    if language == "zh":
        if _contains_cjk(cleaned) and not _contains_disallowed_latin(cleaned):
            return cleaned, False
        if _contains_cjk(cleaned):
            stripped = _remove_latin_chunks(cleaned)
            if _contains_cjk(stripped) and not _contains_disallowed_latin(stripped):
                return stripped, False
            return "", True
        if _contains_cjk(text):
            stripped = _remove_latin_chunks(text)
            if _contains_cjk(stripped) and not _contains_disallowed_latin(stripped):
                return stripped, False
        return "", _contains_disallowed_latin(text)

    if language == "ja":
        if _contains_japanese(cleaned) and not _contains_disallowed_latin(cleaned):
            return cleaned, False
        if _contains_japanese(cleaned):
            stripped = _remove_latin_chunks(cleaned)
            if _contains_japanese(stripped) and not _contains_disallowed_latin(stripped):
                return stripped, False
        return "", _contains_disallowed_latin(text)

    return cleaned, False


def _strip_embedded_english(text: str, english: str) -> str:
    if not text or not english:
        return text

    result = text
    variants = _english_variants(english)
    for variant in variants:
        pattern = re.escape(variant)
        result = re.sub(
            rf"[（(]\s*{pattern}\s*[)）]",
            " ",
            result,
            flags=re.IGNORECASE,
        )
        result = re.sub(pattern, " ", result, flags=re.IGNORECASE)

    return result


def _english_variants(english: str) -> list[str]:
    variants = {_clean_text(english)}
    simplified = _clean_text(english).replace("™", "").replace("®", "").replace("©", "")
    variants.add(simplified.strip())
    return [v for v in variants if v]


def _remove_latin_chunks(text: str) -> str:
    result = re.sub(r"[A-Za-z0-9][A-Za-z0-9\s:：'’‘\"“”\-\–\—_\./\\!！\?？&+]*", " ", text)
    return _trim_localized_text(result)


def _trim_localized_text(text: str) -> str:
    text = re.sub(r"\s+", " ", text).strip()
    return text.strip(_LEADING_TRAILING_SEPARATORS).strip()


def _compute_quality_score(
    *,
    source: str,
    matched: bool,
    zh_clean: str,
    zht_clean: str,
    ja_clean: str,
    has_english_leak: bool,
) -> int:
    score = SOURCE_QUALITY_BASE[source]
    if matched:
        score += 10
    else:
        score -= 20

    if zh_clean:
        score += 12
    if zht_clean:
        score += 6
    if ja_clean:
        score += 10
    if has_english_leak:
        score -= 25

    return score


def _extract_external_id(source: str, entry: dict) -> str:
    key_map = {
        "igdb": "igdb_id",
        "steam": "steam_appid",
        "wikidata": "wikidata_id",
    }
    value = entry.get(key_map[source])
    if value is None:
        return ""
    return str(value)


def _extract_external_name(source: str, entry: dict) -> str:
    key_map = {
        "igdb": "english",
        "steam": "steam_english",
        "wikidata": "wikidata_english",
    }
    return _clean_text(entry.get(key_map[source]))


def _extract_external_url(source: str, entry: dict) -> str:
    key_map = {
        "igdb": "",
        "steam": "steam_url",
        "wikidata": "wikidata_url",
    }
    key = key_map[source]
    if not key:
        return ""
    return _clean_text(entry.get(key))


def _clean_text(value: object) -> str:
    return html.unescape(value).strip() if isinstance(value, str) else ""


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", html.unescape(value)).casefold().strip()
    normalized = normalized.translate(_PUNCT_TRANSLATION)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _contains_latin(value: str) -> bool:
    return bool(_LATIN_RE.search(value))


def _contains_disallowed_latin(value: str) -> bool:
    tokens = _LATIN_TOKEN_RE.findall(value)
    if not tokens:
        return False
    return any(not _is_allowed_latin_token(token) for token in tokens)


def _is_allowed_latin_token(token: str) -> bool:
    upper = token.upper()
    if len(upper) == 1:
        return True
    return all(ch in "IVXLCDM" for ch in upper)


def _contains_cjk(value: str) -> bool:
    return bool(_CJK_RE.search(value))


def _contains_japanese(value: str) -> bool:
    return bool(_JAPANESE_RE.search(value))


def _extract_game_name(trainer_name: str) -> str:
    for suffix in (
        " Trainer Updated",
        " Trainer x64",
        " Trainer 64 Bit",
        " (Trainer",
        " Trainer",
    ):
        if trainer_name.endswith(suffix):
            return trainer_name[: -len(suffix)].strip()
    return trainer_name.strip()
