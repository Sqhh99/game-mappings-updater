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

SOURCE_PRIORITY = {
    "fling": 100,
    "igdb": 90,
    "steam": 80,
    "wikidata": 70,
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
        _insert_metadata(conn, trainers, source_records)
        _insert_games(conn, games)
        _insert_source_records(conn, games, source_records)
        _insert_aliases(conn, games, source_records)
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
            chinese_simplified TEXT NOT NULL DEFAULT '',
            chinese_traditional TEXT NOT NULL DEFAULT '',
            japanese TEXT NOT NULL DEFAULT '',
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
            source_priority INTEGER NOT NULL,
            FOREIGN KEY (english) REFERENCES games(english) ON DELETE CASCADE,
            UNIQUE (english, alias, language, source)
        );

        CREATE INDEX idx_game_aliases_lookup
            ON game_aliases(normalized_alias, language, source_priority DESC);
        CREATE INDEX idx_game_aliases_english
            ON game_aliases(english, language, source_priority DESC);
        """
    )


def _insert_metadata(
    conn: sqlite3.Connection,
    trainers: list[dict],
    source_records: dict[str, dict[str, dict]],
) -> None:
    games = _build_games(trainers)
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

            rows.append({
                "english": english,
                "source": source,
                "matched": 1 if entry.get("matched") else 0,
                "chinese_simplified": _clean_text(entry.get("chinese_simplified")),
                "chinese_traditional": _clean_text(entry.get("chinese_traditional")),
                "japanese": _clean_text(entry.get("japanese")),
                "external_id": _extract_external_id(source, entry),
                "external_name": _extract_external_name(source, entry),
                "external_url": _extract_external_url(source, entry),
                "payload_json": json.dumps(entry, ensure_ascii=False, sort_keys=True),
            })

    conn.executemany(
        """
        INSERT INTO source_records(
            english, source, matched, chinese_simplified, chinese_traditional,
            japanese, external_id, external_name, external_url, payload_json
        )
        VALUES(
            :english, :source, :matched, :chinese_simplified, :chinese_traditional,
            :japanese, :external_id, :external_name, :external_url, :payload_json
        )
        """,
        rows,
    )


def _insert_aliases(
    conn: sqlite3.Connection,
    games: dict[str, dict],
    source_records: dict[str, dict[str, dict]],
) -> None:
    aliases: list[dict] = []
    seen: set[tuple[str, str, str, str]] = set()

    for english, game in games.items():
        _append_alias(
            aliases,
            seen,
            english=english,
            alias=game["english"],
            language="en",
            source="fling",
            is_primary=True,
        )

    for source, records in source_records.items():
        for english, entry in records.items():
            if english not in games:
                continue
            if not entry.get("matched"):
                continue

            _append_alias(
                aliases,
                seen,
                english=english,
                alias=entry.get("english"),
                language="en",
                source=source,
                is_primary=(source == "igdb"),
            )

            if source == "steam":
                _append_alias(
                    aliases,
                    seen,
                    english=english,
                    alias=entry.get("steam_english"),
                    language="en",
                    source=source,
                    is_primary=False,
                )
            elif source == "wikidata":
                _append_alias(
                    aliases,
                    seen,
                    english=english,
                    alias=entry.get("wikidata_english"),
                    language="en",
                    source=source,
                    is_primary=False,
                )

            _append_alias(
                aliases,
                seen,
                english=english,
                alias=entry.get("chinese_simplified"),
                language="zh-Hans",
                source=source,
                is_primary=True,
            )
            _append_alias(
                aliases,
                seen,
                english=english,
                alias=entry.get("chinese_traditional"),
                language="zh-Hant",
                source=source,
                is_primary=True,
            )
            _append_alias(
                aliases,
                seen,
                english=english,
                alias=entry.get("japanese"),
                language="ja",
                source=source,
                is_primary=True,
            )

    conn.executemany(
        """
        INSERT INTO game_aliases(
            english, alias, normalized_alias, language, source, is_primary, source_priority
        )
        VALUES(
            :english, :alias, :normalized_alias, :language, :source, :is_primary, :source_priority
        )
        """,
        aliases,
    )


def _append_alias(
    aliases: list[dict],
    seen: set[tuple[str, str, str, str]],
    *,
    english: str,
    alias: object,
    language: str,
    source: str,
    is_primary: bool,
) -> None:
    clean_alias = _clean_text(alias)
    if not clean_alias:
        return

    key = (english, clean_alias, language, source)
    if key in seen:
        return

    seen.add(key)
    aliases.append({
        "english": english,
        "alias": clean_alias,
        "normalized_alias": _normalize_text(clean_alias),
        "language": language,
        "source": source,
        "is_primary": 1 if is_primary else 0,
        "source_priority": SOURCE_PRIORITY[source],
    })


def _extract_external_id(source: str, entry: dict) -> str:
    key_map = {
        "igdb": "igdb_id",
        "steam": "steam_appid",
        "wikidata": "wikidata_id",
    }
    key = key_map[source]
    value = entry.get(key)
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
