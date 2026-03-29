"""
Build a SQLite database from manual game mappings and FLiNG scrape outputs.
"""

from __future__ import annotations

import html
import json
import re
import sqlite3
import unicodedata
from datetime import datetime, timezone
from pathlib import Path

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

_TRAINER_SUFFIXES = (
    " Trainer Updated",
    " Trainer x64",
    " Trainer 64 Bit",
    " (Trainer",
    " Trainer",
)


def build_sqlite_database(output_dir: Path) -> Path:
    """Build the SQLite database from JSON outputs in ``output_dir``."""
    output_dir.mkdir(parents=True, exist_ok=True)

    manual_rows = _load_json_list(output_dir / "game_mappings_manual.json", required=True)
    fling_game_names = _load_string_list(output_dir / "fling_game_names.json", required=True)
    trainers = _load_json_list(output_dir / "fling_all_trainers.json", required=True)

    manual_index, manual_stats = _load_manual_mappings(manual_rows)
    trainer_index = _build_trainer_index(trainers)
    existing_first_seen = _load_existing_first_seen(output_dir / "fling_translations.db")

    generated_at = _utc_now()
    games = _build_games(
        manual_index=manual_index,
        manual_stats=manual_stats,
        fling_game_names=fling_game_names,
        trainer_index=trainer_index,
        existing_first_seen=existing_first_seen,
        generated_at=generated_at,
    )

    db_path = output_dir / "fling_translations.db"
    temp_path = output_dir / "fling_translations.db.tmp"
    if temp_path.exists():
        temp_path.unlink()

    conn = sqlite3.connect(temp_path)
    try:
        _initialize_schema(conn)
        _insert_metadata(
            conn,
            generated_at=generated_at,
            manual_rows=manual_rows,
            manual_index=manual_index,
            manual_stats=manual_stats,
            fling_game_names=fling_game_names,
            trainers=trainers,
            games=games,
        )
        _insert_games(conn, games)
        _create_views(conn)
        conn.commit()
    finally:
        conn.close()

    temp_path.replace(db_path)
    return db_path


def _load_json_list(path: Path, *, required: bool) -> list:
    if not path.exists():
        if required:
            raise FileNotFoundError(f"Required file not found: {path}")
        return []

    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError(f"Expected JSON list in {path}")
    return data


def _load_string_list(path: Path, *, required: bool) -> list[str]:
    values = _load_json_list(path, required=required)
    result: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = _clean_text(value)
        if not clean or clean in seen:
            continue
        seen.add(clean)
        result.append(clean)
    return result


def _load_manual_mappings(rows: list[dict]) -> tuple[dict[str, dict], dict[str, int]]:
    manual_index: dict[str, dict] = {}
    duplicate_rows = 0
    conflict_games = 0

    grouped: dict[str, list[dict]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        english = _clean_text(row.get("en"))
        if not english:
            continue
        grouped.setdefault(english, []).append(row)

    for english, entries in grouped.items():
        duplicate_rows += max(0, len(entries) - 1)

        merged_zh = ""
        merged_ja = ""
        zh_variants: set[str] = set()
        ja_variants: set[str] = set()

        for entry in entries:
            zh = _clean_text(entry.get("zh"))
            ja = _clean_text(entry.get("ja"))

            if zh:
                merged_zh = zh
                zh_variants.add(_normalize_mapping_value(zh))
            if ja:
                merged_ja = ja
                ja_variants.add(_normalize_mapping_value(ja))

        manual_conflict = len(zh_variants) > 1 or len(ja_variants) > 1
        if manual_conflict:
            conflict_games += 1

        manual_index[english] = {
            "english": english,
            "chinese_simplified": merged_zh,
            "japanese": merged_ja,
            "manual_entry_count": len(entries),
            "manual_conflict": 1 if manual_conflict else 0,
        }

    return manual_index, {
        "manual_rows": len(rows),
        "manual_unique_games": len(manual_index),
        "manual_duplicate_rows": duplicate_rows,
        "manual_conflict_games": conflict_games,
    }


def _build_trainer_index(trainers: list[dict]) -> dict[str, dict]:
    trainer_index: dict[str, dict] = {}
    for trainer in trainers:
        if not isinstance(trainer, dict):
            continue

        trainer_name = _clean_text(trainer.get("name"))
        trainer_url = _clean_text(trainer.get("url"))
        trainer_source = _clean_text(trainer.get("source"))
        english = _extract_game_name(trainer_name)

        if not english or english in trainer_index:
            continue

        trainer_index[english] = {
            "trainer_name": trainer_name,
            "trainer_url": trainer_url,
            "trainer_source": trainer_source,
        }

    return trainer_index


def _load_existing_first_seen(db_path: Path) -> dict[str, str]:
    if not db_path.exists():
        return {}

    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.Error:
        return {}

    try:
        columns = {
            row[1]
            for row in conn.execute("PRAGMA table_info(games)")
        }
        if "english" not in columns or "first_seen_at" not in columns:
            return {}

        result: dict[str, str] = {}
        for english, first_seen_at in conn.execute(
            "SELECT english, first_seen_at FROM games"
        ):
            clean_english = _clean_text(english)
            clean_first_seen = _clean_text(first_seen_at)
            if clean_english and clean_first_seen:
                result[clean_english] = clean_first_seen
        return result
    except sqlite3.Error:
        return {}
    finally:
        conn.close()


def _build_games(
    *,
    manual_index: dict[str, dict],
    manual_stats: dict[str, int],
    fling_game_names: list[str],
    trainer_index: dict[str, dict],
    existing_first_seen: dict[str, str],
    generated_at: str,
) -> list[dict]:
    ordered_english = _build_ordered_english_list(
        fling_game_names=fling_game_names,
        trainer_index=trainer_index,
        manual_index=manual_index,
    )

    fling_name_set = set(fling_game_names)
    trainer_set = set(trainer_index)

    rows: list[dict] = []
    for english in ordered_english:
        manual = manual_index.get(english, {})
        trainer = trainer_index.get(english, {})

        has_manual_mapping = english in manual_index
        has_fling_name = english in fling_name_set
        has_trainer = english in trainer_set

        chinese_simplified = _clean_text(manual.get("chinese_simplified"))
        japanese = _clean_text(manual.get("japanese"))
        missing_mapping = not has_manual_mapping
        missing_chinese = 1 if has_manual_mapping and not chinese_simplified else 0
        missing_japanese = 1 if has_manual_mapping and not japanese else 0
        manual_conflict = int(manual.get("manual_conflict", 0))

        status = _determine_status(
            has_manual_mapping=has_manual_mapping,
            has_fling_name=has_fling_name,
            has_trainer=has_trainer,
            missing_chinese=bool(missing_chinese),
            missing_japanese=bool(missing_japanese),
        )

        rows.append({
            "english": english,
            "normalized_english": _normalize_text(english),
            "chinese_simplified": chinese_simplified,
            "japanese": japanese,
            "trainer_name": _clean_text(trainer.get("trainer_name")),
            "trainer_url": _clean_text(trainer.get("trainer_url")),
            "trainer_source": _clean_text(trainer.get("trainer_source")),
            "has_manual_mapping": 1 if has_manual_mapping else 0,
            "has_fling_name": 1 if has_fling_name else 0,
            "has_trainer": 1 if has_trainer else 0,
            "manual_entry_count": int(manual.get("manual_entry_count", 0)),
            "manual_conflict": manual_conflict,
            "missing_mapping": 1 if missing_mapping else 0,
            "missing_chinese": missing_chinese,
            "missing_japanese": missing_japanese,
            "status": status,
            "first_seen_at": existing_first_seen.get(english, generated_at),
            "last_seen_at": generated_at,
        })

    _ = manual_stats  # kept for signature clarity and future use
    return rows


def _build_ordered_english_list(
    *,
    fling_game_names: list[str],
    trainer_index: dict[str, dict],
    manual_index: dict[str, dict],
) -> list[str]:
    ordered: list[str] = []
    seen: set[str] = set()

    def add(value: str) -> None:
        clean = _clean_text(value)
        if not clean or clean in seen:
            return
        seen.add(clean)
        ordered.append(clean)

    for english in fling_game_names:
        add(english)
    for english in trainer_index:
        add(english)
    for english in manual_index:
        add(english)

    return ordered


def _determine_status(
    *,
    has_manual_mapping: bool,
    has_fling_name: bool,
    has_trainer: bool,
    missing_chinese: bool,
    missing_japanese: bool,
) -> str:
    if not has_manual_mapping:
        return "missing_manual_mapping"
    if not has_fling_name and not has_trainer:
        return "manual_only"
    if missing_chinese and missing_japanese:
        return "missing_translations"
    if missing_chinese:
        return "missing_chinese"
    if missing_japanese:
        return "missing_japanese"
    return "ok"


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
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            english TEXT NOT NULL UNIQUE,
            normalized_english TEXT NOT NULL,
            chinese_simplified TEXT NOT NULL DEFAULT '',
            japanese TEXT NOT NULL DEFAULT '',
            trainer_name TEXT NOT NULL DEFAULT '',
            trainer_url TEXT NOT NULL DEFAULT '',
            trainer_source TEXT NOT NULL DEFAULT '',
            has_manual_mapping INTEGER NOT NULL DEFAULT 0,
            has_fling_name INTEGER NOT NULL DEFAULT 0,
            has_trainer INTEGER NOT NULL DEFAULT 0,
            manual_entry_count INTEGER NOT NULL DEFAULT 0,
            manual_conflict INTEGER NOT NULL DEFAULT 0,
            missing_mapping INTEGER NOT NULL DEFAULT 0,
            missing_chinese INTEGER NOT NULL DEFAULT 0,
            missing_japanese INTEGER NOT NULL DEFAULT 0,
            status TEXT NOT NULL,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL
        );

        CREATE INDEX idx_games_status ON games(status);
        CREATE INDEX idx_games_missing_mapping ON games(missing_mapping);
        CREATE INDEX idx_games_english ON games(english);
        """
    )


def _insert_metadata(
    conn: sqlite3.Connection,
    *,
    generated_at: str,
    manual_rows: list[dict],
    manual_index: dict[str, dict],
    manual_stats: dict[str, int],
    fling_game_names: list[str],
    trainers: list[dict],
    games: list[dict],
) -> None:
    rows = [
        ("build_generated_at", generated_at),
        ("manual_rows", str(len(manual_rows))),
        ("manual_unique_games", str(len(manual_index))),
        ("manual_duplicate_rows", str(manual_stats["manual_duplicate_rows"])),
        ("manual_conflict_games", str(manual_stats["manual_conflict_games"])),
        ("fling_game_names_count", str(len(fling_game_names))),
        ("fling_trainers_count", str(len(trainers))),
        ("games_count", str(len(games))),
    ]
    conn.executemany("INSERT INTO metadata(key, value) VALUES(?, ?)", rows)


def _insert_games(conn: sqlite3.Connection, games: list[dict]) -> None:
    conn.executemany(
        """
        INSERT INTO games(
            english, normalized_english,
            chinese_simplified, japanese,
            trainer_name, trainer_url, trainer_source,
            has_manual_mapping, has_fling_name, has_trainer,
            manual_entry_count, manual_conflict,
            missing_mapping, missing_chinese, missing_japanese,
            status, first_seen_at, last_seen_at
        )
        VALUES(
            :english, :normalized_english,
            :chinese_simplified, :japanese,
            :trainer_name, :trainer_url, :trainer_source,
            :has_manual_mapping, :has_fling_name, :has_trainer,
            :manual_entry_count, :manual_conflict,
            :missing_mapping, :missing_chinese, :missing_japanese,
            :status, :first_seen_at, :last_seen_at
        )
        """,
        games,
    )


def _create_views(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE VIEW db_status AS
        SELECT
            (SELECT value FROM metadata WHERE key = 'build_generated_at') AS build_generated_at,
            COUNT(*) AS total_games,
            SUM(CASE WHEN status = 'ok' THEN 1 ELSE 0 END) AS ok_games,
            SUM(CASE WHEN missing_mapping = 1 THEN 1 ELSE 0 END) AS missing_mapping_games,
            SUM(CASE WHEN missing_chinese = 1 THEN 1 ELSE 0 END) AS missing_chinese_games,
            SUM(CASE WHEN missing_japanese = 1 THEN 1 ELSE 0 END) AS missing_japanese_games,
            SUM(CASE WHEN status = 'manual_only' THEN 1 ELSE 0 END) AS manual_only_games,
            SUM(CASE WHEN manual_conflict = 1 THEN 1 ELSE 0 END) AS manual_conflict_games
        FROM games;

        CREATE VIEW needs_review AS
        SELECT
            english,
            chinese_simplified,
            japanese,
            trainer_name,
            trainer_url,
            has_manual_mapping,
            has_fling_name,
            has_trainer,
            manual_entry_count,
            manual_conflict,
            missing_mapping,
            missing_chinese,
            missing_japanese,
            status,
            first_seen_at,
            last_seen_at
        FROM games
        WHERE status != 'ok' OR manual_conflict = 1
        ORDER BY
            CASE status
                WHEN 'missing_manual_mapping' THEN 1
                WHEN 'missing_translations' THEN 2
                WHEN 'missing_chinese' THEN 3
                WHEN 'missing_japanese' THEN 4
                WHEN 'manual_only' THEN 5
                ELSE 6
            END,
            manual_conflict DESC,
            english ASC;
        """
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _clean_text(value: object) -> str:
    return html.unescape(value).strip() if isinstance(value, str) else ""


def _normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", html.unescape(value)).casefold().strip()
    normalized = normalized.translate(_PUNCT_TRANSLATION)
    normalized = re.sub(r"\s+", " ", normalized)
    return normalized.strip()


def _normalize_mapping_value(value: str) -> str:
    return re.sub(r"\s+", "", _normalize_text(value))


def _extract_game_name(trainer_name: str) -> str:
    for suffix in _TRAINER_SUFFIXES:
        if trainer_name.endswith(suffix):
            return trainer_name[: -len(suffix)].strip()
    return trainer_name.strip()
