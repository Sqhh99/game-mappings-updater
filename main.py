"""
game-mappings-updater — 从 flingtrainer.com 爬取所有修改器名称，
并通过 IGDB / Steam / Wikidata 获取官方中文/日文译名。

子命令:
  scrape           爬取 flingtrainer.com 所有修改器名称
  update           刷新 FLiNG 抓取结果、重建 SQLite，并导出缺失映射模板
  translate        通过 IGDB API 翻译游戏名为中文/日文
  translate-steam  通过 Steam 商店接口补充中文/日文标题
  translate-wikidata  通过 Wikidata API 补充中文/日文标题
  translate-all    并发执行 IGDB / Steam / Wikidata 翻译
  build-sqlite     将 manual 映射和 FLiNG 抓取结果汇总为 SQLite 数据库
  sqlite-status    查看 SQLite 数据库状态和待处理映射
  export-missing   导出缺失翻译映射模板 JSON
  import-missing   校验并导入补齐后的缺失映射 JSON
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
import json
import re
import sqlite3
import sys
from pathlib import Path
from typing import Callable

import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# Load .env before any module that reads env vars
load_dotenv(Path(__file__).resolve().parent / ".env")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://flingtrainer.com"
ALL_TRAINERS_URL = f"{BASE_URL}/all-trainers/"
ARCHIVE_URL = f"{BASE_URL}/trainer/my-trainers-archive/"

REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/131.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Referer": f"{BASE_URL}/",
}

OUTPUT_DIR = Path(__file__).resolve().parent / "output"
SQLITE_DB_PATH = OUTPUT_DIR / "fling_translations.db"
MANUAL_MAPPINGS_PATH = OUTPUT_DIR / "game_mappings_manual.json"
MISSING_MAPPINGS_PATH = OUTPUT_DIR / "game_mappings_missing.json"
MISSING_EXPORTABLE_STATUSES = (
    "missing_manual_mapping",
    "missing_chinese",
    "missing_japanese",
    "missing_translations",
)

# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def fetch_page(url: str) -> str:
    """Fetch a page and return its HTML content."""
    print(f"  Fetching {url} ...")
    resp = requests.get(url, headers=REQUEST_HEADERS, timeout=30)
    print(f"  Status {resp.status_code}, {len(resp.text):,} bytes")
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch {url}: HTTP {resp.status_code}")
    return resp.text


# ---------------------------------------------------------------------------
# Scrapers
# ---------------------------------------------------------------------------


def scrape_modern_trainers() -> list[dict]:
    """Scrape trainers from /all-trainers/ (post-2019.05)."""
    soup = BeautifulSoup(fetch_page(ALL_TRAINERS_URL), "html.parser")

    az_listing = soup.find("div", class_="az-listing")
    if az_listing is None:
        print("  WARNING: <div class='az-listing'> not found")
        return []

    trainers: list[dict] = []
    for a in az_listing.find_all("a", href=True):
        href: str = a["href"]
        name: str = a.get_text(strip=True)
        if "/trainer/" in href and name and not re.match(r"^[A-Z#]$", name):
            trainers.append({
                "name": html.unescape(name),
                "url": href,
                "source": "modern",
            })

    print(f"  → {len(trainers)} modern trainers")
    return trainers


def scrape_archived_trainers() -> list[dict]:
    """Scrape trainers from /trainer/my-trainers-archive/ (2012 – 2019.05)."""
    soup = BeautifulSoup(fetch_page(ARCHIVE_URL), "html.parser")

    entry = soup.find("div", class_="entry")
    if entry is None:
        print("  WARNING: <div class='entry'> not found")
        return []

    trainers: list[dict] = []
    for p in entry.find_all("p", style=True):
        if "padding-left" not in (p.get("style") or ""):
            continue
        raw = re.sub(r"<br\s*/?\s*>", "\n", str(p))
        text = BeautifulSoup(raw, "html.parser").get_text()
        for line in text.split("\n"):
            line = line.strip()
            if not line or len(line) < 3 or line.lower().startswith("list of"):
                continue
            trainers.append({
                "name": html.unescape(line),
                "url": "https://archive.flingtrainer.com/",
                "source": "archive",
            })

    print(f"  → {len(trainers)} archived trainers")
    return trainers


# ---------------------------------------------------------------------------
# Post-processing helpers
# ---------------------------------------------------------------------------

_TRAINER_SUFFIXES = [
    " Trainer Updated",
    " Trainer x64",
    " Trainer 64 Bit",
    " (Trainer",
    " Trainer",
]


def extract_game_name(trainer_name: str) -> str:
    """Strip the trailing ``Trainer`` (and variants) from a trainer name."""
    for suffix in _TRAINER_SUFFIXES:
        if trainer_name.endswith(suffix):
            return trainer_name[: -len(suffix)].strip()
    return trainer_name.strip()


def deduplicate(trainers: list[dict]) -> list[dict]:
    """Return a deduplicated list, preserving first-seen order."""
    seen: set[str] = set()
    result: list[dict] = []
    for t in trainers:
        if t["name"] not in seen:
            seen.add(t["name"])
            result.append(t)
    return result


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------


def cmd_scrape() -> None:
    """Scrape flingtrainer.com and write results to ``output/``."""
    print("[1/2] Scraping modern trainers …")
    modern = scrape_modern_trainers()

    print("[2/2] Scraping archived trainers …")
    archived = scrape_archived_trainers()

    all_trainers = deduplicate(modern + archived)
    modern_count = sum(1 for t in all_trainers if t["source"] == "modern")
    archive_count = len(all_trainers) - modern_count

    print()
    print(f"{'=' * 50}")
    print(f"Total unique trainers : {len(all_trainers)}")
    print(f"  Modern  (post-2019.05) : {modern_count}")
    print(f"  Archive (2012-2019.05) : {archive_count}")
    print(f"{'=' * 50}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    full_path = OUTPUT_DIR / "fling_all_trainers.json"
    full_path.write_text(
        json.dumps(all_trainers, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\n✔ Full list       → {full_path}")

    names = [extract_game_name(t["name"]) for t in all_trainers]
    names_path = OUTPUT_DIR / "fling_game_names.json"
    names_path.write_text(
        json.dumps(names, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"✔ Game names only → {names_path}")


def cmd_translate() -> None:
    """Translate game names to Chinese/Japanese using the IGDB API."""
    from igdb import IGDBClient  # lazy import to avoid import errors when not using

    names_path = OUTPUT_DIR / "fling_game_names.json"
    translations_path = OUTPUT_DIR / "fling_translations_igdb.json"

    # -- Load game names ---------------------------------------------------
    if not names_path.exists():
        print(f"ERROR: {names_path} not found. Run 'scrape' first.")
        sys.exit(1)

    game_names: list[str] = json.loads(names_path.read_text(encoding="utf-8"))
    print(f"Loaded {len(game_names)} game names from {names_path.name}")

    # -- Load existing cache (incremental) ---------------------------------
    cache: dict[str, dict] = {}
    if translations_path.exists():
        existing = json.loads(translations_path.read_text(encoding="utf-8"))
        for entry in existing:
            cache[entry["english"]] = entry
        print(f"Loaded {len(cache)} cached translations")

    # -- Query IGDB --------------------------------------------------------
    client = IGDBClient()
    client.authenticate()
    print("✔ IGDB authenticated\n")

    total = len(game_names)
    new_count = 0
    skip_count = 0
    fail_count = 0

    for i, name in enumerate(game_names, 1):
        # Skip if already cached
        if name in cache:
            skip_count += 1
            continue

        prefix = f"[{i}/{total}]"
        try:
            result = client.search_game_translations(name)
            cache[name] = result.to_dict()
            new_count += 1

            if result.matched:
                parts = []
                if result.chinese_simplified:
                    parts.append(f"zh={result.chinese_simplified}")
                if result.japanese:
                    parts.append(f"ja={result.japanese}")
                info = ", ".join(parts) if parts else "(no translations)"
                print(f"  {prefix} ✔ {name} → {info}")
            else:
                fail_count += 1
                print(f"  {prefix} ✘ {name} (no match)")
        except Exception as e:
            cache[name] = {"english": name, "matched": False, "error": str(e)}
            fail_count += 1
            print(f"  {prefix} ✘ {name} (error: {e})")

        # Save periodically (every 50 games)
        if new_count % 50 == 0:
            _save_translations(translations_path, cache, game_names)

    # -- Final save --------------------------------------------------------
    _save_translations(translations_path, cache, game_names)

    matched = sum(1 for v in cache.values() if v.get("matched"))
    has_zh = sum(1 for v in cache.values() if v.get("chinese_simplified"))
    has_ja = sum(1 for v in cache.values() if v.get("japanese"))

    print()
    print(f"{'=' * 50}")
    print(f"Total games      : {total}")
    print(f"  Matched (IGDB) : {matched}")
    print(f"  Has Chinese    : {has_zh}")
    print(f"  Has Japanese   : {has_ja}")
    print(f"  Skipped (cached): {skip_count}")
    print(f"  New queries    : {new_count}")
    print(f"{'=' * 50}")
    print(f"\n✔ Translations → {translations_path}")


def cmd_translate_steam() -> None:
    """Translate game names to Chinese/Japanese using the Steam store."""
    from steam import SteamClient  # lazy import to avoid import errors when not using

    names_path = OUTPUT_DIR / "fling_game_names.json"
    translations_path = OUTPUT_DIR / "fling_translations_steam.json"

    if not names_path.exists():
        print(f"ERROR: {names_path} not found. Run 'scrape' first.")
        sys.exit(1)

    game_names: list[str] = json.loads(names_path.read_text(encoding="utf-8"))
    print(f"Loaded {len(game_names)} game names from {names_path.name}")

    cache: dict[str, dict] = {}
    if translations_path.exists():
        existing = json.loads(translations_path.read_text(encoding="utf-8"))
        for entry in existing:
            if entry.get("matched"):
                cache[entry["english"]] = entry
        print(f"Loaded {len(cache)} cached matched Steam translations")

    client = SteamClient()
    print("✔ Steam client ready\n")

    total = len(game_names)
    new_count = 0
    skip_count = 0
    fail_count = 0

    for i, name in enumerate(game_names, 1):
        if name in cache:
            skip_count += 1
            continue

        prefix = f"[{i}/{total}]"
        try:
            result = client.search_game_translations(name)
            cache[name] = result.to_dict()
            new_count += 1

            if result.matched:
                parts = [f"appid={result.steam_appid}"]
                if result.chinese_simplified:
                    parts.append(f"zh={result.chinese_simplified}")
                if result.japanese:
                    parts.append(f"ja={result.japanese}")
                info = ", ".join(parts)
                print(f"  {prefix} ✔ {name} → {info}")
            else:
                fail_count += 1
                print(f"  {prefix} ✘ {name} (no match)")
        except Exception as e:
            cache[name] = {"english": name, "matched": False, "error": str(e)}
            fail_count += 1
            print(f"  {prefix} ✘ {name} (error: {e})")

        if new_count % 50 == 0:
            _save_translations(translations_path, cache, game_names)

    _save_translations(translations_path, cache, game_names)

    matched = sum(1 for v in cache.values() if v.get("matched"))
    has_zh = sum(1 for v in cache.values() if v.get("chinese_simplified"))
    has_ja = sum(1 for v in cache.values() if v.get("japanese"))

    print()
    print(f"{'=' * 50}")
    print(f"Total games       : {total}")
    print(f"  Matched (Steam) : {matched}")
    print(f"  Has Chinese     : {has_zh}")
    print(f"  Has Japanese    : {has_ja}")
    print(f"  Skipped (cached): {skip_count}")
    print(f"  New queries     : {new_count}")
    print(f"  Failed          : {fail_count}")
    print(f"{'=' * 50}")
    print(f"\n✔ Translations → {translations_path}")


def cmd_translate_wikidata() -> None:
    """Translate game names to Chinese/Japanese using Wikidata."""
    from wikidata import WikidataClient  # lazy import to avoid import errors when not using

    names_path = OUTPUT_DIR / "fling_game_names.json"
    translations_path = OUTPUT_DIR / "fling_translations_wikidata.json"

    if not names_path.exists():
        print(f"ERROR: {names_path} not found. Run 'scrape' first.")
        sys.exit(1)

    game_names: list[str] = json.loads(names_path.read_text(encoding="utf-8"))
    print(f"Loaded {len(game_names)} game names from {names_path.name}")

    cache: dict[str, dict] = {}
    if translations_path.exists():
        existing = json.loads(translations_path.read_text(encoding="utf-8"))
        for entry in existing:
            if entry.get("matched"):
                cache[entry["english"]] = entry
        print(f"Loaded {len(cache)} cached matched Wikidata translations")

    client = WikidataClient()
    print("✔ Wikidata client ready\n")

    total = len(game_names)
    new_count = 0
    skip_count = 0
    fail_count = 0

    for i, name in enumerate(game_names, 1):
        if name in cache:
            skip_count += 1
            continue

        prefix = f"[{i}/{total}]"
        try:
            result = client.search_game_translations(name)
            cache[name] = result.to_dict()
            new_count += 1

            if result.matched:
                parts = [f"qid={result.wikidata_id}"]
                if result.chinese_simplified:
                    parts.append(f"zh={result.chinese_simplified}")
                if result.japanese:
                    parts.append(f"ja={result.japanese}")
                info = ", ".join(parts)
                print(f"  {prefix} ✔ {name} → {info}")
            else:
                fail_count += 1
                print(f"  {prefix} ✘ {name} (no match)")
        except Exception as e:
            cache[name] = {"english": name, "matched": False, "error": str(e)}
            fail_count += 1
            print(f"  {prefix} ✘ {name} (error: {e})")

        if new_count % 50 == 0:
            _save_translations(translations_path, cache, game_names)

    _save_translations(translations_path, cache, game_names)

    matched = sum(1 for v in cache.values() if v.get("matched"))
    has_zh = sum(1 for v in cache.values() if v.get("chinese_simplified"))
    has_ja = sum(1 for v in cache.values() if v.get("japanese"))

    print()
    print(f"{'=' * 50}")
    print(f"Total games          : {total}")
    print(f"  Matched (Wikidata) : {matched}")
    print(f"  Has Chinese        : {has_zh}")
    print(f"  Has Japanese       : {has_ja}")
    print(f"  Skipped (cached)   : {skip_count}")
    print(f"  New queries        : {new_count}")
    print(f"  Failed             : {fail_count}")
    print(f"{'=' * 50}")
    print(f"\n✔ Translations → {translations_path}")


def cmd_build_sqlite() -> None:
    """Build a SQLite database from manual mappings and FLiNG scrape outputs."""
    from sqlite_export import build_sqlite_database

    try:
        db_path = build_sqlite_database(OUTPUT_DIR)
    except (FileNotFoundError, ValueError) as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"✔ SQLite database → {db_path}")


def cmd_update() -> None:
    """Refresh FLiNG data, rebuild SQLite, and export missing mappings."""
    print("[1/3] Refreshing FLiNG scrape outputs …")
    cmd_scrape()

    print("\n[2/3] Rebuilding SQLite database …")
    cmd_build_sqlite()

    print("\n[3/3] Exporting missing mappings …")
    export_path = cmd_export_missing(MISSING_MAPPINGS_PATH)
    print(f"✔ Update finished → {export_path}")


def cmd_export_missing(output_path: Path) -> Path:
    """Export missing mappings from SQLite into a JSON template."""
    rows = _load_missing_mapping_rows()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    exported = [
        {
            "en": english,
            "zh": chinese_simplified or "",
            "ja": japanese or "",
            "status": status,
            "trainer_name": trainer_name or "",
            "trainer_url": trainer_url or "",
        }
        for english, chinese_simplified, japanese, status, trainer_name, trainer_url in rows
    ]

    output_path.write_text(
        json.dumps(exported, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    print(f"✔ Missing mappings exported: {len(exported)}")
    print(f"✔ Missing mappings JSON → {output_path}")
    return output_path


def cmd_import_missing(input_path: Path, *, check_only: bool) -> None:
    """Validate and import missing mappings into the manual mappings file."""
    missing_rows = _load_missing_mapping_rows()
    missing_map = {
        english: {
            "zh": chinese_simplified or "",
            "ja": japanese or "",
            "status": status,
        }
        for english, chinese_simplified, japanese, status, _trainer_name, _trainer_url in missing_rows
    }

    imported_rows = _load_import_rows(input_path)
    validated = _validate_import_rows(imported_rows, missing_map, input_path)

    print(f"✔ Import file validated: {len(validated)} entries")
    if check_only:
        return

    manual_rows = _load_manual_rows(MANUAL_MAPPINGS_PATH)
    added_count, updated_count = _merge_import_rows(manual_rows, validated)

    MANUAL_MAPPINGS_PATH.write_text(
        json.dumps(manual_rows, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"✔ Manual mappings updated → {MANUAL_MAPPINGS_PATH}")
    print(f"  Added   : {added_count}")
    print(f"  Updated : {updated_count}")

    print("\n[1/2] Rebuilding SQLite database …")
    cmd_build_sqlite()
    print("\n[2/2] Exporting refreshed missing mappings …")
    cmd_export_missing(MISSING_MAPPINGS_PATH)


def _load_missing_mapping_rows() -> list[tuple[str, str, str, str, str, str]]:
    if not SQLITE_DB_PATH.exists():
        print(f"ERROR: {SQLITE_DB_PATH} not found. Run 'build-sqlite' first.")
        sys.exit(1)

    try:
        conn = sqlite3.connect(SQLITE_DB_PATH)
    except sqlite3.Error as e:
        print(f"ERROR: failed to open SQLite database: {e}")
        sys.exit(1)

    placeholders = ", ".join("?" for _ in MISSING_EXPORTABLE_STATUSES)
    try:
        rows = conn.execute(
            f"""
            SELECT
                english,
                chinese_simplified,
                japanese,
                status,
                trainer_name,
                trainer_url
            FROM games
            WHERE status IN ({placeholders})
            ORDER BY
                CASE status
                    WHEN 'missing_manual_mapping' THEN 1
                    WHEN 'missing_translations' THEN 2
                    WHEN 'missing_chinese' THEN 3
                    WHEN 'missing_japanese' THEN 4
                    ELSE 5
                END,
                english ASC
            """,
            MISSING_EXPORTABLE_STATUSES,
        ).fetchall()
    except sqlite3.Error as e:
        print(f"ERROR: failed to query missing mappings: {e}")
        sys.exit(1)
    finally:
        conn.close()

    return [
        (
            _clean_mapping_text(english),
            _clean_mapping_text(chinese_simplified),
            _clean_mapping_text(japanese),
            _clean_mapping_text(status),
            _clean_mapping_text(trainer_name),
            _clean_mapping_text(trainer_url),
        )
        for english, chinese_simplified, japanese, status, trainer_name, trainer_url in rows
    ]


def _load_manual_rows(path: Path) -> list[dict]:
    if not path.exists():
        print(f"ERROR: {path} not found.")
        sys.exit(1)

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"ERROR: invalid JSON in {path}: {e}")
        sys.exit(1)

    if not isinstance(data, list):
        print(f"ERROR: expected JSON array in {path}")
        sys.exit(1)

    normalized_rows: list[dict] = []
    for index, row in enumerate(data, 1):
        if not isinstance(row, dict):
            print(f"ERROR: manual row #{index} is not an object")
            sys.exit(1)
        normalized_rows.append({
            "en": _clean_mapping_text(row.get("en")),
            "zh": _clean_mapping_text(row.get("zh")),
            "ja": _clean_mapping_text(row.get("ja")),
        })
    return normalized_rows


def _load_import_rows(path: Path) -> list[dict]:
    if not path.exists():
        print(f"ERROR: import file not found: {path}")
        sys.exit(1)

    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        print(f"ERROR: invalid JSON in {path}: {e}")
        sys.exit(1)

    if not isinstance(data, list):
        print(f"ERROR: expected JSON array in {path}")
        sys.exit(1)

    return data


def _validate_import_rows(
    rows: list[dict],
    missing_map: dict[str, dict[str, str]],
    source_path: Path,
) -> list[dict[str, str]]:
    seen: set[str] = set()
    validated: list[dict[str, str]] = []

    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            print(f"ERROR: {source_path} row #{index} is not an object")
            sys.exit(1)

        missing_keys = [key for key in ("en", "zh", "ja") if key not in row]
        if missing_keys:
            print(
                f"ERROR: {source_path} row #{index} is missing keys: {', '.join(missing_keys)}"
            )
            sys.exit(1)

        english = _clean_mapping_text(row.get("en"))
        zh = _clean_mapping_text(row.get("zh"))
        ja = _clean_mapping_text(row.get("ja"))

        if not english:
            print(f"ERROR: {source_path} row #{index} has empty 'en'")
            sys.exit(1)
        if not isinstance(row.get("zh"), str) or not isinstance(row.get("ja"), str):
            print(f"ERROR: {source_path} row #{index} requires string 'zh' and 'ja'")
            sys.exit(1)
        if english in seen:
            print(f"ERROR: {source_path} has duplicate 'en': {english}")
            sys.exit(1)
        if english not in missing_map:
            print(f"ERROR: {source_path} row #{index} references unknown or non-missing game: {english}")
            sys.exit(1)

        status = missing_map[english]["status"]
        if status in {"missing_manual_mapping", "missing_translations"}:
            if not zh or not ja:
                print(
                    f"ERROR: {source_path} row #{index} for {english} requires both 'zh' and 'ja'"
                )
                sys.exit(1)
        elif status == "missing_chinese" and not zh:
            print(f"ERROR: {source_path} row #{index} for {english} requires non-empty 'zh'")
            sys.exit(1)
        elif status == "missing_japanese" and not ja:
            print(f"ERROR: {source_path} row #{index} for {english} requires non-empty 'ja'")
            sys.exit(1)

        seen.add(english)
        validated.append({
            "en": english,
            "zh": zh,
            "ja": ja,
        })

    return validated


def _merge_import_rows(
    manual_rows: list[dict],
    imported_rows: list[dict[str, str]],
) -> tuple[int, int]:
    index_by_english: dict[str, list[int]] = {}
    for idx, row in enumerate(manual_rows):
        english = _clean_mapping_text(row.get("en"))
        if not english:
            continue
        index_by_english.setdefault(english, []).append(idx)

    added_count = 0
    updated_count = 0

    for row in imported_rows:
        english = row["en"]
        zh = row["zh"]
        ja = row["ja"]

        if english not in index_by_english:
            manual_rows.append({"en": english, "zh": zh, "ja": ja})
            index_by_english[english] = [len(manual_rows) - 1]
            added_count += 1
            continue

        changed = False
        for idx in index_by_english[english]:
            manual_row = manual_rows[idx]
            current_zh = _clean_mapping_text(manual_row.get("zh"))
            current_ja = _clean_mapping_text(manual_row.get("ja"))

            if zh and not current_zh:
                manual_row["zh"] = zh
                changed = True
            if ja and not current_ja:
                manual_row["ja"] = ja
                changed = True

        if changed:
            updated_count += 1

    return added_count, updated_count


def _clean_mapping_text(value: object) -> str:
    return html.unescape(value).strip() if isinstance(value, str) else ""


def cmd_sqlite_status(limit: int) -> None:
    """Print the current SQLite database status summary."""
    db_path = SQLITE_DB_PATH
    if not db_path.exists():
        print(f"ERROR: {db_path} not found. Run 'build-sqlite' first.")
        sys.exit(1)

    try:
        conn = sqlite3.connect(db_path)
    except sqlite3.Error as e:
        print(f"ERROR: failed to open SQLite database: {e}")
        sys.exit(1)

    try:
        row = conn.execute(
            """
            SELECT
                build_generated_at,
                total_games,
                ok_games,
                missing_mapping_games,
                missing_chinese_games,
                missing_japanese_games,
                manual_only_games,
                manual_conflict_games
            FROM db_status
            """
        ).fetchone()
        review_rows = conn.execute(
            """
            SELECT
                english,
                status,
                manual_conflict,
                missing_mapping,
                missing_chinese,
                missing_japanese
            FROM needs_review
            LIMIT ?
            """,
            (max(0, limit),),
        ).fetchall()
    except sqlite3.Error as e:
        conn.close()
        print(f"ERROR: failed to query database status: {e}")
        sys.exit(1)
    else:
        conn.close()

    assert row is not None
    (
        build_generated_at,
        total_games,
        ok_games,
        missing_mapping_games,
        missing_chinese_games,
        missing_japanese_games,
        manual_only_games,
        manual_conflict_games,
    ) = row

    print(f"{'=' * 60}")
    print(f"DB Path              : {db_path}")
    print(f"Build Generated At   : {build_generated_at}")
    print(f"Total Games          : {total_games}")
    print(f"OK Games             : {ok_games}")
    print(f"Missing Manual       : {missing_mapping_games}")
    print(f"Missing Chinese      : {missing_chinese_games}")
    print(f"Missing Japanese     : {missing_japanese_games}")
    print(f"Manual Only          : {manual_only_games}")
    print(f"Manual Conflicts     : {manual_conflict_games}")
    print(f"{'=' * 60}")

    if not review_rows:
        print("✔ No review items")
        return

    print()
    print(f"Needs Review (top {len(review_rows)})")
    for english, status, manual_conflict, missing_mapping, missing_chinese, missing_japanese in review_rows:
        flags: list[str] = [status]
        if manual_conflict:
            flags.append("manual_conflict")
        if missing_mapping:
            flags.append("missing_mapping")
        if missing_chinese:
            flags.append("missing_chinese")
        if missing_japanese:
            flags.append("missing_japanese")
        print(f"  - {english} [{', '.join(flags)}]")


def cmd_translate_all(workers: int) -> None:
    """Run all translation sources concurrently."""
    tasks: list[tuple[str, Callable[[], None]]] = [
        ("IGDB", cmd_translate),
        ("Steam", cmd_translate_steam),
        ("Wikidata", cmd_translate_wikidata),
    ]
    max_workers = max(1, min(workers, len(tasks)))

    print(f"Starting parallel translation jobs with {max_workers} worker(s) ...\n")
    for label, _ in tasks:
        print(f"[start] {label}")
    print()

    failures = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(func): label for label, func in tasks}

        for future in as_completed(futures):
            label = futures[future]
            try:
                future.result()
                print(f"✔ {label} completed")
            except SystemExit as e:
                code = e.code if isinstance(e.code, int) else 1
                failures += 1
                print(f"✘ {label} failed: exited with status {code}")
            except Exception as e:
                failures += 1
                print(f"✘ {label} failed: {e}")

    if failures:
        print(f"ERROR: {failures} translation job(s) failed.")
        sys.exit(1)

    print("✔ All translation jobs completed")


def _save_translations(
    path: Path, cache: dict[str, dict], ordered_names: list[str]
) -> None:
    """Save translations in the same order as the game names list."""
    ordered = [cache[n] for n in ordered_names if n in cache]
    path.write_text(
        json.dumps(ordered, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def cli(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="game-mappings-updater",
        description="从 flingtrainer.com 爬取修改器名称，并生成可供搜索系统使用的游戏映射数据",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("scrape", help="爬取所有修改器名称并保存到 output/")
    sub.add_parser("update", help="刷新 FLiNG 抓取结果、重建 SQLite，并导出缺失映射模板")
    sub.add_parser("translate", help="通过 IGDB API 翻译游戏名为中文/日文")
    sub.add_parser("translate-steam", help="通过 Steam 商店接口翻译游戏名为中文/日文")
    sub.add_parser("translate-wikidata", help="通过 Wikidata API 翻译游戏名为中文/日文")
    translate_all_parser = sub.add_parser(
        "translate-all",
        help="并发执行 IGDB / Steam / Wikidata 翻译",
    )
    translate_all_parser.add_argument(
        "--workers",
        type=int,
        default=3,
        help="并发 worker 数，默认 3",
    )
    sub.add_parser("build-sqlite", help="将 manual 映射和 FLiNG 抓取结果汇总为 SQLite 数据库")
    sqlite_status_parser = sub.add_parser(
        "sqlite-status",
        help="查看 SQLite 数据库状态和待处理映射",
    )
    sqlite_status_parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="最多显示多少条 needs_review 记录，默认 20",
    )
    export_missing_parser = sub.add_parser(
        "export-missing",
        help="导出缺失翻译映射模板 JSON",
    )
    export_missing_parser.add_argument(
        "--output",
        type=Path,
        default=MISSING_MAPPINGS_PATH,
        help=f"输出 JSON 路径，默认 {MISSING_MAPPINGS_PATH}",
    )
    import_missing_parser = sub.add_parser(
        "import-missing",
        help="校验并导入补齐后的缺失映射 JSON",
    )
    import_missing_parser.add_argument(
        "--input",
        type=Path,
        default=MISSING_MAPPINGS_PATH,
        help=f"输入 JSON 路径，默认 {MISSING_MAPPINGS_PATH}",
    )
    import_missing_parser.add_argument(
        "--check-only",
        action="store_true",
        help="只校验 JSON，不写入 manual，也不重建数据库",
    )

    args = parser.parse_args(argv)

    if args.command == "scrape":
        cmd_scrape()
    elif args.command == "update":
        cmd_update()
    elif args.command == "translate":
        cmd_translate()
    elif args.command == "translate-steam":
        cmd_translate_steam()
    elif args.command == "translate-wikidata":
        cmd_translate_wikidata()
    elif args.command == "translate-all":
        cmd_translate_all(args.workers)
    elif args.command == "build-sqlite":
        cmd_build_sqlite()
    elif args.command == "sqlite-status":
        cmd_sqlite_status(args.limit)
    elif args.command == "export-missing":
        cmd_export_missing(args.output)
    elif args.command == "import-missing":
        cmd_import_missing(args.input, check_only=args.check_only)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    cli()
