"""
game-mappings-updater — 从 flingtrainer.com 爬取所有修改器名称，
并通过 IGDB / Steam / Wikidata 获取官方中文/日文译名。

子命令:
  scrape           爬取 flingtrainer.com 所有修改器名称
  translate        通过 IGDB API 翻译游戏名为中文/日文
  translate-steam  通过 Steam 商店接口补充中文/日文标题
  translate-wikidata  通过 Wikidata API 补充中文/日文标题
  translate-all    并发执行 IGDB / Steam / Wikidata 翻译
  build-sqlite     将三个翻译源汇总为 SQLite 数据库
"""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
import html
import json
import re
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
    """Build a SQLite database from the generated JSON outputs."""
    from sqlite_export import build_sqlite_database

    try:
        db_path = build_sqlite_database(OUTPUT_DIR)
    except FileNotFoundError as e:
        print(f"ERROR: {e}")
        sys.exit(1)

    print(f"✔ SQLite database → {db_path}")


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
        description="从 flingtrainer.com 爬取修改器名称，通过 IGDB / Steam / Wikidata 获取官方中日文译名",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("scrape", help="爬取所有修改器名称并保存到 output/")
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
    sub.add_parser("build-sqlite", help="将 JSON 翻译结果汇总为 SQLite 数据库")

    args = parser.parse_args(argv)

    if args.command == "scrape":
        cmd_scrape()
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
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    cli()
