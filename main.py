"""
game-mappings-updater — 从 flingtrainer.com 爬取所有修改器名称

支持的子命令:
  scrape  爬取 flingtrainer.com，获取所有修改器名称并保存到 output/ 目录
"""

from __future__ import annotations

import argparse
import html
import json
import re
import sys
from pathlib import Path

import requests
from bs4 import BeautifulSoup

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
    """Scrape trainers from /all-trainers/ (post-2019.05).

    The page uses the WordPress *A-Z Listing* plugin.  Trainer links live
    inside ``<div class="az-listing"> … <ul class="az-columns …"> <li><a>``.
    """
    soup = BeautifulSoup(fetch_page(ALL_TRAINERS_URL), "html.parser")

    az_listing = soup.find("div", class_="az-listing")
    if az_listing is None:
        print("  WARNING: <div class='az-listing'> not found")
        return []

    trainers: list[dict] = []
    for a in az_listing.find_all("a", href=True):
        href: str = a["href"]
        name: str = a.get_text(strip=True)
        # Skip the letter-navigation anchors (A, B, … Z, #)
        if "/trainer/" in href and name and not re.match(r"^[A-Z#]$", name):
            trainers.append({
                "name": html.unescape(name),
                "url": href,
                "source": "modern",
            })

    print(f"  → {len(trainers)} modern trainers")
    return trainers


def scrape_archived_trainers() -> list[dict]:
    """Scrape trainers from /trainer/my-trainers-archive/ (2012 – 2019.05).

    Archived trainers are plain-text lines inside ``<p style="padding-left:
    40px">`` elements, separated by ``<br/>`` tags.
    """
    soup = BeautifulSoup(fetch_page(ARCHIVE_URL), "html.parser")

    entry = soup.find("div", class_="entry")
    if entry is None:
        print("  WARNING: <div class='entry'> not found")
        return []

    trainers: list[dict] = []
    for p in entry.find_all("p", style=True):
        if "padding-left" not in (p.get("style") or ""):
            continue
        # Replace <br/> with newlines, then split
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

    # Ensure output directory exists
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Full list
    full_path = OUTPUT_DIR / "fling_all_trainers.json"
    full_path.write_text(
        json.dumps(all_trainers, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"\n✔ Full list       → {full_path}")

    # Game names only
    names = [extract_game_name(t["name"]) for t in all_trainers]
    names_path = OUTPUT_DIR / "fling_game_names.json"
    names_path.write_text(
        json.dumps(names, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    print(f"✔ Game names only → {names_path}")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def cli(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="game-mappings-updater",
        description="从 flingtrainer.com 爬取修改器名称，维护游戏名映射表",
    )
    sub = parser.add_subparsers(dest="command")

    sub.add_parser("scrape", help="爬取所有修改器名称并保存到 output/ 目录")

    args = parser.parse_args(argv)

    if args.command == "scrape":
        cmd_scrape()
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    cli()
