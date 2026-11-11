"""HCPCS scraper (module 2): requests + BeautifulSoup
to JSON in raw/ (module 3).
"""

import json
import re
from datetime import UTC, datetime
from pathlib import Path

import requests
from bs4 import BeautifulSoup

BASE_URL = "https://www.hcpcsdata.com"
RAW_DIR = Path("raw")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

LETTER_HREF = re.compile(r"^/Codes/([A-Z])/?$")
CODE_HREF = re.compile(r"^/Codes/([A-Z])/([A-Z0-9]{5})/?$")


def get_soup(session: requests.Session, path: str) -> BeautifulSoup:
    response = session.get(f"{BASE_URL}{path}", headers=HEADERS, timeout=30)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")


def parse_letters(soup: BeautifulSoup) -> list[str]:
    letters = {
        m.group(1)
        for a in soup.find_all("a", href=True)
        if (m := LETTER_HREF.match(a["href"]))
    }
    return sorted(letters)


def parse_category_name(soup: BeautifulSoup) -> str | None:
    heading = soup.find("h1")
    sibling = heading.find_next_sibling() if heading else None
    return sibling.get_text(" ", strip=True) or None if sibling else None


def parse_rows(
    soup: BeautifulSoup, letter: str
) -> list[dict[str, str | None]]:
    category_name = parse_category_name(soup)
    rows: list[dict[str, str | None]] = []
    seen: set[str] = set()

    for a in soup.find_all("a", href=True):
        m = CODE_HREF.match(a["href"])
        if not m or m.group(1) != letter or m.group(2) in seen:
            continue
        code = m.group(2)
        seen.add(code)
        container = a.find_parent(["tr", "li"])
        text = container.get_text(" ", strip=True) if container else ""
        rows.append(
            {
                "hcpcs_code": code,
                "group_code": letter,
                "category_name": category_name,
                "long_description": text.removeprefix(code).strip(),
            }
        )
    return rows


def main() -> None:
    rows: list[dict[str, str | None]] = []
    with requests.Session() as session:
        for letter in parse_letters(get_soup(session, "/Codes")):
            rows.extend(
                parse_rows(get_soup(session, f"/Codes/{letter}"), letter)
            )

    RAW_DIR.mkdir(exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    with (RAW_DIR / f"hcpcs_{stamp}.json").open("x", encoding="utf-8") as fh:
        json.dump(rows, fh, ensure_ascii=False, indent=2)


if __name__ == "__main__":
    main()
