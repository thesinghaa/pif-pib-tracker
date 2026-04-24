#!/usr/bin/env python3
"""
PIF PIB Tracker — Scraper
=========================
Client  : Pahle India Foundation (PIF)
Purpose : Scrape all 28 PIB regional RSS feeds (English, Lang=1),
          filter press releases across 6 PIF research verticals.
          Last 48hrs releases only. Unmatched go to "other" section.

Output  : docs/pib.json  (committed by GitHub Actions)
"""

import feedparser
import json
import logging
import os
import hashlib
import re
import time
from datetime import datetime, timedelta, timezone

import requests
from bs4 import BeautifulSoup

# ─────────────────────────────────────────────
# Logging
# ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────
OUTPUT_PATH = os.path.join(
    os.path.dirname(__file__), "..", "docs", "pib.json"
)
REQUEST_DELAY      = 0.5    # polite delay between feeds (seconds)
MAX_RELEASES_KEPT  = 500    # rolling window stored in pib.json
SNIPPET_LENGTH     = 400    # characters of summary to keep
FRESH_WINDOW_HOURS = 48     # show releases from last 48 hours
SUMMARY_SENTENCES  = 3      # sentences to extract as card summary

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/rss+xml, application/xml, text/xml, */*",
}


# ─────────────────────────────────────────────
# PIB RSS Feed URLs — Lang=1 is English
# ─────────────────────────────────────────────
PIB_RSS_FEEDS = {
    "3":  ("Delhi",              "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=3&reg=3&Langid=1"),
    "1":  ("Mumbai",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=1&reg=3&Langid=1"),
    "5":  ("Hyderabad",          "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=5&reg=3&Langid=1"),
    "6":  ("Chennai",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=6&reg=3&Langid=1"),
    "17": ("Chandigarh",         "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=17&reg=3&Langid=1"),
    "19": ("Kolkata",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=19&reg=3&Langid=1"),
    "20": ("Bengaluru",          "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=20&reg=3&Langid=1"),
    "21": ("Bhubaneswar",        "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=21&reg=3&Langid=1"),
    "22": ("Ahmedabad",          "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=22&reg=3&Langid=1"),
    "23": ("Guwahati",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=23&reg=3&Langid=1"),
    "24": ("Thiruvananthapuram", "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=24&reg=3&Langid=1"),
    "30": ("Imphal",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=30&reg=3&Langid=1"),
    "31": ("Mizoram",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=31&reg=3&Langid=1"),
    "32": ("Agartala",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=32&reg=3&Langid=1"),
    "33": ("Gangtok",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=33&reg=3&Langid=1"),
    "34": ("Kohima",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=34&reg=3&Langid=1"),
    "35": ("Shillong",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=35&reg=3&Langid=1"),
    "36": ("Itanagar",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=36&reg=3&Langid=1"),
    "37": ("Lucknow",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=37&reg=3&Langid=1"),
    "38": ("Bhopal",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=38&reg=3&Langid=1"),
    "39": ("Jaipur",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=39&reg=3&Langid=1"),
    "40": ("Patna",              "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=40&reg=3&Langid=1"),
    "41": ("Ranchi",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=41&reg=3&Langid=1"),
    "42": ("Shimla",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=42&reg=3&Langid=1"),
    "43": ("Raipur",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=43&reg=3&Langid=1"),
    "44": ("Jammu & Kashmir",    "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=44&reg=3&Langid=1"),
    "45": ("Vijayawada",         "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=45&reg=3&Langid=1"),
    "46": ("Dehradun",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid=46&reg=3&Langid=1"),
}
# ─────────────────────────────────────────────
# PIF VERTICALS & KEYWORDS
# ─────────────────────────────────────────────
VERTICALS = {
    "EooDB": {
        "label": "Ease of Doing Business & Manufacturing",
        "color": "#E8620A",
        "emoji": "🏭",
        "keywords": [
            "msme", "pli", "ease of doing business", "make in india",
            "fta", "export", "manufacturing", "gst", "investment",
            "startup", "semiconductor", "logistics", "import duty",
            "anti-dumping", "wto", "sez", "regulatory reform",
            "dpiit", "commerce", "production linked incentive",
            "free trade agreement", "special economic zone",
            "foreign direct investment", "fdi", "industrial corridor",
            "trade policy", "customs duty", "business reform",
            "industrial policy", "msme credit", "udyam",
        ]
    },
    "CoDED": {
        "label": "Data for Economic Decision-making",
        "color": "#2471A3",
        "emoji": "📊",
        "keywords": [
            "gdp", "mospi", "nso", "inflation", "cpi", "wpi", "iip",
            "plfs", "economic survey", "census", "statistical",
            "economic data", "economic growth", "national accounts",
            "base year", "economic indicators", "national statistical",
            "consumer price index", "wholesale price index",
            "index of industrial production", "labour force survey",
            "gross domestic product", "economic census",
            "data governance", "national data", "data policy",
        ]
    },
    "iLEAP": {
        "label": "i-LEAP: Lead Elimination & Public Health",
        "color": "#C0392B",
        "emoji": "🩺",
        "keywords": [
            "lead poisoning", "lead paint", "lead exposure",
            "public health", "ayushman", "maternal health",
            "malnutrition", "vaccination", "air pollution", "icmr",
            "health budget", "health policy", "cervical cancer",
            "poshan", "anaemia", "immunisation", "generic medicine",
            "jan aushadhi", "tobacco", "mental health",
            "health scheme", "ministry of health", "healthcare",
            "child health", "immunization", "health program",
            "pm2.5", "non communicable disease", "ncd",
        ]
    },
    "ELS": {
        "label": "Jobs, Livelihoods & Women in Work",
        "color": "#7D3C98",
        "emoji": "💼",
        "keywords": [
            "employment", "nrega", "mgnregs", "gig workers",
            "women workforce", "skill development", "pmkvy",
            "informal sector", "shg", "labour", "minimum wage",
            "mudra", "rozgar", "unemployment", "vocational",
            "job creation", "livelihood", "self help group",
            "skill india", "women employment", "female workforce",
            "pradhan mantri kaushal", "labour market",
            "e-shram", "esic", "epfo", "lakhpati didi",
        ]
    },
    "Sustainability": {
        "label": "Sustainability, Climate & Environment",
        "color": "#1E8449",
        "emoji": "🌱",
        "keywords": [
            "climate", "renewable energy", "solar", "net zero",
            "electric vehicle", "green hydrogen", "air pollution",
            "water", "waste management", "swachh bharat",
            "energy transition", "carbon", "forest", "biodiversity",
            "mnre", "environment", "pollution control",
            "clean energy", "climate change", "energy efficiency",
            "green energy", "wind energy", "ev charging",
            "pm surya ghar", "national clean air", "biofuel",
        ]
    },
    "Political_Economy": {
        "label": "Political Economy & Governance",
        "color": "#117A65",
        "emoji": "🏛️",
        "keywords": [
            "governance", "policy reform", "parliament", "federalism",
            "foreign policy", "niti aayog", "disinvestment", "election",
            "bilateral", "diplomacy", "cabinet", "legislation",
            "lok sabha", "rajya sabha", "administrative reform",
            "public administration", "institutional reform",
            "state capacity", "decentralisation", "psu reform",
            "cooperative federalism", "centre state",
        ]
    },
}
# Build keyword map sorted by length (longest first = more specific matches win)
KEYWORD_MAP = {
    vid: sorted(vdata["keywords"], key=len, reverse=True)
    for vid, vdata in VERTICALS.items()
}


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def make_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def clean_text(raw: str) -> str:
    if not raw:
        return ""
    text = BeautifulSoup(raw, "html.parser").get_text(separator=" ")
    return re.sub(r"\s+", " ", text).strip()[:SNIPPET_LENGTH]

def score_release(title: str, snippet: str) -> dict:
    """
    Score a release against all verticals.
    Title match  = 3 pts (title is authoritative)
    Snippet match = 1 pt
    Short abbreviations (≤4 chars, no spaces) use word-boundary matching
    to avoid false positives (e.g. 'ev' in 'every').
    Assign primary vertical = highest-scoring.  Score 0 → 'Other'.
    """
    title_low   = title.lower()
    snippet_low = snippet.lower()
    scores: dict = {}

    for vertical, keywords in KEYWORD_MAP.items():
        score = 0
        for kw in keywords:
            if len(kw) <= 4 and " " not in kw:
                pat = r"\b" + re.escape(kw) + r"\b"
                in_title   = bool(re.search(pat, title_low))
                in_snippet = bool(re.search(pat, snippet_low))
            else:
                in_title   = kw in title_low
                in_snippet = kw in snippet_low

            if in_title:
                score += 3
            elif in_snippet:
                score += 1

        if score > 0:
            scores[vertical] = score

    return scores


def parse_rss_date(entry) -> str:
    """Extract date from RSS entry, return YYYY-MM-DD string."""
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                dt = datetime(*val[:6], tzinfo=timezone.utc)
                return dt.strftime("%Y-%m-%d")
            except Exception:
                pass
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def is_within_window(date_str: str) -> bool:
    try:
        release = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        cutoff  = datetime.now(timezone.utc) - timedelta(hours=FRESH_WINDOW_HOURS)
        return release >= cutoff
    except ValueError:
        return True


def relative_time(date_str: str) -> str:
    try:
        release = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        delta   = (datetime.now(timezone.utc) - release).days
        if delta == 0: return "Today"
        if delta == 1: return "Yesterday"
        if delta < 7:  return f"{delta} days ago"
        return release.strftime("%d %b %Y")
    except ValueError:
        return "Recently"


def primary_vertical(scores: dict) -> str:
    if not scores:
        return ""
    return max(scores, key=scores.get)


def to_ist(dt: datetime) -> str:
    ist = dt + timedelta(hours=5, minutes=30)
    return ist.strftime("%d %b %Y, %I:%M %p IST")


UPSWING_WORDS = [
    "launch", "inaugurate", "sanction", "approve", "allocate", "record",
    "highest", "growth", "boost", "strengthen", "expand", "achieve",
    "milestone", "initiative", "new scheme", "sign", "award", "increase",
]
DOWNSWING_WORDS = [
    "shortage", "deficiency", "decline", "fall", "challenge", "concern",
    "crisis", "delay", "suspend", "cancel", "loss", "failure", "reduce",
    "cut", "deficient", "poor", "slow", "drop", "risk",
]


def detect_sentiment(title: str, snippet: str) -> str:
    text = (title + " " + snippet).lower()
    up   = sum(1 for w in UPSWING_WORDS   if w in text)
    dn   = sum(1 for w in DOWNSWING_WORDS if w in text)
    if up > dn:  return "up"
    if dn > up:  return "down"
    return "neutral"


def extract_summary(full_content: str, fallback: str = "") -> str:
    """First 2-3 meaningful sentences from full press release text."""
    src = full_content or fallback
    if not src:
        return ""
    para_lines = [ln.strip() for ln in src.split("\n")
                  if ln.strip() and not ln.strip().startswith("•")]
    text = " ".join(para_lines[:8])
    sentences = re.split(r"(?<=[.!?])\s+", text)
    good = [s for s in sentences if len(s) > 50]
    return " ".join(good[:SUMMARY_SENTENCES])


def fetch_full_content(url: str) -> str:
    """
    Fetches full text of PIB press release HTML page.
    Returns formatted text with bullet points preserved.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        content_div = (
            soup.select_one("div.innner-page-main-about-us-content-right-part") or
            soup.select_one("div.ContentDiv") or
            soup.select_one("div.content_area") or
            soup.select_one("div#content")
        )

        if not content_div:
            return ""

        lines = []
        for tag in content_div.find_all(["p", "li", "h3", "h4"]):
            text = tag.get_text(separator=" ").strip()
            text = re.sub(r"\s+", " ", text)
            if text and len(text) > 15:
                if tag.name == "li":
                    lines.append(f"• {text}")
                else:
                    lines.append(text)

        return "\n".join(lines)[:3000]

    except Exception as exc:
        log.warning("Content fetch failed [%s]: %s", url, exc)
        return ""


# ─────────────────────────────────────────────
# Main RSS scrape loop
# ─────────────────────────────────────────────

def scrape_all_regions() -> list:
    all_releases  = []
    seen_ids: set = set()
    total_matched = 0
    total_other   = 0
    total_skipped = 0

    for reg_id, (region_name, feed_url) in PIB_RSS_FEEDS.items():
        log.info("Scraping %-22s  %s", region_name, feed_url)

        try:
            feed    = feedparser.parse(feed_url, request_headers=HEADERS)
            entries = feed.entries
            log.info("  → %d entries found", len(entries))

            for entry in entries[:50]:
                title   = clean_text(entry.get("title", ""))
                url     = entry.get("link", "").strip()
                snippet = clean_text(entry.get("summary", entry.get("description", "")))

                if not title or not url:
                    continue
                # Skip non-English titles
                non_ascii = sum(1 for c in title if ord(c) > 0x0900)
                if non_ascii > 3:
                    continue

                uid = make_id(url)
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)

                # Date check
                date_str = parse_rss_date(entry)
                if not is_within_window(date_str):
                    total_skipped += 1
                    continue

                # Score against verticals
                scores      = score_release(title, snippet)
                total_score = sum(scores.values())

                # Fetch full content for matched articles; use snippet for others
                full_content = ""
                if scores:
                    log.info("  [MATCH] %s | %s score=%d",
                             title[:60], list(scores.keys()), total_score)
                    full_content = fetch_full_content(url)
                    total_matched += 1
                    time.sleep(0.3)
                else:
                    log.info("  [OTHER] %s", title[:60])
                    total_other += 1

                release = {
                    "id":               uid,
                    "title":            title,
                    "url":              url,
                    "date":             date_str,
                    "relative_time":    relative_time(date_str),
                    "region":           region_name,
                    "verticals":        sorted(scores.keys()),
                    "primary_vertical": primary_vertical(scores),
                    "relevance_score":  total_score,
                    "snippet":          snippet,
                    "full_content":     full_content,
                    "summary":          extract_summary(full_content, snippet),
                    "sentiment":        detect_sentiment(title, snippet),
                    "vertical_scores":  scores,
                    "section":          "vertical" if scores else "other",
                }

                all_releases.append(release)

        except Exception as exc:
            log.warning("Feed error [%s]: %s", region_name, exc)

        time.sleep(REQUEST_DELAY)

    log.info(
        "Scrape complete — matched=%d  other=%d  skipped=%d  total=%d",
        total_matched, total_other, total_skipped, len(all_releases)
    )
    return all_releases


# ─────────────────────────────────────────────
# Merge with existing pib.json
# ─────────────────────────────────────────────

def load_existing(path: str) -> list:
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        return data.get("articles", data.get("releases", []))
    except (json.JSONDecodeError, KeyError):
        log.warning("Could not load existing pib.json — starting fresh")
        return []


def merge_releases(existing: list, fresh: list) -> list:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=FRESH_WINDOW_HOURS)
    # Prune existing to the live window so stale articles don't persist
    existing = [
        r for r in existing
        if datetime.strptime(r.get("date", "2000-01-01"), "%Y-%m-%d")
           .replace(tzinfo=timezone.utc) >= cutoff
    ]
    by_id = {r["id"]: r for r in existing}
    added = 0
    for r in fresh:
        if r["id"] not in by_id:
            by_id[r["id"]] = r
            added += 1
    log.info("Merged: %d new releases added (window pool: %d)", added, len(by_id))
    return sorted(by_id.values(), key=lambda r: r.get("date", ""), reverse=True)


# ─────────────────────────────────────────────
# Write output
# ─────────────────────────────────────────────

def write_output(releases: list, path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    vertical_counts = {v: 0 for v in VERTICALS}
    other_count     = 0
    regions_seen    = set()

    for r in releases:
        regions_seen.add(r["region"])
        if r["verticals"]:
            for v in r["verticals"]:
                vertical_counts[v] = vertical_counts.get(v, 0) + 1
        else:
            other_count += 1

    now_utc = datetime.now(timezone.utc)

    output = {
        "last_updated":     now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_updated_ist": to_ist(now_utc),
        "total":            len(releases),
        "articles":         releases,
        "other_count":      other_count,
        "vertical_counts":  vertical_counts,
        "regions_scraped":  len(regions_seen),
        "verticals": {
            vid: {
                "label": vdata["label"],
                "color": vdata["color"],
                "emoji": vdata["emoji"],
            }
            for vid, vdata in VERTICALS.items()
        },
        "regions": {
            reg_id: name
            for reg_id, (name, _) in PIB_RSS_FEEDS.items()
        },
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info("Written %d articles → %s", len(releases), path)
    for vid, cnt in vertical_counts.items():
        log.info("  %-25s : %d", VERTICALS[vid]["label"], cnt)
    log.info("  %-25s : %d", "Other Releases", other_count)


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main() -> None:
    log.info("=" * 60)
    log.info("PIF PIB Scraper — starting")
    log.info("Regions: %d  |  Window: last %dh  |  Output: %s",
             len(PIB_RSS_FEEDS), FRESH_WINDOW_HOURS, OUTPUT_PATH)
    log.info("=" * 60)

    fresh    = scrape_all_regions()
    existing = load_existing(OUTPUT_PATH)
    merged   = merge_releases(existing, fresh)

    write_output(merged, OUTPUT_PATH)
    log.info("Done. ✓")


if __name__ == "__main__":
    main()
