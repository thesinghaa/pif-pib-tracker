#!/usr/bin/env python3
"""
PIF PIB Tracker — Scraper
=========================
Client  : Pahle India Foundation (PIF)
Purpose : Scrape all 28 PIB regional RSS feeds (English, Lang=1),
          filter press releases across 6 PIF research verticals.
          Shows only today + yesterday + day-before-yesterday in IST.
          Unmatched go to "other" section.

Output  : docs/pib.json  (committed by GitHub Actions)

PATCH NOTES (2026-04-24 v2)
----------------------------
1. Window changed from rolling 48h to IST calendar days:
   keeps articles whose IST date is today, yesterday, or day-before-yesterday.
   This means the oldest day drops automatically at IST midnight.

2. parse_rss_date() sanity-checks RSS pubDate; dates older than
   RSS_DATE_MAX_AGE_DAYS are flagged as unreliable.

3. verify_posted_date() extracts "Posted On: DD MMM YYYY" from page HTML;
   overrides RSS date on mismatch.

4. merge_releases() prunes using the same IST 3-day window.

5. All dates stored as YYYY-MM-DD (IST calendar date).
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
try:
    import pytz
    IST = pytz.timezone("Asia/Kolkata")
    _HAS_PYTZ = True
except ImportError:
    _HAS_PYTZ = False

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
KEEP_IST_DAYS      = 3      # keep today + yesterday + day-before in IST
SUMMARY_SENTENCES  = 3      # sentences to extract as card summary

# PIB RSS pubDates are sometimes re-stamped. Reject dates older than this
# many days even if the RSS claims they are fresh.
RSS_DATE_MAX_AGE_DAYS = 3

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
            "nhm", "national health mission", "aiims",
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
            "ncap", "emission", "greenhouse gas",
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
            "summit", "g20", "g7", "prime minister visit",
        ]
    },
}

# Build keyword map sorted by length (longest first = more specific matches win)
KEYWORD_MAP = {
    vid: sorted(vdata["keywords"], key=len, reverse=True)
    for vid, vdata in VERTICALS.items()
}

# Regex to extract "Posted On: DD MMM YYYY" from press release pages.
# PIB pages consistently use this format.
POSTED_ON_RE = re.compile(
    r"Posted\s+On\s*:\s*(\d{1,2})\s+([A-Za-z]{3})\s+(\d{4})",
    re.IGNORECASE,
)

# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def make_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()
def get_prid(url: str) -> int:
    """Extract PRID number from PIB press release URL."""
    m = re.search(r'PRID=(\d+)', url or '')
    return int(m.group(1)) if m else 0


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


def parse_rss_date(entry) -> tuple[str, bool]:
    """
    Extract date from RSS entry. Returns (YYYY-MM-DD, is_reliable).

    is_reliable = False means the date came from the RSS feed but is
    suspiciously old (> RSS_DATE_MAX_AGE_DAYS days). The caller should
    verify against the page's Posted On date before trusting it.

    Returns today's date with is_reliable=False if no parseable date found.
    """
    today = datetime.now(timezone.utc)

    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if not val:
            continue
        try:
            dt = datetime(*val[:6], tzinfo=timezone.utc)
            age_days = (today - dt).days

            # Future dates (clock skew) — clamp to today
            if age_days < 0:
                log.debug("RSS date is in the future (%s) — using today", dt.date())
                return today.strftime("%Y-%m-%d"), True

            date_str = dt.strftime("%Y-%m-%d")

            # Within the safe window — trust it
            if age_days <= RSS_DATE_MAX_AGE_DAYS:
                return date_str, True

            # Older than RSS_DATE_MAX_AGE_DAYS — return it but flag as unreliable.
            # The scraper will attempt to verify against the page's Posted On.
            log.debug("RSS date %s is %d days old — flagged for verification", date_str, age_days)
            return date_str, False

        except Exception:
            continue

    # No date at all — default to today, mark unreliable
    log.debug("No RSS date found — defaulting to today")
    return today.strftime("%Y-%m-%d"), False


def extract_posted_date_from_text(text: str) -> str:
    """
    Parse the 'Posted On: DD MMM YYYY' stamp from press release page text.
    Returns YYYY-MM-DD string, or empty string if not found.
    """
    match = POSTED_ON_RE.search(text)
    if not match:
        return ""
    day, mon, year = match.groups()
    try:
        dt = datetime.strptime(f"{day} {mon} {year}", "%d %b %Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return ""


def ist_today() -> datetime:
    """Return current datetime in IST (timezone-aware)."""
    utc_now = datetime.now(timezone.utc)
    if _HAS_PYTZ:
        return utc_now.astimezone(IST)
    # Fallback: manual UTC+5:30
    return utc_now + timedelta(hours=5, minutes=30)


def ist_date_today() -> str:
    """Return today's date string in IST as YYYY-MM-DD."""
    return ist_today().strftime("%Y-%m-%d")


def allowed_ist_dates() -> set:
    """
    Return the set of YYYY-MM-DD strings that are allowed:
    today (IST), yesterday (IST), day-before-yesterday (IST).
    """
    today_ist = ist_today().replace(hour=0, minute=0, second=0, microsecond=0)
    return {
        (today_ist - timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range(KEEP_IST_DAYS)
    }


def is_within_window(date_str: str) -> bool:
    """True if the date is within the allowed IST 3-day window."""
    try:
        return date_str in allowed_ist_dates()
    except Exception:
        return True  # give benefit of doubt on parse failure


def relative_time(date_str: str) -> str:
    try:
        today_ist = ist_date_today()
        yesterday = (ist_today() - timedelta(days=1)).strftime("%Y-%m-%d")
        day_before = (ist_today() - timedelta(days=2)).strftime("%Y-%m-%d")
        if date_str == today_ist:       return "Today"
        if date_str == yesterday:       return "Yesterday"
        if date_str == day_before:      return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b")
        return datetime.strptime(date_str, "%Y-%m-%d").strftime("%d %b %Y")
    except ValueError:
        return "Recently"


def primary_vertical(scores: dict) -> str:
    if not scores:
        return ""
    return max(scores, key=scores.get)


def to_ist(dt: datetime) -> str:
    if _HAS_PYTZ:
        ist_dt = dt.astimezone(IST)
    else:
        ist_dt = dt + timedelta(hours=5, minutes=30)
    return ist_dt.strftime("%d %b %Y, %I:%M %p IST")


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
    """
    First SUMMARY_SENTENCES meaningful sentences from full press release text.
    Strips datelines like 'New Delhi:' or 'Posted On: ...' at the start
    to avoid boilerplate appearing as the summary.
    """
    src = full_content or fallback
    if not src:
        return ""

    # Remove Posted On / dateline lines entirely
    lines = []
    for ln in src.split("\n"):
        stripped = ln.strip()
        if not stripped:
            continue
        if stripped.startswith("•"):
            continue
        # Drop lines that are just datelines: "New Delhi:", "Mumbai, April 24:"
        if re.match(r"^[A-Za-z\s,]+:\s*$", stripped):
            continue
        # Drop "Posted On" lines
        if re.match(r"^Posted\s+On\s*:", stripped, re.IGNORECASE):
            continue
        lines.append(stripped)

    text = " ".join(lines[:8])

    # Strip leading dateline from the joined text:
    # e.g. "New Delhi: The government today..." → "The government today..."
    text = re.sub(r"^[A-Za-z\s,]+:\s+", "", text)

    sentences = re.split(r"(?<=[.!?])\s+", text)
    good = [s for s in sentences if len(s) > 50]
    return " ".join(good[:SUMMARY_SENTENCES])


def fetch_full_content(url: str) -> tuple[str, str]:
    """
    Fetches full text of a PIB press release HTML page.
    Returns (full_content_text, posted_date_YYYY_MM_DD).
    posted_date is extracted from the 'Posted On:' stamp on the page —
    this is the ground-truth date, independent of the RSS feed.
    """
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Extract the Posted On date from raw page text before we strip HTML
        page_text_raw = soup.get_text(separator=" ")
        posted_date   = extract_posted_date_from_text(page_text_raw)

        content_div = (
            soup.select_one("div.innner-page-main-about-us-content-right-part") or
            soup.select_one("div.ContentDiv") or
            soup.select_one("div.content_area") or
            soup.select_one("div#content")
        )

        if not content_div:
            return "", posted_date

        lines = []
        for tag in content_div.find_all(["p", "li", "h3", "h4"]):
            text = tag.get_text(separator=" ").strip()
            text = re.sub(r"\s+", " ", text)
            if text and len(text) > 15:
                if tag.name == "li":
                    lines.append(f"• {text}")
                else:
                    lines.append(text)

        return "\n".join(lines)[:3000], posted_date

    except Exception as exc:
        log.warning("Content fetch failed [%s]: %s", url, exc)
        return "", ""


# ─────────────────────────────────────────────
# Main RSS scrape loop
# ─────────────────────────────────────────────

def scrape_all_regions() -> list:
    all_releases  = []
    seen_ids: set = set()
    total_matched = 0
    total_other   = 0
    total_skipped = 0
    total_date_corrected = 0

    for reg_id, (region_name, feed_url) in PIB_RSS_FEEDS.items():
        log.info("Scraping %-22s  %s", region_name, feed_url)

        try:
            feed    = feedparser.parse(feed_url, request_headers=HEADERS)
            entries = feed.entries
            log.info("  → %d entries found", len(entries))
            # Compute max PRID in this feed. PIB regional feeds sometimes
            # re-publish old articles with a refreshed pubDate. Anything
            # more than 15,000 PRIDs below the feed's own max is stale.
            feed_prids    = [get_prid(e.get("link", "")) for e in entries[:50]]
            feed_prid_max = max((p for p in feed_prids if p > 0), default=0)
            PRID_GAP      = 15000

            for entry in entries[:50]:
                title   = clean_text(entry.get("title", ""))
                url     = entry.get("link", "").strip()
                snippet = clean_text(entry.get("summary", entry.get("description", "")))

                if not title or not url:
                    continue

                # Skip non-English titles (Devanagari / other Indic scripts)
                non_ascii = sum(1 for c in title if ord(c) > 0x0900)
                if non_ascii > 3:
                    continue

                uid = make_id(url)
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)

                # ── PRID GAP CHECK ───────────────────────────────────────
                prid = get_prid(url)
                if feed_prid_max > 0 and prid > 0 and (feed_prid_max - prid) > PRID_GAP:
                    log.info("  [SKIP] PRID %d is %d behind feed max %d (stale): %s",
                             prid, feed_prid_max - prid, feed_prid_max, title[:55])
                    total_skipped += 1
                    continue
                          
                # ── DATE EXTRACTION ──────────────────────────────────────
                # Step 1: Get RSS date and reliability flag
                date_str, rss_reliable = parse_rss_date(entry)

                # Step 2: First-pass window check using RSS date.
                # If RSS date is clearly old AND reliable, drop early without fetching page.
                if rss_reliable and not is_within_window(date_str):
                    log.info("  [SKIP]  Out of window (RSS date %s): %s", date_str, title[:60])
                    total_skipped += 1
                    continue

                # ── SCORING ──────────────────────────────────────────────
                scores      = score_release(title, snippet)
                total_score = sum(scores.values())

                # ── CONTENT FETCH (matched articles only) ────────────────
                full_content = ""
                posted_date  = ""

                if scores:
                    log.info("  [MATCH] %s | %s score=%d",
                             title[:60], list(scores.keys()), total_score)
                    full_content, posted_date = fetch_full_content(url)
                    total_matched += 1
                    time.sleep(0.3)
                else:
                    log.info("  [OTHER] %s", title[:60])
                    total_other += 1

                # ── DATE VERIFICATION ────────────────────────────────────
                # For matched articles: we have the page, so we can read the
                # authoritative "Posted On" stamp and override the RSS date.
                # For other articles: if the RSS date was flagged unreliable,
                # we have to trust it as-is (no page fetch), but we drop it
                # if it's outside the IST 3-day window anyway.

                if posted_date and posted_date != date_str:
                    log.warning(
                        "  [DATE MISMATCH] RSS=%s  Posted On=%s  → using Posted On | %s",
                        date_str, posted_date, title[:55]
                    )
                    date_str = posted_date
                    total_date_corrected += 1
                elif not posted_date and not rss_reliable:
                    # No page date available and RSS date was already flagged.
                    # Be conservative: treat as stale and skip.
                    log.info(
                        "  [SKIP]  Unreliable RSS date %s, no page date found: %s",
                        date_str, title[:60]
                    )
                    total_skipped += 1
                    continue

                # ── FINAL WINDOW CHECK ───────────────────────────────────
                # Re-run after possible date correction (uses IST 3-day window).
                if not is_within_window(date_str):
                    log.info(
                        "  [SKIP]  Out of window after date verification (date=%s): %s",
                        date_str, title[:60]
                    )
                    total_skipped += 1
                    continue

                # ── BUILD RELEASE RECORD ─────────────────────────────────
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
        "Scrape complete — matched=%d  other=%d  skipped=%d  date_corrected=%d  total=%d",
        total_matched, total_other, total_skipped, total_date_corrected, len(all_releases)
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
    """
    Merge freshly scraped articles into the existing pool.
    Prunes any existing article whose IST date falls outside the 3-day window
    (today, yesterday, day-before-yesterday in IST) so stale articles never
    persist across scraper runs. The oldest day drops automatically at IST midnight.
    """
    allowed = allowed_ist_dates()

    before_prune = len(existing)
    existing = [r for r in existing if r.get("date", "") in allowed]
    pruned = before_prune - len(existing)
    if pruned:
        log.info("Pruned %d stale articles from existing pool (IST window: %s)",
                 pruned, sorted(allowed))

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
    log.info("Regions: %d  |  Window: last %d IST calendar days  |  Output: %s",
             len(PIB_RSS_FEEDS), KEEP_IST_DAYS, OUTPUT_PATH)
    log.info("=" * 60)

    fresh    = scrape_all_regions()
    existing = load_existing(OUTPUT_PATH)
    merged   = merge_releases(existing, fresh)

    write_output(merged, OUTPUT_PATH)
    log.info("Done. ✓")


if __name__ == "__main__":
    main()
