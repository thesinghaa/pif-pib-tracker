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
            "ease of doing business", "eodb", "business reform",
            "regulatory reform", "single window clearance",
            "business facilitation", "investor facilitation",
            "compliance burden", "compliance reduction",
            "decriminalization", "decriminalisation",
            "license reform", "permit reform",
            "business registration", "company registration",
            "msme registration", "udyam registration",
            "national single window", "nsws", "invest india",
            "faceless assessment", "digital approval",
            "industrial licensing", "environmental clearance reform",
            "building permit reform", "contract enforcement",
            "commercial courts", "insolvency resolution",
            "ibc reform", "bankruptcy code", "nclt reform",
            "fdi policy", "foreign direct investment policy",
            "investment climate", "doing business ranking",
            "brap", "business reform action plan",
            "state business ranking", "dpiit",
            "industrial policy", "manufacturing policy",
            "industrial corridor", "special economic zone", "sez",
            "industrial park", "pli scheme",
            "production linked incentive",
            "make in india", "startup policy",
            "logistics policy", "pm gati shakti",
            "multimodal logistics", "national logistics policy",
            "mudra scheme", "stand up india",
            "credit guarantee", "cgtmse", "msme credit",
            "labour code", "industrial relations code",
            "wage code", "labour law", "shram suvidha",
            "gst simplification", "tax reform",
            "faceless appeal", "direct tax reform",
            "gst council", "gst rate rationalisation",
            "export promotion", "export competitiveness",
            "import substitution", "trade policy",
            "free trade agreement", "fta",
            "anti-dumping", "customs duty reform",
            "trade facilitation", "rare earth", "lithium",
            "critical mineral", "semiconductor manufacturing",
            "electronics manufacturing", "garment export",
            "textile industry",
        ]
    },
    "CoDED": {
        "label": "Data for Economic Decision-making",
        "color": "#2471A3",
        "emoji": "📊",
        "keywords": [
            "economic data", "statistical data", "official statistics",
            "data governance", "data policy", "data infrastructure",
            "national data", "government data", "public data",
            "national statistical office", "nso", "mospi",
            "ministry of statistics", "central statistics office",
            "nsso", "national sample survey",
            "economic census", "annual survey of industries",
            "national statistical commission",
            "population census", "census data",
            "census 2021", "census 2026", "digital census",
            "gdp data", "gdp growth estimate", "gdp revision",
            "gross domestic product data", "gdp base year",
            "inflation data", "cpi data", "consumer price index data",
            "wpi data", "wholesale price index data",
            "iip data", "index of industrial production",
            "periodic labour force survey", "plfs report",
            "consumption expenditure survey", "hces",
            "national accounts statistics",
            "advance estimate gdp", "data quality",
            "statistical methodology", "base year revision",
            "sampling methodology", "price statistics",
            "data analytics platform", "ai governance framework",
            "data exchange", "data sharing framework",
            "open data policy", "open government data",
            "data protection law", "data privacy regulation",
            "pdp bill", "digital personal data protection",
            "dpdp act", "national data governance",
            "gst data", "tax data statistics",
            "e-way bill statistics", "gstn data",
            "mca21", "epfo statistics",
            "economic survey india", "rbi annual report",
            "rbi monetary policy report", "rbi bulletin",
            "statistical yearbook india", "sdg india index",
            "ndap", "national data analytics platform",
            "data catalogue", "data.gov.in",
            "unified data platform", "india data portal",
        ]
    },
    "iLEAP": {
        "label": "i-LEAP: Lead Elimination & Public Health",
        "color": "#C0392B",
        "emoji": "🩺",
        "keywords": [
            "lead poisoning", "lead exposure", "lead contamination",
            "blood lead level", "bll", "lead paint", "lead in paint",
            "lead battery", "lead acid battery", "battery recycling lead",
            "lead smelting", "lead pollution", "lead toxicity",
            "lead free paint", "lead elimination", "lead phase out",
            "childhood lead", "lead testing", "lead screening",
            "lead abatement", "lead remediation",
            "lead in fuel", "leaded petrol",
            "lead paint standard", "toy safety lead",
            "cosmetic lead", "fssai lead",
            "lead in spices", "lead paint ban", "lead safe",
            "heavy metal contamination", "heavy metal pollution",
            "heavy metal toxicity", "mercury contamination",
            "cadmium contamination", "arsenic contamination",
            "chromium contamination", "toxic metal",
            "neurotoxic metal", "hazardous substance regulation",
            "hpv vaccine", "cervical cancer", "vaccination programme",
            "public health india", "preterm birth", "maternal health",
            "garbh-ini", "infant mortality", "child mortality",
            "nutrition policy", "malnutrition", "anaemia",
            "universal health coverage", "ayushman bharat",
            "generic medicine", "drug pricing", "pharmaceutical policy",
            "air pollution health", "pm2.5",
            "menstrual leave", "women health policy",
            "reproductive health", "mental health india",
            "suicide prevention", "alcohol tobacco policy",
            "tobacco control", "cotpa",
            "non communicable disease", "ncd", "diabetes india",
            "cancer screening", "esic health",
            "pollution control board", "cpcb",
            "industrial effluent", "hazardous waste",
            "e-waste lead", "soil contamination",
            "groundwater arsenic", "water contamination",
            "occupational health", "worker health safety",
        ]
    },
    "Political_Economy": {
        "label": "Political Economy & Governance",
        "color": "#117A65",
        "emoji": "🏛️",
        "keywords": [
            "political economy", "governance reform",
            "institutional reform", "policy implementation",
            "state capacity", "public administration",
            "decentralisation", "decentralization",
            "federalism india", "cooperative federalism",
            "centre state relations",
            "public policy india", "administrative reform",
            "civil service reform", "electoral reform",
            "district administration", "state government policy",
            "urban governance", "municipal reform",
            "psu reform", "public sector undertaking",
            "judicial reform", "data protection india", "dpdp act",
            "digital governance india", "ai regulation india",
            "india foreign policy", "india diplomacy",
            "geopolitical risk", "india strategic autonomy",
            "wto ministerial", "wto india", "multilateral trade",
            "election commission", "governance index",
            "transparency india", "anti-corruption",
            "vigilance commission", "right to information", "rti",
            "lokpal", "lokayukta",
            "direct benefit transfer", "dbt",
            "niti aayog", "cabinet committee",
        ]
    },
    "Jobs_Livelihood": {
        "label": "Jobs, Livelihoods & Women in Work",
        "color": "#7D3C98",
        "emoji": "💼",
        "keywords": [
            "employment generation", "job creation",
            "unemployment rate", "unemployment data",
            "labour market reform", "workforce development",
            "employment scheme", "employment program",
            "net employment", "new jobs created",
            "minimum wage revision", "minimum wage notification",
            "wage board", "wage revision", "equal remuneration",
            "floor wage", "national floor wage",
            "skill development scheme", "skill training program",
            "vocational training scheme", "skill india mission",
            "pmkvy", "pradhan mantri kaushal vikas yojana",
            "iti training", "iti upgradation",
            "polytechnic scheme", "nsqf",
            "apprenticeship scheme", "apprenticeship promotion",
            "recognition of prior learning", "nsdc",
            "sector skill council", "skill certification",
            "mahatma gandhi nrega", "mgnregs", "mnrega",
            "urban employment scheme", "ddu-gky", "rsetis",
            "pmegp", "pm rojgar", "pm internship scheme",
            "national career service",
            "informal sector workers", "informal economy policy",
            "street vendors scheme", "pm svnidhi",
            "unorganized workers", "e-shram registration",
            "labour welfare scheme", "worker welfare fund",
            "construction worker welfare", "building worker cess",
            "esi scheme", "esic benefit", "epfo scheme",
            "employee provident fund", "social security worker",
            "labour pension scheme", "pm shram yogi mandhan",
            "women employment scheme", "women workforce participation",
            "female labour force participation", "working women hostel",
            "maternity benefit scheme", "women entrepreneur scheme",
            "self help group livelihood", "shg employment",
            "pradhan mantri mahila", "women self employment",
            "migrant worker welfare", "migrant labour policy",
            "gig worker rights", "platform worker policy",
            "interstate migrant worker", "labour migration policy",
            "one nation one ration", "onorc",
            "gig economy regulation", "platform economy policy",
            "plfs report", "periodic labour force survey",
            "employment statistics", "labour statistics india",
            "labour force participation rate", "lfpr data",
            "quarterly employment survey",
            "textile employment", "domestic workers rights",
            "plantation labour welfare", "contract labour regulation",
            "youth employment scheme", "youth unemployment data",
            "internship scheme government", "apprenticeship act",
            "lakhpati didi", "orunodoi",
            "women in stem", "gender pay gap india",
        ]
    },
    "Sustainability": {
        "label": "Sustainability, Climate & Environment",
        "color": "#1E8449",
        "emoji": "🌱",
        "keywords": [
            "regenerative farming", "climate change india",
            "waste management india", "circular economy india",
            "sustainable agriculture", "clean energy india",
            "carbon emission india", "green economy",
            "environmental policy india", "renewable energy india",
            "solar energy india", "net zero india",
            "carbon neutral india", "organic farming india",
            "agroecology", "climate adaptation india",
            "air pollution india", "solid waste management",
            "swachh bharat", "climate policy india", "india ndc",
            "nationally determined contribution",
            "green hydrogen india", "plastic waste india",
            "energy security india", "oil import india",
            "crude oil india", "lpg shortage india",
            "cooking gas shortage", "energy transition india",
            "electric vehicle india", "ev india",
            "solar rooftop india", "pm surya ghar",
            "dme fuel india", "ethanol blending india",
            "nuclear energy india", "shanti act nuclear",
            "water governance india", "national water vision",
            "river basin management", "groundwater india",
            "water scarcity india", "integrated water management",
            "climate resilient india", "green infrastructure india",
            "india oil dependence", "petroleum policy india",
            "helium shortage india", "lng india shortage",
            "decarbonisation india", "green finance india",
            "energy geopolitics india", "battery storage india",
            "electrification india", "green manufacturing india",
            "pm kusum", "national solar mission",
            "wind energy india", "biofuel policy india",
            "forest conservation india", "biodiversity india",
            "wildlife protection india", "pollution control india",
            "effluent treatment", "zero liquid discharge",
            "industrial pollution india", "climate finance india",
            "green bonds india", "emission trading india",
        ]
    }
}
# Build keyword map sorted by length (longest first = more specific)
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


def is_negative(title: str) -> bool:
    t = title.lower()
    return any(neg in t for neg in NEGATIVE_SET)


def score_release(title: str, snippet: str) -> dict:
    """
    Score against all verticals.
    Title hits: 2pts (multi-word) or 1pt (single word)
    Snippet hits: 1pt (multi-word phrases only)
    """
    title_low   = title.lower()
    snippet_low = snippet.lower()
    scores = {}

    for vertical, keywords in KEYWORD_MAP.items():
        score = 0
        for kw in keywords:
            word_count = len(kw.split())
            if kw in title_low:
                score += 2 if word_count >= 2 else 1
            elif word_count >= 2 and kw in snippet_low:
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
                # English text is ASCII + common punctuation
                # Hindi/regional text contains Unicode above U+0900
                non_ascii = sum(1 for c in title if ord(c) > 0x0900)
                if non_ascii > 3:
                    log.debug("  [SKIP-LANG] %s", title[:60])
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

                # Negative filter
                if is_negative(title):
                    total_skipped += 1
                    continue

                # Score against verticals
                scores      = score_release(title, snippet)
                total_score = sum(scores.values())

                # Fetch full content for matched articles only
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
    by_id = {r["id"]: r for r in existing}
    added = 0
    for r in fresh:
        if r["id"] not in by_id:
            by_id[r["id"]] = r
            added += 1
    log.info("Merged: %d new releases added (total pool: %d)", added, len(by_id))
    merged = sorted(by_id.values(), key=lambda r: r.get("date", ""), reverse=True)
    return merged[:MAX_RELEASES_KEPT]


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
