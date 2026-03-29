#!/usr/bin/env python3
"""
PIF PIB Tracker — Scraper v3 (RSS-based)
=========================================
Client  : Pahle India Foundation (PIF)
Purpose : Scrape all 28 PIB regional RSS feeds, filter press releases
          across 4 research verticals using weighted keyword scoring.

Why RSS instead of HTML scraping:
- PIB's Allrel.aspx pages return 403 Forbidden from cloud/datacenter IPs
- PIB's RSS feeds (RssMain.aspx) work reliably from anywhere
- RSS is cleaner, faster, and more stable

Output  : docs/pib.json  (committed by GitHub Actions)
"""

import feedparser
import json
import logging
import os
import hashlib
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
REQUEST_DELAY      = 0.5        # polite delay between feeds
MAX_RELEASES_KEPT  = 500        # rolling window stored in pib.json
SNIPPET_LENGTH     = 300        # characters of summary to keep
FRESH_WINDOW_HOURS = 24         # only keep releases from last 24 hours

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ─────────────────────────────────────────────
# PIB RSS Feed URLs — Lang=2 is English
# ─────────────────────────────────────────────
PIB_RSS_FEEDS = {
    "3":  ("Delhi",              "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=3"),
    "1":  ("Mumbai",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=1"),
    "5":  ("Hyderabad",          "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=5"),
    "6":  ("Chennai",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=6"),
    "17": ("Chandigarh",         "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=17"),
    "19": ("Kolkata",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=19"),
    "20": ("Bengaluru",          "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=20"),
    "21": ("Bhubaneswar",        "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=21"),
    "22": ("Ahmedabad",          "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=22"),
    "23": ("Guwahati",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=23"),
    "24": ("Thiruvananthapuram", "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=24"),
    "30": ("Imphal",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=30"),
    "31": ("Mizoram",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=31"),
    "32": ("Agartala",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=32"),
    "33": ("Gangtok",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=33"),
    "34": ("Kohima",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=34"),
    "35": ("Shillong",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=35"),
    "36": ("Itanagar",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=36"),
    "37": ("Lucknow",            "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=37"),
    "38": ("Bhopal",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=38"),
    "39": ("Jaipur",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=39"),
    "40": ("Patna",              "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=40"),
    "41": ("Ranchi",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=41"),
    "42": ("Shimla",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=42"),
    "43": ("Raipur",             "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=43"),
    "44": ("Jammu & Kashmir",    "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=44"),
    "45": ("Vijayawada",         "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=45"),
    "46": ("Dehradun",           "https://pib.gov.in/RssMain.aspx?ModId=6&Lang=2&Regid=46"),
}

# ─────────────────────────────────────────────
# ██  KEYWORD LISTS  ██
# ─────────────────────────────────────────────

EODB_KEYWORDS = [
    "ease of doing business", "eodb", "business reform", "regulatory reform",
    "single window clearance", "single window system",
    "business facilitation", "investor facilitation",
    "compliance burden", "compliance reduction",
    "decriminalization", "decriminalisation",
    "license reform", "permit reform",
    "business registration", "company registration", "startup registration",
    "msme registration", "udyam registration", "gst registration",
    "national single window", "nsws", "invest india",
    "faceless assessment", "digital approval", "paperless approval",
    "e-governance reform", "contactless approval",
    "industrial licensing reform", "environmental clearance reform",
    "building permit reform", "construction permit simplification",
    "fire noc reform", "labour compliance simplification",
    "factory license reform", "trade license reform",
    "contract enforcement reform", "commercial courts",
    "insolvency resolution", "ibc reform", "bankruptcy code reform",
    "nclt reform", "debt recovery tribunal",
    "fdi policy reform", "foreign direct investment policy",
    "investment climate reform", "business climate index",
    "investor confidence index", "doing business ranking",
    "world bank ease of doing business", "global competitiveness index",
    "brap", "business reform action plan",
    "state business ranking", "district business ranking",
    "dpiit reform", "reform implementation dpiit",
    "state investment promotion",
    "manufacturing policy reform", "industrial policy reform",
    "industrial corridor development",
    "special economic zone reform", "sez policy",
    "industrial park development", "nimz",
    "pli scheme reform", "production linked incentive policy",
    "make in india policy", "make in india reform",
    "startup policy reform", "startup ecosystem reform",
    "logistics policy", "logistics ease",
    "pm gati shakti network", "pm gati shakti masterplan",
    "logistics efficiency", "logistics cost reduction",
    "multimodal logistics", "logistics infrastructure reform",
    "national logistics policy",
    "credit access msme", "psb loans reform",
    "mudra scheme", "stand up india scheme",
    "credit guarantee scheme", "cgtmse",
    "invoice financing", "treds platform", "msme credit flow",
    "labour code reform", "industrial relations code",
    "wage code implementation", "social security code",
    "osh code", "fixed term employment reform",
    "labour inspection reform", "shram suvidha portal",
    "labour law consolidation",
    "land acquisition reform", "land bank policy",
    "plug and play infrastructure", "industrial infrastructure reform",
    "land records digitization",
    "gst simplification", "tax reform compliance",
    "faceless appeal income tax", "vivad se vishwas scheme",
    "direct tax reform", "tax compliance simplification",
    "gst council reform", "gst rate rationalisation",
    "gst rate rationalization",
]

CODED_KEYWORDS = [
    "economic data", "statistical data", "official statistics",
    "data governance", "data policy", "data infrastructure",
    "national data", "government data", "public data",
    "data ecosystem", "data architecture",
    "national statistical office", "nso", "mospi",
    "ministry of statistics", "central statistics office", "cso",
    "nsso", "national sample survey", "registrar general",
    "economic census", "annual survey of industries",
    "national statistical commission",
    "population census", "census data", "census commissioner",
    "census enumeration", "census 2021", "census 2026",
    "digital census", "house listing census",
    "gdp data", "gdp growth estimate", "gdp revision",
    "gross domestic product data", "gdp base year",
    "inflation data", "cpi data", "consumer price index data",
    "wpi data", "wholesale price index data",
    "iip data", "index of industrial production",
    "periodic labour force survey", "plfs report",
    "consumption expenditure survey", "hces",
    "national accounts statistics", "supply use table",
    "advance estimate gdp", "first advance estimate",
    "second advance estimate",
    "data quality assessment", "data accuracy improvement",
    "statistical methodology", "survey methodology",
    "base year revision", "data revision gdp",
    "sampling methodology", "survey design statistics",
    "price statistics", "volume index",
    "data analytics platform government",
    "ai governance framework", "ai policy regulation",
    "data exchange protocol", "data sharing framework",
    "open data policy", "open government data",
    "data protection law", "data privacy regulation",
    "pdp bill", "digital personal data protection",
    "dpdp act", "data principal", "data fiduciary",
    "national data governance", "data governance framework",
    "gst data analysis", "tax data statistics",
    "e-way bill statistics", "gstn data",
    "mca21", "company data registry", "epfo statistics",
    "administrative data use", "administrative data linkage",
    "economic survey india", "rbi annual report",
    "rbi monetary policy report", "rbi bulletin statistics",
    "statistical yearbook india", "india statistics compendium",
    "sdg india index", "state statistics bureau",
    "niti aayog data report", "india data handbook",
    "ndap", "national data analytics platform",
    "data catalogue government", "data.gov.in",
    "unified data platform", "india data portal",
    "data linkage government", "integrated data platform",
    "data sharing agreement government",
]

ILEAP_KEYWORDS = [
    "lead poisoning", "lead exposure", "lead contamination",
    "blood lead level", "bll", "lead paint", "lead in paint",
    "lead battery", "lead acid battery", "battery recycling lead",
    "lead smelting", "lead pollution", "lead toxicity",
    "lead free paint", "lead elimination", "lead phase out",
    "childhood lead", "lead testing", "lead screening",
    "lead abatement", "lead remediation", "lead monitoring",
    "lead in fuel", "lead in petrol", "leaded petrol",
    "lead paint standard", "toy safety lead", "cosmetic lead",
    "fssai lead", "food lead contamination",
    "lead in spices", "lead in paint ban",
    "lead safe", "lead hazard",
    "heavy metal contamination", "heavy metal pollution",
    "heavy metal toxicity", "heavy metal exposure",
    "mercury contamination", "mercury pollution", "mercury poisoning",
    "cadmium contamination", "cadmium poisoning",
    "arsenic contamination", "arsenic poisoning",
    "chromium contamination", "chromium poisoning",
    "metal contamination", "metal poisoning",
    "toxic metal", "neurotoxic metal",
    "is 16088", "lead limit regulation", "lead regulation",
    "lead ban", "lead phase out policy",
    "hazardous substance regulation",
    "rohs compliance", "restriction of hazardous substances",
    "neurotoxic exposure", "neurotoxicity children",
    "cognitive impairment children", "iq loss children",
    "developmental neurotoxicity", "child neurotoxin",
    "prenatal lead", "fetal lead exposure",
    "pollution control board lead", "cpcb lead", "spcb lead",
    "industrial effluent heavy metal", "hazardous waste metal",
    "e-waste lead", "e-waste heavy metal",
    "soil lead contamination", "groundwater arsenic",
    "groundwater lead", "water lead contamination",
    "particulate matter heavy metal", "air toxic metal",
    "pm2.5 lead", "dust lead exposure",
    "toxic waste dump", "contaminated site cleanup",
    "occupational lead exposure", "occupational heavy metal",
    "lead worker health", "smelter worker health",
    "battery worker health", "paint worker lead exposure",
    "occupational toxic exposure",
    "national lead elimination", "global lead network",
    "pure earth", "ipen lead", "unep lead",
    "lead paint alliance", "who lead guideline",
    "unicef lead", "global burden lead",
    "lead elimination program",
]

ELS_KEYWORDS = [
    "employment generation", "job creation",
    "unemployment rate", "unemployment data",
    "labour market reform", "workforce development",
    "employment scheme", "employment program",
    "employment exchange", "job portal government",
    "net employment", "new jobs created",
    "minimum wage revision", "minimum wage notification",
    "wage board", "wage revision",
    "equal remuneration act", "wage compliance",
    "wage theft", "wage arrears",
    "floor wage", "national floor wage",
    "skill development scheme",
    "skill training program government",
    "vocational training scheme", "skill india mission",
    "pmkvy", "pradhan mantri kaushal vikas yojana",
    "iti training", "iti upgradation",
    "polytechnic scheme", "national skills qualifications framework",
    "apprenticeship scheme", "national apprenticeship promotion",
    "recognition of prior learning",
    "nsdc", "sector skill council", "skill certification",
    "jan shikshan sansthan",
    "mahatma gandhi nrega", "mgnregs", "mnrega",
    "pm employment guarantee", "urban employment scheme",
    "deen dayal upadhyaya", "ddu-gky", "rsetis",
    "pmegp", "pm rojgar", "pm internship scheme",
    "national career service",
    "informal sector workers", "informal economy policy",
    "street vendors scheme", "pm svnidhi",
    "unorganized workers", "e-shram registration",
    "e-shram portal", "unorganised sector scheme",
    "labour welfare scheme", "worker welfare fund",
    "construction worker welfare", "building worker cess",
    "esi scheme", "esic benefit", "epfo scheme",
    "employee provident fund", "social security worker",
    "labour pension scheme", "unorganized sector pension",
    "pm shram yogi mandhan", "atal pension yojana labour",
    "women employment scheme", "women workforce participation",
    "female labour force participation", "working women hostel",
    "maternity benefit scheme", "women entrepreneur scheme",
    "self help group livelihood", "shg employment",
    "pradhan mantri mahila shakti", "women self employment",
    "migrant worker welfare", "migrant labour policy",
    "gig worker rights", "platform worker policy",
    "interstate migrant worker", "labour migration policy",
    "one nation one ration", "onorc",
    "gig economy regulation", "platform economy policy",
    "plfs report", "periodic labour force survey",
    "employment unemployment survey", "labour bureau survey",
    "employment statistics", "labour statistics india",
    "labour force participation rate", "lfpr data",
    "worker population ratio", "formal employment data",
    "quarterly employment survey",
    "textile employment", "construction workers welfare",
    "domestic workers rights", "domestic workers code",
    "plantation labour welfare", "mining workers welfare",
    "beedi workers welfare", "contract labour regulation",
    "youth employment scheme", "youth unemployment data",
    "first time job seeker", "campus placement scheme",
    "internship scheme government", "apprenticeship act",
    "national career centre",
]

NEGATIVE_KEYWORDS = [
    "condolence", "obituary", "death anniversary", "birth anniversary",
    "greetings on", "wishes on", "festival greetings",
    "republic day parade", "independence day celebration",
    "diwali", "holi", "eid", "christmas", "pongal", "onam",
    "new year message", "mann ki baat",
    "takes charge", "assumes charge",
    "retirement function", "superannuation",
    "swearing in ceremony", "oath taking ceremony",
    "foundation stone laying", "lays foundation stone",
    "flag hoisting ceremony",
    "cultural program", "cultural event", "cultural festival",
    "sports meet", "sports day", "marathon", "cyclothon",
    "yoga day event", "fit india movement",
    "state visit", "bilateral visit",
    "foreign minister visit", "head of state visit",
    "mou signing ceremony", "agreement signing ceremony",
    "ambassador presents credentials",
    "foreign delegation visits", "parliamentary delegation visits",
    "cultural exchange program", "people to people contact",
    "diaspora event", "pravasi bharatiya divas",
    "india caucus", "friendship group",
    "military exercise", "naval exercise", "air exercise",
    "passing out parade", "commissioning ceremony",
    "defence expo", "aero india", "defexpo",
    "gallantry award", "vir chakra", "param vir chakra",
    "sainik school", "rashtriya military school",
    "bsf raising day", "crpf raising day",
    "cisf raising day", "coast guard day",
    "navy day", "air force day", "army day",
    "award ceremony", "prize distribution", "felicitation ceremony",
    "padma awards", "national awards ceremony",
    "farewell function", "book launch event",
    "commemorative stamp release", "coin release ceremony",
    "convocation ceremony", "degree distribution",
    "international day celebration", "world day celebration",
    "hospital inauguration", "medical college inauguration",
    "health camp", "blood donation camp",
    "pulse polio campaign", "vaccination camp",
    "highway inauguration", "road inauguration",
    "bridge inauguration", "tunnel inauguration",
    "airport inauguration", "port inauguration",
    "railway line inauguration", "metro inauguration",
    "dam inauguration", "power plant inauguration",
    "expressway inauguration",
    "election schedule", "election notification",
    "model code of conduct", "voter turnout",
    "polling station", "ballot paper",
    "election results", "by-election notification",
    "pib fact check", "fake news alert",
    "all india radio", "doordarshan programme",
]

# ─────────────────────────────────────────────
# Build lookup structures
# ─────────────────────────────────────────────
KEYWORD_MAP = {
    "EoDB":  sorted(EODB_KEYWORDS,  key=len, reverse=True),
    "CoDED": sorted(CODED_KEYWORDS, key=len, reverse=True),
    "iLEAP": sorted(ILEAP_KEYWORDS, key=len, reverse=True),
    "ELS":   sorted(ELS_KEYWORDS,   key=len, reverse=True),
}

NEGATIVE_SET = set(kw.lower() for kw in NEGATIVE_KEYWORDS)


# ─────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────

def make_id(url: str) -> str:
    return hashlib.md5(url.encode()).hexdigest()


def clean_html(raw: str) -> str:
    if not raw:
        return ""
    text = BeautifulSoup(raw, "html.parser").get_text(separator=" ")
    import re
    return re.sub(r"\s+", " ", text).strip()[:SNIPPET_LENGTH]


def is_negative(title: str) -> bool:
    t = title.lower()
    return any(neg in t for neg in NEGATIVE_SET)


def score_release(title: str, snippet: str) -> dict:
    """Score against all verticals. Title hits = 2pts, snippet hits = 1pt."""
    title_low   = title.lower()
    snippet_low = snippet.lower()
    scores = {}
    for vertical, keywords in KEYWORD_MAP.items():
        score = 0
        for kw in keywords:
            if kw in title_low:
                score += 2
            elif kw in snippet_low:
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
        if delta == 0:   return "Today"
        if delta == 1:   return "Yesterday"
        if delta < 7:    return f"{delta} days ago"
        return release.strftime("%-d %b %Y")
    except ValueError:
        return "Recently"


def primary_vertical(scores: dict) -> str:
    if not scores:
        return "Other"
    return max(scores, key=scores.get)


def to_ist(dt: datetime) -> str:
    ist = dt + timedelta(hours=5, minutes=30)
    return ist.strftime("%-d %b %Y, %-I:%M %p IST")


# ─────────────────────────────────────────────
# Main RSS scrape loop
# ─────────────────────────────────────────────

def scrape_all_regions() -> list:
    all_releases = []
    seen_ids: set = set()

    for reg_id, (region_name, feed_url) in PIB_RSS_FEEDS.items():
        log.info("Scraping %-22s  %s", region_name, feed_url)

        try:
            feed = feedparser.parse(feed_url, request_headers=HEADERS)
            entries = feed.entries
            log.info("  → %d entries found", len(entries))

            for entry in entries[:50]:
                title   = clean_html(entry.get("title", ""))
                url     = entry.get("link", "").strip()
                snippet = clean_html(entry.get("summary", entry.get("description", "")))

                if not title or not url:
                    continue

                uid = make_id(url)
                if uid in seen_ids:
                    continue
                seen_ids.add(uid)

                # Date check — last 24hrs only
                date_str = parse_rss_date(entry)
                if not is_within_window(date_str):
                    log.debug("  [SKIP-OLD] %s (%s)", title[:60], date_str)
                    continue

                # Score against verticals
                scores      = score_release(title, snippet)
                total_score = sum(scores.values())
                section     = "vertical" if scores else "other"

                release = {
                    "id":               uid,
                    "title":            title,
                    "url":              url,
                    "date":             date_str,
                    "relative_time":    relative_time(date_str),
                    "region":           region_name,
                    "verticals":        sorted(scores.keys()),
                    "section":          section,
                    "primary_vertical": primary_vertical(scores),
                    "relevance_score":  total_score,
                    "snippet":          snippet,
                    "vertical_scores":  scores,
                }

                all_releases.append(release)

                if scores:
                    log.info("  [MATCH] %s | %s score=%d",
                             title[:60], list(scores.keys()), total_score)
                else:
                    log.info("  [OTHER] %s", title[:60])

        except Exception as exc:
            log.warning("Feed error [%s]: %s", region_name, exc)

        time.sleep(REQUEST_DELAY)

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

    vertical_counts = {v: 0 for v in KEYWORD_MAP}
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
            "EoDB":  {"label": "Ease of Doing Business & Manufacturing", "color": "#E8620A"},
            "CoDED": {"label": "Data for Economic Decision-making",       "color": "#2471A3"},
            "iLEAP": {"label": "Lead Elimination & Public Health",        "color": "#C0392B"},
            "ELS":   {"label": "Employment & Livelihood Systems",         "color": "#7D3C98"},
        },
        "regions": {reg_id: name for reg_id, (name, _) in PIB_RSS_FEEDS.items()},
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(
        "Written %d articles → %s  (EoDB=%d CoDED=%d iLEAP=%d ELS=%d Other=%d)",
        len(releases), path,
        vertical_counts.get("EoDB",  0),
        vertical_counts.get("CoDED", 0),
        vertical_counts.get("iLEAP", 0),
        vertical_counts.get("ELS",   0),
        other_count,
    )


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main() -> None:
    log.info("═" * 60)
    log.info("PIF PIB Scraper v3 (RSS) — starting")
    log.info("Regions: %d  |  Window: last %dh  |  Output: %s",
             len(PIB_RSS_FEEDS), FRESH_WINDOW_HOURS, OUTPUT_PATH)
    log.info("═" * 60)

    fresh    = scrape_all_regions()
    log.info("Scrape complete — %d releases found", len(fresh))

    existing = load_existing(OUTPUT_PATH)
    merged   = merge_releases(existing, fresh)

    write_output(merged, OUTPUT_PATH)
    log.info("Done. ✓")


if __name__ == "__main__":
    main()
