"""
PIF PIB Tracker — Scraper v2
============================
Client  : Pahle India Foundation (PIF)
Purpose : Scrape all 28 PIB regional offices, filter press releases
          across 4 research verticals using weighted keyword scoring.

Scoring logic
-------------
- Every keyword match in (title + snippet) adds points.
- Title matches are worth 2x (more signal than body text).
- Each vertical has a minimum score threshold that must be met
  before the release is tagged to that vertical.
- Releases that meet NO vertical threshold are still saved under
  verticals=[] and surface in the "All" section of the dashboard.
- Releases that pass the negative keyword check but score below
  threshold on all verticals are kept (not discarded) — they appear
  only in "All", never in a vertical tab.
- Only releases published within the last 48 hours are ingested
  from each fresh scrape run.  Older releases already in pib.json
  are preserved via the rolling merge window.

Output  : docs/pib.json  (committed by GitHub Actions)
"""

import hashlib
import json
import logging
import os
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
REQUEST_TIMEOUT    = 15         # seconds per HTTP request
REQUEST_DELAY      = 0.4        # polite delay between region fetches
MAX_RELEASES_KEPT  = 500        # rolling window stored in pib.json
SNIPPET_LENGTH     = 220        # characters
FRESH_WINDOW_HOURS = 24         # only ingest releases newer than this

PIB_BASE = "https://www.pib.gov.in"
PIB_LIST = PIB_BASE + "/Allrel.aspx?reg={reg}&lang=1"
PIB_PAGE = PIB_BASE + "/PressReleasePage.aspx?PRID={prid}"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; PIF-PIB-Tracker/2.0; "
        "+https://github.com/thesinghaa/pif-pib-tracker)"
    )
}

# ─────────────────────────────────────────────
# Regions
# ─────────────────────────────────────────────
PIB_REGIONS = {
    "3":  "Delhi",
    "1":  "Mumbai",
    "5":  "Hyderabad",
    "6":  "Chennai",
    "17": "Chandigarh",
    "19": "Kolkata",
    "20": "Bengaluru",
    "21": "Bhubaneswar",
    "22": "Ahmedabad",
    "23": "Guwahati",
    "24": "Thiruvananthapuram",
    "30": "Imphal",
    "31": "Mizoram",
    "32": "Agartala",
    "33": "Gangtok",
    "34": "Kohima",
    "35": "Shillong",
    "36": "Itanagar",
    "37": "Lucknow",
    "38": "Bhopal",
    "39": "Jaipur",
    "40": "Patna",
    "41": "Ranchi",
    "42": "Shimla",
    "43": "Raipur",
    "44": "Jammu & Kashmir",
    "45": "Vijayawada",
    "46": "Dehradun",
}

# ─────────────────────────────────────────────
# ██  KEYWORD LISTS  ██
# ─────────────────────────────────────────────

# ── EoDB ─────────────────────────────────────
EODB_KEYWORDS = [
    # Core
    "ease of doing business", "eodb", "business reform", "regulatory reform",
    "single window clearance", "single window system",
    "business facilitation", "investor facilitation",
    "compliance burden", "compliance reduction",
    "decriminalization", "decriminalisation",
    "license reform", "permit reform",
    "business registration", "company registration", "startup registration",
    "msme registration", "udyam registration", "gst registration",

    # Digital/Portal
    "national single window", "nsws", "invest india",
    "faceless assessment", "digital approval", "paperless approval",
    "e-governance reform", "contactless approval",

    # Specific reforms
    "industrial licensing reform", "environmental clearance reform",
    "building permit reform", "construction permit simplification",
    "fire noc reform", "labour compliance simplification",
    "factory license reform", "trade license reform",
    "contract enforcement reform", "commercial courts",
    "insolvency resolution", "ibc reform", "bankruptcy code reform",
    "nclt reform", "debt recovery tribunal",

    # Investment climate
    "fdi policy reform", "foreign direct investment policy",
    "investment climate reform", "business climate index",
    "investor confidence index", "doing business ranking",
    "world bank ease of doing business", "global competitiveness index",

    # State reforms
    "brap", "business reform action plan",
    "state business ranking", "district business ranking",
    "dpiit reform", "reform implementation dpiit",
    "state investment promotion",

    # Sector-specific
    "manufacturing policy reform", "industrial policy reform",
    "industrial corridor development",
    "special economic zone reform", "sez policy",
    "industrial park development", "nimz",
    "pli scheme reform", "production linked incentive policy",
    "make in india policy", "make in india reform",
    "startup policy reform", "startup ecosystem reform",

    # Logistics (tightened)
    "logistics policy", "logistics ease",
    "pm gati shakti network", "pm gati shakti masterplan",
    "logistics efficiency", "logistics cost reduction",
    "multimodal logistics", "logistics infrastructure reform",
    "national logistics policy",

    # Credit & Finance
    "credit access msme", "psb loans reform",
    "mudra scheme", "stand up india scheme",
    "credit guarantee scheme", "cgtmse",
    "invoice financing", "treds platform",
    "msme credit flow",

    # Labour reforms
    "labour code reform", "industrial relations code",
    "wage code implementation", "social security code",
    "osh code", "fixed term employment reform",
    "labour inspection reform", "shram suvidha portal",
    "labour law consolidation",

    # Land & Infrastructure
    "land acquisition reform", "land bank policy",
    "plug and play infrastructure", "industrial infrastructure reform",
    "land records digitization",

    # Taxation
    "gst simplification", "tax reform compliance",
    "faceless appeal income tax", "vivad se vishwas scheme",
    "direct tax reform", "tax compliance simplification",
    "gst council reform", "gst rate rationalisation",
    "gst rate rationalization",
]

# ── CoDED ─────────────────────────────────────
CODED_KEYWORDS = [
    # Core
    "economic data", "statistical data", "official statistics",
    "data governance", "data policy", "data infrastructure",
    "national data", "government data", "public data",
    "data ecosystem", "data architecture",

    # Institutions
    "national statistical office", "nso", "mospi",
    "ministry of statistics", "central statistics office", "cso",
    "nsso", "national sample survey", "registrar general",
    "economic census", "annual survey of industries",
    "national statistical commission",

    # Census (tightened)
    "population census", "census data", "census commissioner",
    "census enumeration", "census 2021", "census 2026",
    "digital census", "house listing census",

    # Indicators & Surveys
    "gdp data", "gdp growth estimate", "gdp revision",
    "gross domestic product data", "gdp base year",
    "inflation data", "cpi data", "consumer price index data",
    "wpi data", "wholesale price index data", "iip data",
    "index of industrial production",
    "periodic labour force survey", "plfs report",
    "consumption expenditure survey", "hces",
    "national accounts statistics", "supply use table",
    "advance estimate gdp", "first advance estimate",
    "second advance estimate",

    # Data quality (tightened)
    "data quality assessment", "data accuracy improvement",
    "statistical methodology", "survey methodology",
    "base year revision", "data revision gdp",
    "sampling methodology", "survey design statistics",
    "price statistics", "volume index",

    # Digital data governance (tightened — no plain "ai" or "big data")
    "data analytics platform government",
    "ai governance framework", "ai policy regulation",
    "data exchange protocol", "data sharing framework",
    "open data policy", "open government data",
    "data protection law", "data privacy regulation",
    "pdp bill", "digital personal data protection",
    "dpdp act", "data principal", "data fiduciary",
    "national data governance", "data governance framework",

    # Administrative data
    "gst data analysis", "tax data statistics",
    "e-way bill statistics", "gstn data",
    "mca21", "company data registry", "epfo statistics",
    "administrative data use", "administrative data linkage",

    # Reports & Publications
    "economic survey india", "rbi annual report",
    "rbi monetary policy report", "rbi bulletin statistics",
    "statistical yearbook india", "india statistics compendium",
    "sdg india index", "state statistics bureau",
    "niti aayog data report", "india data handbook",

    # Data systems & Platforms
    "ndap", "national data analytics platform",
    "data catalogue government", "data.gov.in",
    "unified data platform", "india data portal",
    "data linkage government", "integrated data platform",
    "data sharing agreement government",
]

# ── iLEAP ─────────────────────────────────────
ILEAP_KEYWORDS = [
    # Lead-specific — highly targeted, keep all
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

    # Heavy metals (specific)
    "heavy metal contamination", "heavy metal pollution",
    "heavy metal toxicity", "heavy metal exposure",
    "mercury contamination", "mercury pollution", "mercury poisoning",
    "cadmium contamination", "cadmium poisoning",
    "arsenic contamination", "arsenic poisoning",
    "chromium contamination", "chromium poisoning",
    "metal contamination", "metal poisoning",
    "toxic metal", "neurotoxic metal",

    # Regulations & Standards (tightened)
    "is 16088",                        # BIS standard specific to lead paint
    "lead limit regulation", "lead regulation",
    "lead ban", "lead phase out policy",
    "hazardous substance regulation",
    "rohs compliance", "restriction of hazardous substances",

    # Neurotoxicity (paired — not standalone)
    "neurotoxic exposure", "neurotoxicity children",
    "cognitive impairment children", "iq loss children",
    "developmental neurotoxicity", "child neurotoxin",
    "prenatal lead", "fetal lead exposure",

    # Environment (tightened — only metal/toxin-specific)
    "pollution control board lead", "cpcb lead", "spcb lead",
    "industrial effluent heavy metal", "hazardous waste metal",
    "e-waste lead", "e-waste heavy metal",
    "soil lead contamination", "groundwater arsenic",
    "groundwater lead", "water lead contamination",
    "particulate matter heavy metal", "air toxic metal",
    "pm2.5 lead", "dust lead exposure",
    "toxic waste dump", "contaminated site cleanup",

    # Occupational health (tightened)
    "occupational lead exposure", "occupational heavy metal",
    "lead worker health", "smelter worker health",
    "battery worker health", "paint worker lead exposure",
    "occupational toxic exposure",

    # Global programs & bodies
    "national lead elimination", "global lead network",
    "pure earth", "ipen lead", "unep lead",
    "lead paint alliance", "who lead guideline",
    "unicef lead", "global burden lead",
    "lead elimination program",
]

# ── ELS ───────────────────────────────────────
ELS_KEYWORDS = [
    # Core employment
    "employment generation", "job creation",
    "unemployment rate", "unemployment data",
    "labour market reform", "workforce development",
    "employment scheme", "employment program",
    "employment exchange", "job portal government",
    "net employment", "new jobs created",

    # Wage & conditions
    "minimum wage revision", "minimum wage notification",
    "wage board", "wage revision",
    "equal remuneration act", "wage compliance",
    "wage theft", "wage arrears",
    "floor wage", "national floor wage",

    # Skill development (tightened)
    "skill development scheme", "skill training program government",
    "vocational training scheme", "skill india mission",
    "pmkvy", "pradhan mantri kaushal vikas yojana",
    "iti training", "iti upgradation",
    "polytechnic scheme", "national skills qualifications framework",
    "apprenticeship scheme", "national apprenticeship promotion",
    "recognition of prior learning",
    "nsdc", "sector skill council", "skill certification",
    "jan shikshan sansthan",

    # Employment Schemes
    "mahatma gandhi nrega", "mgnregs", "mnrega",
    "pm employment guarantee", "urban employment scheme",
    "deen dayal upadhyaya", "ddu-gky", "rsetis",
    "pmegp", "pm rojgar", "pm internship scheme",
    "national career service",

    # Informal economy
    "informal sector workers", "informal economy policy",
    "street vendors scheme", "pm svnidhi",
    "unorganized workers", "e-shram registration",
    "e-shram portal", "unorganised sector scheme",

    # Labour welfare (tightened — no plain "pension")
    "labour welfare scheme", "worker welfare fund",
    "construction worker welfare", "building worker cess",
    "esi scheme", "esic benefit", "epfo scheme",
    "employee provident fund", "social security worker",
    "labour pension scheme", "unorganized sector pension",
    "pm shram yogi mandhan", "atal pension yojana labour",

    # Women employment (tightened)
    "women employment scheme", "women workforce participation",
    "female labour force participation", "working women hostel",
    "maternity benefit scheme", "women entrepreneur scheme",
    "self help group livelihood", "shg employment",
    "pradhan mantri mahila shakti", "women self employment",

    # Migration & Gig
    "migrant worker welfare", "migrant labour policy",
    "gig worker rights", "platform worker policy",
    "interstate migrant worker", "labour migration policy",
    "one nation one ration", "onorc",
    "gig economy regulation", "platform economy policy",

    # Labour statistics
    "plfs report", "periodic labour force survey",
    "employment unemployment survey", "labour bureau survey",
    "employment statistics", "labour statistics india",
    "labour force participation rate", "lfpr data",
    "worker population ratio", "formal employment data",
    "quarterly employment survey",

    # Industry-specific
    "textile employment", "construction workers welfare",
    "domestic workers rights", "domestic workers code",
    "plantation labour welfare", "mining workers welfare",
    "beedi workers welfare", "contract labour regulation",

    # Youth employment
    "youth employment scheme", "youth unemployment data",
    "first time job seeker", "campus placement scheme",
    "internship scheme government", "apprenticeship act",
    "national career centre",
]

# ── Negative keywords ─────────────────────────
NEGATIVE_KEYWORDS = [
    # Routine/Administrative
    "condolence", "obituary", "death anniversary", "birth anniversary",
    "greetings on", "wishes on", "festival greetings",
    "republic day parade", "independence day celebration",
    "diwali", "holi", "eid", "christmas", "pongal", "onam",
    "new year message", "mann ki baat",

    # Appointments/Transfers
    "takes charge", "assumes charge",
    "retirement function", "superannuation",
    "swearing in ceremony", "oath taking ceremony",

    # Ceremonies
    "foundation stone laying", "lays foundation stone",
    "flag hoisting ceremony",
    "cultural program", "cultural event", "cultural festival",
    "sports meet", "sports day", "marathon", "cyclothon",
    "yoga day event", "fit india movement",

    # Foreign visits/Diplomacy
    "state visit", "bilateral visit",
    "foreign minister visit", "head of state visit",
    "mou signing ceremony", "agreement signing ceremony",
    "bilateral relations", "diplomatic ties",
    "ambassador presents credentials",
    "foreign delegation visits", "parliamentary delegation visits",
    "cultural exchange program", "people to people contact",
    "diaspora event", "pravasi bharatiya divas",
    "india caucus", "friendship group",

    # Defence/Security
    "military exercise", "naval exercise", "air exercise",
    "passing out parade", "commissioning ceremony",
    "defence expo", "aero india", "defexpo",
    "gallantry award", "vir chakra", "param vir chakra",
    "sainik school", "rashtriya military school",
    "bsf raising day", "crpf raising day",
    "cisf raising day", "coast guard day",
    "navy day", "air force day", "army day",
    "defence procurement policy", "defence indigenisation",

    # Awards/Felicitation
    "award ceremony", "prize distribution", "felicitation ceremony",
    "padma awards", "national awards ceremony",
    "excellence award", "best performance award",

    # Ceremonial/Political noise
    "farewell function", "book launch event",
    "commemorative stamp release", "coin release ceremony",
    "convocation ceremony", "degree distribution",
    "national conference inauguration", "national convention inauguration",
    "international day celebration", "world day celebration",
    "international women's day event", "world water day event",
    "world environment day event", "world health day event",
    "world tuberculosis day", "world aids day",

    # Generic health events (iLEAP fix)
    "hospital inauguration", "medical college inauguration",
    "dispensary inauguration", "health camp",
    "blood donation camp", "eye check-up camp",
    "pulse polio campaign", "immunization drive",
    "vaccination camp", "covid vaccination drive",
    "cancer awareness drive", "diabetes awareness",
    "heart disease awareness", "mental health awareness day",
    "tb awareness drive", "malaria awareness",
    "ayush day", "yoga for health",

    # Infrastructure inaugurations (EoDB fix)
    "highway inauguration", "road inauguration",
    "bridge inauguration", "tunnel inauguration",
    "airport inauguration", "port inauguration",
    "railway line inauguration", "metro inauguration",
    "dam inauguration", "power plant inauguration",
    "expressway inauguration",

    # Elections
    "election", "election schedule", "election notification",
    "model code of conduct", "voter turnout",
    "polling station", "ballot paper",
    "election results", "by-election notification",
    "voter registration drive",

    # AI/Tech events (CoDED fix)
    "ai summit", "ai expo", "ai impact summit",
    "tech summit", "digital india week",
    "startup india festival", "hackathon event",
    "innovation challenge", "technology exhibition",
    "india ai expo",

    # General noise
    "pib fact check", "fake news alert",
    "all india radio", "doordarshan programme",
]

# ─────────────────────────────────────────────
# Build lookup structures
# ─────────────────────────────────────────────
KEYWORD_MAP = {
    "EoDB":  EODB_KEYWORDS,
    "CoDED": CODED_KEYWORDS,
    "iLEAP": ILEAP_KEYWORDS,
    "ELS":   ELS_KEYWORDS,
}

# Pre-sort keywords longest-first so longer phrases match before substrings
for _v in KEYWORD_MAP:
    KEYWORD_MAP[_v] = sorted(KEYWORD_MAP[_v], key=len, reverse=True)

NEGATIVE_SET = set(NEGATIVE_KEYWORDS)


# ─────────────────────────────────────────────
# Scoring helpers
# ─────────────────────────────────────────────

def is_negative(title: str) -> bool:
    """Return True if the title matches any negative keyword."""
    t = title.lower()
    return any(neg in t for neg in NEGATIVE_SET)


def matched_keywords_debug(title: str, snippet: str) -> dict[str, list]:
    """Return which keywords fired per vertical (for debugging)."""
    title_low   = title.lower()
    snippet_low = snippet.lower()
    result = {}
    for vertical, keywords in KEYWORD_MAP.items():
        hits = [kw for kw in keywords if kw in title_low or kw in snippet_low]
        if hits:
            result[vertical] = hits
    return result


# ─────────────────────────────────────────────
# HTTP helpers
# ─────────────────────────────────────────────

def get_session() -> requests.Session:
    s = requests.Session()
    s.headers.update(HEADERS)
    return s


def safe_get(session: requests.Session, url: str) -> requests.Response | None:
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
        r.raise_for_status()
        return r
    except requests.RequestException as exc:
        log.warning("GET failed: %s  — %s", url, exc)
        return None


# ─────────────────────────────────────────────
# PIB parsing
# ─────────────────────────────────────────────

def parse_release_list(html: str, region_name: str) -> list[dict]:
    """
    Parse the Allrel.aspx listing page.
    Returns list of {title, url, date, region} dicts.
    """
    soup = BeautifulSoup(html, "lxml")
    releases = []

    # PIB Allrel.aspx uses <ul class="ReleaseListing"> or similar;
    # fall back to scanning all <li> tags containing <a> with PRID links.
    for a_tag in soup.select("a[href*='PressReleasePage'], a[href*='PressReleseDetail']"):
        title = a_tag.get_text(strip=True)
        if not title or len(title) < 10:
            continue

        href = a_tag.get("href", "")
        if not href.startswith("http"):
            href = PIB_BASE + href

        # Try to grab sibling date text (usually in a <span> nearby)
        date_str = ""
        parent = a_tag.find_parent(["li", "div", "tr"])
        if parent:
            spans = parent.find_all("span")
            for sp in spans:
                txt = sp.get_text(strip=True)
                if txt and any(m in txt for m in [
                    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
                ]):
                    date_str = txt
                    break

        releases.append({
            "title":  title,
            "url":    href,
            "date":   parse_date(date_str),
            "region": region_name,
        })

    return releases


def parse_date(raw: str) -> str:
    """
    Attempt to normalise various PIB date strings to YYYY-MM-DD.
    Returns today's date string if parsing fails.
    """
    raw = raw.strip()
    formats = [
        "%d %B %Y",   # 28 March 2026
        "%d %b %Y",   # 28 Mar 2026
        "%B %d, %Y",  # March 28, 2026
        "%d/%m/%Y",   # 28/03/2026
        "%Y-%m-%d",   # already ISO
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def fetch_snippet(session: requests.Session, url: str) -> str:
    """
    Fetch the press release page and return the first SNIPPET_LENGTH
    characters of body text.  Returns empty string on failure.
    """
    resp = safe_get(session, url)
    if not resp:
        return ""
    soup = BeautifulSoup(resp.text, "lxml")

    # PIB detail pages have content in <div id="content"> or similar
    content_div = (
        soup.find("div", {"id": "content"})
        or soup.find("div", class_=lambda c: c and "release" in c.lower())
        or soup.find("div", class_=lambda c: c and "content" in c.lower())
    )
    if content_div:
        text = content_div.get_text(" ", strip=True)
    else:
        # Fallback: grab all paragraph text
        paragraphs = soup.find_all("p")
        text = " ".join(p.get_text(" ", strip=True) for p in paragraphs)

    # Clean up whitespace
    text = " ".join(text.split())
    return text[:SNIPPET_LENGTH]


def make_id(url: str) -> str:
    """MD5 hash of the URL as a stable unique ID."""
    return hashlib.md5(url.encode()).hexdigest()


def to_ist(dt: datetime) -> str:
    """Convert a UTC datetime to a human-readable IST string. No pytz needed."""
    ist = dt + timedelta(hours=5, minutes=30)
    return ist.strftime("%-d %b %Y, %-I:%M %p IST")


def relative_time(date_str: str) -> str:
    """
    Return a human-friendly relative label from a YYYY-MM-DD date.
    e.g. 'Today', 'Yesterday', '2 days ago'
    """
    try:
        release = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError:
        return "Recently"

    delta = (datetime.now(timezone.utc) - release).days
    if delta == 0:
        return "Today"
    elif delta == 1:
        return "Yesterday"
    elif delta < 7:
        return f"{delta} days ago"
    else:
        return release.strftime("%-d %b %Y")


def primary_vertical(scores: dict) -> str:
    """
    Pick the single highest-scoring vertical.
    Returns 'Other' when no vertical threshold was met (All-only releases).
    """
    if not scores:
        return "Other"
    return max(scores, key=scores.get)


def is_within_window(date_str: str, hours: int = FRESH_WINDOW_HOURS) -> bool:
    """
    Return True if date_str (YYYY-MM-DD) falls within the last `hours` hours.

    Because PIB only provides a date (no time), we treat the release as
    published at midnight UTC on that date — meaning a release dated
    'today' is always included, and releases older than the window are
    skipped.  This gives a safe, inclusive interpretation.
    """
    try:
        release_date = datetime.strptime(date_str, "%Y-%m-%d").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        # If date couldn't be parsed we defaulted to today — keep it.
        return True

    cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
    return release_date >= cutoff


# ─────────────────────────────────────────────
# Main scrape loop
# ─────────────────────────────────────────────

def scrape_all_regions(session: requests.Session) -> list[dict]:
    all_releases = []
    seen_ids: set[str] = set()

    for reg_id, region_name in PIB_REGIONS.items():
        url = PIB_LIST.format(reg=reg_id)
        log.info("Scraping %-22s  (%s)", region_name, url)

        resp = safe_get(session, url)
        if not resp:
            log.warning("Skipping %s — fetch failed", region_name)
            time.sleep(REQUEST_DELAY)
            continue

        raw_releases = parse_release_list(resp.text, region_name)
        log.info("  → found %d links", len(raw_releases))

        for rel in raw_releases:
            uid = make_id(rel["url"])
            if uid in seen_ids:
                continue                    # deduplicate across regions
            seen_ids.add(uid)

            title = rel["title"]

            # ── Step 0: 48-hour freshness gate ──────────────────────
            if not is_within_window(rel["date"]):
                log.debug("  [SKIP-OLD] %s  (%s)", title[:70], rel["date"])
                continue

            # ── Step 1: negative filter (title only — fast) ──
            if is_negative(title):
                log.debug("  [SKIP-NEG] %s", title[:80])
                continue

            # ── Step 2: fetch snippet for richer matching ──
            snippet = fetch_snippet(session, rel["url"])
            time.sleep(REQUEST_DELAY)

            # ── Step 3: score against all verticals ──
            scores = score_release(title, snippet)

            # Total relevance score = sum of all vertical scores
            total_score = sum(scores.values())

            release = {
                "id":               uid,
                "title":            title,
                "url":              rel["url"],
                "date":             rel["date"],
                "relative_time":    relative_time(rel["date"]),   # "Today" / "Yesterday" / "2 days ago"
                "region":           rel["region"],
                "verticals":        sorted(scores.keys()),         # [] = "All" only
                "primary_vertical": primary_vertical(scores),      # highest-scoring vertical
                "relevance_score":  total_score,
                "snippet":          snippet,
                "vertical_scores":  scores,
            }

            all_releases.append(release)

            if scores:
                log.info(
                    "  [MATCH] %s | verticals=%s score=%d",
                    title[:70], list(scores.keys()), total_score,
                )
            else:
                log.debug("  [ALL]   %s", title[:70])

        time.sleep(REQUEST_DELAY)

    return all_releases


# ─────────────────────────────────────────────
# Merge with existing pib.json
# ─────────────────────────────────────────────

def load_existing(path: str) -> list[dict]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        # Support both old key ("releases") and new key ("articles")
        return data.get("articles", data.get("releases", []))
    except (json.JSONDecodeError, KeyError):
        log.warning("Could not load existing pib.json — starting fresh")
        return []


def merge_releases(existing: list[dict], fresh: list[dict]) -> list[dict]:
    """
    Merge fresh scrape into existing, deduplicate by id,
    sort newest-first, keep MAX_RELEASES_KEPT.
    """
    by_id: dict[str, dict] = {r["id"]: r for r in existing}
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

def write_output(releases: list[dict], path: str) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)

    vertical_counts: dict[str, int] = {v: 0 for v in KEYWORD_MAP}
    all_only = 0
    regions_seen: set[str] = set()

    for r in releases:
        regions_seen.add(r["region"])
        if r["verticals"]:
            for v in r["verticals"]:
                vertical_counts[v] = vertical_counts.get(v, 0) + 1
        else:
            all_only += 1

    now_utc = datetime.now(timezone.utc)

    output = {
        # ── Fields index.html reads directly ──────────────────────────
        "last_updated":     now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_updated_ist": to_ist(now_utc),          # "29 Mar 2026, 10:15 AM IST"
        "total":            len(releases),             # data.total
        "articles":         releases,                  # data.articles[]

        # ── Extra metadata (useful for debugging / future features) ───
        "all_only_count":   all_only,
        "vertical_counts":  vertical_counts,
        "regions_scraped":  len(regions_seen),

        # ── Vertical & region metadata (mirrors your hand-crafted json) ─
        "verticals": {
            "EoDB":  {"label": "Ease of Doing Business & Export-Led Manufacturing", "color": "#E8620A"},
            "CoDED": {"label": "Center of Data for Economic Decision-making",       "color": "#2471A3"},
            "iLEAP": {"label": "Lead Elimination & Public Health",                  "color": "#C0392B"},
            "ELS":   {"label": "Employment & Livelihood Systems",                   "color": "#7D3C98"},
        },
        "regions": PIB_REGIONS,
    }

    with open(path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    log.info(
        "Written %d articles → %s  (EoDB=%d CoDED=%d iLEAP=%d ELS=%d AllOnly=%d)",
        len(releases), path,
        vertical_counts.get("EoDB",  0),
        vertical_counts.get("CoDED", 0),
        vertical_counts.get("iLEAP", 0),
        vertical_counts.get("ELS",   0),
        all_only,
    )


# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

def main() -> None:
    log.info("═" * 60)
    log.info("PIF PIB Scraper v2 — starting")
    log.info("Regions: %d  |  Window: last %dh  |  Output: %s",
             len(PIB_REGIONS), FRESH_WINDOW_HOURS, OUTPUT_PATH)
    log.info("═" * 60)

    session = get_session()

    # 1. Scrape all regions
    fresh = scrape_all_regions(session)
    log.info("Scrape complete — %d releases before merge", len(fresh))

    # 2. Merge with existing data
    existing = load_existing(OUTPUT_PATH)
    merged   = merge_releases(existing, fresh)

    # 3. Write to docs/pib.json
    write_output(merged, OUTPUT_PATH)

    log.info("Done. ✓")


if __name__ == "__main__":
    main()
