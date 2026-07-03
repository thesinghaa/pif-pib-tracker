#!/usr/bin/env python3
"""
scraper_core.py — Shared engine for PIF News Intelligence
==========================================================
Imported by scraper_v3.py, scraper_gdelt.py, scraper_newsapi.py, combine.py.
Contains: verticals, seed index, scoring, dedup, date utils, fetch helpers.
Nothing here does any network I/O except fetch_article_body().
"""

import hashlib
import datetime
import re
import requests
from collections import defaultdict

try:
    import trafilatura
    _TRAF = True
except ImportError:
    _TRAF = False

try:
    from bs4 import BeautifulSoup
    _BS4 = True
except ImportError:
    _BS4 = False

# ─────────────────────────────────────────────────────────────
#  CONSTANTS
# ─────────────────────────────────────────────────────────────
CUTOFF_HOURS    = 72
TITLE_W         = 3.0
SUMMARY_W       = 1.5
BODY_W          = 1.0
LEAD_BONUS      = 2.0
CROSS_BONUS     = 3.0
DIR_BONUS       = 1.5
DIR_WINDOW      = 8

TIER_FLAGSHIP   = 15
TIER_PRIMARY    = 8
TIER_PERIPHERAL = 3

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ─────────────────────────────────────────────────────────────
#  DIRECTIONAL LEXICON
# ─────────────────────────────────────────────────────────────
UPSWING = [
    "improv", "accelerat", "increas", "expand", "growth", "grew",
    "boost", "surge", "rise", "risen", "rose", "recover", "rebound",
    "reform", "liberalis", "liberaliz", "ease", "simplif", "promot",
    "approv", "invest", "inflow", "gain", "higher", "record",
    "creat", "generat", "achiev", "success", "target met",
    "breakthrough", "milestone", "outperform", "strengthen", "better",
    "launch", "rollout", "deploy", "allocat", "fund",
]
DOWNSWING = [
    "declin", "decreas", "contract", "slow", "shrink",
    "fall", "fell", "fallen", "drop", "miss", "weaken",
    "deteriorat", "worsen", "concern", "challeng", "barrier", "obstacle",
    "delay", "stall", "burden", "red tape", "fail", "shortfall",
    "job loss", "layoff", "retrench", "unemploy",
    "pollut", "contaminat", "toxic", "hazard", "poison",
    "drought", "crisis", "disruption", "shortage", "risk",
]
STRONG_UPSWING  = ["recovery", "turnaround", "boom", "bumper", "record high",
                   "all time high", "historic", "fastest growing"]
STRONG_DOWNSWING= ["recession", "crisis", "contraction", "slump", "collapse",
                   "mass layoff", "bankruptcy", "default", "stagnation"]
UNCERTAINTY     = ["uncertain", "unclear", "ambiguous", "pending", "delayed",
                   "under review", "volatile", "unpredictable", "at risk"]

# ─────────────────────────────────────────────────────────────
#  NEGATIVE KEYWORDS
# ─────────────────────────────────────────────────────────────
HARD_NEG = [
    "murder", "rape", "sexual assault", "robbery", "theft", "kidnap",
    "arrested", "police custody", "fir filed", "chargesheet",
    "bail granted", "convicted", "sentenced", "acquitted",
    "gang war", "mob lynching", "ed raids",
    "bollywood", "box office", "celebrity gossip", "film review",
    "web series", "reality show", "bigg boss",
    "ipl match", "cricket match result", "fifa", "nba game",
    "happy diwali", "happy holi", "eid mubarak",
    "horoscope", "astrology", "fashion week",
    "weight loss tips", "skin care routine", "recipe",
    "road accident", "train accident", "plane crash",
    "building collapse", "fire accident",
    "passes away", "condolence", "funeral", "prayer meet",
    "trailer launch", "song release", "concert tour", "award ceremony",
    "filmfare", "oscars india",
]
SOFT_NEG = [
    ("share price", 0.4), ("stock surges", 0.4),
    ("nifty", 0.5),       ("sensex", 0.5),
    ("quarterly result", 0.4), ("profit rises", 0.4),
    ("ipo listing", 0.4), ("campaign trail", 0.6),
    ("rally held", 0.6),  ("weather update", 0.5),
]

# ─────────────────────────────────────────────────────────────
#  VERTICALS
#  Seed shape: {"kw": str, "var": [str...], "s": "strong"|"usable",
#               "sub": "iLEAP"|"PAVANA"|None}
#  Signal strength drives weight (see STRENGTH_W). "drop"-rated seeds
#  from the keyword sheet are omitted entirely (too broad to index).
#  Source: PIF_Newstracker_Keywords.xlsx (2026-06-29)
# ─────────────────────────────────────────────────────────────
def S(kw, var=None, s="strong", sub=None):
    return {"kw": kw, "var": var or [], "s": s, "sub": sub}

VERTICALS = {
    "General": {
        "label": "General (Cross-Cutting)",
        "color": "#6B7280", "emoji": "🗞️",
        "seeds": [
            S("union budget", ["union budget india", "budget 2025", "annual budget india",
                               "finance minister budget", "budget speech india", "budget announcements"]),
            S("economic survey india", ["economic survey 2025", "annual economic survey",
                                        "chief economic adviser india"]),
            S("niti aayog", ["niti aayog report", "niti aayog india",
                             "national institution for transforming india"]),
            S("ministry of finance", ["finance ministry india", "mof india",
                                      "department of economic affairs", "dea india"]),
            S("india policy reform", ["policy reform india", "structural reform india",
                                      "economic reform india", "reform agenda india"]),
            S("state budget india", ["state budget 2025", "state fiscal policy india",
                                     "state government budget", "state finance commission"]),
            S("india gdp", ["india gdp growth", "gdp estimate india", "india economic output",
                            "india national income"], "usable"),
            S("india growth forecast", ["india growth projection", "india growth estimate",
                                        "india economic outlook", "india gdp forecast"], "usable"),
            S("rbi policy", ["rbi monetary policy", "reserve bank india policy", "rbi repo rate",
                             "rbi mpc", "monetary policy committee india"], "usable"),
            S("india inflation", ["inflation india data", "india price rise",
                                  "india consumer prices", "india wholesale prices"], "usable"),
            S("world bank india", ["world bank india report", "world bank india poverty",
                                   "world bank india economy"], "usable"),
            S("imf india", ["imf india outlook", "imf india forecast",
                            "international monetary fund india"], "usable"),
        ]
    },
    "CoDED": {
        "label": "Centre for Data for Economic Decision-Making",
        "color": "#2471A3", "emoji": "📊",
        "seeds": [
            S("mospi", ["ministry of statistics india", "national statistical office", "nso india",
                        "statistical system india", "mospi report"]),
            S("plfs", ["periodic labour force survey", "plfs report", "plfs data",
                       "plfs quarterly", "labour force survey india"]),
            S("iip", ["index of industrial production", "industrial output india",
                      "industrial production data", "iip data release"]),
            S("nsso", ["national sample survey office", "nss survey india", "nsso data",
                       "household survey india"]),
            S("gdp base year revision", ["gdp rebasing india", "base year revision india",
                                         "national accounts revision", "gdp methodology india"]),
            S("national statistical commission", ["statistics commission india", "nsc india",
                                                  "statistical standards india", "data quality india"]),
            S("gsdp", ["gross state domestic product", "state gdp india", "gsdp growth",
                       "state economic output"]),
            S("cpi data release", ["consumer price index india", "cpi inflation india",
                                   "retail inflation india", "cpi data india"]),
            S("wpi data release", ["wholesale price index india", "wpi inflation india",
                                   "wpi data india", "wholesale prices india"]),
            S("nowcasting india", ["nowcast gdp india", "real-time data india",
                                   "high frequency nowcast", "economic nowcasting"]),
            S("administrative data india", ["administrative records india",
                                            "government administrative data", "admin data policy india"]),
            S("district level data india", ["district data india", "local area statistics india",
                                            "district economy india", "block level data india"]),
            S("state statistical system india", ["state statistics bureau india",
                                                 "directorate of economics statistics", "des india",
                                                 "state data systems"]),
            S("cmie data", ["centre for monitoring indian economy", "cmie report",
                            "cmie unemployment", "cmie consumer sentiment"]),
            S("rbi klems", ["klems india", "capital labour energy materials india",
                            "productivity data india", "rbi growth accounting"]),
            S("india statistical capacity", ["statistical capacity india", "data infrastructure india",
                                             "statistics reform india", "data ecosystem india"]),
            S("economic indicators india", ["macroeconomic indicators india", "macro data india",
                                            "economic dashboard india"], "usable"),
            S("high frequency data india", ["high frequency indicators india",
                                            "real time economic data india", "alternative data india"], "usable"),
            S("india data governance", ["data governance framework india", "data regulation india",
                                        "national data policy"], "usable"),
            S("data policy india", ["india data policy", "data infrastructure policy",
                                    "digital data india", "data marketplace india"], "usable"),
        ]
    },
    "ELS": {
        "label": "Employment & Livelihood Systems",
        "color": "#7D3C98", "emoji": "💼",
        "seeds": [
            S("plfs labour force", ["periodic labour force survey", "labour force participation india",
                                    "plfs quarterly report", "employment survey india"]),
            S("unemployment rate india", ["joblessness india", "india unemployment data",
                                          "unemployment statistics india", "cmie unemployment"]),
            S("mgnregs", ["mgnrega india", "mahatma gandhi national rural employment guarantee",
                          "nrega work demand", "nregs wages"]),
            S("pm vishwakarma", ["vishwakarma yojana", "pm vishwakarma scheme",
                                 "artisan skilling india", "traditional craftsmen scheme"]),
            S("skill india", ["skill india mission", "skill india programme",
                              "pradhan mantri kaushal vikas yojana", "pmkvy", "skilling india"]),
            S("nsdc", ["national skill development corporation", "skill development india",
                       "vocational training india", "nsdc training"]),
            S("informal sector india", ["informal economy india", "unorganised sector india",
                                        "informal workers india", "informal employment india"]),
            S("formalisation labour india", ["labour formalisation india",
                                             "formal employment growth india",
                                             "organised sector employment", "formal jobs india"]),
            S("epfo data", ["epfo payroll data", "employees provident fund",
                            "epfo subscribers india", "provident fund enrolment"]),
            S("esic enrolment", ["employees state insurance", "esic data india",
                                 "social security coverage india", "esic registration"]),
            S("gig workers india", ["platform workers india", "gig economy india",
                                    "app-based workers india", "gig worker policy india"]),
            S("women labour force participation india", ["female labour force participation india",
                                                         "women employment india", "flfpr india",
                                                         "women workforce india"]),
            S("msme ministry india", ["ministry of msme india", "msme policy india",
                                      "small business india policy", "msme sector india"]),
            S("udyam registration", ["udyam portal india", "msme registration india",
                                     "udyam certificate", "msme formalisation india"]),
            S("agriculture livelihoods india", ["farm livelihoods india", "agricultural income india",
                                                "farmer welfare india", "kisan india"], "usable"),
            S("rural employment india", ["rural jobs india", "rural labour market india",
                                         "village employment india", "rural work india"], "usable"),
            S("shg india livelihoods", ["self help group india", "shg livelihoods",
                                        "mahila samiti india", "women shg india"], "usable"),
            S("srlm state rural livelihoods", ["state rural livelihoods mission", "srlm india",
                                               "rural livelihoods programme india", "aajeevika india"], "usable"),
            S("msme credit india", ["msme loans india", "small business credit india",
                                    "msme financing india", "mudra loans"], "usable"),
            S("micro enterprise india", ["micro enterprise policy india", "nano enterprise india",
                                         "street vendors india", "own account workers"], "usable"),
        ]
    },
    "Environmental_Health": {
        "label": "Environmental Health",
        "color": "#C0392B", "emoji": "🩺",
        "seeds": [
            # ── iLEAP — lead poisoning ──
            S("lead poisoning india", ["lead toxicity india", "lead exposure india",
                                       "lead contamination india", "lead health effects india"], sub="iLEAP"),
            S("blood lead levels children india", ["bll children india", "lead in blood india",
                                                   "childhood lead exposure", "lead screening india"], sub="iLEAP"),
            S("lead paint regulation india", ["lead paint standard india", "paint lead limits india",
                                              "bis lead paint", "lead paint ban india"], sub="iLEAP"),
            S("lead in spices india", ["turmeric adulteration india", "spice contamination india",
                                       "lead chromate spices", "food adulteration lead"], sub="iLEAP"),
            S("lead battery recycling india", ["ulab recycling india", "used lead acid battery india",
                                               "informal battery recycling india", "lead smelting india"], sub="iLEAP"),
            S("pure earth india", ["pure earth foundation india", "blacksmith institute india",
                                   "toxic sites india"], sub="iLEAP"),
            S("fssai lead limits", ["fssai heavy metals", "food safety lead india",
                                    "fssai contaminants", "food standards lead india"], sub="iLEAP"),
            S("heavy metal contamination india", ["heavy metal pollution india",
                                                  "arsenic contamination india", "cadmium india",
                                                  "toxic metals india"], "usable", "iLEAP"),
            S("environmental health india", ["environment public health india",
                                             "pollution health india", "toxic exposure india"], "usable", "iLEAP"),
            # ── PAVANA — air pollution ──
            S("aqi india", ["air quality index india", "aqi data india", "daily aqi india",
                            "city aqi india"], sub="PAVANA"),
            S("ncap india", ["national clean air programme", "ncap targets india",
                             "clean air cities india", "ncap progress"], sub="PAVANA"),
            S("pm2.5 india", ["particulate matter india", "fine particles india", "pm10 india",
                              "air particulates india"], sub="PAVANA"),
            S("air pollution health burden india", ["air pollution deaths india", "pollution dalys india",
                                                    "respiratory disease pollution india",
                                                    "pollution mortality india"], sub="PAVANA"),
            S("stubble burning india", ["parali burning india", "crop residue burning india",
                                        "farm fire punjab haryana", "paddy straw burning india"], sub="PAVANA"),
            S("cpcb air quality", ["central pollution control board", "cpcb data india", "cpcb report",
                                   "state pollution control board india"], sub="PAVANA"),
            S("clean air india", ["clean air policy india", "air pollution reduction india",
                                  "clean air fund india", "clean air initiative"], "usable", "PAVANA"),
            S("air quality policy india", ["air pollution regulation india", "emission standards india",
                                           "vehicular pollution policy india"], "usable", "PAVANA"),
            # ── Climate & environment (no sub-tag) ──
            S("climate change", ["climate policy", "climate action", "climate finance",
                                 "climate resilience", "climate adaptation"]),
            S("renewable energy", ["clean energy", "green energy", "solar wind"]),
            S("solar power india", ["solar capacity india", "rooftop solar india", "pm surya ghar"], "usable"),
            S("net zero india", ["carbon neutral india", "decarbonisation india", "net zero 2070",
                                 "carbon emission india", "greenhouse gas"], "usable"),
            S("electric vehicle india", ["ev india", "ev policy india", "ev adoption india",
                                         "ev charging india"], "usable"),
            S("green hydrogen india", ["hydrogen energy india", "hydrogen fuel india",
                                       "green hydrogen mission"], "usable"),
            S("energy transition india", ["clean energy transition india", "coal to clean india"], "usable"),
            S("water management india", ["groundwater india", "water scarcity india",
                                         "jal jeevan mission", "namami gange"], "usable"),
            S("waste management india", ["solid waste india", "plastic waste india",
                                         "swachh bharat", "circular economy india"], "usable"),
            S("ethanol blending india", ["biofuel india", "ethanol india"], "usable"),
        ]
    },
    "Corporate_Advisory": {
        "label": "Corporate Advisory",
        "color": "#E8620A", "emoji": "🏭",
        "seeds": [
            S("ease of doing business india", ["eodb india", "doing business ranking india",
                                               "business reforms india", "business environment india",
                                               "investment ease india"]),
            S("dpiit india", ["department for promotion of industry", "dpiit policy", "dpiit fdi",
                              "dpiit startup india"]),
            S("fdi india", ["foreign direct investment india", "fdi inflows india", "fdi policy india",
                            "fdi equity india"]),
            S("pli scheme india", ["production linked incentive india", "pli policy india",
                                   "pli sectors india", "pli disbursement india"]),
            S("csr india impact", ["corporate social responsibility india", "csr spending india",
                                   "csr law india", "section 135 csr", "csr impact assessment"]),
            S("esg india corporate", ["esg reporting india", "esg disclosure india", "brsr india",
                                      "business responsibility sustainability report", "esg ratings india"]),
            S("regulatory reform india", ["regulation simplification india", "compliance burden india",
                                          "red tape india", "regulatory sandbox india", "deregulation india"]),
            S("india investment climate", ["investment environment india", "business confidence india",
                                           "investor sentiment india"], "usable"),
            S("corporate india policy", ["corporate affairs india", "mca india policy",
                                         "company law india", "corporate governance india"], "usable"),
            S("make in india", ["make in india initiative", "manufacturing india policy",
                                "atmanirbhar bharat manufacturing", "domestic manufacturing india"], "usable"),
        ]
    },
    "Government_Practice": {
        "label": "Government Practice (State Advisory)",
        "color": "#117A65", "emoji": "🏛️",
        "seeds": [
            S("state government policy", ["state policy india", "state government initiative india",
                                          "state reform india", "state governance india"]),
            S("export promotion", ["india export policy", "export incentive india", "dgft india",
                                   "merchandise exports india", "state export policy"]),
            S("gi tag india", ["geographical indication india", "gi product india",
                               "gi tag application india", "gi certification india"]),
            S("tourism policy india", ["india tourism policy", "state tourism india",
                                       "tourism ministry india", "destination india",
                                       "tourism infrastructure india"]),
            S("logistics policy india", ["national logistics policy india", "nlp india",
                                         "logistics cost india", "multimodal logistics india",
                                         "pm gati shakti"]),
            S("transport infrastructure india", ["road infrastructure india", "highway india policy",
                                                 "transport connectivity india", "freight corridor india"]),
            S("district planning india", ["district development plan india", "district administration india",
                                          "aspirational districts india", "district collector india"]),
            S("competitive federalism india", ["states competition india", "federal competition india",
                                               "inter-state ranking india", "state performance index"], "usable"),
            S("state competitiveness ranking india", ["state ranking india", "dpiit state ranking",
                                                      "ease of living index",
                                                      "state performance ranking india"], "usable"),
            S("public expenditure india state", ["state spending india", "state fiscal health india",
                                                 "state capex india", "state revenue expenditure india"], "usable"),
        ]
    },
}

# ─────────────────────────────────────────────────────────────
#  GDELT THEME MAPPINGS  (used by scraper_gdelt.py)
# ─────────────────────────────────────────────────────────────
GDELT_QUERIES = {
    "General": (
        '"union budget" OR "economic survey" OR "niti aayog" OR "RBI policy" OR '
        '"ministry of finance" OR "world bank" OR "IMF" OR "policy reform" OR '
        '"state budget" sourcecountry:IN'
    ),
    "CoDED": (
        '"MOSPI" OR "PLFS" OR "IIP" OR "NSSO" OR "GSDP" OR "nowcasting" OR '
        '"CPI inflation" OR "WPI" OR "national statistical" OR "CMIE" OR '
        '"administrative data" OR "statistical capacity" sourcecountry:IN'
    ),
    "ELS": (
        '"MGNREGS" OR "PLFS" OR "gig workers" OR "skill india" OR "NSDC" OR '
        '"informal sector" OR "EPFO" OR "ESIC" OR "udyam" OR "MSME ministry" OR '
        '"labour force participation" OR "unemployment" sourcecountry:IN'
    ),
    "Environmental_Health": (
        '"lead poisoning" OR "blood lead" OR "lead paint" OR "FSSAI" OR '
        '"air quality index" OR "NCAP" OR "PM2.5" OR "stubble burning" OR '
        '"CPCB" OR "climate change" OR "renewable energy" OR "air pollution" '
        'sourcecountry:IN'
    ),
    "Corporate_Advisory": (
        '"ease of doing business" OR "DPIIT" OR "FDI" OR "PLI scheme" OR '
        '"CSR" OR "ESG" OR "BRSR" OR "regulatory reform" OR '
        '"make in India" OR "investment climate" sourcecountry:IN'
    ),
    "Government_Practice": (
        '"state government policy" OR "export promotion" OR "GI tag" OR '
        '"tourism policy" OR "logistics policy" OR "gati shakti" OR '
        '"transport infrastructure" OR "aspirational districts" OR '
        '"competitive federalism" sourcecountry:IN'
    ),
}

# ─────────────────────────────────────────────────────────────
#  NEWSAPI QUERY MAPPINGS  (used by scraper_newsapi.py)
# ─────────────────────────────────────────────────────────────
NEWSAPI_QUERIES = {
    "General": (
        "\"union budget India\" OR \"economic survey India\" OR \"Niti Aayog\" OR "
        "\"RBI policy\" OR \"World Bank India\" OR \"IMF India\""
    ),
    "CoDED": (
        "MOSPI OR PLFS OR \"index of industrial production\" OR NSSO OR "
        "\"India CPI inflation\" OR \"nowcasting India\" OR CMIE"
    ),
    "ELS": (
        "MGNREGS OR \"gig workers India\" OR \"skill India\" OR NSDC OR "
        "\"informal sector India\" OR EPFO OR \"udyam registration\" OR "
        "\"unemployment India\""
    ),
    "Environmental_Health": (
        "\"lead poisoning India\" OR \"lead paint India\" OR \"blood lead\" OR "
        "\"air quality index India\" OR NCAP OR \"PM2.5 India\" OR "
        "\"stubble burning\" OR \"climate change India\" OR \"renewable energy India\""
    ),
    "Corporate_Advisory": (
        "\"ease of doing business India\" OR DPIIT OR \"FDI India\" OR "
        "\"PLI scheme\" OR \"CSR India\" OR \"ESG India\" OR \"BRSR\" OR "
        "\"regulatory reform India\""
    ),
    "Government_Practice": (
        "\"state government policy India\" OR \"export promotion India\" OR "
        "\"GI tag India\" OR \"tourism policy India\" OR \"logistics policy India\" OR "
        "\"PM Gati Shakti\" OR \"aspirational districts\""
    ),
}

# ─────────────────────────────────────────────────────────────
#  BUILD PHRASE INDEX
#  Weight is driven by Signal Strength (from the keyword sheet),
#  not word count. "strong" seeds carry more weight than "usable".
#  Each index entry: (phrase, weight, sub_vertical | None)
# ─────────────────────────────────────────────────────────────
_INDEX = {}

STRENGTH_W = {"strong": 3.0, "usable": 1.5}

# Keyword sheet phrases are heavily India-qualified ("FDI india",
# "lead paint regulation india"). The corpus is already India-only, so
# real headlines rarely repeat "India" — treat it as an ignorable token
# on both the seed and the article side so matching actually fires.
GEO_STOP = {"india", "indian"}

def _norm(text):
    text = text.lower()
    text = re.sub(r"[^\w\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()

def _norm_geo(text):
    toks = [t for t in _norm(text).split() if t not in GEO_STOP]
    return " ".join(toks)

def _build_index():
    for vid, vdata in VERTICALS.items():
        entries = []
        for seed in vdata["seeds"]:
            w   = STRENGTH_W.get(seed["s"], 1.5)
            sub = seed.get("sub")
            for phrase in [seed["kw"], *seed["var"]]:
                norm = _norm_geo(phrase)
                if norm:
                    entries.append((norm, w, sub))
        _INDEX[vid] = entries

_build_index()

# ─────────────────────────────────────────────────────────────
#  MATCHING ENGINE
# ─────────────────────────────────────────────────────────────
def _tokens(text):
    return re.sub(r"[^\w\s]", " ", text.lower()).split()

def _ngrams(tokens, max_n=5):
    out = set()
    for n in range(1, max_n + 1):
        for i in range(len(tokens) - n + 1):
            out.add(" ".join(tokens[i:i+n]))
    return out

def _directional_score(tokens, keyword_tokens):
    klen = len(keyword_tokens)
    up_hits = 0; down_hits = 0; uncertain = False
    for i in range(len(tokens) - klen + 1):
        if tokens[i:i+klen] == keyword_tokens:
            window   = tokens[max(0, i-DIR_WINDOW): i+klen+DIR_WINDOW]
            win_text = " ".join(window)
            for root in UPSWING:
                if any(w.startswith(root) for w in window):
                    up_hits += 1; break
            for root in DOWNSWING:
                if any(w.startswith(root) for w in window):
                    down_hits += 1; break
            for u in UNCERTAINTY:
                if u in win_text:
                    uncertain = True
    full = " ".join(tokens)
    for s in STRONG_UPSWING:
        if s in full: up_hits += 1
    for s in STRONG_DOWNSWING:
        if s in full: down_hits += 1
    if up_hits > down_hits:   return  1, uncertain
    elif down_hits > up_hits: return -1, uncertain
    return 0, uncertain

def _drop():
    return {"verticals": [], "relevance_score": 0, "tier": "drop",
            "sentiment": {}, "uncertainty": {}, "sub_verticals": []}

def match_article(title, summary="", body=""):
    """
    Core scoring function. Returns:
      verticals, relevance_score, tier, sentiment, uncertainty, sub_verticals
    Shared by all three scrapers and combine.py.

    verticals     — matched vertical ids, score-ordered. Low-confidence
                    matches additionally carry "General".
    sub_verticals — iLEAP / PAVANA tags hit (Environmental_Health only).
    """
    title_lower = title.lower()
    full_text   = f"{title_lower} {summary.lower()} {body.lower()}"

    for neg in HARD_NEG:
        if neg in full_text:
            return _drop()

    def _gtoks(text):
        return [t for t in _tokens(text) if t not in GEO_STOP]

    title_toks = _gtoks(title);   title_ng   = _ngrams(title_toks)
    summary_ng = _ngrams(_gtoks(summary))
    body_ng    = _ngrams(_gtoks(body))
    all_toks   = _gtoks(full_text)
    lead_words = set(title_toks[:5])

    v_scores = {}; v_sentiment = {}; v_uncertain = {}; v_subs = {}

    for vid, phrases in _INDEX.items():
        best_score = 0.0; best_phrase_t = None; subs = set()
        for (phrase, kw_weight, sub) in phrases:
            score = 0.0
            if phrase in title_ng:
                score = kw_weight * TITLE_W
                if set(phrase.split()).issubset(lead_words):
                    score += LEAD_BONUS
            elif phrase in summary_ng:
                score = kw_weight * SUMMARY_W
            elif phrase in body_ng:
                score = kw_weight * BODY_W
            if score > 0:
                if sub:
                    subs.add(sub)
                if score > best_score:
                    best_score = score; best_phrase_t = phrase.split()
        if best_score > 0:
            direction, uncertain = _directional_score(all_toks, best_phrase_t)
            if direction != 0: best_score += DIR_BONUS
            v_scores[vid]    = best_score
            v_sentiment[vid] = direction
            v_uncertain[vid] = uncertain
            v_subs[vid]      = subs

    if not v_scores:
        return _drop()

    penalty = 1.0
    for (neg, factor) in SOFT_NEG:
        if neg in full_text: penalty = min(penalty, factor)

    total = sum(v_scores.values()) * penalty
    if len(v_scores) >= 2:
        total += CROSS_BONUS * (len(v_scores) - 1)

    if   total >= TIER_FLAGSHIP:   tier = "flagship"
    elif total >= TIER_PRIMARY:    tier = "primary"
    elif total >= TIER_PERIPHERAL: tier = "peripheral"
    else:                          tier = "drop"

    if tier == "drop":
        return _drop()

    # Score-ordered vertical list
    ordered = sorted(v_scores, key=lambda k: -v_scores[k])

    # General fallback: peripheral-tier (low-score) topical matches are
    # low-confidence, so also tag them General → displayed as "<vertical> / General".
    # Articles that matched General's own seeds keep it regardless of tier.
    topical = [v for v in ordered if v != "General"]
    if topical and "General" not in ordered and tier == "peripheral":
        ordered.append("General")

    # Collect sub-tags (iLEAP / PAVANA) from matched verticals
    sub_verticals = sorted({s for v in ordered for s in v_subs.get(v, set())})

    return {
        "verticals":       ordered,
        "relevance_score": round(total, 2),
        "tier":            tier,
        "sentiment":       v_sentiment,
        "uncertainty":     v_uncertain,
        "sub_verticals":   sub_verticals,
    }

# ─────────────────────────────────────────────────────────────
#  DEDUPLICATION  (Jaccard on title tokens)
# ─────────────────────────────────────────────────────────────
_STOP = {"the","a","an","in","of","on","at","to","for","is","are","was",
         "were","and","or","but","with","by","from","its","india","indian",
         "says","said","after","over","amid","as","be","been","has","have"}

class Dedup:
    def __init__(self, threshold=0.72):
        self.t = threshold
        self._seen = []

    def is_dup(self, title):
        fp = frozenset(w for w in re.sub(r"[^\w\s]"," ",title.lower()).split()
                       if w not in _STOP and len(w) > 2)
        for seen_fp in self._seen:
            inter = len(fp & seen_fp)
            union = len(fp | seen_fp)
            if union and inter/union >= self.t:
                return True
        self._seen.append(fp)
        return False

# ─────────────────────────────────────────────────────────────
#  DATE UTILITIES
# ─────────────────────────────────────────────────────────────
def make_id(title, source):
    return hashlib.md5(f"{title}|{source}".encode()).hexdigest()[:14]

def parse_iso_date(date_str):
    """
    Parse an ISO date string to (datetime, iso_string).
    Handles: YYYYMMDDTHHMMSSZ, YYYY-MM-DDTHH:MM:SS, YYYY-MM-DD, etc.
    Returns (None, "") if unparseable. Returns (dt, dt_string) always — callers use within_window() to reject old dates.
    """
    now     = datetime.datetime.utcnow()
    max_age = now - datetime.timedelta(hours=CUTOFF_HOURS)
    if not date_str:
        return None, ""
    # Normalise GDELT compact format: 20260423T113000Z → 2026-04-23T11:30:00
    clean = re.sub(r"(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z?",
                   r"\1-\2-\3T\4:\5:\6", date_str)
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            dt = datetime.datetime.strptime(clean[:19], fmt)
            if dt > now:   dt = now
            return dt, dt.strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    return None, ""

def within_window(dt):
    if dt is None: return False
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=CUTOFF_HOURS)
    return dt >= cutoff

# ─────────────────────────────────────────────────────────────
#  ARTICLE BODY FETCH  (trafilatura → BS4 fallback)
# ─────────────────────────────────────────────────────────────
def fetch_article_body(url, max_chars=2000):
    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        html = resp.text
        if _TRAF:
            text = trafilatura.extract(
                html, include_comments=False,
                include_tables=False, no_fallback=False,
            ) or ""
        elif _BS4:
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(html, "html.parser")
            for tag in soup(["nav","header","footer","aside","script","style","noscript"]):
                tag.decompose()
            text = re.sub(r"\s+", " ", soup.get_text(separator=" ")).strip()
        else:
            text = ""
        return text[:max_chars]
    except Exception as e:
        print(f"      Body fetch failed [{url[:55]}]: {e}")
        return ""

# ─────────────────────────────────────────────────────────────
#  SORT  (newest first → tier → score)
# ─────────────────────────────────────────────────────────────
TIER_ORDER = {"flagship": 0, "primary": 1, "peripheral": 2}

def sort_articles(articles):
    def key(a):
        try:
            dt = datetime.datetime.fromisoformat(a["date"])
        except Exception:
            dt = datetime.datetime.min
        return (-dt.timestamp(), TIER_ORDER.get(a["tier"], 3), -a["relevance_score"])
    return sorted(articles, key=key)

# ─────────────────────────────────────────────────────────────
#  STANDARD ARTICLE DICT  (all scrapers produce this shape)
# ─────────────────────────────────────────────────────────────
def make_article(title, url, summary, body, source_name,
                 short, date_str, opinion, result, source_type="rss"):
    return {
        "id":              make_id(title, source_name),
        "title":           title,
        "summary":         summary[:350],
        "full_content":    body if source_name == "PIB (Govt. of India)" else "",
        "url":             url,
        "source":          source_name,
        "short":           short,
        "date":            date_str,
        "opinion":         opinion,
        "verticals":       result["verticals"],
        "sub_verticals":   result.get("sub_verticals", []),
        "relevance_score": result["relevance_score"],
        "tier":            result["tier"],
        "sentiment":       result["sentiment"],
        "uncertainty":     result["uncertainty"],
        "source_type":     source_type,   # "rss" | "gdelt" | "newsapi" | "pib"
        "screenshot":      "",
    }
