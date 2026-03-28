#!/usr/bin/env python3
"""
PIF - PIB Press Release Scraper (v2 — March 2026)
Scrapes all 28 PIB regional offices with comprehensive keyword matching.
Tailored for Pahle India Foundation's 4 verticals based on their actual research focus.

v2 changes:
- Tightened keyword lists across all 4 verticals to reduce false positives
- Expanded negative keywords with PIB-specific noise patterns
- Added per-vertical minimum score thresholds (iLEAP=2, CoDED=2, EoDB=1, ELS=1)

v2.1 changes:
- All 4 verticals now require min score = 2 (EoDB and ELS raised from 1)
- Broad-but-useful terms restored to all verticals (thresholds now protect them)
  e.g. iLEAP: "nutrition", "hospital", "anganwadi"; EoDB: "logistics", "make in india"
       ELS: "governance", "sustainability", "pension"; CoDED: "statistics", "census"

v2.2 changes:
- Two-tier keyword system introduced to prevent future "Other" misclassifications
  Tier 1 (HIGH_CONFIDENCE_KEYWORDS): single match is enough to qualify — terms that
  are unambiguous by themselves (e.g. "union budget" → CoDED, "lead poisoning" → iLEAP,
  "mgnrega" → ELS, "ease of doing business" → EoDB)
  Tier 2 (VERTICALS keywords): broad contextual terms still require min score = 2
- match_verticals() checks Tier 1 first, then falls through to threshold logic
"""

import requests
from bs4 import BeautifulSoup
import json
import datetime
import hashlib
import os
import re
import time

# ==============================================================================
# NEGATIVE KEYWORDS — Articles containing these are dropped entirely
# Merged: original crime/entertainment list + v2 PIB-specific government noise
# ==============================================================================
NEGATIVE_KEYWORDS = [
    # === Crime & Courts (original) ===
    "murder", "rape", "assault", "robbery", "theft", "kidnap", "arrested",
    "police custody", "fir filed", "accused", "chargesheet", "bail",
    "convicted", "sentenced", "acquitted", "gang war", "mob lynching",
    "scam accused", "remanded", "missing person", "dacoity", "extortion",

    # === Entertainment & Sports (original) ===
    "bollywood", "box office", "celebrity", "film review", "movie release",
    "web series", "reality show", "bigg boss", "ipl match", "cricket match",
    "fifa", "nba", "tennis tournament", "world cup final", "olympic medal",
    "kabaddi league", "hockey league", "badminton tournament",

    # === Festivals & Lifestyle (original) ===
    "happy diwali", "happy holi", "navratri celebration", "eid mubarak",
    "christmas celebration", "recipe", "horoscope", "astrology", "zodiac",
    "fashion week", "weight loss", "diet plan", "skin care", "beauty tips",

    # === Accidents (original) ===
    "road accident", "train accident", "plane crash", "building collapse",
    "fire accident", "earthquake kills", "flood kills", "landslide kills",

    # === Obituary (original) ===
    "passes away", "condolence message", "funeral", "prayer meet", "dies at",
    "death anniversary", "last rites", "mortal remains",

    # === Political Campaign Noise (original) ===
    "rally held", "campaign trail", "joins party", "quits party",
    "election rally", "roadshow", "poll campaign",

    # === Entertainment Events (original) ===
    "trailer launch", "song release", "album launch", "concert", "award ceremony",
    "filmfare", "oscars", "grammy",

    # === Routine/Administrative (v2 — PIB-specific) ===
    "condolence", "obituary", "birth anniversary",
    "greeting", "wishes", "festival", "celebration",
    "republic day parade", "independence day celebration",
    "diwali", "holi", "eid", "christmas", "pongal", "onam",
    "new year message", "mann ki baat",

    # === Appointments/Transfers (v2 — PIB-specific compound phrases only) ===
    # WARNING: never use single words like "promotion", "transfer", "appointment"
    # here — they substring-match "export promotion", "technology transfer" etc.
    "promoted to the rank", "transferred as", "appointed as secretary",
    "appointed as joint secretary", "appointed as additional secretary",
    "transfer and posting", "transfer order issued",
    "retirement function", "superannuation",
    "swearing in", "oath ceremony",

    # === Ceremonies/Events (v2) ===
    "foundation stone", "inauguration ceremony",
    "flag hoisting", "cultural program", "cultural event",
    "sports event", "sports day", "marathon", "run for",
    "yoga day", "yoga event", "fit india",

    # === Foreign Visits/Protocol (v2) ===
    "state visit", "official visit", "bilateral visit",
    "summit meeting", "g20 presidency", "g7",
    "foreign minister visit", "head of state visit",
    "mou signing", "agreement signing",
    "bilateral relations", "diplomatic relations",
    "ambassador presents credentials",
    "foreign delegation", "parliamentary delegation",
    "cultural exchange", "people to people",
    "diaspora event", "pravasi bharatiya",

    # === Defence/Security (v2) ===
    "military exercise", "naval exercise", "air exercise",
    "passing out parade", "commissioning ceremony",
    "defence expo", "aero india", "defexpo",
    "gallantry award", "vir chakra", "param vir",
    "sainik school", "military school",
    "border security force", "bsf raising day",
    "crpf raising day", "cisf raising day",
    "coast guard day", "navy day", "air force day", "army day",
    "defence production", "defence procurement",
    "indigenisation defence",

    # === Awards/Felicitation (v2) ===
    "medal ceremony", "felicitation",
    "honour", "recognition", "prize distribution",

    # === Ceremonial/Political noise (v2) ===
    "lok sabha speaker", "rajya sabha chairman",
    "farewell", "retirement function",
    "book launch", "book release ceremony",
    "stamp release", "coin release", "commemorative stamp",
    "convocation ceremony", "degree ceremony",
    "national convention", "national conference inauguration",
    "international day", "world day", "global day",
    "international women's day", "world water day",
    "world environment day", "world health day",
    "world tuberculosis day", "world aids day",

    # === Generic health events (v2 — prevents iLEAP false positives) ===
    "hospital inauguration", "medical college inauguration",
    "ayush day", "yoga health",
    "blood donation camp", "health camp",
    "pulse polio", "immunization drive", "vaccination camp",
    "national immunization", "covid vaccination",
    "cancer awareness", "diabetes awareness",
    "heart disease awareness", "mental health awareness",
    "tb awareness", "malaria awareness",

    # === General infrastructure inaugurations (v2 — prevents EoDB false positives) ===
    "highway inauguration", "road inauguration",
    "bridge inauguration", "tunnel inauguration",
    "airport inauguration", "port inauguration",
    "railway inauguration", "metro inauguration",
    "dam inauguration", "power plant inauguration",

    # === Elections (v2) ===
    "election schedule", "election commission",
    "model code of conduct", "voter turnout",
    "polling station", "ballot paper", "evms",
    "election results", "by-election",

    # === AI/Tech events (v2 — prevents CoDED false positives) ===
    "ai summit", "ai expo", "ai impact summit",
    "tech summit", "digital india week",
    "startup india festival", "hackathon",
    "innovation challenge", "technology exhibition",

    # === General noise (v2) ===
    "press conference", "media briefing", "doordarshan",
    "all india radio", "pib fact check", "fake news alert",
]

# ==============================================================================
# PER-VERTICAL MINIMUM SCORE THRESHOLDS (v2)
# A release must match this many keywords before being tagged to a vertical.
# Prevents single broad-keyword false positives.
# ==============================================================================
VERTICAL_MIN_SCORES = {
    "iLEAP": 2,   # Prevents single generic health term hits
    "CoDED": 2,   # Prevents "methodology" or "data" alone from qualifying
    "EoDB":  2,   # Prevents single inauguration/logistics hits
    "ELS":   2,   # Prevents single governance/awards hits
}

# ==============================================================================
# TIER 1 — HIGH-CONFIDENCE KEYWORDS (v2.2)
# A single match on any of these is enough to tag a release to that vertical.
# These terms are unambiguous — they can only mean one thing in a PIB context.
# Rule: if in doubt, do NOT add here. Add to VERTICALS (Tier 2) instead.
# ==============================================================================
HIGH_CONFIDENCE_KEYWORDS = {
    "EoDB": [
        # Institutional / programme names that are EoDB by definition
        "ease of doing business", "eodb",
        "business reform action plan", "brap",
        "national single window system", "nsws",
        "dpiit", "department for promotion of industry",
        "invest india",
        "jan vishwas",
        # Specific reform instruments
        "ibc reform", "insolvency and bankruptcy code",
        "nclt", "nclat",
        "faceless assessment",
        "vivad se vishwas",
        "production linked incentive scheme", "pli scheme",
        "sez reform", "special economic zone reform",
        "udyam registration",
        "shram suvidha",
        "world bank doing business",
        "doing business ranking",
        "treds platform",
        "cgtmse",
    ],

    "CoDED": [
        # Budget & surveys — always economic decision-making
        "union budget", "state budget",
        "economic survey",
        # Statistical institutions/surveys — unambiguous
        "mospi", "national statistical office",
        "national sample survey", "nsso",
        "periodic labour force survey", "plfs",
        "household consumption expenditure survey", "hces",
        "annual survey of industries",
        "census commissioner",
        "registrar general of india",
        # Data systems
        "ndap", "national data analytics platform",
        "data.gov.in",
        # Reports with fixed names
        "gdp estimate", "gdp revision", "advance estimate",
        "base year revision",
        "statistical yearbook",
        "sdg india index",
        "rbi monetary policy report",
        "rbi bulletin",
    ],

    "iLEAP": [
        # Lead — any single mention is always relevant
        "lead poisoning", "lead exposure", "lead contamination",
        "blood lead level", "bll",
        "lead paint", "lead in paint",
        "lead acid battery", "ulab",
        "lead smelting", "lead pollution", "lead toxicity",
        "lead elimination", "lead phase out", "lead abatement",
        "lead remediation", "lead screening", "lead testing",
        "lead in petrol", "leaded petrol",
        "fssai lead", "food lead contamination",
        "lead regulation", "lead ban", "lead limit",
        "is 16088",
        # Heavy metals — specific enough
        "heavy metal contamination", "heavy metal poisoning",
        "mercury contamination", "cadmium contamination",
        "arsenic contamination",
        # Programmes — by name
        "pure earth", "ipen", "unep lead",
        "lead paint alliance",
        "national lead elimination",
        "global lead network",
    ],

    "ELS": [
        # Flagship employment schemes — unambiguous
        "mgnrega", "mgnregs", "mahatma gandhi nrega",
        "pmkvy", "pradhan mantri kaushal vikas yojana",
        "pm shram yogi mandhan",
        "e-shram", "e shram portal",
        "national rural livelihood mission", "nrlm",
        "deen dayal upadhyaya grameen kaushalya yojana", "ddu-gky",
        "pm svnidhi",
        "one nation one ration card", "onorc",
        "pmegp",
        "stand up india scheme",
        # Labour statistics — named reports
        "plfs report", "periodic labour force survey report",
        "labour bureau survey",
        # Labour codes — by name
        "industrial relations code",
        "wage code",
        "social security code",
        "occupational safety health code", "osh code",
        # Portals / registries
        "shram suvidha portal",
        "e shram",
        "udyam",
    ],
}

# ==============================================================================
# PIF VERTICALS & v2 REFINED KEYWORD LISTS
# All 4 lists audited against real PIB content (March 2026).
# Overly broad terms removed or replaced with compound/specific phrases.
# ==============================================================================
VERTICALS = {

    # ==========================================================================
    # VERTICAL 1: EoDB — Ease of Doing Business
    # v2: Added "reform"/"policy" qualifiers to broad sector terms.
    #     Removed plain "logistics", "make in india", "manufacturing sector".
    # ==========================================================================
    "EoDB": {
        "label": "Ease of Doing Business & Export-Led Manufacturing",
        "color": "#E8620A",
        "keywords": [
            # Core EoDB
            "ease of doing business", "eodb", "business reform", "regulatory reform",
            "single window clearance", "single window system",
            "business facilitation", "investor facilitation",
            "compliance burden", "compliance reduction", "decriminalization", "decriminalisation",
            "license reform", "permit reform",
            "business registration", "company registration", "startup registration",
            "msme registration", "udyam registration", "gst registration",

            # Digital/Portal
            "national single window", "nsws", "invest india",
            "faceless assessment", "digital approval", "paperless approval",

            # Specific reforms
            "industrial licensing reform", "environmental clearance reform",
            "building permit reform", "construction permit simplification",
            "fire noc reform", "labour compliance simplification",
            "factory license reform", "trade license reform",
            "contract enforcement reform", "commercial courts",
            "insolvency resolution", "ibc reform", "bankruptcy code",
            "nclt reform",

            # Investment climate
            "fdi policy reform", "foreign direct investment policy",
            "investment climate reform", "business climate index",
            "investor confidence index", "doing business ranking",
            "world bank ease of doing business",

            # State reforms
            "brap", "business reform action plan", "state business ranking",
            "district business ranking", "reform implementation dpiit", "dpiit reform",

            # Manufacturing & Industry (restored — safe at score=2)
            "make in india", "manufacturing sector", "manufacturing hub",
            "manufacturing policy", "manufacturing policy reform", "industrial policy reform",
            "industrial corridor", "industrial corridor development",
            "special economic zone", "sez", "special economic zone reform", "sez policy",
            "industrial park", "industrial park development", "nimz",
            "pli scheme", "pli scheme reform", "production linked incentive",
            "production linked incentive policy",

            # Logistics (restored — safe at score=2)
            "logistics", "logistics cost", "logistics policy", "logistics ease",
            "pm gati shakti", "pm gati shakti network",
            "national logistics policy", "logistics efficiency", "logistics cost reduction",
            "multimodal logistics", "logistics park", "logistics infrastructure reform",

            # Trade & Exports
            "export promotion", "export promotion policy", "export growth",
            "export policy", "export incentive", "export incentive scheme",
            "import duty", "import duty reform", "customs duty", "customs duty reform",
            "tariff", "tariff reduction", "tariff policy reform",
            "foreign trade policy", "trade policy",
            "free trade agreement", "fta", "cepa", "ceca", "bilateral trade",
            "global value chain", "global value chain integration",
            "supply chain", "supply chain reform",

            # FDI & Investment
            "foreign direct investment", "fdi", "fdi inflow", "fdi reform",
            "ease of investment", "investment promotion", "investment facilitation reform",
            "investor summit", "investor summit policy", "investment climate",

            # MSME & Startups
            "msme", "msme policy", "msme credit reform", "msme support scheme",
            "msme loan", "msme credit",
            "startup india", "startup policy", "startup ecosystem",
            "entrepreneur", "entrepreneurship",

            # Credit & Finance
            "credit access msme", "psb loans reform", "mudra scheme",
            "stand up india scheme", "credit guarantee scheme",
            "cgtmse", "invoice financing", "treds platform",

            # Labour reforms
            "labour code reform", "industrial relations code",
            "wage code implementation", "social security code",
            "osh code", "fixed term employment reform",
            "labour inspection reform", "shram suvidha portal",

            # Land & Infrastructure
            "land acquisition reform", "land bank policy",
            "plug and play infrastructure", "industrial infrastructure reform",

            # Taxation
            "gst council", "gst reform", "gst simplification", "gst rate",
            "tax reform", "tax reform compliance", "direct tax reform",
            "corporate tax", "tax simplification", "tax compliance simplification",
            "faceless appeal income tax", "vivad se vishwas scheme",

            # Port, Aviation, Infrastructure (restored — safe at score=2)
            "port", "port development", "sagarmala", "port modernization",
            "airport", "air cargo", "aviation", "civil aviation",
            "freight corridor", "dedicated freight corridor", "bharatmala",
            "warehouse", "warehousing", "cold chain", "cold storage",

            # Competition & Insolvency
            "competition commission", "cci", "cci merger", "anti trust",
            "insolvency", "insolvency reform", "ibc", "ibc amendment",
            "nclt", "bankruptcy", "resolution plan", "nclat",

            # Critical Minerals
            "critical mineral", "critical mineral policy", "rare earth",
            "rare earth policy", "mineral policy reform", "mmdr", "mmdr amendment",
            "mining reform", "mineral exploration",

            # Sectors
            "semiconductor", "chip manufacturing", "electronics manufacturing",
            "textile industry", "textile export", "garment export",
            "leather industry", "footwear", "pharma export",
            "auto industry", "automobile sector", "ev manufacturing",
            "food processing", "food processing industry", "food park",
            "gems jewellery", "gold policy", "bullion",
            "telecom policy", "telecom reform", "spectrum",
            "power sector", "power reform", "electricity reform", "discoms",
            "capital market", "sebi",
            "ai policy", "data center policy", "compute capacity",

            # Atmanirbhar
            "atmanirbhar bharat", "atmanirbhar bharat policy",
            "self reliant india", "viksit bharat",
        ]
    },

    # ==========================================================================
    # VERTICAL 2: CoDED — Center of Data for Economic Decision-making
    # v2: Removed plain "statistics", "census", "gdp", "ai", "api", "data center".
    #     All terms now compound or institution-specific.
    # ==========================================================================
    "CoDED": {
        "label": "Center of Data for Economic Decision-making",
        "color": "#2471A3",
        "keywords": [
            # Core data terms (restored — safe at score=2)
            "statistics", "statistical system", "economic data", "statistical data",
            "official statistics", "data governance", "data policy", "data infrastructure",
            "data quality", "national data", "government data", "public data",

            # Institutions
            "national statistical office", "nso", "mospi",
            "ministry of statistics", "central statistics office", "cso",
            "nsso", "national sample survey", "registrar general",
            "economic census", "annual survey of industries",

            # Census (restored — safe at score=2)
            "census", "population census", "census data", "census commissioner",
            "census enumeration", "census 2021", "census 2026",

            # GDP & National Accounts (restored)
            "gdp", "gross domestic product", "gdp growth", "gdp data",
            "gdp growth estimate", "gdp revision", "gross domestic product data",
            "gdp base year", "gsdp", "state gdp", "gross state domestic product",
            "gddp", "district domestic product", "district gdp",
            "gva", "gross value added", "national accounts statistics",
            "supply use table", "economic growth", "growth rate", "growth estimate",
            "advance estimate", "quarterly estimate", "provisional estimate",

            # Indicators & Surveys
            "inflation data", "cpi data", "consumer price index data",
            "wpi data", "wholesale price index", "iip data",
            "index of industrial production",
            "employment data", "plfs", "periodic labour force survey",
            "consumption expenditure survey", "hces",

            # Data quality
            "data quality assessment", "data accuracy improvement",
            "statistical methodology", "survey methodology",
            "base year revision", "data revision gdp",
            "sampling methodology", "survey design statistics",

            # Digital data (restored — safe at score=2)
            "ai", "big data", "machine learning",
            "data analytics", "data analytics platform government",
            "data center", "ai governance framework", "ai policy regulation",
            "data exchange protocol", "data sharing framework",
            "open data", "open data policy", "open government data",
            "data protection law", "data privacy regulation",
            "pdp bill", "digital personal data protection",
            "dpdp act", "data principal", "data fiduciary",

            # Administrative data
            "gst data analysis", "tax data statistics",
            "e-way bill statistics", "gstn data",
            "mca21", "company data registry", "epfo statistics",

            # Research & Reports
            "economic survey", "economic survey india", "rbi annual report",
            "rbi monetary policy report", "rbi bulletin statistics",
            "statistical yearbook india", "india statistics compendium",
            "sdg india index", "state statistics bureau",

            # Data systems
            "ndap", "national data analytics platform",
            "data catalogue government", "data.gov.in",
            "unified data platform", "india data portal",
            "administrative data use", "data linkage government",
            "digital public infrastructure", "dpi",

            # High-frequency & alternative data
            "high frequency data", "high frequency indicator", "real time economic data",
            "nowcasting", "economic nowcast", "gdp nowcast",
            "night time lights data", "satellite economic data",
            "electricity consumption data",
            "digital payment data", "upi transaction data",
            "e way bill data", "freight data statistics",

            # District-level development
            "district domestic product", "district gdp", "gddp",
            "district economic planning", "district development data",
            "bottom up planning", "decentralized planning",

            # Monetary & Financial data
            "rbi", "reserve bank", "monetary policy", "mpc",
            "credit growth data", "bank credit statistics",
            "financial inclusion data",

            # Welfare & Inequality data
            "poverty data", "consumption inequality", "gini coefficient",
            "welfare targeting data", "social sector statistics",
            "inclusive growth", "inequality",
        ]
    },

    # ==========================================================================
    # VERTICAL 3: iLEAP — India Lead Elimination Action Partnership
    # v2: Removed generic "public health", "health scheme", "aiims", "hospital",
    #     "nutrition" standalone. Now requires 2+ keyword matches (min score=2).
    # ==========================================================================
    "iLEAP": {
        "label": "Lead Elimination & Public Health",
        "color": "#C0392B",
        "keywords": [
            # Lead-specific (core — highly specific, keep all)
            "lead poisoning", "lead exposure", "lead contamination",
            "blood lead level", "bll", "lead paint", "lead in paint",
            "lead battery", "lead acid battery", "battery recycling",
            "lead smelting", "lead pollution", "lead toxicity",
            "lead free", "lead elimination", "lead phase out",
            "childhood lead", "lead testing", "lead screening",
            "lead abatement", "lead remediation", "lead monitoring",
            "lead in fuel", "lead in petrol", "leaded petrol",
            "lead paint standard", "toy safety lead", "cosmetic lead",
            "fssai lead", "food lead contamination",

            # Heavy metals
            "heavy metal contamination", "heavy metal pollution", "heavy metal toxicity",
            "mercury contamination", "mercury pollution",
            "cadmium contamination", "arsenic contamination", "chromium contamination",
            "metal contamination", "metal poisoning",

            # Regulations & Standards
            "is 16088",
            "lead limit", "lead regulation", "lead ban",

            # Neurotoxicity & Child development (paired terms)
            "neurotoxic exposure", "neurotoxicity children",
            "cognitive impairment children", "iq loss children",
            "developmental neurotoxicity", "child neurotoxin",

            # Environment (contaminant-specific)
            "pollution control board lead", "cpcb lead", "spcb lead",
            "industrial effluent heavy metal", "hazardous waste metal",
            "e-waste lead", "e-waste heavy metal",
            "soil lead contamination", "groundwater arsenic",
            "groundwater lead", "water lead contamination",
            "particulate matter heavy metal", "air toxic metal",
            "pm2.5 lead", "dust lead exposure",

            # Occupational health
            "occupational lead exposure", "occupational heavy metal",
            "lead worker health", "smelter worker", "battery worker health",
            "paint worker exposure",

            # Specific programs/bodies
            "national lead elimination", "global lead network",
            "pure earth", "ipen", "unep lead",
            "lead paint alliance", "who lead", "unicef lead",

            # Public health infrastructure (restored — safe at score=2)
            "public health", "health ministry", "ministry of health",
            "national health mission", "nhm", "health program", "health scheme",
            "ayushman bharat", "pmjay", "health insurance", "health coverage",
            "hospital", "phc", "primary health center", "community health",
            "district hospital", "medical college",

            # Nutrition & Health Linkage (restored — safe at score=2)
            "nutrition", "malnutrition", "micronutrient", "iron deficiency",
            "zinc deficiency", "calcium", "nutritional status",
            "poshan", "poshan abhiyaan", "mid day meal",
            "anganwadi", "icds",

            # Vulnerable populations (restored)
            "children health", "child health", "paediatric", "pediatric",
            "pregnant women", "maternal health", "prenatal exposure",
            "infant health", "newborn", "neonatal",
            "school children", "school going children",

            # Research & Testing
            "blood test", "screening", "health screening", "lead screening",
            "biomarker", "health assessment", "health survey", "epidemiological",
            "icmr", "medical research", "health research",
            "csir neeri", "environmental research",

            # International Standards
            "who", "world health organization", "who standard", "who guideline",
            "unicef", "cdc", "reference value", "safe level",

            # Environmental Health
            "environmental health", "soil contamination",
            "water quality", "air quality", "pollution control",
            "environmental monitoring", "contamination assessment",

            # Policy & Regulation
            "food safety", "fssai", "drug control", "cosmetic regulation",
            "paint regulation", "hazardous waste", "waste management",
            "pollution control board", "cpcb", "spcb", "environmental clearance",
        ]
    },

    # ==========================================================================
    # VERTICAL 4: ELS — Employment & Livelihood Systems
    # v2: Removed plain "governance", "sustainability", "climate", "pension",
    #     "insurance", "cooperative". Now scheme-specific or compound terms.
    # ==========================================================================
    "ELS": {
        "label": "Employment & Livelihood Systems",
        "color": "#7D3C98",
        "keywords": [
            # Core employment
            "employment", "employment generation", "job creation", "jobs",
            "unemployment", "unemployment rate", "jobless", "job market",
            "labour market", "labor market", "workforce", "manpower",
            "employment scheme", "employment program",
            "employment exchange", "job portal government",
            "rozgar", "rojgar", "job fair", "rozgar mela", "placement",

            # Wage & conditions
            "minimum wage", "wage", "wage revision", "wage board",
            "equal remuneration", "wage theft", "wage compliance",
            "salary", "remuneration", "income",
            "labour welfare", "worker welfare", "labour rights",
            "trade union", "industrial relations", "collective bargaining",
            "contract labour",

            # Skill development (restored — safe at score=2)
            "skill development", "skill india", "skilling", "skill training",
            "reskilling", "upskilling", "skill gap",
            "skill development scheme", "skill training program government",
            "vocational training", "vocational training scheme",
            "skill india mission", "vocational education",
            "iti", "industrial training institute",
            "pmkvy", "pradhan mantri kaushal vikas yojana",
            "apprenticeship scheme", "national apprenticeship",
            "recognition of prior learning",
            "nsdc", "sector skill council", "skill certification",

            # Schemes
            "mahatma gandhi nrega", "mgnregs", "mnrega", "nrega",
            "pm employment guarantee", "urban employment scheme",
            "deen dayal upadhyaya", "ddu-gky", "rsetis",
            "pmegp", "pm rojgar",

            # Informal & Gig economy
            "informal sector", "informal employment", "unorganised sector",
            "informal sector workers", "informal economy",
            "informal worker", "unorganised worker", "daily wage",
            "gig economy", "gig worker", "platform worker", "freelance",
            "street vendor", "hawker", "street vendors scheme", "pm svnidhi",
            "unorganized workers", "e-shram",

            # Labour welfare (restored — safe at score=2)
            "pension", "labour welfare scheme", "worker welfare fund",
            "construction worker welfare", "building worker cess",
            "esi scheme", "esic benefit", "epfo scheme",
            "employee provident fund", "social security",
            "social security worker", "labour pension scheme",
            "unorganized sector pension", "pm shram yogi mandhan",
            "atal pension", "pm shram yogi", "gratuity",
            "insurance", "life insurance", "health insurance worker",

            # Women employment
            "women employment", "female employment", "women workforce",
            "female labour force", "flfp", "women worker", "working women",
            "women employment scheme", "women workforce participation",
            "maternity benefit", "maternity benefit scheme", "maternity leave",
            "creche", "childcare", "gender pay gap", "equal pay",
            "self help group", "shg", "self help group livelihood", "shg employment",
            "women entrepreneur scheme", "working women hostel",

            # Migration
            "migrant worker", "migrant labour",
            "interstate migrant", "labour migration",
            "one nation one ration", "onorc",

            # Labour statistics
            "plfs report", "periodic labour force survey",
            "employment unemployment survey", "labour bureau survey",
            "employment statistics", "labour statistics",
            "labour force participation", "lfpr", "worker population ratio",
            "formal employment",

            # Industry-specific
            "textile employment", "construction workers welfare",
            "domestic workers rights", "plantation labour welfare",
            "mining workers welfare", "beedi workers",

            # Youth
            "youth employment", "youth unemployment",
            "first time job seeker", "campus placement scheme",
            "internship scheme", "fresher", "graduate employment",

            # MSME & Entrepreneurship
            "msme employment", "mudra loan", "mudra", "pmmy",
            "stand up india", "entrepreneurship", "self employment",
            "own account worker", "udyam",

            # Rural livelihoods
            "rural livelihood", "rural livelihood mission", "nrlm",
            "national rural livelihood mission",
            "rural employment scheme", "non farm livelihood",
            "shg bank linkage", "farmer producer organization",
            "cooperative", "cooperative society", "fpo",

            # Traditional livelihoods
            "handloom", "handicraft", "artisan", "craftsman", "weaver",
            "handloom scheme", "handicraft scheme", "artisan welfare",
            "khadi", "khadi scheme", "village industry", "village industry scheme",
            "cottage industry", "tribal livelihood scheme",

            # Governance & Policy (restored — safe at score=2)
            "governance", "good governance", "e governance", "digital governance",
            "administrative reform", "civil service",
            "public administration", "government efficiency",
            "csr", "corporate social responsibility", "csr spending", "csr policy",

            # Sustainability & Green jobs (restored — safe at score=2)
            "sustainability", "sustainable livelihood", "green jobs",
            "green jobs scheme", "green employment",
            "organic farming", "natural farming", "sustainable agriculture",
            "circular economy", "waste to wealth",
            "climate adaptation", "climate resilience", "sustainable development",
            "renewable energy employment", "renewable energy jobs",
            "solar jobs", "green skill development", "solar skill training",
        ]
    }
}

# ==============================================================================
# All 28 PIB Regional Offices
# ==============================================================================
PIB_REGIONS = {
    "3": "Delhi", "1": "Mumbai", "5": "Hyderabad", "6": "Chennai",
    "17": "Chandigarh", "19": "Kolkata", "20": "Bengaluru", "21": "Bhubaneswar",
    "22": "Ahmedabad", "23": "Guwahati", "24": "Thiruvananthapuram",
    "30": "Imphal", "31": "Mizoram", "32": "Agartala", "33": "Gangtok",
    "34": "Kohima", "35": "Shillong", "36": "Itanagar", "37": "Lucknow",
    "38": "Bhopal", "39": "Jaipur", "40": "Patna", "41": "Ranchi",
    "42": "Shimla", "43": "Raipur", "44": "Jammu & Kashmir",
    "45": "Vijayawada", "46": "Dehradun",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


def make_id(title):
    """Generate unique ID from title"""
    return hashlib.md5(title.encode()).hexdigest()[:12]


def clean_text(raw):
    """Clean and normalize text"""
    if not raw:
        return ""
    return re.sub(r"\s+", " ", raw).strip()


def has_negative_keywords(text):
    """Check if text contains any negative keywords"""
    text_lower = text.lower()
    for neg in NEGATIVE_KEYWORDS:
        if neg in text_lower:
            return True
    return False


def match_verticals(title, summary=""):
    """
    Two-tier keyword matching (v2.2):

    Tier 1 — HIGH_CONFIDENCE_KEYWORDS:
        A single match auto-qualifies the vertical regardless of min score.
        Terms here are unambiguous in a PIB context (e.g. "union budget",
        "lead poisoning", "mgnrega"). Tier 1 score adds to total score.

    Tier 2 — VERTICALS keywords:
        Broad contextual terms. Only qualify if score >= VERTICAL_MIN_SCORES.
        This prevents single generic hits (e.g. "governance", "logistics")
        from tagging a release incorrectly.

    Returns: (list of matching vertical IDs sorted by score desc, total score)
    """
    text = f"{title} {summary}".lower()

    # Drop articles matching negative keywords
    if has_negative_keywords(text):
        return ["Filtered"], 0

    matched = []
    scores = {}

    for vid, vdata in VERTICALS.items():
        # --- Tier 1: high-confidence check ---
        hc_score = sum(
            1 for kw in HIGH_CONFIDENCE_KEYWORDS.get(vid, [])
            if kw.lower() in text
        )

        # --- Tier 2: contextual keyword check ---
        ctx_score = sum(
            1 for kw in vdata["keywords"]
            if kw.lower() in text
        )

        total = hc_score + ctx_score

        # Qualify if: any Tier 1 hit (guaranteed) OR Tier 2 meets threshold
        if hc_score > 0 or ctx_score >= VERTICAL_MIN_SCORES[vid]:
            matched.append(vid)
            scores[vid] = total

    # Sort by total score descending (highest relevance first)
    matched.sort(key=lambda x: scores.get(x, 0), reverse=True)

    total_score = sum(scores.values())
    return matched if matched else ["Other"], total_score


def get_relative_time(dt):
    """Get human-readable relative time"""
    now = datetime.datetime.utcnow()
    diff = now - dt
    hours = diff.total_seconds() / 3600
    if hours < 1:
        return f"{int(diff.total_seconds() / 60)} min ago"
    elif hours < 24:
        return f"{int(hours)} hr ago"
    else:
        return f"{int(hours/24)} days ago"


def scrape_region(reg_id, region_name):
    """Scrape a single PIB regional page"""
    url = f"https://www.pib.gov.in/Allrel.aspx?reg={reg_id}&lang=1"
    articles = []

    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")

        # Find all press release links - multiple selectors for robustness
        links_found = []

        # Method 1: Content area
        content_area = soup.find("div", class_="content-area")
        if content_area:
            links_found.extend(content_area.find_all("a", href=True))

        # Method 2: Release list
        release_items = soup.find_all("ul", class_="releases-list") or soup.find_all("div", class_="releases")
        for item in release_items:
            links_found.extend(item.find_all("a", href=True))

        # Method 3: General link search (fallback)
        if not links_found:
            links_found = soup.find_all("a", href=True)

        seen_urls = set()

        for link in links_found:
            href = link.get("href", "")
            title = clean_text(link.get_text())

            # Filter for press release links
            if "PressReleasePage" in href or "Pressreleaseshare" in href or "PressReleseDetail" in href:
                if len(title) < 15:
                    continue

                # Build full URL
                if href.startswith("/"):
                    href = "https://www.pib.gov.in" + href
                elif not href.startswith("http"):
                    href = "https://www.pib.gov.in/" + href

                # Deduplicate
                if href in seen_urls:
                    continue
                seen_urls.add(href)

                articles.append({
                    "title": title,
                    "url": href,
                    "region": region_name,
                    "region_id": reg_id,
                })

        if articles:
            print(f"   [{region_name}] Found {len(articles)} releases")

    except Exception as e:
        print(f"   [{region_name}] Error: {e}")

    return articles


def scrape_pib():
    """Main scraper function"""
    print("=" * 60)
    print("[PIB] PIF Press Release Scraper - v2 (March 2026)")
    print("=" * 60)
    print(f"[PIB] Scraping {len(PIB_REGIONS)} regional offices...\n")

    all_articles = []
    seen_ids = set()
    now = datetime.datetime.utcnow()
    filtered_count = 0
    other_count = 0

    for reg_id, region_name in PIB_REGIONS.items():
        articles = scrape_region(reg_id, region_name)

        for art in articles:
            art_id = make_id(art["title"])
            if art_id in seen_ids:
                continue
            seen_ids.add(art_id)

            verticals, score = match_verticals(art["title"])

            # Skip filtered articles (negative keywords)
            if "Filtered" in verticals:
                filtered_count += 1
                continue

            # Count "Other" (unmatched)
            if "Other" in verticals:
                other_count += 1

            all_articles.append({
                "id": art_id,
                "title": art["title"],
                "summary": "",
                "url": art["url"],
                "region": art["region"],
                "region_id": art["region_id"],
                "date": now.strftime("%Y-%m-%dT%H:%M:%S"),
                "relative_time": "Today",
                "verticals": verticals,
                "primary_vertical": verticals[0] if verticals else "Other",
                "relevance_score": score,
            })

        # Small delay to be polite to server
        time.sleep(0.3)

    # Sort by relevance score (highest first), then by region
    all_articles.sort(key=lambda x: (-x.get("relevance_score", 0), x["region"]))

    print(f"\n{'=' * 60}")
    print(f"[PIB] SCRAPING COMPLETE")
    print(f"{'=' * 60}")
    print(f"   Total articles found: {len(all_articles) + filtered_count}")
    print(f"   Filtered out (noise): {filtered_count}")
    print(f"   Unmatched (Other):    {other_count}")
    print(f"   Final relevant:       {len(all_articles) - other_count}")
    print(f"   Total output:         {len(all_articles)}")

    # Count by vertical
    vertical_counts = {}
    for art in all_articles:
        for v in art["verticals"]:
            vertical_counts[v] = vertical_counts.get(v, 0) + 1

    print(f"\n[PIB] Articles by Vertical:")
    for v, count in sorted(vertical_counts.items(), key=lambda x: -x[1]):
        label = VERTICALS.get(v, {}).get("label", v)
        print(f"   {v}: {count} ({label})")

    # Region stats
    region_counts = {}
    for art in all_articles:
        region_counts[art["region"]] = region_counts.get(art["region"], 0) + 1

    print(f"\n[PIB] Top 10 Regions:")
    for region, count in sorted(region_counts.items(), key=lambda x: -x[1])[:10]:
        print(f"   {region}: {count}")

    # Build output
    output = {
        "last_updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_updated_ist": (now + datetime.timedelta(hours=5, minutes=30)).strftime("%d %b %Y, %I:%M %p IST"),
        "total": len(all_articles),
        "verticals": {k: {"label": v["label"], "color": v["color"]} for k, v in VERTICALS.items()},
        "regions": PIB_REGIONS,
        "articles": all_articles,
    }

    # Save to file
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs", "pib.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)

    print(f"\n[PIB] Saved {len(all_articles)} articles to docs/pib.json")
    print("=" * 60)


if __name__ == "__main__":
    scrape_pib()
