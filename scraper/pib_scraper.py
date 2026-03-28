#!/usr/bin/env python3
"""
PIF - PIB Press Release Scraper (Production Version)
Scrapes all 28 PIB regional offices with comprehensive keyword matching.
Tailored for Pahle India Foundation's 4 verticals based on their actual research focus.
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
# NEGATIVE KEYWORDS - Articles containing these are dropped (noise filtering)
# ==============================================================================
NEGATIVE_KEYWORDS = [
    # Crime & Courts
    "murder", "rape", "assault", "robbery", "theft", "kidnap", "arrested",
    "police custody", "fir filed", "accused", "chargesheet", "bail",
    "convicted", "sentenced", "acquitted", "gang war", "mob lynching",
    "scam accused", "remanded", "missing person", "dacoity", "extortion",
    # Entertainment & Sports
    "bollywood", "box office", "celebrity", "film review", "movie release",
    "web series", "reality show", "bigg boss", "ipl match", "cricket match",
    "fifa", "nba", "tennis tournament", "world cup final", "olympic medal",
    "kabaddi league", "hockey league", "badminton tournament",
    # Festivals & Lifestyle (unless policy)
    "happy diwali", "happy holi", "navratri celebration", "eid mubarak",
    "christmas celebration", "recipe", "horoscope", "astrology", "zodiac",
    "fashion week", "weight loss", "diet plan", "skin care", "beauty tips",
    # Accidents (unless policy response)
    "road accident", "train accident", "plane crash", "building collapse",
    "fire accident", "earthquake kills", "flood kills", "landslide kills",
    # Obituary
    "passes away", "condolence message", "funeral", "prayer meet", "dies at",
    "death anniversary", "last rites", "mortal remains",
    # Political Campaign Noise
    "rally held", "campaign trail", "joins party", "quits party",
    "election rally", "roadshow", "poll campaign",
    # Entertainment Events
    "trailer launch", "song release", "album launch", "concert", "award ceremony",
    "filmfare", "oscars", "grammy",
]

# ==============================================================================
# PIF VERTICALS & COMPREHENSIVE KEYWORDS (500+ total)
# Based on actual work from pahleindia.org and ileap.org.in
# ==============================================================================
VERTICALS = {
    # ==========================================================================
    # VERTICAL 1: EoDB - Ease of Doing Business & Export-Led Manufacturing
    # Source: https://pahleindia.org/eodb/
    # ==========================================================================
    "EoDB": {
        "label": "Ease of Doing Business & Export-Led Manufacturing",
        "color": "#E8620A",
        "keywords": [
            # Core EoDB Concepts
            "ease of doing business", "eodb", "business reform", "regulatory reform",
            "compliance burden", "compliance cost", "regulatory compliance",
            "single window clearance", "single window system", "one nation one license",
            "decriminalization", "decriminalisation", "jan vishwas", "business facilitation",
            "license raj", "permit raj", "approval process", "clearance process",
            "business registration", "company registration", "startup registration",
            
            # Manufacturing & Industry
            "manufacturing sector", "manufacturing hub", "manufacturing policy",
            "industrial policy", "industrial growth", "industrial production",
            "factory", "plant inauguration", "manufacturing unit", "production unit",
            "make in india", "production linked incentive", "pli scheme", "pli policy",
            "industrial corridor", "industrial park", "industrial estate", "industrial zone",
            "national industrial corridor", "delhi mumbai industrial corridor", "dmic",
            "manufacturing competitiveness", "manufacturing exports", "export led growth",
            
            # Trade & Exports
            "export promotion", "export growth", "export policy", "export incentive",
            "import duty", "customs duty", "tariff", "tariff reduction", "tariff policy",
            "trade policy", "foreign trade policy", "trade agreement", "trade deal",
            "free trade agreement", "fta", "cepa", "ceca", "bilateral trade",
            "trade deficit", "trade surplus", "trade balance", "current account deficit",
            "global value chain", "gvc", "supply chain", "value chain",
            "wto", "world trade", "international trade", "merchandise exports",
            
            # FDI & Investment
            "foreign direct investment", "fdi", "fdi inflow", "fdi policy",
            "foreign investment", "overseas investment", "investment promotion",
            "investor summit", "investor meet", "investment climate",
            "ease of investment", "investment facilitation", "fdi reform",
            
            # MSME & Startups
            "msme", "micro small medium enterprise", "small enterprise", "small industry",
            "msme policy", "msme growth", "msme support", "msme loan", "msme credit",
            "startup india", "startup policy", "startup ecosystem", "startup hub",
            "entrepreneur", "entrepreneurship", "incubator", "accelerator",
            "venture capital", "angel investor", "seed funding",
            
            # SEZ & Clusters
            "special economic zone", "sez", "sez policy", "export processing zone",
            "industrial cluster", "manufacturing cluster", "textile cluster",
            
            # Sectors (as per PIF projects)
            "semiconductor", "chip manufacturing", "fab", "electronics manufacturing",
            "textile industry", "textile export", "garment export", "apparel",
            "leather industry", "footwear", "pharma export", "pharmaceutical",
            "auto industry", "automobile sector", "ev manufacturing", "vehicle",
            "defence manufacturing", "defence production", "aerospace", "shipbuilding",
            "food processing", "food processing industry", "food park", "mega food park",
            "gems jewellery", "gold industry", "gold policy", "bullion",
            "telecom sector", "telecom policy", "telecom reform", "spectrum",
            "power sector", "power reform", "electricity reform", "discoms",
            "capital market", "securities market", "sebi", "stock exchange",
            "e commerce", "digital commerce", "online retail", "marketplace",
            "online gaming", "gaming sector", "gaming policy", "gaming regulation",
            "direct selling", "mlm", "direct sales",
            "ai policy", "artificial intelligence", "compute capacity", "data center",
            
            # Infrastructure for Business
            "logistics", "logistics cost", "logistics policy", "pm gati shakti",
            "national logistics policy", "multimodal logistics", "logistics park",
            "port", "port development", "sagarmala", "port modernization",
            "airport", "air cargo", "aviation", "civil aviation",
            "freight corridor", "dedicated freight corridor", "bharatmala",
            "warehouse", "warehousing", "cold chain", "cold storage",
            "national infrastructure pipeline", "nip", "infrastructure investment",
            
            # Regulatory Bodies & Government
            "ministry of commerce", "ministry of industry", "dpiit", "dgft",
            "commerce ministry", "commerce minister", "industry minister",
            "commerce secretary", "industry secretary",
            "cabinet approves", "cabinet approval", "cabinet decision",
            "union minister", "central government", "government notification",
            "niti aayog", "economic advisory", "policy commission",
            "mou signed", "agreement signed", "pact signed", "deal signed",
            
            # Competition & Insolvency
            "competition commission", "cci", "anti trust", "merger approval",
            "insolvency", "ibc", "nclt", "bankruptcy", "resolution plan", "nclat",
            
            # Tax & GST Reform
            "gst council", "gst reform", "gst rate", "tax reform", "direct tax",
            "corporate tax", "tax simplification", "tax compliance",
            
            # Atmanirbhar & Vision
            "atmanirbhar bharat", "self reliant india", "vocal for local",
            "viksit bharat", "developed india", "india 2047", "vision 2047",
            
            # State-level EoDB
            "state eodb", "state ranking", "brap", "business reform action plan",
            "state reform", "state policy", "chief minister", "state government",
            
            # Critical Minerals (from recent PIF work)
            "critical mineral", "rare earth", "lithium", "cobalt", "nickel",
            "mineral policy", "mmdr", "mining reform", "mineral exploration",
        ]
    },
    
    # ==========================================================================
    # VERTICAL 2: CoDED - Center of Data for Economic Decision-making
    # Source: https://pahleindia.org/coded/
    # ==========================================================================
    "CoDED": {
        "label": "Center of Data for Economic Decision-making",
        "color": "#2471A3",
        "keywords": [
            # Core Statistics & Data
            "economic data", "statistical data", "statistics", "statistical system",
            "data governance", "data policy", "data infrastructure", "data quality",
            "official statistics", "government data", "administrative data",
            
            # GDP & National Accounts
            "gdp", "gross domestic product", "gdp growth", "gdp estimate",
            "gsdp", "state gdp", "state domestic product", "gross state domestic product",
            "gddp", "district domestic product", "district gdp", "district economy",
            "gva", "gross value added", "national accounts", "state accounts",
            "economic growth", "growth rate", "growth estimate", "advance estimate",
            "quarterly estimate", "annual estimate", "provisional estimate",
            
            # Key Surveys & Data Sources
            "economic survey", "budget", "union budget", "state budget",
            "census", "population census", "economic census", "agriculture census",
            "national sample survey", "nss", "nsso", "sample survey",
            "annual survey of industries", "asi", "factory sector",
            "annual survey of unincorporated enterprises", "asuse",
            "unincorporated sector", "informal sector data",
            "labour force survey", "lfs", "plfs", "periodic labour force survey",
            "employment survey", "unemployment survey", "labour statistics",
            "household consumption expenditure survey", "hces", "consumption survey",
            "consumer expenditure", "household expenditure",
            
            # Statistical Organizations
            "nso", "national statistical office", "mospi", "ministry of statistics",
            "cso", "central statistical office", "nsso survey",
            "directorate of economics and statistics", "des", "state des",
            "registrar general", "rgi", "demographic data",
            
            # Economic Indicators
            "economic indicator", "macro indicator", "leading indicator",
            "index of industrial production", "iip", "industrial growth",
            "inflation", "cpi", "consumer price index", "wpi", "wholesale price",
            "retail inflation", "food inflation", "core inflation",
            "fiscal deficit", "revenue deficit", "primary deficit",
            "current account", "balance of payment", "forex reserve",
            
            # High-Frequency & Alternative Data (CoDED specialty)
            "high frequency data", "high frequency indicator", "real time data",
            "nowcasting", "nowcast", "economic nowcast", "gdp nowcast",
            "night time lights", "nightlight data", "satellite data",
            "electricity consumption", "power consumption", "electricity data",
            "gst collection", "gst revenue", "tax collection data",
            "digital payment", "upi transaction", "digital transaction",
            "mobility data", "google mobility", "traffic data",
            "e way bill", "freight data", "cargo data",
            
            # District-Level Development (CoDED focus)
            "district development", "district planning", "district economy",
            "district collector", "district administration", "dm office",
            "bottom up planning", "decentralized planning", "local planning",
            "district blueprint", "district growth", "district dashboard",
            "block level", "tehsil level", "taluka level", "sub district",
            
            # Data Systems & Infrastructure
            "data center", "data exchange", "data marketplace", "open data",
            "digital public infrastructure", "dpi", "data sharing",
            "statistical capacity", "statistical modernization",
            "data analytics", "economic analytics", "policy analytics",
            
            # Economic Planning & Research
            "economic planning", "development planning", "perspective plan",
            "five year plan", "annual plan", "state plan", "district plan",
            "economic research", "policy research", "economic analysis",
            "economic advisory council", "eac pm", "economic think tank",
            
            # Monetary & Financial Data
            "rbi", "reserve bank", "monetary policy", "mpc", "repo rate",
            "interest rate", "credit growth", "bank credit", "banking data",
            "financial inclusion", "financial data", "banking sector",
            
            # State-specific (CoDED MoUs)
            "assam economy", "madhya pradesh economy", "maharashtra economy",
            "state economy", "regional economy", "state planning",
            
            # Inclusive Growth & Welfare
            "inclusive growth", "bottom 10 percent", "poverty data",
            "welfare targeting", "social sector data", "development data",
            "inequality", "gini coefficient", "consumption inequality",
        ]
    },
    
    # ==========================================================================
    # VERTICAL 3: iLEAP - India Lead Elimination Action Partnership
    # Source: https://ileap.org.in/ and research portal
    # ==========================================================================
    "iLEAP": {
        "label": "Lead Elimination & Public Health",
        "color": "#C0392B",
        "keywords": [
            # Lead Poisoning Core
            "lead poisoning", "lead toxicity", "lead exposure", "lead contamination",
            "blood lead level", "bll", "elevated blood lead", "lead in blood",
            "lead pollution", "environmental lead", "lead hazard",
            "lead free", "lead elimination", "lead mitigation", "lead prevention",
            
            # Heavy Metals & Toxicity
            "heavy metal", "heavy metal poisoning", "toxic metal", "metal toxicity",
            "mercury", "cadmium", "arsenic", "chromium", "toxic substance",
            "neurotoxin", "neurotoxicity", "neurodevelopmental",
            
            # Lead Sources (from iLEAP website)
            "lead paint", "paint contamination", "lead in paint",
            "lead acid battery", "battery recycling", "ulab", "used lead acid battery",
            "informal recycling", "battery waste", "e waste", "electronic waste",
            "lead in spices", "spice adulteration", "turmeric adulteration",
            "adulterated food", "food adulteration", "food contamination",
            "lead in cosmetics", "cosmetic contamination", "surma", "kohl", "sindoor",
            "ceramic", "ceramic glaze", "pottery", "cookware", "utensil",
            "traditional medicine", "ayurvedic medicine", "herbal medicine",
            "lead solder", "plumbing", "lead pipe", "water contamination",
            "lead mining", "lead smelting", "lead ore", "mining area",
            
            # Health Impacts
            "cognitive impairment", "cognitive development", "iq reduction",
            "intelligence quotient", "learning disability", "developmental delay",
            "attention deficit", "behavioral problem", "neurological damage",
            "brain development", "child development", "early childhood development",
            "anemia", "anaemia", "hemoglobin", "blood disorder",
            "kidney damage", "renal damage", "cardiovascular", "hypertension",
            
            # Vulnerable Populations
            "children health", "child health", "paediatric", "pediatric",
            "pregnant women", "maternal health", "prenatal exposure", "fetal exposure",
            "umbilical cord blood", "placental transfer", "breast milk",
            "infant health", "newborn", "neonatal", "toddler",
            "school children", "school going children", "student health",
            "occupational exposure", "worker exposure", "industrial worker",
            
            # Public Health Infrastructure
            "public health", "public health policy", "health ministry",
            "ministry of health", "health minister", "health secretary",
            "national health mission", "nhm", "health program", "health scheme",
            "ayushman bharat", "pmjay", "health insurance", "health coverage",
            "health infrastructure", "hospital", "phc", "primary health center",
            "community health", "district hospital", "medical college",
            
            # Research & Testing
            "blood test", "screening", "health screening", "lead screening",
            "biomarker", "health assessment", "health survey", "epidemiological",
            "icmr", "medical research", "health research", "clinical study",
            "niti aayog health", "csir neeri", "environmental research",
            
            # International Standards
            "who", "world health organization", "who standard", "who guideline",
            "unicef", "pure earth", "international standard",
            "cdc", "reference value", "threshold", "safe level",
            
            # Environmental Health
            "environmental health", "environment pollution", "soil contamination",
            "water quality", "air quality", "pollution control",
            "environmental monitoring", "contamination assessment",
            
            # Policy & Regulation
            "lead regulation", "lead standard", "bis standard", "food safety",
            "fssai", "drug control", "cosmetic regulation", "paint regulation",
            "hazardous waste", "waste management", "pollution control board",
            "cpcb", "spcb", "environmental clearance",
            
            # Nutrition & Health Linkage
            "nutrition", "malnutrition", "micronutrient", "iron deficiency",
            "zinc deficiency", "calcium", "nutritional status",
            "poshan", "poshan abhiyaan", "mid day meal", "anganwadi", "icds",
            
            # Disease Control
            "disease control", "disease prevention", "health awareness",
            "health education", "health campaign", "awareness program",
            
            # State Roundtables (iLEAP events)
            "rajasthan health", "madhya pradesh health", "odisha health",
            "chhattisgarh health", "andhra pradesh health", "telangana health",
            "north east health", "guwahati health", "state health",
        ]
    },
    
    # ==========================================================================
    # VERTICAL 4: ELS - Employment & Livelihood Systems
    # Source: https://pahleindia.org/els/
    # ==========================================================================
    "ELS": {
        "label": "Employment & Livelihood Systems",
        "color": "#7D3C98",
        "keywords": [
            # Employment Core
            "employment", "employment generation", "job creation", "jobs",
            "unemployment", "unemployment rate", "jobless", "job market",
            "labour market", "labor market", "workforce", "manpower",
            "employment policy", "employment scheme", "rozgar", "rojgar",
            "job fair", "rozgar mela", "placement", "recruitment",
            
            # Labour & Workers
            "labour", "labor", "worker", "labourer", "working class",
            "labour reform", "labor reform", "labour code", "labor code",
            "minimum wage", "wage", "salary", "remuneration", "income",
            "labour welfare", "worker welfare", "labour rights",
            "trade union", "industrial relations", "collective bargaining",
            "contract labour", "migrant worker", "migrant labour",
            
            # Skills & Training
            "skill development", "skill india", "skilling", "skill training",
            "reskilling", "upskilling", "skill gap", "skill mismatch",
            "vocational training", "vocational education", "vet",
            "industrial training", "iti", "industrial training institute",
            "pmkvy", "pradhan mantri kaushal vikas yojana", "skill center",
            "apprentice", "apprenticeship", "on job training",
            "national skill", "skill university", "skill council",
            
            # Women & Work
            "women employment", "female employment", "women workforce",
            "female labour force", "flfp", "female labour force participation",
            "women worker", "working women", "gender employment",
            "women entrepreneur", "female entrepreneur", "mahila udyami",
            "maternity benefit", "maternity leave", "creche", "childcare",
            "gender pay gap", "equal pay", "gender equality work",
            "self help group", "shg", "mahila mandal", "women collective",
            
            # Rural & Agriculture Employment
            "rural employment", "rural jobs", "farm employment", "agriculture employment",
            "nrega", "mgnrega", "mgnregs", "mahatma gandhi rural employment",
            "rural livelihood", "nrlm", "national rural livelihood mission",
            "rural development", "rural economy", "village economy",
            "non farm employment", "non farm livelihood", "rural diversification",
            "panchayat", "gram panchayat", "gram sabha", "local governance",
            
            # Informal & Gig Economy
            "informal sector", "informal employment", "unorganised sector",
            "informal worker", "unorganised worker", "daily wage",
            "gig economy", "gig worker", "platform worker", "freelance",
            "street vendor", "hawker", "small trader",
            
            # MSME & Entrepreneurship
            "msme employment", "small business", "micro enterprise",
            "mudra loan", "mudra", "pmmy", "stand up india",
            "entrepreneurship", "self employment", "own account worker",
            "business ownership", "enterprise", "udyam",
            
            # SHG & Collectives
            "self help group", "shg federation", "shg bank linkage",
            "livelihood collective", "producer company", "fpo",
            "farmer producer organization", "cooperative", "cooperative society",
            
            # Traditional Livelihoods (PIF focus)
            "handloom", "handicraft", "artisan", "craftsman", "weaver",
            "traditional craft", "traditional livelihood", "heritage craft",
            "khadi", "village industry", "cottage industry",
            "tribal livelihood", "tribal entrepreneur", "tribal women",
            
            # Youth Employment
            "youth employment", "youth job", "young workforce",
            "campus placement", "internship", "fresher", "first job",
            "youth unemployment", "educated unemployed", "graduate employment",
            
            # Social Security
            "social security", "pension", "epfo", "pf", "provident fund",
            "esic", "esi", "gratuity", "labour welfare fund",
            "insurance", "life insurance", "health insurance worker",
            "atal pension", "pm shram yogi", "e shram",
            
            # Education & Employability
            "higher education", "technical education", "professional education",
            "engineering college", "iit", "nit", "iiit", "polytechnic",
            "education policy", "nep", "national education policy",
            "employability", "industry academia", "placement cell",
            
            # Governance & Policy (ELS covers this)
            "governance", "good governance", "e governance", "digital governance",
            "administrative reform", "bureaucratic reform", "civil service",
            "public administration", "government efficiency", "public service",
            "electoral system", "electoral reform", "election commission",
            "federal governance", "centre state relation", "cooperative federalism",
            "csr", "corporate social responsibility", "csr spending", "csr policy",
            "science technology policy", "dst", "science advisor",
            
            # Sustainability & Green Jobs (ELS includes this)
            "sustainability", "sustainable livelihood", "green jobs", "green employment",
            "organic farming", "natural farming", "regenerative agriculture",
            "soil health", "sustainable agriculture", "climate smart agriculture",
            "circular economy", "waste to wealth", "bio economy",
            "climate adaptation", "climate resilience", "sustainable development",
            "renewable energy jobs", "solar jobs", "green skill",
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
    """Match text against vertical keywords - returns list of matching verticals"""
    text = f"{title} {summary}".lower()
    
    # First check negative keywords
    if has_negative_keywords(text):
        return ["Filtered"], 0
    
    matched = []
    scores = {}
    
    for vid, vdata in VERTICALS.items():
        keyword_matches = 0
        for kw in vdata["keywords"]:
            if kw.lower() in text:
                keyword_matches += 1
        if keyword_matches > 0:
            matched.append(vid)
            scores[vid] = keyword_matches
    
    # Sort by score (most matches first)
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
    print("[PIB] PIF Press Release Scraper - Production Version")
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
