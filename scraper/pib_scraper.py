#!/usr/bin/env python3
"""
PIF - PIB Press Release Scraper (Production Version)
Scrapes all 28 PIB regional offices with comprehensive keyword matching.
"""

import requests
from bs4 import BeautifulSoup
import json
import datetime
import hashlib
import os
import re
import time

# ------------------------------------------------------------------------------
# NEGATIVE KEYWORDS - Articles containing these are dropped
# ------------------------------------------------------------------------------
NEGATIVE_KEYWORDS = [
    # Crime & Courts
    "murder", "rape", "assault", "robbery", "theft", "kidnap", "arrested",
    "police custody", "fir filed", "accused", "chargesheet", "bail",
    "convicted", "sentenced", "acquitted", "gang war", "mob lynching",
    "scam accused", "remanded", "missing person",
    # Entertainment & Sports
    "bollywood", "box office", "celebrity", "film review", "movie release",
    "web series", "reality show", "bigg boss", "ipl", "cricket match",
    "fifa", "nba", "tennis tournament", "world cup", "olympic medal",
    # Festivals & Lifestyle
    "happy diwali", "happy holi", "navratri", "eid mubarak", "christmas",
    "recipe", "horoscope", "astrology", "zodiac", "fashion week",
    "weight loss", "diet plan", "skin care", "hair care", "beauty tips",
    # Accidents & Disasters
    "road accident", "train accident", "plane crash", "building collapse",
    "fire accident", "earthquake kills", "flood kills", "landslide",
    # Obituary
    "passes away", "condolence", "funeral", "prayer meet", "dies at",
    "death of", "killed in", "bodies found", "last rites",
    # Stock Market (unless policy related)
    "share price", "stock surges", "nifty", "sensex", "quarterly result",
    "q1 result", "q2 result", "q3 result", "q4 result", "profit rises",
    # Political Noise
    "rally held", "campaign trail", "joins party", "quits party",
    "election rally", "roadshow",
    # Entertainment Events
    "trailer launch", "song release", "album", "concert", "award ceremony",
    "filmfare", "oscars",
    # Weather (unless policy)
    "weather update", "temperature today",
]

# ------------------------------------------------------------------------------
# PIF VERTICALS & COMPREHENSIVE KEYWORDS
# ------------------------------------------------------------------------------
VERTICALS = {
    "EooDB": {
        "label": "Ease of Doing Business",
        "color": "#E8620A",
        "keywords": [
            # Core EODB
            "ease of doing business", "eodb", "business reform", "regulatory reform",
            "compliance burden", "single window", "one nation one license",
            # MSME & Manufacturing
            "msme", "micro small medium", "small enterprise", "manufacturing hub",
            "make in india", "production linked incentive", "pli scheme",
            "industrial policy", "industrial corridor", "industrial park",
            "manufacturing sector", "factory", "plant inaugurat",
            # Trade & Investment
            "foreign direct investment", "fdi", "foreign investment", "bilateral trade",
            "export promotion", "import duty", "customs duty", "tariff",
            "trade policy", "trade agreement", "free trade", "fta", "cepa",
            "trade deficit", "trade surplus", "export growth", "import substitution",
            # SEZ & Startup
            "special economic zone", "sez", "startup india", "startup policy",
            "entrepreneur", "incubator", "accelerator", "venture capital",
            # Ease of Compliance
            "gst council", "tax reform", "direct tax", "indirect tax",
            "corporate tax", "compliance", "regulation", "deregulation",
            "license raj", "permit", "clearance", "approval process",
            # Sectors
            "semiconductor", "electronics manufacturing", "chip", "fab",
            "textile industry", "garment export", "leather", "pharma export",
            "auto industry", "automobile", "ev manufacturing",
            "defence manufacturing", "aerospace", "shipbuilding",
            # Infrastructure for Business
            "logistics", "supply chain", "warehouse", "cold chain",
            "port", "airport", "freight corridor", "sagarmala", "bharatmala",
            "pm gati shakti", "national infrastructure pipeline",
            # Investment & MoU
            "mou signed", "investment promotion", "investor summit",
            "business summit", "industry meet", "cii", "ficci", "assocham",
            # Government
            "ministry of commerce", "ministry of industry", "dpiit", "dgft",
            "cabinet approves", "union minister", "niti aayog",
            # Competition
            "competition commission", "cci", "anti-trust", "merger approval",
            # Insolvency
            "insolvency", "ibc", "nclt", "bankruptcy", "resolution plan",
            # Atmanirbhar
            "atmanirbhar bharat", "self reliant", "vocal for local",
            "viksit bharat", "developed india",
        ]
    },
    "CoDED": {
        "label": "Data & Economic Decision-making",
        "color": "#2471A3",
        "keywords": [
            # Statistics & Data
            "economic data", "statistics", "statistical", "data governance",
            "national statistical office", "nso", "mospi", "cso",
            "census", "population census", "economic census",
            # GDP & Growth
            "gdp", "gross domestic product", "gsdp", "state gdp",
            "economic growth", "growth rate", "growth estimate",
            "economic survey", "annual survey", "quarterly estimate",
            # Economic Indicators
            "economic indicator", "national accounts", "gva",
            "gross value added", "index of industrial production", "iip",
            "inflation", "cpi", "wpi", "consumer price", "wholesale price",
            "fiscal deficit", "revenue deficit", "current account",
            # Surveys
            "national sample survey", "nsso", "plfs", "labour force survey",
            "household survey", "consumption survey", "employment survey",
            # Data Infrastructure
            "data infrastructure", "data center", "data exchange",
            "data marketplace", "open data", "data policy",
            "digital public infrastructure", "dpi",
            # Economic Planning
            "economic planning", "five year plan", "annual plan",
            "planning commission", "economic advisory",
            # Monetary
            "rbi", "reserve bank", "monetary policy", "repo rate",
            "interest rate", "credit growth", "banking sector",
            # Research
            "economic research", "policy research", "think tank",
        ]
    },
    "iLEAP": {
        "label": "Lead Elimination & Public Health",
        "color": "#C0392B",
        "keywords": [
            # Lead Specific
            "lead poisoning", "lead contamination", "blood lead level",
            "heavy metal", "lead paint", "lead exposure", "lead free",
            "lead pollution", "lead toxicity", "neurotoxin",
            # Public Health Infrastructure
            "public health", "health ministry", "ministry of health",
            "health minister", "health policy", "health mission",
            "national health mission", "nhm", "health infrastructure",
            "hospital", "medical college", "aiims", "phc", "health center",
            # Health Schemes
            "ayushman bharat", "pmjay", "jan arogya", "health insurance",
            "universal health coverage", "health card",
            # Vaccination & Immunization
            "vaccination", "vaccine", "immunization", "immunisation",
            "polio", "covid vaccine", "routine immunization",
            # Maternal & Child Health
            "maternal health", "child health", "infant mortality", "imr",
            "maternal mortality", "mmr", "antenatal", "postnatal",
            "nutrition", "malnutrition", "poshan abhiyaan", "poshan",
            "midday meal", "anganwadi", "icds", "stunting", "wasting",
            # Disease Control
            "disease", "epidemic", "pandemic", "outbreak",
            "tuberculosis", "tb", "malaria", "dengue", "cancer",
            "diabetes", "hypertension", "cardiovascular",
            "non communicable disease", "ncd", "communicable disease",
            # Pharma & Medicine
            "pharmaceutical", "drug", "medicine", "generic medicine",
            "jan aushadhi", "drug pricing", "pharma policy",
            # Mental Health
            "mental health", "depression", "anxiety", "psychiatr",
            "suicide prevention", "mental wellness",
            # Research
            "icmr", "medical research", "clinical trial", "health research",
            # Sanitation & Hygiene
            "sanitation", "swachh bharat", "toilet", "open defecation",
            "clean india", "hygiene",
            # Pollution & Health
            "air pollution health", "pm2.5", "air quality", "pollution health",
            # Reproductive Health
            "reproductive health", "family planning", "contraceptive",
            # Tobacco & Alcohol
            "tobacco control", "smoking", "alcohol policy", "substance abuse",
        ]
    },
    "Political_Economy": {
        "label": "Political Economy & Governance",
        "color": "#117A65",
        "keywords": [
            # Governance
            "governance", "good governance", "e-governance", "digital governance",
            "administrative reform", "bureaucratic reform", "civil service",
            "public administration", "government efficiency",
            # Policy & Reform
            "policy reform", "policy implementation", "policy framework",
            "institutional reform", "structural reform",
            # Centre-State
            "federalism", "cooperative federalism", "centre state",
            "inter-state council", "zonal council", "state government",
            "chief minister", "governor",
            # Parliament & Legislature
            "parliament", "lok sabha", "rajya sabha", "legislative",
            "bill passed", "act enacted", "ordinance", "legislation",
            # Executive
            "prime minister", "cabinet", "cabinet minister", "union minister",
            "cabinet committee", "cabinet approval", "pmo",
            "niti aayog", "planning",
            # Judiciary
            "supreme court", "high court", "judicial reform", "judiciary",
            "justice", "court ruling", "legal reform",
            # Election & Democracy
            "election commission", "electoral reform", "voting",
            "delimitation", "representation",
            # Foreign Policy
            "foreign policy", "foreign minister", "mea", "ministry of external affairs",
            "diplomacy", "diplomatic", "bilateral", "multilateral",
            "g20", "brics", "sco", "quad", "asean", "saarc",
            "india us", "india china", "india pakistan", "india russia",
            "indo pacific", "strategic partnership",
            # Defence & Security
            "defence", "defense", "military", "armed forces", "army", "navy", "air force",
            "national security", "border", "strategic", "geopolitical",
            # Public Sector
            "psu", "public sector", "disinvestment", "privatization",
            "cpse", "navratna", "maharatna",
            # Urban Governance
            "urban governance", "municipal", "smart city", "urban planning",
            "urban development", "city administration",
            # Data Protection
            "data protection", "privacy", "dpdp", "digital personal data",
            # Decentralization
            "panchayat", "panchayati raj", "local governance", "gram sabha",
            "decentralization", "devolution",
        ]
    },
    "Jobs_Livelihood": {
        "label": "Jobs & Livelihoods",
        "color": "#7D3C98",
        "keywords": [
            # Employment
            "employment", "unemployment", "job creation", "job", "jobs",
            "employment generation", "hiring", "recruitment",
            "rozgar", "rozgar mela", "job fair", "placement",
            # Labour
            "labour", "labor", "worker", "workforce", "labour market",
            "labour reform", "labour code", "minimum wage", "wage",
            "trade union", "industrial relations",
            # Skills
            "skill india", "skill development", "skilling", "reskilling",
            "upskilling", "vocational training", "vocational education",
            "iti", "industrial training", "pmkvy", "apprentice",
            "national skill", "skill center",
            # Women in Work
            "women employment", "female workforce", "women in work",
            "female labour force", "flfp", "women worker",
            "maternity benefit", "creche", "gender pay", "equal pay",
            "women entrepreneur", "mahila", "self help group", "shg",
            # Rural Employment
            "rural employment", "nrega", "mgnrega", "mgnregs",
            "rural livelihood", "nrlm", "rural job", "farm employment",
            # Informal Sector
            "informal sector", "informal worker", "gig economy", "gig worker",
            "platform worker", "unorganised sector", "street vendor",
            # Entrepreneurship
            "entrepreneur", "entrepreneurship", "startup", "self employment",
            "mudra loan", "mudra", "stand up india", "small business",
            "micro enterprise",
            # Youth Employment
            "youth employment", "youth job", "campus placement",
            "internship", "fresher",
            # Social Security
            "social security", "pension", "epfo", "pf", "provident fund",
            "esic", "gratuity", "labour welfare",
            # Livelihood Programs
            "livelihood", "livelihood program", "income generation",
            "poverty alleviation", "bpl",
            # Education for Jobs
            "higher education", "technical education", "engineering college",
            "iit", "nit", "iiit", "education policy", "nep",
        ]
    },
    "Sustainability": {
        "label": "Sustainability & Climate",
        "color": "#1E8449",
        "keywords": [
            # Climate Change
            "climate change", "climate action", "climate policy",
            "global warming", "greenhouse gas", "carbon emission",
            "carbon neutral", "net zero", "decarbonization", "cop",
            "paris agreement", "ndc", "nationally determined contribution",
            # Renewable Energy
            "renewable energy", "solar energy", "solar power", "solar plant",
            "wind energy", "wind power", "hydro power", "hydroelectric",
            "green energy", "clean energy", "green hydrogen",
            "pm kusum", "rooftop solar", "solar park",
            # Environment
            "environment", "environmental", "ecology", "ecosystem",
            "biodiversity", "wildlife", "forest", "afforestation",
            "deforestation", "conservation", "national park", "sanctuary",
            "ministry of environment", "moefcc", "green tribunal", "ngt",
            # Pollution Control
            "pollution", "air pollution", "water pollution", "noise pollution",
            "pollution control", "emission standard", "bs6",
            "air quality", "aqi", "smog",
            # Waste Management
            "waste management", "solid waste", "plastic waste", "e-waste",
            "waste to energy", "recycling", "circular economy",
            "single use plastic", "swachh bharat",
            # Water
            "water conservation", "water management", "groundwater",
            "water scarcity", "water harvesting", "watershed",
            "jal jeevan", "jal shakti", "namami gange", "river cleaning",
            "river rejuvenation", "dam", "irrigation",
            # Sustainable Agriculture
            "sustainable agriculture", "organic farming", "natural farming",
            "zero budget", "agroecology", "crop diversification",
            "soil health", "fertilizer", "pesticide",
            # Electric Vehicles
            "electric vehicle", "ev", "ev policy", "ev charging",
            "battery", "lithium", "fame scheme",
            # Energy Transition
            "energy transition", "energy security", "energy efficiency",
            "energy conservation", "led", "ujala",
            # Green Finance
            "green finance", "green bond", "sustainable finance",
            "climate finance", "esg",
            # Nuclear
            "nuclear energy", "nuclear power", "atomic energy",
        ]
    }
}

# All 28 PIB Regional Offices
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
    return hashlib.md5(title.encode()).hexdigest()[:12]

def clean_text(raw):
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
    """Match text against vertical keywords"""
    text = f"{title} {summary}".lower()
    
    # First check negative keywords
    if has_negative_keywords(text):
        return ["Filtered"]
    
    matched = []
    for vid, vdata in VERTICALS.items():
        for kw in vdata["keywords"]:
            if kw.lower() in text:
                matched.append(vid)
                break
    
    return matched if matched else ["Other"]

def get_relative_time(dt):
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
        
        # Find all press release links - multiple selectors
        links_found = []
        
        # Method 1: Look for content list
        content_area = soup.find("div", class_="content-area")
        if content_area:
            links_found.extend(content_area.find_all("a", href=True))
        
        # Method 2: Look for release list items
        release_items = soup.find_all("ul", class_="releases-list") or soup.find_all("div", class_="releases")
        for item in release_items:
            links_found.extend(item.find_all("a", href=True))
        
        # Method 3: General link search
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
    print("[PIB] Starting PIB Press Release Scraper...")
    print(f"[PIB] Scraping {len(PIB_REGIONS)} regional offices...\n")
    
    all_articles = []
    seen_ids = set()
    now = datetime.datetime.utcnow()
    filtered_count = 0
    
    for reg_id, region_name in PIB_REGIONS.items():
        articles = scrape_region(reg_id, region_name)
        
        for art in articles:
            art_id = make_id(art["title"])
            if art_id in seen_ids:
                continue
            seen_ids.add(art_id)
            
            verticals = match_verticals(art["title"])
            
            # Skip filtered articles (negative keywords)
            if "Filtered" in verticals:
                filtered_count += 1
                continue
            
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
            })
        
        # Small delay to be polite to server
        time.sleep(0.5)
    
    print(f"\n[PIB] Summary:")
    print(f"   Total articles found: {len(all_articles) + filtered_count}")
    print(f"   Filtered out (negative): {filtered_count}")
    print(f"   Final articles: {len(all_articles)}")
    
    # Count by vertical
    vertical_counts = {}
    for art in all_articles:
        for v in art["verticals"]:
            vertical_counts[v] = vertical_counts.get(v, 0) + 1
    
    print(f"\n[PIB] By Vertical:")
    for v, count in sorted(vertical_counts.items(), key=lambda x: -x[1]):
        print(f"   {v}: {count}")
    
    output = {
        "last_updated": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_updated_ist": (now + datetime.timedelta(hours=5, minutes=30)).strftime("%d %b %Y, %I:%M %p IST"),
        "total": len(all_articles),
        "verticals": {k: {"label": v["label"], "color": v["color"]} for k, v in VERTICALS.items()},
        "regions": PIB_REGIONS,
        "articles": all_articles,
    }
    
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs", "pib.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"\n[PIB] Saved {len(all_articles)} articles to docs/pib.json")

if __name__ == "__main__":
    scrape_pib()
