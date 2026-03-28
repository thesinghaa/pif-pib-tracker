#!/usr/bin/env python3
"""
PIF - PIB Press Release Scraper (HTML Version)
Scrapes PIB website directly instead of RSS feeds.
"""

import requests
from bs4 import BeautifulSoup
import json
import datetime
import hashlib
import os
import re

VERTICALS = {
    "EooDB": {
        "label": "Ease of Doing Business",
        "color": "#E8620A",
        "keywords": ["ease of doing business", "eodb", "msme", "make in india", "pli scheme", "production linked incentive", "industrial policy", "foreign investment", "special economic zone", "sez", "startup india", "viksit bharat", "atmanirbhar bharat", "trade deficit", "export", "import", "tariff", "trade policy", "free trade agreement", "fta", "wto", "customs", "manufacturing", "industry", "business", "investment", "commerce", "semiconductor", "electronics", "textile", "garment", "logistics", "supply chain", "industrial corridor", "dgft", "bilateral trade", "cabinet approves", "union minister", "ministry of commerce", "mou signed", "export promotion", "import duty"]
    },
    "CoDED": {
        "label": "Data & Economic Decision-making",
        "color": "#2471A3",
        "keywords": ["economic data", "gdp", "gsdp", "statistics", "census", "nso", "mospi", "economic survey", "economic indicators", "national accounts", "index of industrial production", "inflation", "cpi", "wpi", "data governance", "economic planning", "statistical"]
    },
    "iLEAP": {
        "label": "Lead Elimination & Public Health",
        "color": "#C0392B",
        "keywords": ["lead poisoning", "lead contamination", "public health", "health minister", "ministry of health", "ayushman bharat", "vaccination", "immunization", "icmr", "medical", "hospital", "healthcare", "disease", "malnutrition", "nutrition", "poshan", "maternal health", "child health", "infant mortality", "drug", "pharmaceutical", "medicine", "epidemic", "pandemic", "mental health", "cancer", "diabetes", "tuberculosis", "tb"]
    },
    "Political_Economy": {
        "label": "Political Economy & Governance",
        "color": "#117A65",
        "keywords": ["governance", "policy", "reform", "administration", "bureaucrat", "prime minister", "cabinet", "niti aayog", "parliament", "legislative", "federalism", "centre state", "election commission", "judicial", "supreme court", "high court", "law", "regulation", "ministry", "foreign affairs", "diplomacy", "bilateral", "g20", "brics", "india us", "india china", "strategic", "defense", "defence"]
    },
    "Jobs_Livelihood": {
        "label": "Jobs & Livelihoods",
        "color": "#7D3C98",
        "keywords": ["employment", "unemployment", "job", "jobs", "livelihood", "skill india", "pmkvy", "vocational training", "labour", "labor", "worker", "workforce", "women employment", "female workforce", "self help group", "shg", "nrega", "mgnregs", "rozgar", "mudra loan", "entrepreneur", "startup", "micro enterprise", "small business", "informal sector"]
    },
    "Sustainability": {
        "label": "Sustainability & Climate",
        "color": "#1E8449",
        "keywords": ["climate", "environment", "renewable energy", "solar", "wind energy", "green hydrogen", "carbon", "emission", "pollution", "air quality", "sustainable", "sustainability", "electric vehicle", "ev", "battery", "waste management", "plastic", "recycle", "biodiversity", "forest", "water", "river", "groundwater", "jal jeevan", "namami gange", "ministry of environment", "ministry of power", "energy transition", "net zero", "green finance", "agriculture", "farming", "organic"]
    }
}

# All 28 PIB Regional Offices with their reg= codes
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

def match_verticals(title, summary=""):
    text = f"{title} {summary}".lower()
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
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Find all press release links
        content_div = soup.find("div", class_="content-area") or soup.find("div", id="container") or soup
        
        # Look for links that go to press releases
        for link in content_div.find_all("a", href=True):
            href = link.get("href", "")
            title = clean_text(link.get_text())
            
            # Filter for press release links
            if "PressReleasePage" in href or "Pressreleaseshare" in href:
                if len(title) < 20:
                    continue
                
                # Build full URL
                if href.startswith("/"):
                    href = "https://www.pib.gov.in" + href
                elif not href.startswith("http"):
                    href = "https://www.pib.gov.in/" + href
                
                articles.append({
                    "title": title,
                    "url": href,
                    "region": region_name,
                    "region_id": reg_id,
                })
        
        print(f"   [{region_name}] Found {len(articles)} releases")
        
    except Exception as e:
        print(f"   [{region_name}] Error: {e}")
    
    return articles

def scrape_pib():
    print("[PIB] Starting PIB HTML Scraper...")
    print(f"[PIB] Scraping {len(PIB_REGIONS)} regional offices...")
    
    all_articles = []
    seen_ids = set()
    now = datetime.datetime.utcnow()
    
    for reg_id, region_name in PIB_REGIONS.items():
        articles = scrape_region(reg_id, region_name)
        
        for art in articles:
            art_id = make_id(art["title"])
            if art_id in seen_ids:
                continue
            seen_ids.add(art_id)
            
            verticals = match_verticals(art["title"])
            
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
    
    print(f"\n[PIB] Total unique articles: {len(all_articles)}")
    
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
    
    print(f"[PIB] Saved {len(all_articles)} articles to docs/pib.json")

if __name__ == "__main__":
    scrape_pib()
