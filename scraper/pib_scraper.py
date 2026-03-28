#!/usr/bin/env python3
"""
PIF - PIB Press Release Scraper
Fetches all 28 regional PIB feeds (English only) for last 24 hours.
Filters by PIF verticals and outputs to docs/pib.json
"""

import feedparser
import json
import datetime
import hashlib
import os
import re
from bs4 import BeautifulSoup

VERTICALS = {
    "EooDB": {
        "label": "Ease of Doing Business",
        "color": "#E8620A",
        "keywords": [
            "ease of doing business", "eodb", "msme", "make in india", "pli scheme",
            "production linked incentive", "industrial policy", "foreign investment",
            "special economic zone", "sez", "startup india", "viksit bharat",
            "atmanirbhar bharat", "trade deficit", "export", "import", "tariff",
            "trade policy", "free trade agreement", "fta", "wto", "customs",
            "manufacturing", "industry", "business", "investment", "commerce",
            "semiconductor", "electronics", "textile", "garment", "logistics",
            "supply chain", "industrial corridor", "dgft", "bilateral trade",
            "cabinet approves", "union minister", "ministry of commerce",
            "mou signed", "export promotion", "import duty",
        ]
    },
    "CoDED": {
        "label": "Data & Economic Decision-making",
        "color": "#2471A3",
        "keywords": [
            "economic data", "gdp", "gsdp", "statistics", "census", "nso", "mospi",
            "economic survey", "economic indicators", "national accounts",
            "index of industrial production", "inflation", "cpi", "wpi",
            "data governance", "economic planning", "statistical",
        ]
    },
    "iLEAP": {
        "label": "Lead Elimination & Public Health",
        "color": "#C0392B",
        "keywords": [
            "lead poisoning", "lead contamination", "public health", "health minister",
            "ministry of health", "ayushman bharat", "vaccination", "immunization",
            "icmr", "medical", "hospital", "healthcare", "disease", "malnutrition",
            "nutrition", "poshan", "maternal health", "child health", "infant mortality",
            "drug", "pharmaceutical", "medicine", "epidemic", "pandemic",
            "mental health", "cancer", "diabetes", "tuberculosis", "tb",
        ]
    },
    "Political_Economy": {
        "label": "Political Economy & Governance",
        "color": "#117A65",
        "keywords": [
            "governance", "policy", "reform", "administration", "bureaucrat",
            "prime minister", "cabinet", "niti aayog", "parliament", "legislative",
            "federalism", "centre state", "election commission", "judicial",
            "supreme court", "high court", "law", "regulation", "ministry",
            "foreign affairs", "diplomacy", "bilateral", "g20", "brics",
            "india us", "india china", "strategic", "defense", "defence",
        ]
    },
    "Jobs_Livelihood": {
        "label": "Jobs & Livelihoods",
        "color": "#7D3C98",
        "keywords": [
            "employment", "unemployment", "job", "jobs", "livelihood", "skill india",
            "pmkvy", "vocational training", "labour", "labor", "worker", "workforce",
            "women employment", "female workforce", "self help group", "shg",
            "nrega", "mgnregs", "rozgar", "mudra loan", "entrepreneur",
            "startup", "micro enterprise", "small business", "informal sector",
        ]
    },
    "Sustainability": {
        "label": "Sustainability & Climate",
        "color": "#1E8449",
        "keywords": [
            "climate", "environment", "renewable energy", "solar", "wind energy",
            "green hydrogen", "carbon", "emission", "pollution", "air quality",
            "sustainable", "sustainability", "electric vehicle", "ev", "battery",
            "waste management", "plastic", "recycle", "biodiversity", "forest",
            "water", "river", "groundwater", "jal jeevan", "namami gange",
            "ministry of environment", "ministry of power", "energy transition",
            "net zero", "green finance", "agriculture", "farming", "organic",
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

PIB_FEEDS = [f"https://pib.gov.in/RssMain.aspx?ModId=6&Lang=1&Regid={rid}" for rid in PIB_REGIONS.keys()]

def make_id(title):
    return hashlib.md5(title.encode()).hexdigest()[:12]

def clean_text(raw):
    if not raw:
        return ""
    text = BeautifulSoup(raw, "html.parser").get_text(separator=" ")
    return re.sub(r"\s+", " ", text).strip()

def match_verticals(title, summary=""):
    text = f"{title} {summary}".lower()
    matched = []
    for vid, vdata in VERTICALS.items():
        for kw in vdata["keywords"]:
            if kw.lower() in text:
                matched.append(vid)
                break
    return matched if matched else ["Other"]

def parse_date(entry):
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                dt = datetime.datetime(*val[:6])
                return dt.strftime("%Y-%m-%dT%H:%M:%S"), dt
            except:
                pass
    now = datetime.datetime.utcnow()
    return now.strftime("%Y-%m-%dT%H:%M:%S"), now

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

def scrape_pib():
    print("[PIB] Starting PIB Press Release scraper...")
    print(f"[PIB] Fetching {len(PIB_FEEDS)} regional feeds (English)...")
    
    articles = []
    seen_ids = set()
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=24)
    
    for feed_url in PIB_FEEDS:
        region_id = feed_url.split("Regid=")[-1]
        region_name = PIB_REGIONS.get(region_id, "Unknown")
        
        try:
            feed = feedparser.parse(feed_url)
            if not feed.entries:
                continue
            
            print(f"   [{region_name}] Found {len(feed.entries)} entries")
            
            for entry in feed.entries[:30]:
                title = clean_text(entry.get("title", ""))
                link = entry.get("link", "").strip()
                summary = clean_text(entry.get("summary", entry.get("description", "")))
                
                if not title or not link:
                    continue
                
                date_str, date_obj = parse_date(entry)
                
                if date_obj < cutoff:
                    continue
                
                art_id = make_id(title)
                if art_id in seen_ids:
                    continue
                seen_ids.add(art_id)
                
                verticals = match_verticals(title, summary)
                
                articles.append({
                    "id": art_id,
                    "title": title,
                    "summary": summary[:300] if summary else "",
                    "url": link,
                    "region": region_name,
                    "region_id": region_id,
                    "date": date_str,
                    "relative_time": get_relative_time(date_obj),
                    "verticals": verticals,
                    "primary_vertical": verticals[0] if verticals else "Other",
                })
        except Exception as e:
            print(f"   [{region_name}] Error: {e}")
    
    articles.sort(key=lambda x: x["date"], reverse=True)
    
    print(f"\n[PIB] Found {len(articles)} articles within 24hrs")
    
    output = {
        "last_updated": datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "last_updated_ist": (datetime.datetime.utcnow() + datetime.timedelta(hours=5, minutes=30)).strftime("%d %b %Y, %I:%M %p IST"),
        "total": len(articles),
        "verticals": {k: {"label": v["label"], "color": v["color"]} for k, v in VERTICALS.items()},
        "regions": PIB_REGIONS,
        "articles": articles,
    }
    
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "docs", "pib.json")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print(f"[PIB] Saved {len(articles)} articles to docs/pib.json")

if __name__ == "__main__":
    scrape_pib()
