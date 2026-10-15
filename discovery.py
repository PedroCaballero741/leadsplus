"""
Lead Discovery Providers
Finds REAL businesses through official, terms-compliant APIs. No records are ever fabricated:
if the providers return fewer businesses than requested, the harvest returns fewer leads.

Providers (tried in this order, each only if configured):
    google      Google Places API (New) Text Search   -> GOOGLE_PLACES_API_KEY
    foursquare  Foursquare Places API                  -> FOURSQUARE_API_KEY
    osm         OpenStreetMap (Nominatim + Overpass)   -> no key needed (free, sparse coverage)

Override the order/selection with DISCOVERY_PROVIDERS, e.g. "google,osm" or "osm".
"""

import json
import logging
import os
import re
import time
from typing import Callable, Dict, List, Optional

import requests

import ai_assist  # noqa: F401  (side effect: loads .env so keys are available)

logger = logging.getLogger("lead_harvester.discovery")

# Identify ourselves honestly to public APIs (Nominatim/Overpass usage policies require it).
API_USER_AGENT = os.getenv(
    "DISCOVERY_USER_AGENT", "LeadHarvester/1.0 (B2B lead research; contact: admin@localhost)"
)
HTTP_TIMEOUT = 30

GOOGLE_TEXT_SEARCH_URL = "https://places.googleapis.com/v1/places:searchText"
GOOGLE_FIELD_MASK = ",".join([
    "places.id",
    "places.displayName",
    "places.formattedAddress",
    "places.nationalPhoneNumber",
    "places.websiteUri",
    "places.businessStatus",
    "nextPageToken",
])

FOURSQUARE_SEARCH_URL = "https://places-api.foursquare.com/places/search"
FOURSQUARE_API_VERSION = os.getenv("FOURSQUARE_API_VERSION", "2025-06-17")

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
OVERPASS_URLS = [
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
]

# Non-business words dropped when building an OSM name regex from a search term.
_STOPWORDS = {"and", "the", "of", "for", "in", "services", "service", "company", "companies", "&"}


def _empty_socials() -> Dict[str, str]:
    return {"linkedin": "", "facebook": "", "instagram": ""}


def _candidate(name, website, phone, street_address, city, source, email=None, socials=None) -> dict:
    """Normalized discovery record consumed by LeadHarvester.harvest()."""
    return {
        "name": (name or "").strip(),
        "website": (website or "").strip() or None,
        "phone": (phone or "").strip() or None,
        "email": (email or "").strip() or None,
        "street_address": (street_address or "").strip() or "Unlisted",
        "city": city,
        "socials": socials or _empty_socials(),
        "source": source,
    }


# --------------------------------------------------------------------------- Google Places
def search_google_places(city: str, query: str, max_results: int) -> List[dict]:
    api_key = os.getenv("GOOGLE_PLACES_API_KEY")
    if not api_key:
        return []

    headers = {
        "Content-Type": "application/json",
        "X-Goog-Api-Key": api_key,
        "X-Goog-FieldMask": GOOGLE_FIELD_MASK,
    }
    body = {"textQuery": f"{query} in {city}", "pageSize": 20, "languageCode": "en"}
    results: List[dict] = []

    for _ in range(3):  # Text Search returns at most 60 results (3 pages of 20)
        resp = requests.post(GOOGLE_TEXT_SEARCH_URL, headers=headers, json=body, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            raise RuntimeError(f"Google Places error {resp.status_code}: {resp.text[:200]}")
        data = resp.json()
        for place in data.get("places", []):
            if place.get("businessStatus") not in (None, "OPERATIONAL"):
                continue
            results.append(_candidate(
                name=(place.get("displayName") or {}).get("text"),
                website=place.get("websiteUri"),
                phone=place.get("nationalPhoneNumber"),
                street_address=place.get("formattedAddress"),
                city=city,
                source="google_places",
            ))
        token = data.get("nextPageToken")
        if not token or len(results) >= max_results:
            break
        body["pageToken"] = token
        time.sleep(1.5)  # next page token needs a moment to become valid

    return results[:max_results]


# --------------------------------------------------------------------------- Foursquare
def search_foursquare(city: str, query: str, max_results: int) -> List[dict]:
    api_key = os.getenv("FOURSQUARE_API_KEY")
    if not api_key:
        return []

    # Accept both raw API key and keys with pre-attached "Bearer "
    auth_header = api_key.strip()
    if not auth_header.startswith("Bearer "):
        auth_header = f"Bearer {auth_header}"

    headers = {
        "Authorization": auth_header,
        "X-Places-Api-Version": FOURSQUARE_API_VERSION,
        "Accept": "application/json",
    }
    params = {
        "query": query,
        "near": city,
        "limit": min(50, max(1, max_results)),
        "fields": "name,website,tel,email,location,social_media,categories,description",
    }
    results: List[dict] = []
    url: Optional[str] = FOURSQUARE_SEARCH_URL

    while url and len(results) < max_results:
        resp = requests.get(url, headers=headers, params=params, timeout=HTTP_TIMEOUT)
        if resp.status_code != 200:
            raise RuntimeError(f"Foursquare error {resp.status_code}: {resp.text[:200]}")
        for place in resp.json().get("results", []):
            loc = place.get("location") or {}
            sm = place.get("social_media") or {}
            socials = _empty_socials()
            if sm.get("facebook_id"):
                socials["facebook"] = f"https://facebook.com/{sm['facebook_id']}"
            if sm.get("instagram"):
                socials["instagram"] = f"https://instagram.com/{sm['instagram']}"
            if sm.get("twitter"):
                socials["twitter"] = f"https://twitter.com/{sm['twitter']}"

            # Format clean full street address
            addr_parts = [loc.get("address")]
            if loc.get("postcode"):
                addr_parts.append(loc.get("postcode"))
            full_addr = loc.get("formatted_address") or " ".join(p for p in addr_parts if p)

            results.append(_candidate(
                name=place.get("name"),
                website=place.get("website"),
                phone=place.get("tel"),
                email=place.get("email"),
                street_address=full_addr,
                city=city,
                socials=socials,
                source="foursquare",
            ))
        # Cursor pagination via RFC 5988 Link header; params are embedded in the next URL.
        url = resp.links.get("next", {}).get("url")
        params = None

    return results[:max_results]


# --------------------------------------------------------------------------- OpenStreetMap
_GEOCODE_CACHE: Dict[str, List[float]] = {}
OVERPASS_MIRRORS = [
    "https://z.overpass-api.de/api/interpreter",
    "https://lz4.overpass-api.de/api/interpreter",
    "https://overpass-api.de/api/interpreter",
    "https://overpass.kumi.systems/api/interpreter",
    "https://overpass.private.coffee/api/interpreter",
]


def _geocode_bbox(city: str) -> Optional[List[float]]:
    """Geocodes target city bounding box with structured fallback and in-memory cache."""
    cache_key = city.strip().lower()
    if cache_key in _GEOCODE_CACHE:
        return _GEOCODE_CACHE[cache_key]

    params = {"format": "json", "limit": 1}
    # Check if city includes state (e.g., 'Austin, TX') for structured query accuracy
    if "," in city:
        parts = [p.strip() for p in city.split(",", 1)]
        params.update({"city": parts[0], "state": parts[1], "country": "USA"})
    else:
        params["q"] = city

    resp = requests.get(
        NOMINATIM_URL,
        params=params,
        headers={"User-Agent": API_USER_AGENT},
        timeout=HTTP_TIMEOUT,
    )
    if resp.status_code == 200 and resp.json():
        south, north, west, east = (float(x) for x in resp.json()[0]["boundingbox"])
        bbox = [south, west, north, east]
        _GEOCODE_CACHE[cache_key] = bbox
        return bbox

    # Freeform fallback
    resp = requests.get(
        NOMINATIM_URL,
        params={"q": city, "format": "json", "limit": 1},
        headers={"User-Agent": API_USER_AGENT},
        timeout=HTTP_TIMEOUT,
    )
    resp.raise_for_status()
    hits = resp.json()
    if not hits:
        return None
    south, north, west, east = (float(x) for x in hits[0]["boundingbox"])
    bbox = [south, west, north, east]
    _GEOCODE_CACHE[cache_key] = bbox
    return bbox


_GENERIC_LEAD_WORDS = {"real", "commercial", "business", "general", "local", "professional"}


def _osm_name_regex(query: str) -> str:
    words = [w for w in re.findall(r"[a-zA-Z]+", query.lower()) if w not in _STOPWORDS and len(w) > 2]
    if not words:
        return re.escape(query)
    if words[0] in _GENERIC_LEAD_WORDS and len(words) > 1:
        return f"{re.escape(words[0])}.?{re.escape(words[1])}"
    return re.escape(words[0])


def _build_osm_industry_clauses(query: str, bbox_str: str) -> List[str]:
    """Expands OpenStreetMap search query with semantic business tags tailored to industry."""
    q_lower = query.lower()
    clauses = []
    b = bbox_str

    if "propert" in q_lower:
        clauses.append(f'node["office"="property_management"]({b});')
        clauses.append(f'way["office"="property_management"]({b});')
        clauses.append(f'node["office"="estate_agent"]({b});')
        clauses.append(f'way["office"="estate_agent"]({b});')
        clauses.append(f'node["office"="real_estate"]({b});')
        clauses.append(f'way["office"="real_estate"]({b});')
    elif "dent" in q_lower:
        clauses.append(f'node["amenity"="dentist"]({b});')
        clauses.append(f'way["amenity"="dentist"]({b});')
        clauses.append(f'node["healthcare"="dentist"]({b});')
        clauses.append(f'way["healthcare"="dentist"]({b});')
    elif "hoa" in q_lower or "association" in q_lower:
        clauses.append(f'node["office"="association"]({b});')
        clauses.append(f'way["office"="association"]({b});')
    elif "broker" in q_lower or "commercial" in q_lower:
        clauses.append(f'node["office"="estate_agent"]({b});')
        clauses.append(f'way["office"="estate_agent"]({b});')
        clauses.append(f'node["office"="financial"]({b});')
        clauses.append(f'way["office"="financial"]({b});')
    elif "invest" in q_lower or "asset" in q_lower:
        clauses.append(f'node["office"="financial"]({b});')
        clauses.append(f'way["office"="financial"]({b});')
        clauses.append(f'node["office"="investment"]({b});')
        clauses.append(f'way["office"="investment"]({b});')
    elif "doctor" in q_lower or "clinic" in q_lower or "health" in q_lower:
        clauses.append(f'node["amenity"="doctors"]({b});')
        clauses.append(f'way["amenity"="doctors"]({b});')
        clauses.append(f'node["amenity"="clinic"]({b});')
        clauses.append(f'way["amenity"="clinic"]({b});')
        clauses.append(f'node["healthcare"="clinic"]({b});')
        clauses.append(f'way["healthcare"="clinic"]({b});')

    return clauses


def search_openstreetmap(city: str, query: str, max_results: int) -> List[dict]:
    """
    Enhanced OpenStreetMap directory discovery:
    - Multi-mirror Overpass query failover with timeouts
    - Semantic industry tag matching + name regex matching
    - Full street address, suite, phone, email, and social handles extraction
    """
    bbox = _geocode_bbox(city)
    if not bbox:
        raise RuntimeError(f"Could not geocode '{city}' with Nominatim")
    b = ",".join(f"{x:.5f}" for x in bbox)
    name_re = _osm_name_regex(query).replace('"', "")

    # Build targeted query: use specific industry clauses when identified to stay fast
    industry_clauses = _build_osm_industry_clauses(query, b)
    if industry_clauses:
        clause_str = "\n  ".join(industry_clauses)
        overpass_q = f"""[out:json][timeout:25];
(
  {clause_str}
  node["name"~"{name_re}",i]["office"]({b});
  way["name"~"{name_re}",i]["office"]({b});
);
out tags center {max_results * 3};"""
    else:
        overpass_q = f"""[out:json][timeout:25];
(
  node["name"~"{name_re}",i]["office"]({b});
  way["name"~"{name_re}",i]["office"]({b});
  node["name"~"{name_re}",i]["amenity"]({b});
  way["name"~"{name_re}",i]["amenity"]({b});
  node["name"~"{name_re}",i]["healthcare"]({b});
  way["name"~"{name_re}",i]["healthcare"]({b});
  node["name"~"{name_re}",i]["shop"]({b});
  way["name"~"{name_re}",i]["shop"]({b});
  node["name"~"{name_re}",i]["craft"]({b});
  way["name"~"{name_re}",i]["craft"]({b});
);
out tags center {max_results * 3};"""

    elements = None
    last_err: Optional[Exception] = None
    for endpoint in OVERPASS_MIRRORS:
        try:
            resp = requests.post(
                endpoint,
                data={"data": overpass_q},
                headers={"User-Agent": API_USER_AGENT},
                timeout=18,
            )
            if resp.status_code == 200:
                elements = resp.json().get("elements", [])
                break
            else:
                last_err = RuntimeError(f"Mirror {endpoint} returned HTTP {resp.status_code}")
        except Exception as e:
            last_err = e
            continue

    if elements is None:
        raise RuntimeError(f"OpenStreetMap Overpass service unavailable across all mirrors: {last_err}")

    results: List[dict] = []
    seen_names = set()

    for el in elements:
        t = el.get("tags", {})
        name = t.get("name")
        if not name or name.strip().lower() in seen_names:
            continue
        seen_names.add(name.strip().lower())

        # Construct comprehensive street address
        addr_parts = [t.get("addr:housenumber"), t.get("addr:street")]
        street = " ".join(x for x in addr_parts if x)
        unit = t.get("addr:unit") or t.get("addr:suite")
        if street and unit:
            street += f" Ste {unit}"
        if not street:
            street = t.get("addr:full") or "Unlisted"

        # Social channels
        socials = _empty_socials()
        socials["linkedin"] = t.get("contact:linkedin") or t.get("linkedin") or ""
        socials["facebook"] = t.get("contact:facebook") or t.get("facebook") or ""
        socials["instagram"] = t.get("contact:instagram") or t.get("instagram") or ""
        if t.get("contact:twitter") or t.get("twitter"):
            socials["twitter"] = t.get("contact:twitter") or t.get("twitter")

        # Contact info
        phone = t.get("contact:phone") or t.get("phone") or t.get("contact:mobile")
        email = t.get("contact:email") or t.get("email")
        website = t.get("contact:website") or t.get("website") or t.get("url")

        results.append(_candidate(
            name=name,
            website=website,
            phone=phone,
            email=email,
            street_address=street,
            city=city,
            socials=socials,
            source="openstreetmap",
        ))

    # Prioritize businesses with websites so live enrichment and dynamic scoring can evaluate them
    results.sort(key=lambda r: 0 if r["website"] else 1)
    return results[:max_results]


# --------------------------------------------------------------------------- Orchestration
PROVIDERS: Dict[str, Dict] = {
    "google": {"label": "Google Places", "fn": search_google_places, "env": "GOOGLE_PLACES_API_KEY"},
    "foursquare": {"label": "Foursquare Places", "fn": search_foursquare, "env": "FOURSQUARE_API_KEY"},
    "osm": {"label": "OpenStreetMap", "fn": search_openstreetmap, "env": None},
}


def active_providers() -> List[str]:
    """Providers that are both selected (DISCOVERY_PROVIDERS) and configured (key present)."""
    requested = [p.strip().lower() for p in os.getenv("DISCOVERY_PROVIDERS", "google,foursquare,osm").split(",")]
    out = []
    for name in requested:
        prov = PROVIDERS.get(name)
        if prov and (prov["env"] is None or os.getenv(prov["env"])):
            out.append(name)
    return out


def discovery_status() -> dict:
    return {
        "active_providers": [PROVIDERS[p]["label"] for p in active_providers()],
        "configured": {name: (p["env"] is None or bool(os.getenv(p["env"]))) for name, p in PROVIDERS.items()},
    }


def _dedupe_key(rec: dict) -> str:
    site = (rec.get("website") or "").lower()
    site = re.sub(r"^https?://(www\.)?", "", site).split("/")[0]
    return site or re.sub(r"[^a-z0-9]", "", rec["name"].lower())


def discover_businesses(
    city: str, query: str, max_results: int, log: Optional[Callable[[str], None]] = None
) -> List[dict]:
    """
    Queries configured providers in priority order until max_results unique businesses are found.
    Never fabricates records. Returns an empty list if no provider is configured or all fail.
    """
    log = log or (lambda m: None)
    seen: set = set()
    found: List[dict] = []

    providers = active_providers()
    if not providers:
        log("[DIR] No discovery provider configured. Set GOOGLE_PLACES_API_KEY or FOURSQUARE_API_KEY, or enable 'osm'.")
        return []

    for name in providers:
        if len(found) >= max_results:
            break
        prov = PROVIDERS[name]
        need = max_results - len(found)
        log(f"[DIR] Querying {prov['label']} for '{query}' in '{city}' (need {need})...")
        try:
            batch = prov["fn"](city, query, need + 10)
        except Exception as e:
            logger.warning(f"{prov['label']} failed: {e}")
            log(f"[DIR] {prov['label']} failed: {e}")
            continue

        added = 0
        for rec in batch:
            if not rec["name"]:
                continue
            key = _dedupe_key(rec)
            if key in seen:
                continue
            seen.add(key)
            found.append(rec)
            added += 1
            if len(found) >= max_results:
                break
        log(f"[DIR] {prov['label']} returned {len(batch)} businesses ({added} new after dedup)")

    return found
