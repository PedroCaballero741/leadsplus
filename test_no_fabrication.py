"""
Offline regression test: the harvest pipeline must NEVER fabricate leads or lead data.
Discovery and website crawling are stubbed, so this runs without network or API keys.

Run: python test_no_fabrication.py
"""

import asyncio

import pipeline
from pipeline import LeadHarvester

DISCOVERED = [
    {   # Has website + directory phone; site exposes an email and real volume
        "name": "Real Property Co", "website": "https://realpropertyco.test", "phone": "(512) 555-0101",
        "email": None, "street_address": "100 Congress Ave", "city": "Austin, TX",
        "socials": {"linkedin": "", "facebook": "", "instagram": ""}, "source": "stub",
    },
    {   # No website at all: must not get a guessed domain, email, socials, or volume
        "name": "No Site Management", "website": None, "phone": "(512) 555-0102",
        "email": None, "street_address": "Unlisted", "city": "Austin, TX",
        "socials": {"linkedin": "", "facebook": "", "instagram": ""}, "source": "stub",
    },
    {   # Website whose crawl returns nothing: must stay unenriched
        "name": "Quiet Site LLC", "website": "https://quietsite.test", "phone": None,
        "email": None, "street_address": "Unlisted", "city": "Austin, TX",
        "socials": {"linkedin": "", "facebook": "", "instagram": ""}, "source": "stub",
    },
]

SITE_DATA = {
    "https://realpropertyco.test": (
        None, "jane@realpropertyco.test", None, 240,
        {"linkedin": "https://linkedin.com/company/realpropertyco", "facebook": "", "instagram": ""},
        "We manage 240 doors using AppFolio with an owner portal and trust accounting.",
    ),
}
EMPTY_SITE = (None, None, None, 0, {"linkedin": "", "facebook": "", "instagram": ""}, "")


def run_harvest(limit: int):
    pipeline.discover_businesses = lambda city, q, n, log=None: [dict(d) for d in DISCOVERED]
    h = LeadHarvester()
    h.check_robots_txt = lambda url: (True, f"stub allow {url}")
    h._scrape_website_signals = lambda url: SITE_DATA.get(url, EMPTY_SITE)

    async def collect():
        events = []
        async for ev in h.harvest(city="Austin, TX", search_objective="property management", limit=limit):
            events.append(ev)
        return events

    return asyncio.run(collect())


def test_no_fabrication():
    events = run_harvest(limit=25)
    leads = {e["data"]["name"]: e["data"] for e in events if e["type"] == "lead"}
    logs = [e["message"] for e in events if e["type"] == "log"]

    # 1. Only discovered businesses come out, even though 25 were requested.
    assert set(leads) == {d["name"] for d in DISCOVERED}, f"Unexpected leads: {list(leads)}"
    assert any("Only 3 of 25" in m for m in logs), "Shortfall should be reported, not padded"

    # 2. Real data is preserved.
    real = leads["Real Property Co"]
    assert real["email"] == "jane@realpropertyco.test"
    assert real["phone"] == "(512) 555-0101"
    assert real["volume_metric"] == 240
    assert "AppFolio" in real["matched_signals"]
    assert real["industry_category"] == "Property Management", real["industry_category"]

    # 3. Missing data stays missing.
    nosite = leads["No Site Management"]
    assert nosite["website"] == "", f"Website was guessed: {nosite['website']}"
    assert nosite["email"] == "Missing", f"Email was invented: {nosite['email']}"
    assert nosite["volume_metric"] == 0, f"Volume was invented: {nosite['volume_metric']}"
    assert '"linkedin": ""' in nosite["social_media"], f"Socials were invented: {nosite['social_media']}"

    quiet = leads["Quiet Site LLC"]
    assert quiet["email"] == "Missing", f"Email was invented: {quiet['email']}"
    assert quiet["phone"] == "Missing"
    assert quiet["is_unreachable"] is True
    assert quiet["opportunity_score"] == 0.0
    assert quiet["volume_metric"] == 0

    print("ALL NO-FABRICATION TESTS PASSED")


if __name__ == "__main__":
    test_no_fabrication()
