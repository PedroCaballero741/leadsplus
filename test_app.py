"""
Integration test for Blaze Lead Harvester server & endpoints
"""

import sys
import time
import requests

BASE_URL = "http://127.0.0.1:8000"

def run_tests():
    print("Testing GET / ...")
    resp = requests.get(f"{BASE_URL}/")
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}"
    assert "LEAD HARVESTER" in resp.text
    assert "#FFE600" in resp.text
    print("GET / passed!")

    print("Testing GET /api/health ...")
    resp = requests.get(f"{BASE_URL}/api/health")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"
    print("GET /api/health passed!")

    print("Testing GET /api/segments ...")
    resp = requests.get(f"{BASE_URL}/api/segments")
    assert resp.status_code == 200
    assert len(resp.json()["segments"]) >= 5
    print("GET /api/segments passed!")

    print("Testing POST /api/harvest (limit 25) ...")
    payload = {
        "city": "Austin, TX",
        "objective": "property management",
        "limit": 25
    }
    resp = requests.post(f"{BASE_URL}/api/harvest", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    job_id = data["job_id"]
    print(f"Harvest job created: {job_id}")

    print("Testing GET /api/stream/{job_id} (SSE stream) ...")
    stream_resp = requests.get(f"{BASE_URL}/api/stream/{job_id}", stream=True, timeout=120)
    assert stream_resp.status_code == 200
    
    events_received = 0
    leads_received = 0
    for line in stream_resp.iter_lines():
        if not line:
            continue
        line_str = line.decode("utf-8")
        if line_str.startswith("data: "):
            events_received += 1
            import json
            payload = json.loads(line_str[6:])
            if payload.get("type") == "lead":
                leads_received += 1
                lead = payload["data"]
                # Verify all 16 fields exist
                for f in [
                    "name", "website", "street_address", "city", "country", "is_location_verified",
                    "type", "industry_category", "volume_metric", "phone", "email", "social_media",
                    "matched_signals", "opportunity_score", "tier", "is_unreachable"
                ]:
                    assert f in lead, f"Missing field {f}"
            elif payload.get("type") == "complete":
                print(f"Received completion event! Total leads: {payload.get('total')}")
                break

    print(f"Events received: {events_received}, Leads streamed: {leads_received}")
    # Real discovery returns only businesses that exist; it never pads to the requested limit.
    assert leads_received >= 1, "Expected at least one real lead (is a discovery provider reachable?)"
    if leads_received < 25:
        print(f"NOTE: only {leads_received}/25 real leads found. Add GOOGLE_PLACES_API_KEY or FOURSQUARE_API_KEY for more.")

    print(f"Testing GET /api/export/{job_id} ...")
    export_resp = requests.get(f"{BASE_URL}/api/export/{job_id}")
    assert export_resp.status_code == 200
    assert export_resp.headers["content-type"].startswith("text/csv")
    csv_content = export_resp.text
    lines = csv_content.strip().splitlines()
    headers = lines[0].split(",")
    print(f"CSV Headers: {headers}")
    assert len(headers) == 16
    assert len(lines) == 1 + leads_received, f"CSV rows {len(lines) - 1} != streamed leads {leads_received}"
    print("CSV Export test passed!")

    print("\nALL INTEGRATION TESTS PASSED SUCCESSFULLY!")

if __name__ == "__main__":
    run_tests()
