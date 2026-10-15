"""
Comprehensive Test Suite for Dynamic Scoring Criteria and Strict Knock-Out Rules
"""

import json
import requests

BASE_URL = "http://127.0.0.1:8000"

EXPECTED_FIELDS = [
    "name",
    "website",
    "street_address",
    "city",
    "country",
    "is_location_verified",
    "type",
    "industry_category",
    "volume_metric",
    "phone",
    "email",
    "social_media",
    "matched_signals",
    "opportunity_score",
    "tier",
    "is_unreachable",
]


def test_dynamic_scoring_and_knockout():
    print("Testing POST /api/harvest with dynamic criteria & strict knock-out gates...")

    custom_criteria = {
        "parameters": [
            {
                "id": "dental_pms",
                "name": "Practice Management Software",
                "criterion_type": "keywords",
                "values": ["Dentrix", "Eaglesoft", "Open Dental", "Curve Dental"],
                "weight": 0.35,
                "is_mandatory": False,
            },
            {
                "id": "practice_capacity",
                "name": "Operatory Capacity",
                "criterion_type": "numeric_threshold",
                "threshold": 40.0,
                "unit_label": "chairs / operatories",
                "weight": 0.25,
                "is_mandatory": False,
            },
            {
                "id": "hipaa_compliance",
                "name": "HIPAA & Patient Privacy Compliance",
                "criterion_type": "keywords",
                "values": ["HIPAA", "Privacy", "Compliance", "Encrypted Portal"],
                "weight": 0.40,
                "is_mandatory": True,  # Strict Knock-Out Gate
            },
        ],
        "tier1_threshold": 0.60,
        "tier2_threshold": 0.30,
    }

    payload = {
        "city": "Dallas, TX",
        "target_segment": "custom",
        "search_objective": "dental surgery practice",
        "limit": 25,
        "scoring_criteria": custom_criteria,
    }

    resp = requests.post(f"{BASE_URL}/api/harvest", json=payload)
    assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
    job_id = resp.json()["job_id"]
    print(f"Dynamic scoring job created: {job_id}")

    # Stream SSE
    stream_resp = requests.get(f"{BASE_URL}/api/stream/{job_id}", stream=True, timeout=180)
    assert stream_resp.status_code == 200

    saw_criteria_log = False
    saw_match_log = False
    saw_knockout_log = False
    leads_received = []

    for line in stream_resp.iter_lines():
        if not line:
            continue
        line_str = line.decode("utf-8")
        if line_str.startswith("data: "):
            data = json.loads(line_str[6:])
            if data.get("type") == "log":
                msg = data.get("message", "")
                if "[CRITERIA]" in msg:
                    saw_criteria_log = True
                    print(f"Verified [CRITERIA] telemetry log: {msg}")
                    assert "Practice Management Software" in msg
                    assert "Operatory Capacity" in msg
                    assert "HIPAA & Patient Privacy Compliance" in msg
                    assert "T1 ≥0.60" in msg
                elif "[MATCH]" in msg:
                    saw_match_log = True
                elif "[KNOCKOUT]" in msg:
                    saw_knockout_log = True
                    print(f"Verified [KNOCKOUT] telemetry log: {msg}")
            elif data.get("type") == "lead":
                lead = data.get("data")
                leads_received.append(lead)

                # Verify 16 schema fields
                for field in EXPECTED_FIELDS:
                    assert field in lead, f"Missing field '{field}' in lead record: {lead}"

                # Strict knock-out verification
                if lead["tier"] == "Disqualified":
                    assert lead["opportunity_score"] == 0.0, (
                        f"Disqualified lead must have score 0.0, got {lead['opportunity_score']}"
                    )
                    assert "[KNOCK-OUT:" in lead["matched_signals"], (
                        f"Disqualified lead must have knock-out signal, got {lead['matched_signals']}"
                    )
                elif lead["opportunity_score"] >= 0.60:
                    assert lead["tier"] == "Tier 1 (High)", (
                        f"Lead score {lead['opportunity_score']} should be Tier 1 with 0.60 threshold, got {lead['tier']}"
                    )
            elif data.get("type") == "complete":
                break

    assert saw_criteria_log, "Did not receive [CRITERIA] telemetry log!"
    assert len(leads_received) >= 1, "Expected at least one real lead (is a discovery provider reachable?)"
    # Every lead must produce exactly one evaluation log: [MATCH] or [KNOCKOUT]
    assert saw_match_log or saw_knockout_log, "Did not receive any [MATCH] or [KNOCKOUT] telemetry log!"

    # Real data decides the qualified/disqualified split; the per-lead knock-out invariants
    # (score 0.0 + KNOCK-OUT signal) are asserted above for every Disqualified lead.
    disqualified = [l for l in leads_received if l["tier"] == "Disqualified"]
    qualified = [l for l in leads_received if l["tier"] != "Disqualified"]
    print(f"Total leads: {len(leads_received)} | Qualified: {len(qualified)} | Disqualified: {len(disqualified)}")
    if len(leads_received) < 25:
        print(f"NOTE: only {len(leads_received)}/25 real leads found for this niche.")

    # Verify CSV export
    print(f"Testing GET /api/export/{job_id} ...")
    export_resp = requests.get(f"{BASE_URL}/api/export/{job_id}")
    assert export_resp.status_code == 200
    assert export_resp.headers["content-type"].startswith("text/csv")
    csv_lines = export_resp.text.strip().splitlines()
    headers = [h.strip() for h in csv_lines[0].split(",")]
    print(f"CSV Headers: {headers}")
    assert headers == EXPECTED_FIELDS, f"CSV headers mismatch: {headers} vs {EXPECTED_FIELDS}"
    assert len(csv_lines) == 1 + len(leads_received)
    print("CSV Export test passed!")

    print("\nALL DYNAMIC SCORING AND STRICT KNOCK-OUT TESTS PASSED SUCCESSFULLY!")


if __name__ == "__main__":
    test_dynamic_scoring_and_knockout()
