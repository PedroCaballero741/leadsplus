# Blaze B2B Lead Harvester & Enrichment Engine

A standalone, compliance-first Python web application powered by **FastAPI** with an embedded **Tailwind CSS** frontend reflecting the **Blaze (`blaze.money`) visual identity**. It autonomously discovers, crawls, enriches, scores, and exports B2B leads via a live browser interface.

---

## Design System & Brand Identity

- **Brand:** Blaze (`blaze.money` visual identity)
- **Canvas:** Pure white (`#FFFFFF`) with neutral dividers (`#E5E7EB`)
- **Accent:** Blaze electric yellow (`#FFE600` / `#FDF06F`)
- **Surfaces:** Light neutral cards (`#F9F9FB`) with 1px border (`#E5E5E5`) and `rounded-2xl` corners
- **Typography:** Inter grotesque sans-serif with tight tracking and uppercase micro-labels (`tracking-widest text-xs text-neutral-500`)
- **Buttons:** Pill-shaped (`rounded-full`); primary CTA is solid black (`#000000`) hovering to `#FFE600` with black text
- **Status Dots:** Mint green (`#10B981`) for active/verified records; muted amber/red for missing contacts

---

## Pipeline & Compliance

1. **Public Directory Discovery:** Scrapes and queries open business directories for target city and objective.
2. **Strict robots.txt Enforcement:** Evaluates `robots.txt` using `urllib.robotparser` before fetching any domain. Skips and logs any disallowed domain.
3. **Domain Deduplication:** Automatically deduplicates domains before enrichment.
4. **Polite Crawling:** Enforces 0.5s polite delay between network requests.
5. **Dynamic Target Segment Profiles:** Select from structured industry profiles with grouped characteristics, or enter custom objectives:
   - **Property Management** (AppFolio, Buildium, Yardi, Owner/Tenant Portals, Trust Accounting)
   - **Commercial Real Estate & Brokerages** (CoStar, LoopNet, Crexi, Client Deal Rooms, Commercial Lease Accounting)
   - **HOA & Community Associations** (Vantaca, Cinc Systems, Frontsteps, Board Portals, Reserve Studies)
   - **Real Estate Investment & Asset Management** (Juniper Square, Yardi Matrix, Investor Portals, Waterfall Distributions)
   - **Custom Target Segment (Editable)**

---

## Data Schema (16 Fields)

| Field Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | Business operating name |
| `website` | `string` | Canonical company URL |
| `direction` | `string` | Physical street address, or `'Unlisted'` |
| `city` | `string` | Target city |
| `country` | `string` | Country (`"USA"`) |
| `location_based` | `boolean` | `True` if local physical address found; `False` if virtual |
| `type` | `string` | `"Company"` |
| `activity_economic` | `string` | Selected segment objective (e.g., Commercial Real Estate Brokerage, Property Management) |
| `amount` | `integer` | Volume metric (doors, units, or listings; default 30) |
| `phone` | `string` | Primary office or WhatsApp-ready phone (`'Missing'` if none) |
| `email` | `string` | Clean contact email filtered against static assets (`'Missing'` if none) |
| `social_media` | `json_string` | JSON object: `{"linkedin": "", "facebook": "", "instagram": ""}` |
| `signal` | `string` | Operational characteristics matched for the target segment |
| `signal_score` | `float` | Calculated via scoring formula (`0.0` - `1.0`) |
| `height` | `string` | `'Tier 1 (High)'` (≥ 0.70), `'Tier 2 (Mid)'` (≥ 0.35), `'Tier 3 (Low)'` (< 0.35) |
| `missing_contact_flag` | `boolean` | `True` if BOTH email and phone are `'Missing'`; otherwise `False` |

---

## Dynamic Scoring Algorithm & Configuration

The scoring engine supports fully customizable dynamic criteria per harvest job:

- **Configurable Criteria:**
  - `tech_stack_keywords`: List of target software/PMS keywords (e.g. AppFolio, Buildium, Yardi, CoStar, Juniper Square).
  - `portal_keywords`: List of portal detection keywords (e.g. owner portal, resident portal, investor portal).
  - `accounting_keywords`: List of back-office/financial keywords (e.g. trust accounting, waterfall distributions).
  - `volume_threshold` & `volume_unit`: Cutoff threshold (default 50) and label (e.g. doors, units, listings).
  - `weight_tech_stack`: Weight for software detection (default 0.40).
  - `weight_portal`: Weight for portal presence (default 0.25).
  - `weight_volume`: Weight for meeting volume threshold (default 0.20).
  - `weight_accounting`: Weight for accounting keywords (default 0.15).
  - `tier1_threshold`: Cutoff for Tier 1 (High) (default 0.70).
  - `tier2_threshold`: Cutoff for Tier 2 (Mid) (default 0.35).

- **Reachability Gate:**
  - If `missing_contact_flag == True`: **× 0.0**
  - If only one of `(email, phone)` is present: **× 0.7**
  - If both are present: **× 1.0**

- **Tier Assignment:**
  - `score >= tier1_threshold`: **Tier 1 (High)**
  - `score >= tier2_threshold`: **Tier 2 (Mid)**
  - `score < tier2_threshold`: **Tier 3 (Low)**

---

## Quickstart & Commands

### 1. Install Dependencies
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2. Run the Application
```bash
python app.py
```
Or with uvicorn:
```bash
uvicorn app:app --host 0.0.0.0 --port 8000
```

### 3. Open in Browser
Open [http://localhost:8000](http://localhost:8000) in your web browser.

### AI Email Assistant (DeepSeek)
Copy `.env.example` to `.env`, set `DEEPSEEK_API_KEY` (and optionally `SENDER_COMPANY` / `SENDER_PITCH`), then restart the server. Clicking **Stage Draft** on a lead calls `POST /api/draft`, which returns a personalized subject/body, a lead summary, and a target decision-maker role signed by Antonio Madera. If the key is missing or DeepSeek fails, the built-in template draft is shown as a safe fallback. Check configuration at `GET /api/ai/status`.

### Real Business Discovery Providers
Leads are discovered strictly through real directory and map APIs (no synthetic/fabricated businesses):
- **Google Places API (New)** (`GOOGLE_PLACES_API_KEY`): Recommended for comprehensive high-accuracy business directories with verified phone numbers, websites, and addresses.
- **Foursquare Places API** (`FOURSQUARE_API_KEY`): Commercial venue and business directory.
- **OpenStreetMap** (Nominatim + Overpass): Free open-source fallback (no API key required).
Configure your preferred provider order in `.env` using `DISCOVERY_PROVIDERS=google,foursquare,osm`. Check active discovery provider status at `GET /api/discovery/status`.

### 4. Run Integration Tests
```bash
python test_no_fabrication.py     # Deterministic verification that no leads or metadata are fabricated
python test_app.py                # Full end-to-end API, SSE stream, and CSV export verification
python test_dynamic_scoring.py    # Multi-parameter scoring, knock-out rules, and tier threshold validation
```

---

## Project Structure

```
├── app.py                   # Clean FastAPI backend server (API endpoints & SSE streaming)
├── discovery.py             # Pluggable real business discovery (Google Places, Foursquare, OSM)
├── ai_assist.py             # DeepSeek AI cold outreach generator & decision-maker role suggester
├── pipeline.py              # Compliance-first crawling, robots.txt check, dynamic enrichment & scoring
├── templates/
│   └── index.html           # Frontend interface with dynamic scoring inspector & AI email drafting modal
├── test_no_fabrication.py   # Regression test ensuring zero synthetic or hallucinated leads
├── test_app.py              # Full end-to-end integration test suite
├── test_dynamic_scoring.py  # Automated test verifying dynamic scoring weights and tier cutoffs
├── requirements.txt         # Python dependencies
└── .env.example             # Template for API keys and sender configuration
```
