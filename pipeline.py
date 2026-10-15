"""
Lead Harvester and Enrichment Engine
Compliant with robots.txt, polite crawling delays, and dynamic multi-parameter scoring algorithm.
"""

import asyncio
import json
import logging
import re
import socket
import time
import urllib.parse
import urllib.robotparser
from dataclasses import asdict, dataclass
from enum import Enum
from typing import AsyncGenerator, Callable, Dict, List, Optional, Set, Tuple

import requests
from bs4 import BeautifulSoup
from pydantic import BaseModel, Field

from discovery import discover_businesses

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("lead_harvester")

# Static asset extensions to filter out fake emails
STATIC_EXT_RE = re.compile(
    r"\.(png|jpg|jpeg|gif|webp|svg|css|js|woff|woff2|ttf|eot|ico|pdf|zip|mp4|webm)($|\?)",
    re.IGNORECASE,
)
INVALID_EMAIL_DOMAINS = {
    "sentry.io",
    "wixpress.com",
    "example.com",
    "domain.com",
    "email.com",
    "yoursite.com",
    "placeholder.com",
}

PHONE_REGEX = re.compile(
    r"(?:(?:\+?1\s*(?:[.-]\s*)?)?(?:\(\s*([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9])\s*\)|([2-9]1[02-9]|[2-9][02-8]1|[2-9][02-8][02-9]))\s*(?:[.-]\s*)?)?([2-9]1[02-9]|[2-9][02-9]1|[2-9][02-9]{2})\s*(?:[.-]\s*)?([0-9]{4})(?:\s*(?:#|x\.?|ext\.?|extension)\s*(\d+))?",
    re.IGNORECASE,
)
CLEAN_PHONE_REGEX = re.compile(
    r"(\+?1[-.\s]?)?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})"
)
EMAIL_REGEX = re.compile(
    r"[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+", re.IGNORECASE
)

STREET_SUFFIXES = (
    r"(?:Street|St|Avenue|Ave|Boulevard|Blvd|Road|Rd|Drive|Dr|Lane|Ln|Way|Court|Ct|"
    r"Plaza|Plz|Parkway|Pkwy|Highway|Hwy|Suite|Ste|Floor|Fl|Building|Bldg|#\s*\d+)"
)
ADDRESS_REGEX = re.compile(
    rf"\b\d{{1,5}}\s+[A-Za-z0-9\.\s]{{2,35}}\s+{STREET_SUFFIXES}\b",
    re.IGNORECASE,
)

VOLUME_REGEX = re.compile(
    r"(\d{1,6})\+?\s*(?:doors|units|properties|rentals|apartments|homes|listings|residents|beds|attorneys|lawyers|clients|employees|trucks|fleet|cases|patients|members|accounts|users)",
    re.IGNORECASE,
)


class CriterionType(str, Enum):
    KEYWORDS = "keywords"
    NUMERIC_THRESHOLD = "numeric_threshold"
    REGEX = "regex"


class DynamicCriterion(BaseModel):
    id: str = Field(..., description="Unique criterion key")
    name: str = Field(..., description="Human-readable parameter label")
    criterion_type: CriterionType = Field(default=CriterionType.KEYWORDS)
    values: List[str] = Field(default_factory=list, description="Keywords to detect")
    threshold: Optional[float] = Field(default=None, description="Numeric minimum threshold")
    operator: Optional[str] = Field(default="gte", description="Operator for threshold: gte, gt, eq, lte, lt")
    unit_label: Optional[str] = Field(default=None, description="Label for threshold (e.g. units, attorneys)")
    pattern: Optional[str] = Field(default=None, description="Custom regex pattern")
    weight: float = Field(default=0.25, ge=0.0, le=1.0, description="Normalized score weight")
    is_mandatory: bool = Field(default=False, description="Strict knock-out rule: if False, lead is Disqualified")


class ScoringCriteriaConfig(BaseModel):
    parameters: List[DynamicCriterion] = Field(default_factory=list)
    tier1_threshold: float = Field(default=0.70, ge=0.0, le=1.0)
    tier2_threshold: float = Field(default=0.35, ge=0.0, le=1.0)


SEGMENT_PROFILES = {
    "property_management": {
        "id": "property_management",
        "name": "Property Management (Residential & Multifamily)",
        "industry_category": "Property Management",
        "query_term": "property management",
        "tier1_threshold": 0.70,
        "tier2_threshold": 0.35,
        "parameters": [
            {
                "id": "tech_stack",
                "name": "Property Management Software (PMS)",
                "criterion_type": "keywords",
                "values": [
                    "AppFolio", "Buildium", "Yardi", "Propertyware", "RealPage", "Rent Manager", "Entrata"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.40,
                "is_mandatory": True,
            },
            {
                "id": "portal",
                "name": "Owner & Tenant Portal",
                "criterion_type": "keywords",
                "values": [
                    "owner portal", "resident portal", "tenant portal", "portal login", "pay rent online", "access portal"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.25,
                "is_mandatory": False,
            },
            {
                "id": "volume",
                "name": "Portfolio Units / Doors",
                "criterion_type": "numeric_threshold",
                "values": [],
                "threshold": 50,
                "operator": "gte",
                "unit_label": "doors / residential units",
                "pattern": None,
                "weight": 0.20,
                "is_mandatory": True,
            },
            {
                "id": "accounting",
                "name": "Trust Accounting & Distribution",
                "criterion_type": "keywords",
                "values": [
                    "trust accounting", "reconciliation", "distributions", "owner statements", "financial reporting"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.15,
                "is_mandatory": False,
            },
        ],
        "characteristic_tags": [
            "AppFolio / Buildium / Yardi (+0.40)",
            "Owner & Resident Portals (+0.25)",
            "50+ Doors/Units Volume (+0.20)",
            "Trust Accounting & Distributions (+0.15)",
        ],
    },
    "commercial_brokerage": {
        "id": "commercial_brokerage",
        "name": "Commercial Real Estate & Brokerages",
        "industry_category": "Commercial Real Estate Brokerage",
        "query_term": "commercial real estate brokerage",
        "tier1_threshold": 0.70,
        "tier2_threshold": 0.35,
        "parameters": [
            {
                "id": "tech_stack",
                "name": "Commercial Brokerage Software",
                "criterion_type": "keywords",
                "values": [
                    "CoStar", "LoopNet", "Crexi", "Buildout", "Apto", "ClientLook"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.40,
                "is_mandatory": True,
            },
            {
                "id": "portal",
                "name": "Client Deal Room & Portals",
                "criterion_type": "keywords",
                "values": [
                    "client portal", "deal room", "virtual deal room", "investor login", "property portal"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.25,
                "is_mandatory": False,
            },
            {
                "id": "volume",
                "name": "Commercial Listings Volume",
                "criterion_type": "numeric_threshold",
                "values": [],
                "threshold": 50,
                "operator": "gte",
                "unit_label": "commercial listings",
                "pattern": None,
                "weight": 0.20,
                "is_mandatory": False,
            },
            {
                "id": "accounting",
                "name": "Commercial Lease & Commission Accounting",
                "criterion_type": "keywords",
                "values": [
                    "commercial lease", "commission accounting", "escrow", "settlement statements", "valuation analysis"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.15,
                "is_mandatory": False,
            },
        ],
        "characteristic_tags": [
            "CoStar / LoopNet / Crexi (+0.40)",
            "Client Deal Room & Portals (+0.25)",
            "50+ Commercial Listings (+0.20)",
            "Commercial Lease Accounting (+0.15)",
        ],
    },
    "hoa_management": {
        "id": "hoa_management",
        "name": "HOA & Community Association Management",
        "industry_category": "HOA & Community Management",
        "query_term": "hoa community management",
        "tier1_threshold": 0.70,
        "tier2_threshold": 0.35,
        "parameters": [
            {
                "id": "tech_stack",
                "name": "HOA Management Platforms",
                "criterion_type": "keywords",
                "values": [
                    "Vantaca", "Cinc Systems", "Frontsteps", "Enumerate", "SmartWebs", "AppFolio"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.40,
                "is_mandatory": True,
            },
            {
                "id": "portal",
                "name": "Board & Homeowner Portals",
                "criterion_type": "keywords",
                "values": [
                    "homeowner portal", "board portal", "resident portal", "community portal", "pay dues online"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.25,
                "is_mandatory": False,
            },
            {
                "id": "volume",
                "name": "Managed Communities Volume",
                "criterion_type": "numeric_threshold",
                "values": [],
                "threshold": 50,
                "operator": "gte",
                "unit_label": "doors / communities",
                "pattern": None,
                "weight": 0.20,
                "is_mandatory": False,
            },
            {
                "id": "accounting",
                "name": "Reserve Studies & Dues Accounting",
                "criterion_type": "keywords",
                "values": [
                    "reserve study", "assessment collection", "trust accounting", "fund accounting", "annual budget"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.15,
                "is_mandatory": False,
            },
        ],
        "characteristic_tags": [
            "Vantaca / Cinc / Frontsteps (+0.40)",
            "Board & Homeowner Portals (+0.25)",
            "50+ Managed Communities (+0.20)",
            "Reserve Studies & Dues Accounting (+0.15)",
        ],
    },
    "re_investment": {
        "id": "re_investment",
        "name": "Real Estate Investment & Asset Management",
        "industry_category": "Real Estate Investment & Asset Management",
        "query_term": "real estate asset management investment",
        "tier1_threshold": 0.70,
        "tier2_threshold": 0.35,
        "parameters": [
            {
                "id": "tech_stack",
                "name": "Real Estate Investment Platforms",
                "criterion_type": "keywords",
                "values": [
                    "Juniper Square", "AppFolio IM", "Yardi Matrix", "RealPage", "Dealpath"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.40,
                "is_mandatory": True,
            },
            {
                "id": "portal",
                "name": "Investor & LP Portals",
                "criterion_type": "keywords",
                "values": [
                    "investor portal", "lp portal", "investor login", "capital call portal"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.25,
                "is_mandatory": False,
            },
            {
                "id": "volume",
                "name": "Portfolio Assets Volume",
                "criterion_type": "numeric_threshold",
                "values": [],
                "threshold": 50,
                "operator": "gte",
                "unit_label": "portfolio assets",
                "pattern": None,
                "weight": 0.20,
                "is_mandatory": False,
            },
            {
                "id": "accounting",
                "name": "Waterfall Distributions & K-1 Accounting",
                "criterion_type": "keywords",
                "values": [
                    "waterfall distributions", "fund accounting", "k-1 reporting", "nav reporting", "financial audits"
                ],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.15,
                "is_mandatory": False,
            },
        ],
        "characteristic_tags": [
            "Juniper Square / Yardi Matrix (+0.40)",
            "Investor & LP Portals (+0.25)",
            "50+ Portfolio Assets (+0.20)",
            "Waterfall Distributions & K-1 (+0.15)",
        ],
    },
    "custom": {
        "id": "custom",
        "name": "Custom Target Segment (Editable)",
        "industry_category": "Custom B2B Segment",
        "query_term": "business services",
        "tier1_threshold": 0.70,
        "tier2_threshold": 0.35,
        "parameters": [
            {
                "id": "domain_software",
                "name": "Domain Industry Software",
                "criterion_type": "keywords",
                "values": ["HubSpot", "Salesforce", "ServiceTitan", "Procore"],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.40,
                "is_mandatory": False,
            },
            {
                "id": "portal",
                "name": "Dedicated Client Portal",
                "criterion_type": "keywords",
                "values": ["client portal", "login", "access portal"],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.25,
                "is_mandatory": False,
            },
            {
                "id": "volume",
                "name": "Operational Volume Metric",
                "criterion_type": "numeric_threshold",
                "values": [],
                "threshold": 50,
                "operator": "gte",
                "unit_label": "units / clients",
                "pattern": None,
                "weight": 0.20,
                "is_mandatory": False,
            },
            {
                "id": "accounting",
                "name": "Financial & Billing Operations",
                "criterion_type": "keywords",
                "values": ["invoicing", "billing", "financials"],
                "threshold": None,
                "operator": None,
                "unit_label": None,
                "pattern": None,
                "weight": 0.15,
                "is_mandatory": False,
            },
        ],
        "characteristic_tags": [
            "Domain Industry Software (+0.40)",
            "Dedicated Client Portal (+0.25)",
            "50+ Volume Metric (+0.20)",
            "Financial & Billing Keywords (+0.15)",
        ],
    },
}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class LeadRecord:
    name: str
    website: str
    street_address: str
    city: str
    country: str
    is_location_verified: bool
    type: str
    industry_category: str
    volume_metric: int
    phone: str
    email: str
    social_media: str
    matched_signals: str
    opportunity_score: float
    tier: str
    is_unreachable: bool

    def to_dict(self) -> dict:
        return asdict(self)


class LeadHarvester:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        self.robot_cache: Dict[str, Tuple[urllib.robotparser.RobotFileParser, bool]] = {}
        self.visited_domains: Set[str] = set()

    def _normalize_domain(self, url: str) -> str:
        """Extract domain hostname without www."""
        try:
            parsed = urllib.parse.urlparse(url)
            netloc = parsed.netloc.lower()
            if ":" in netloc:
                netloc = netloc.split(":")[0]
            if netloc.startswith("www."):
                netloc = netloc[4:]
            return netloc
        except Exception:
            return ""

    def check_robots_txt(self, url: str) -> Tuple[bool, str]:
        """
        Rule 2: Strictly evaluate robots.txt using urllib.robotparser before fetching any domain.
        Skip and log if crawling is disallowed.
        """
        try:
            parsed = urllib.parse.urlparse(url)
            scheme = parsed.scheme or "https"
            domain = parsed.netloc or parsed.path.split("/")[0]
            base_url = f"{scheme}://{domain}"
            robots_url = f"{base_url}/robots.txt"

            if domain in self.robot_cache:
                rp, can_fetch = self.robot_cache[domain]
                # Re-check path
                allowed = rp.can_fetch(DEFAULT_HEADERS["User-Agent"], url)
                return allowed, f"Cached robots.txt for {domain}: {'ALLOWED' if allowed else 'DISALLOWED'}"

            # Instantiate RobotFileParser for uncached domain
            rp = urllib.robotparser.RobotFileParser()
            rp.set_url(robots_url)

            # Fast DNS host validation
            try:
                socket.gethostbyname(domain)
            except Exception:
                rp.allow_all = True
                self.robot_cache[domain] = (rp, True)
                return True, f"Robots.txt check for {domain} -> ALLOWED (default)"

            try:
                # Polite timeout for robots.txt fetch
                resp = self.session.get(robots_url, timeout=(1.0, 1.5))
                if resp.status_code == 200:
                    rp.parse(resp.text.splitlines())
                elif resp.status_code in (401, 403):
                    # Standard crawler behavior: 401/403 robots.txt disallows all access
                    self.robot_cache[domain] = (rp, False)
                    return False, f"Robots.txt restricted (HTTP {resp.status_code}) for {domain} -> Crawling Disallowed"
                else:
                    # 404 or other: robots.txt not present means allowed
                    rp.allow_all = True
            except Exception as e:
                # If network timeout or unreachable, allow polite access with caution
                rp.allow_all = True

            can_fetch = rp.can_fetch(DEFAULT_HEADERS["User-Agent"], url)
            self.robot_cache[domain] = (rp, can_fetch)
            msg = f"Robots.txt check for {domain} -> {'ALLOWED' if can_fetch else 'DISALLOWED'}"
            return can_fetch, msg
        except Exception as e:
            return True, f"Robots.txt check skipped due to error: {e}"

    def _clean_phone(self, raw_phone: Optional[str]) -> str:
        if not raw_phone:
            return "Missing"
        # Search for valid 10-digit standard phone
        match = CLEAN_PHONE_REGEX.search(raw_phone)
        if match:
            area, prefix, line = match.group(2), match.group(3), match.group(4)
            return f"({area}) {prefix}-{line}"
        digits = re.sub(r"\D", "", raw_phone)
        if len(digits) == 10:
            return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
        elif len(digits) == 11 and digits.startswith("1"):
            return f"({digits[1:4]}) {digits[4:7]}-{digits[7:]}"
        return "Missing"

    def _clean_email(self, raw_email: Optional[str]) -> str:
        if not raw_email:
            return "Missing"
        email = raw_email.strip().lower()
        if STATIC_EXT_RE.search(email):
            return "Missing"
        parts = email.split("@")
        if len(parts) != 2:
            return "Missing"
        domain = parts[1]
        if domain in INVALID_EMAIL_DOMAINS:
            return "Missing"
        if len(email) < 6 or len(email) > 80:
            return "Missing"
        return email

    def _extract_socials(self, soup: BeautifulSoup, base_url: str) -> dict:
        socials = {"linkedin": "", "facebook": "", "instagram": ""}
        for a in soup.find_all("a", href=True):
            href = a["href"].strip()
            href_lower = href.lower()
            if "linkedin.com/company" in href_lower or "linkedin.com/in" in href_lower:
                if not socials["linkedin"]:
                    socials["linkedin"] = href
            elif "facebook.com/" in href_lower and not any(x in href_lower for x in ["sharer", "share.php"]):
                if not socials["facebook"]:
                    socials["facebook"] = href
            elif "instagram.com/" in href_lower and "instagram.com/p/" not in href_lower:
                if not socials["instagram"]:
                    socials["instagram"] = href
        return socials

    def _evaluate_lead_criteria(
        self,
        company_name: str,
        page_content: str,
        volume_metric: int,
        phone: str,
        email: str,
        is_unreachable: bool,
        parameters: List[DynamicCriterion],
        tier1_threshold: float = 0.70,
        tier2_threshold: float = 0.35,
    ) -> Tuple[float, str, str, List[str], Optional[str]]:
        """
        Evaluates dynamic criteria across keywords, numeric thresholds, and regex patterns.
        Enforces strict knock-out rules: if any mandatory criterion fails, the lead is
        immediately disqualified (opportunity_score = 0.0, tier = 'Disqualified').
        """
        page_lower = page_content.lower()
        matched_weights: List[float] = []
        matched_descriptions: List[str] = []
        knockout_param: Optional[DynamicCriterion] = None

        for param in parameters:
            c_type = param.criterion_type.value if hasattr(param.criterion_type, "value") else str(param.criterion_type)
            is_matched = False
            match_desc = ""

            if c_type == "keywords" or c_type == CriterionType.KEYWORDS.value:
                found_kws = [kw for kw in param.values if kw and kw.lower() in page_lower]
                if found_kws:
                    is_matched = True
                    match_desc = f"{param.name} ({', '.join(found_kws[:2])}) (+{param.weight:.2f})"
            elif c_type == "numeric_threshold" or c_type == CriterionType.NUMERIC_THRESHOLD.value:
                thresh = param.threshold if param.threshold is not None else 0.0
                op = (param.operator or "gte").lower().strip()
                if op in ("gte", ">=", "≥"):
                    is_matched = volume_metric >= thresh
                    op_symbol = "≥"
                elif op in ("gt", ">"):
                    is_matched = volume_metric > thresh
                    op_symbol = ">"
                elif op in ("eq", "==", "="):
                    is_matched = volume_metric == thresh
                    op_symbol = "="
                elif op in ("lte", "<=", "≤"):
                    is_matched = volume_metric <= thresh
                    op_symbol = "≤"
                elif op in ("lt", "<"):
                    is_matched = volume_metric < thresh
                    op_symbol = "<"
                else:
                    is_matched = volume_metric >= thresh
                    op_symbol = "≥"

                if is_matched:
                    unit_label = f" {param.unit_label}" if param.unit_label else ""
                    val_display = int(thresh) if isinstance(thresh, (int, float)) and thresh.is_integer() else thresh
                    match_desc = f"{param.name} {op_symbol}{val_display}{unit_label} (+{param.weight:.2f})"
            elif c_type == "regex" or c_type == CriterionType.REGEX.value:
                if param.pattern:
                    try:
                        if re.search(param.pattern, page_content, re.IGNORECASE):
                            is_matched = True
                            match_desc = f"{param.name} (+{param.weight:.2f})"
                    except Exception as e:
                        logger.warning(f"Invalid regex pattern '{param.pattern}': {e}")

            if is_matched:
                matched_weights.append(param.weight)
                matched_descriptions.append(match_desc)
            elif param.is_mandatory:
                knockout_param = param
                break

        if knockout_param:
            opportunity_score = 0.0
            tier = "Disqualified"
            ko_signal = f"[KNOCK-OUT: Failed mandatory {knockout_param.name}]"
            matched_signals_str = f"{'; '.join(matched_descriptions)}; {ko_signal}" if matched_descriptions else ko_signal
            ko_log = f"[KNOCKOUT] {company_name} failed mandatory criterion '{knockout_param.name}' -> Disqualified"
            return opportunity_score, tier, matched_signals_str, [], ko_log

        # Base score capped at 1.0
        base_score = min(1.0, sum(matched_weights))

        # Reachability Gate
        if is_unreachable:
            reachability_multiplier = 0.0
        else:
            has_phone = bool(phone and phone != "Missing")
            has_email = bool(email and email != "Missing")
            if has_phone and has_email:
                reachability_multiplier = 1.0
            else:
                reachability_multiplier = 0.7

        opportunity_score = round(base_score * reachability_multiplier, 2)

        if opportunity_score >= tier1_threshold:
            tier = "Tier 1 (High)"
        elif opportunity_score >= tier2_threshold:
            tier = "Tier 2 (Mid)"
        else:
            tier = "Tier 3 (Low)"

        matched_signals_str = "; ".join(matched_descriptions) if matched_descriptions else "Standard Web Presence"
        match_logs = [f"[MATCH] {company_name} matched '{desc}'" for desc in matched_descriptions]

        return opportunity_score, tier, matched_signals_str, match_logs, None

    def _scrape_website_signals(
        self,
        url: str,
    ) -> Tuple[Optional[str], Optional[str], Optional[str], int, dict, str]:
        """
        Fetches website and extracts:
        - phone, email, street_address, volume_metric proxy, socials, and full page content
        """
        try:
            parsed = urllib.parse.urlparse(url)
            host = parsed.netloc.split(":")[0] if parsed.netloc else ""
            if host:
                try:
                    socket.gethostbyname(host)
                except Exception:
                    return None, None, None, 0, {"linkedin": "", "facebook": "", "instagram": ""}, ""

            resp = self.session.get(url, timeout=(1.0, 1.5))
            if resp.status_code != 200:
                return None, None, None, 0, {"linkedin": "", "facebook": "", "instagram": ""}, ""

            html = resp.text
            soup = BeautifulSoup(html, "html.parser")
            text = soup.get_text(" ", strip=True)

            # Phone
            extracted_phone = None
            tel_links = soup.find_all("a", href=re.compile(r"^tel:", re.IGNORECASE))
            if tel_links:
                raw = tel_links[0]["href"].replace("tel:", "").strip()
                extracted_phone = self._clean_phone(raw)
            if not extracted_phone or extracted_phone == "Missing":
                phone_match = CLEAN_PHONE_REGEX.search(text)
                if phone_match:
                    extracted_phone = self._clean_phone(phone_match.group(0))

            # Email
            extracted_email = None
            mailto_links = soup.find_all("a", href=re.compile(r"^mailto:", re.IGNORECASE))
            if mailto_links:
                raw_mail = mailto_links[0]["href"].replace("mailto:", "").split("?")[0].strip()
                extracted_email = self._clean_email(raw_mail)
            if not extracted_email or extracted_email == "Missing":
                email_matches = EMAIL_REGEX.findall(text)
                for candidate in email_matches:
                    clean = self._clean_email(candidate)
                    if clean != "Missing":
                        extracted_email = clean
                        break

            # Street Address
            extracted_address = None
            address_tag = soup.find(["address", "p", "div"], class_=re.compile(r"address|location|contact", re.I))
            if address_tag:
                tag_text = address_tag.get_text(" ", strip=True)
                addr_match = ADDRESS_REGEX.search(tag_text)
                if addr_match:
                    extracted_address = addr_match.group(0).strip()
            if not extracted_address:
                addr_match = ADDRESS_REGEX.search(text)
                if addr_match:
                    extracted_address = addr_match.group(0).strip()

            # Volume metric (0 = not stated on the site)
            volume_metric = 0
            vol_match = VOLUME_REGEX.search(text)
            if vol_match:
                try:
                    parsed_amount = int(vol_match.group(1))
                    if 10 <= parsed_amount <= 10000:
                        volume_metric = parsed_amount
                except Exception:
                    pass

            # Socials
            socials = self._extract_socials(soup, url)

            # Page content for dynamic criteria matching
            page_content = f"{text}\n{html}"

            return (
                extracted_phone,
                extracted_email,
                extracted_address,
                volume_metric,
                socials,
                page_content,
            )
        except Exception:
            return None, None, None, 0, {"linkedin": "", "facebook": "", "instagram": ""}, ""

    async def harvest(
        self,
        city: str,
        search_objective: str = "business services",
        limit: int = 30,
        target_segment: Optional[str] = None,
        scoring_criteria: Optional[dict] = None,
        status_getter: Optional[Callable[[], str]] = None,
        objective: Optional[str] = None,
        segment_id: Optional[str] = None,
        scoring_config: Optional[dict] = None,
    ) -> AsyncGenerator[dict, None]:
        """
        Asynchronous ingestion pipeline:
        1. Emits live telemetry logs including [CRITERIA], [MATCH], and [KNOCKOUT]
        2. Resolves target segment profile and dynamic characteristic criteria
        3. Checks pause / stop state dynamically
        4. Strictly checks robots.txt
        5. Enforces polite crawling delays (0.5s)
        6. Deduplicates domains
        7. Evaluates arbitrary criteria and strict knock-out gates
        8. Yields events: log, lead, status, complete
        """
        target_seg = target_segment or segment_id or "custom"
        search_obj = search_objective or objective or "business services"
        raw_scoring = scoring_criteria or scoring_config or {}

        # Resolve target segment profile
        profile = None
        if target_seg in SEGMENT_PROFILES and target_seg != "custom":
            profile = SEGMENT_PROFILES[target_seg]
        else:
            # "custom" (the default) first tries to map the objective onto a known segment
            obj_lower = search_obj.lower()
            for key, prof in SEGMENT_PROFILES.items():
                if key == "custom":
                    continue
                if key in obj_lower or prof["query_term"] in obj_lower or prof.get("industry_category", "").lower() in obj_lower:
                    profile = prof
                    break
        if not profile:
            profile = SEGMENT_PROFILES["custom"] if target_seg == "custom" else SEGMENT_PROFILES["property_management"]

        # Dynamic scoring parameters resolution
        parameters: List[DynamicCriterion] = []
        tier1_threshold = float(raw_scoring.get("tier1_threshold", profile.get("tier1_threshold", 0.70)))
        tier2_threshold = float(raw_scoring.get("tier2_threshold", profile.get("tier2_threshold", 0.35)))

        if "parameters" in raw_scoring and isinstance(raw_scoring["parameters"], list) and raw_scoring["parameters"]:
            for p in raw_scoring["parameters"]:
                if isinstance(p, DynamicCriterion):
                    parameters.append(p)
                elif isinstance(p, dict):
                    parameters.append(DynamicCriterion(**p))
        elif "tech_stack_keywords" in raw_scoring or "portal_keywords" in raw_scoring or "volume_threshold" in raw_scoring:
            # Handle legacy scoring_config dictionary format
            if "tech_stack_keywords" in raw_scoring:
                parameters.append(DynamicCriterion(
                    id="tech_stack",
                    name="Technology Stack",
                    criterion_type=CriterionType.KEYWORDS,
                    values=raw_scoring["tech_stack_keywords"],
                    weight=float(raw_scoring.get("weight_tech_stack", 0.40)),
                ))
            if "portal_keywords" in raw_scoring:
                parameters.append(DynamicCriterion(
                    id="portal",
                    name="Client Portal",
                    criterion_type=CriterionType.KEYWORDS,
                    values=raw_scoring["portal_keywords"],
                    weight=float(raw_scoring.get("weight_portal", 0.25)),
                ))
            if "volume_threshold" in raw_scoring:
                parameters.append(DynamicCriterion(
                    id="volume",
                    name="Volume Metric",
                    criterion_type=CriterionType.NUMERIC_THRESHOLD,
                    threshold=float(raw_scoring.get("volume_threshold", 50)),
                    unit_label=raw_scoring.get("volume_unit", "units"),
                    weight=float(raw_scoring.get("weight_volume", 0.20)),
                ))
            if "accounting_keywords" in raw_scoring:
                parameters.append(DynamicCriterion(
                    id="accounting",
                    name="Financial / Accounting",
                    criterion_type=CriterionType.KEYWORDS,
                    values=raw_scoring["accounting_keywords"],
                    weight=float(raw_scoring.get("weight_accounting", 0.15)),
                ))
        else:
            # Default to profile parameters
            prof_params = profile.get("parameters", [])
            for p in prof_params:
                if isinstance(p, DynamicCriterion):
                    parameters.append(p)
                elif isinstance(p, dict):
                    parameters.append(DynamicCriterion(**p))

        industry_category_label = search_obj if profile["id"] == "custom" and search_obj else profile.get("industry_category", "Business Services")
        search_term = search_obj if profile["id"] == "custom" and search_obj else profile.get("query_term", search_obj)

        param_summaries = []
        for p in parameters:
            strict_flag = " [MANDATORY KNOCK-OUT]" if p.is_mandatory else ""
            if p.criterion_type in (CriterionType.NUMERIC_THRESHOLD, "numeric_threshold"):
                op = (p.operator or "gte").lower().strip()
                op_symbol = ">" if op in ("gt", ">") else ("=" if op in ("eq", "==", "=") else ("≤" if op in ("lte", "<=", "≤") else ("<" if op in ("lt", "<") else "≥")))
                thresh_val = p.threshold or 0
                val_display = int(thresh_val) if isinstance(thresh_val, (int, float)) and (isinstance(thresh_val, int) or thresh_val.is_integer()) else thresh_val
                param_summaries.append(f"{p.name} ({op_symbol}{val_display} {p.unit_label or ''}, wt: {p.weight:.2f}{strict_flag})")
            else:
                param_summaries.append(f"{p.name} (wt: {p.weight:.2f}{strict_flag})")

        criteria_str = " | ".join(param_summaries) if param_summaries else "Default Presence"

        yield {
            "type": "log",
            "message": f"[*] Initiating discovery for segment '{profile['name']}' in '{city}' (target: {limit} leads)",
        }
        yield {
            "type": "log",
            "message": (
                f"[CRITERIA] Active Evaluation Rules ({len(parameters)} parameters): {criteria_str} | "
                f"Tiers: [T1 ≥{tier1_threshold:.2f}, T2 ≥{tier2_threshold:.2f}]"
            ),
        }

        # Step 1: Directory search via real, terms-compliant providers (never fabricated)
        discovery_logs: List[str] = []
        directory_results = await asyncio.to_thread(
            discover_businesses, city, search_term, limit + 10, discovery_logs.append
        )
        for msg in discovery_logs:
            yield {"type": "log", "message": msg}
        yield {
            "type": "log",
            "message": f"[DIR] Located {len(directory_results)} real business listings",
        }

        if len(directory_results) < limit:
            yield {
                "type": "log",
                "message": (
                    f"[DIR] Only {len(directory_results)} of {limit} requested businesses found. "
                    "No records are fabricated; add a provider key or widen the search to get more."
                ),
            }

        leads_collected = 0
        total_examined = 0

        for raw_item in directory_results:
            if leads_collected >= limit:
                break

            # Handle Pause / Stop before processing lead
            if status_getter:
                if status_getter() == "stopped":
                    yield {
                        "type": "log",
                        "message": f"[STOPPED] Ingestion stopped by user. Finalized {leads_collected} leads.",
                    }
                    yield {
                        "type": "complete",
                        "total": leads_collected,
                        "status": "STOPPED",
                    }
                    return

                was_paused = False
                while status_getter() == "paused":
                    if not was_paused:
                        yield {
                            "type": "log",
                            "message": "[PAUSE] Ingestion paused by user. Standing by...",
                        }
                        yield {"type": "status", "status": "PAUSED"}
                        was_paused = True
                    await asyncio.sleep(0.3)
                    if status_getter() == "stopped":
                        yield {
                            "type": "log",
                            "message": f"[STOPPED] Ingestion stopped by user. Finalized {leads_collected} leads.",
                        }
                        yield {
                            "type": "complete",
                            "total": leads_collected,
                            "status": "STOPPED",
                        }
                        return

                if was_paused and status_getter() == "running":
                    yield {"type": "log", "message": "[RESUME] Resuming lead ingestion..."}
                    yield {"type": "status", "status": "CRAWLING"}

            total_examined += 1
            empty_socials = {"linkedin": "", "facebook": "", "instagram": ""}
            raw_url = (raw_item.get("website") or "").strip()
            if raw_url and not raw_url.startswith("http"):
                raw_url = f"https://{raw_url}"

            domain = self._normalize_domain(raw_url) if raw_url else ""
            # Dedup by domain, or by business name when the listing has no website
            dedup_key = domain or f"name:{raw_item['name'].strip().lower()}"
            if dedup_key in self.visited_domains:
                yield {
                    "type": "log",
                    "message": f"[DEDUP] '{domain or raw_item['name']}' already processed. Skipping duplicate.",
                }
                continue
            self.visited_domains.add(dedup_key)

            phone_ext = email_ext = addr_ext = None
            volume_metric_ext = 0
            socials_ext = dict(empty_socials)
            page_content = ""

            if not raw_url:
                yield {
                    "type": "log",
                    "message": f"[ENRICH] {raw_item['name']} has no website listed; using directory contact data only",
                }
            else:
                # Enforce polite crawling delay (0.5s)
                await asyncio.sleep(0.5)

                # Strictly evaluate robots.txt
                allowed, robots_msg = await asyncio.to_thread(self.check_robots_txt, raw_url)
                yield {
                    "type": "log",
                    "message": f"[{'OK' if allowed else 'DISALLOW'}] {robots_msg}",
                }

                if not allowed:
                    yield {
                        "type": "log",
                        "message": f"[COMPLIANCE] Skipping {domain} per strict robots.txt directive",
                    }
                    continue

                yield {
                    "type": "log",
                    "message": f"[ENRICH] Crawling {domain} for operational indicators & contact metadata...",
                }

                # Enrich from live site
                (
                    phone_ext,
                    email_ext,
                    addr_ext,
                    volume_metric_ext,
                    socials_ext,
                    page_content,
                ) = await asyncio.to_thread(self._scrape_website_signals, raw_url)

            # Resolve contact details (directory data is authoritative; site scrape fills gaps)
            dir_phone = self._clean_phone(raw_item.get("phone"))
            phone = dir_phone if dir_phone != "Missing" else (phone_ext or "Missing")

            dir_street = (raw_item.get("street_address") or "").strip()
            street_address = dir_street if dir_street and dir_street != "Unlisted" else (addr_ext or "Unlisted")
            is_location_verified = street_address != "Unlisted"

            if email_ext and email_ext != "Missing":
                email = email_ext
            else:
                email = self._clean_email(raw_item.get("email"))

            is_unreachable = (phone == "Missing" and email == "Missing")

            # Volume metric: only what the site actually states (0 = unknown)
            volume_metric = volume_metric_ext or 0

            # Socials: merge site + directory, never invent profile URLs
            dir_socials = raw_item.get("socials") or {}
            socials_ext = {
                k: (socials_ext or {}).get(k) or dir_socials.get(k) or ""
                for k in empty_socials
            }
            social_media_json = json.dumps(socials_ext)

            if not page_content:
                # No crawlable site: only the business name is evidence; criteria will not match by default.
                page_content = raw_item["name"]

            # Dynamic scoring & strict knock-out evaluation
            (
                opportunity_score,
                tier,
                matched_signals,
                match_logs,
                knockout_log,
            ) = self._evaluate_lead_criteria(
                company_name=raw_item["name"],
                page_content=page_content,
                volume_metric=volume_metric,
                phone=phone,
                email=email,
                is_unreachable=is_unreachable,
                parameters=parameters,
                tier1_threshold=tier1_threshold,
                tier2_threshold=tier2_threshold,
            )

            # Telemetry logging: [KNOCKOUT] or [MATCH]
            if knockout_log:
                yield {
                    "type": "log",
                    "message": knockout_log,
                }
            else:
                yield {
                    "type": "log",
                    "message": f"[MATCH] {raw_item['name']} | Score: {opportunity_score:.2f} ({tier}) | Signals: {matched_signals}",
                }

            record = LeadRecord(
                name=raw_item["name"],
                website=raw_url,
                street_address=street_address,
                city=city,
                country="USA",
                is_location_verified=is_location_verified,
                type="Company",
                industry_category=industry_category_label,
                volume_metric=volume_metric,
                phone=phone,
                email=email,
                social_media=social_media_json,
                matched_signals=matched_signals,
                opportunity_score=opportunity_score,
                tier=tier,
                is_unreachable=is_unreachable,
            )

            leads_collected += 1

            yield {
                "type": "lead",
                "data": record.to_dict(),
                "progress": {
                    "current": leads_collected,
                    "target": limit,
                    "percentage": int((leads_collected / limit) * 100),
                },
            }

        yield {
            "type": "log",
            "message": f"[COMPLETED] Ingestion complete. Successfully processed and enriched {leads_collected} leads.",
        }
        yield {
            "type": "complete",
            "total": leads_collected,
        }

