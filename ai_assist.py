"""
AI Email Assistant (DeepSeek)
Generates personalized outreach drafts, a short lead summary, and a suggested
decision-maker role for each lead using the DeepSeek chat API (OpenAI-compatible).

Configuration (environment variables):
    DEEPSEEK_API_KEY   (required)  Your DeepSeek API key
    DEEPSEEK_MODEL     (optional)  Default: deepseek-chat
    DEEPSEEK_BASE_URL  (optional)  Default: https://api.deepseek.com
    SENDER_NAME        (optional)  Default: Antonio Madera
    SENDER_COMPANY     (optional)  Company name used in the signature
    SENDER_PITCH       (optional)  One or two sentences describing what you sell
"""

import json
import os
from typing import Any, Dict, Optional

import requests

DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_MODEL = "deepseek-chat"
DEFAULT_SENDER_NAME = "Antonio Madera"
REQUEST_TIMEOUT_SECONDS = 45

# Fallback value propositions per segment, used when SENDER_PITCH is not set.
SEGMENT_PITCHES = {
    "property_management": "We help property management teams automate vendor invoice coding and owner statement reconciliation without switching PMS software.",
    "commercial_brokerage": "We help commercial brokerages automate investor deal room onboarding and pipeline data consolidation.",
    "hoa_management": "We help HOA and community association managers automate board report compilation and monthly assessment reconciliation.",
    "re_investment": "We help real estate asset managers automate quarterly LP distribution accounting and underwriting pipeline ingestion.",
}

ROLE_BASED_PREFIXES = {
    "info", "leasing", "office", "support", "admin", "contact", "sales", "help",
    "billing", "inquiries", "service", "team", "reception", "frontdesk",
    "management", "general",
}

SYSTEM_PROMPT = """You are a B2B sales development assistant who writes short, honest, highly personalized cold emails.

Rules:
- Use ONLY the facts provided about the lead. Never invent numbers, software, people, clients, or results.
- Never invent a person's name. If no contact first name is provided, open with "Hi there," or "Hi {Company} team,".
- 70-120 words in the body. Plain text, no markdown, no emojis, no spammy words (free, guarantee, act now, etc.).
- One clear, low-friction call to action (e.g. a short call or a reply).
- Sign off with the sender name (and company if provided) exactly as given.
- Write in the same language as the lead's city/country context (default English).

Return ONLY a JSON object with these keys:
{
  "subject": "short subject line, under 60 characters",
  "body": "full email body including greeting and signature",
  "summary": "1-2 sentence summary of why this lead is (or is not) a good fit, based only on the data",
  "contact_role": "the most likely decision-maker job title to target at this company (a role, not a name)",
  "personalization_hooks": ["up to 3 concrete facts from the data that the email uses"]
}"""


class AIConfigError(RuntimeError):
    """Raised when the AI assistant is not configured."""


def _load_dotenv(path: str = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")) -> None:
    """Loads KEY=VALUE lines from a local .env file without overriding real env vars."""
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as fh:
        for raw in fh:
            line = raw.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip().removeprefix("export ").strip()
            os.environ.setdefault(key, value.strip().strip('"').strip("'"))


_load_dotenv()


class AIRequestError(RuntimeError):
    """Raised when the DeepSeek API call fails or returns unusable output."""


def ai_status() -> Dict[str, Any]:
    """Reports whether the AI assistant is configured (never exposes the key)."""
    return {
        "enabled": bool(os.getenv("DEEPSEEK_API_KEY")),
        "provider": "deepseek",
        "model": os.getenv("DEEPSEEK_MODEL", DEFAULT_MODEL),
        "sender_name": os.getenv("SENDER_NAME", DEFAULT_SENDER_NAME),
        "sender_company": os.getenv("SENDER_COMPANY", ""),
    }


def _contact_first_name(lead: Dict[str, Any]) -> Optional[str]:
    """Returns a real contact first name only if one is actually present in the data."""
    verification = lead.get("verification") or {}
    candidate = verification.get("waterfall_candidate") or {}
    full_name = (candidate.get("full_name") or "").strip()
    if full_name:
        return full_name.split()[0]

    email = (lead.get("email") or "").strip().lower()
    if "@" in email:
        local = email.split("@")[0]
        first = local.split(".")[0]
        if first and first.isalpha() and first not in ROLE_BASED_PREFIXES and len(first) > 2:
            return first.capitalize()
    return None


def _build_lead_context(lead: Dict[str, Any], segment_id: Optional[str]) -> Dict[str, Any]:
    """Selects the lead fields worth sending to the model (keeps the prompt small)."""
    social = lead.get("social_media")
    if isinstance(social, str):
        try:
            social = json.loads(social)
        except (ValueError, TypeError):
            social = {}
    signals = [s.strip() for s in str(lead.get("matched_signals") or "").split(";") if s.strip()]

    return {
        "company": lead.get("name"),
        "website": lead.get("website"),
        "city": lead.get("city"),
        "country": lead.get("country"),
        "street_address": lead.get("street_address"),
        "industry_category": lead.get("industry_category"),
        "segment": segment_id,
        "volume_metric": lead.get("volume_metric"),
        "matched_signals": signals,
        "opportunity_score": lead.get("opportunity_score"),
        "tier": lead.get("tier"),
        "recipient_email": lead.get("email"),
        "contact_first_name": _contact_first_name(lead),
        "has_linkedin": bool((social or {}).get("linkedin")),
    }


def _extract_json(text: str) -> Dict[str, Any]:
    """Parses the model output, tolerating code fences around the JSON."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`")
        if cleaned.lower().startswith("json"):
            cleaned = cleaned[4:]
    start, end = cleaned.find("{"), cleaned.rfind("}")
    if start == -1 or end == -1:
        raise AIRequestError("Model did not return JSON")
    return json.loads(cleaned[start:end + 1])


def generate_email_draft(
    lead: Dict[str, Any],
    segment_id: Optional[str] = None,
    sender_name: Optional[str] = None,
    sender_company: Optional[str] = None,
    pitch: Optional[str] = None,
    tone: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Calls DeepSeek and returns {subject, body, summary, contact_role, personalization_hooks, model}.
    Blocking (uses requests) - call via asyncio.to_thread from async code.
    """
    api_key = os.getenv("DEEPSEEK_API_KEY")
    if not api_key:
        raise AIConfigError("DEEPSEEK_API_KEY is not set")

    base_url = os.getenv("DEEPSEEK_BASE_URL", DEFAULT_BASE_URL).rstrip("/")
    model = os.getenv("DEEPSEEK_MODEL", DEFAULT_MODEL)

    sender = {
        "name": sender_name or os.getenv("SENDER_NAME", DEFAULT_SENDER_NAME),
        "company": sender_company or os.getenv("SENDER_COMPANY", ""),
        "pitch": (
            pitch
            or os.getenv("SENDER_PITCH")
            or SEGMENT_PITCHES.get(segment_id or "", SEGMENT_PITCHES["property_management"])
        ),
        "tone": tone or "friendly, concise, consultative",
    }

    user_prompt = (
        "Write a cold outreach email for this lead.\n\n"
        f"SENDER:\n{json.dumps(sender, ensure_ascii=False, indent=2)}\n\n"
        f"LEAD DATA:\n{json.dumps(_build_lead_context(lead, segment_id), ensure_ascii=False, indent=2)}"
    )

    try:
        resp = requests.post(
            f"{base_url}/chat/completions",
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json={
                "model": model,
                "messages": [
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                "temperature": 0.7,
                "max_tokens": 800,
                "response_format": {"type": "json_object"},
            },
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
    except requests.RequestException as e:
        raise AIRequestError(f"Could not reach DeepSeek: {e}") from e

    if resp.status_code != 200:
        detail = resp.text[:300]
        raise AIRequestError(f"DeepSeek API error {resp.status_code}: {detail}")

    try:
        content = resp.json()["choices"][0]["message"]["content"]
        data = _extract_json(content)
    except (KeyError, IndexError, ValueError) as e:
        raise AIRequestError(f"Unexpected DeepSeek response: {e}") from e

    subject = str(data.get("subject") or "").strip()
    body = str(data.get("body") or "").strip()
    if not subject or not body:
        raise AIRequestError("DeepSeek returned an empty subject or body")

    hooks = data.get("personalization_hooks") or []
    if not isinstance(hooks, list):
        hooks = [str(hooks)]

    return {
        "subject": subject,
        "body": body,
        "summary": str(data.get("summary") or "").strip(),
        "contact_role": str(data.get("contact_role") or "").strip(),
        "personalization_hooks": [str(h) for h in hooks][:3],
        "model": model,
    }
