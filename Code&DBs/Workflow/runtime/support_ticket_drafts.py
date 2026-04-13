"""Deterministic support-ticket response drafting.

This module gives the operating-model runtime a local fallback for support
ticket workflows. It avoids an external model call when the card objective is
clearly asking for first-pass customer reply drafts.
"""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

_DRAFT_KEYWORDS = ("draft", "reply", "response", "respond", "template")
_TICKET_KEYWORDS = ("ticket", "case", "support", "customer")

_BODY_KEYS = ("body", "content", "message", "description", "details")
_SUBJECT_KEYS = ("subject", "title", "summary")
_ID_KEYS = ("ticket_id", "id", "case_id", "ticketId", "caseId")
_PRIORITY_KEYS = ("priority", "severity", "urgency")
_CATEGORY_KEYS = ("category", "classification", "issue_type", "type")
_TEAM_KEYS = ("assigned_team", "team", "queue", "owner_group")
_ETA_KEYS = ("estimated_wait", "eta", "next_update_at")


def looks_like_ticket_drafting_task(task: str) -> bool:
    """Return True when the card objective is clearly a ticket drafting task."""
    lowered = task.lower()
    return any(word in lowered for word in _DRAFT_KEYWORDS) and any(
        word in lowered for word in _TICKET_KEYWORDS
    )


def draft_ticket_responses(
    *,
    task: str,
    card: Mapping[str, Any],
    upstream_outputs: Mapping[str, Any],
) -> list[dict[str, str]]:
    """Generate deterministic reply drafts for every discovered ticket."""
    sources = [card, *upstream_outputs.values()]
    tickets = _extract_tickets(sources)
    if not tickets:
        return []

    global_context = _merge_global_context(sources)
    if len(tickets) == 1:
        tickets[0] = _merge_ticket_data(tickets[0], global_context)

    drafts: list[dict[str, str]] = []
    for index, ticket in enumerate(tickets, start=1):
        normalized = _normalize_ticket(ticket, fallback_index=index)
        drafts.append(
            {
                "ticket_id": normalized["ticket_id"],
                "subject": _build_response_subject(normalized["subject"], normalized["category"]),
                "body": _build_body(task=task, ticket=normalized),
                "tone": _tone_for_priority(normalized["priority"]),
            }
        )
    return drafts


def _extract_tickets(sources: Sequence[Any]) -> list[dict[str, Any]]:
    fragments: list[dict[str, Any]] = []
    for source in sources:
        _walk_for_ticket_fragments(source, fragments, seen=set())

    merged: dict[str, dict[str, Any]] = {}
    order: list[str] = []
    for index, fragment in enumerate(fragments, start=1):
        normalized = _normalize_ticket(fragment, fallback_index=index)
        key = normalized["ticket_id"] or f"ticket-{index}"
        if key not in merged:
            merged[key] = {}
            order.append(key)
        merged[key] = _merge_ticket_data(merged[key], normalized)
    return [merged[key] for key in order]


def _walk_for_ticket_fragments(value: Any, fragments: list[dict[str, Any]], seen: set[int]) -> None:
    marker = id(value)
    if marker in seen:
        return
    seen.add(marker)

    if isinstance(value, Mapping):
        if _looks_like_ticket_fragment(value):
            fragments.append(dict(value))
        for nested in value.values():
            _walk_for_ticket_fragments(nested, fragments, seen)
        return

    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for nested in value:
            _walk_for_ticket_fragments(nested, fragments, seen)


def _looks_like_ticket_fragment(value: Mapping[str, Any]) -> bool:
    keys = set(value.keys())
    has_content = any(key in keys for key in _BODY_KEYS + _SUBJECT_KEYS)
    has_id = any(key in keys for key in _ID_KEYS)
    has_context = any(key in keys for key in _PRIORITY_KEYS + _CATEGORY_KEYS + _TEAM_KEYS)
    return has_content and (has_id or has_context)


def _merge_global_context(sources: Sequence[Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    for source in sources:
        if not isinstance(source, Mapping):
            continue
        candidate = {
            "priority": _extract_priority(source),
            "category": _extract_category(source),
            "team": _extract_team(source),
            "eta": _extract_eta(source),
        }
        merged = _merge_ticket_data(merged, candidate)
    return merged


def _normalize_ticket(ticket: Mapping[str, Any], *, fallback_index: int) -> dict[str, str]:
    subject = _extract_subject(ticket)
    body = _extract_body(ticket)
    category = _extract_category(ticket)
    priority = _extract_priority(ticket)
    ticket_id = _extract_ticket_id(ticket) or f"ticket-{fallback_index}"

    if not subject:
        if category:
            subject = f"{_pretty_label(category)} support request"
        elif body:
            subject = _truncate(_first_sentence(body), limit=72)
        else:
            subject = "Support request"

    return {
        "ticket_id": ticket_id,
        "subject": subject,
        "body": body,
        "category": category,
        "priority": priority,
        "team": _extract_team(ticket),
        "eta": _extract_eta(ticket),
    }


def _merge_ticket_data(base: Mapping[str, Any], update: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in update.items():
        if value in (None, "", [], {}):
            continue
        current = merged.get(key)
        if current in (None, "", [], {}):
            merged[key] = value
    return merged


def _extract_ticket_id(ticket: Mapping[str, Any]) -> str:
    for key in _ID_KEYS:
        value = ticket.get(key)
        text = _coerce_text(value)
        if text:
            return text
    return ""


def _extract_subject(ticket: Mapping[str, Any]) -> str:
    for key in _SUBJECT_KEYS:
        text = _coerce_text(ticket.get(key))
        if text:
            return text
    return ""


def _extract_body(ticket: Mapping[str, Any]) -> str:
    for key in _BODY_KEYS:
        text = _coerce_text(ticket.get(key))
        if text:
            return text
    return ""


def _extract_priority(ticket: Mapping[str, Any]) -> str:
    for key in _PRIORITY_KEYS:
        normalized = _normalize_priority(ticket.get(key))
        if normalized:
            return normalized

    for nested_key in ("classification", "triage", "metadata"):
        nested = ticket.get(nested_key)
        if isinstance(nested, Mapping):
            normalized = _extract_priority(nested)
            if normalized:
                return normalized
    return "P2"


def _extract_category(ticket: Mapping[str, Any]) -> str:
    for key in _CATEGORY_KEYS:
        value = ticket.get(key)
        if isinstance(value, Mapping):
            nested = _extract_category(value)
            if nested:
                return nested
        text = _coerce_text(value)
        if text:
            return text

    for nested_key in ("classification", "triage", "metadata"):
        nested = ticket.get(nested_key)
        if isinstance(nested, Mapping):
            text = _extract_category(nested)
            if text:
                return text
    return ""


def _extract_team(ticket: Mapping[str, Any]) -> str:
    for key in _TEAM_KEYS:
        text = _coerce_text(ticket.get(key))
        if text:
            return text

    route = ticket.get("route")
    if isinstance(route, Mapping):
        return _extract_team(route)
    return ""


def _extract_eta(ticket: Mapping[str, Any]) -> str:
    for key in _ETA_KEYS:
        text = _coerce_text(ticket.get(key))
        if text:
            return text
    route = ticket.get("route")
    if isinstance(route, Mapping):
        return _extract_eta(route)
    return ""


def _normalize_priority(value: Any) -> str:
    text = _coerce_text(value).upper()
    if not text:
        return ""
    match = re.search(r"P[0-4]", text)
    if match:
        return match.group(0)
    if text in {"CRITICAL", "SEV1", "HIGH"}:
        return "P1"
    if text in {"MEDIUM", "NORMAL"}:
        return "P2"
    if text in {"LOW", "MINOR"}:
        return "P3"
    return ""


def _tone_for_priority(priority: str) -> str:
    if priority in {"P0", "P1"}:
        return "urgent"
    if priority == "P2":
        return "professional"
    return "friendly"


def _build_response_subject(subject: str, category: str) -> str:
    clean_subject = _coerce_text(subject)
    if clean_subject:
        return clean_subject if clean_subject.lower().startswith("re:") else f"Re: {clean_subject}"
    if category:
        return f"Support update: {_pretty_label(category)}"
    return "Support update on your request"


def _build_body(*, task: str, ticket: Mapping[str, str]) -> str:
    tone = _tone_for_priority(ticket["priority"])
    issue = _issue_summary(ticket)
    category = _pretty_label(ticket["category"]) if ticket["category"] else "support request"
    team_sentence = _team_sentence(ticket.get("team", ""))
    eta_sentence = _eta_sentence(ticket.get("eta", ""))
    next_step = _next_step(ticket)
    ticket_ref = ticket.get("ticket_id", "")

    if tone == "urgent":
        lines = [
            "Hello,",
            "",
            (
                f"Thanks for flagging {issue}. I have marked {ticket_ref} as urgent "
                f"and started triage under {category.lower()}."
            ),
            f"Our team is now {next_step}.",
        ]
        if team_sentence:
            lines.append(team_sentence)
        if eta_sentence:
            lines.append(eta_sentence)
        else:
            lines.append("I will send the next update as soon as we confirm the immediate resolution path.")
        lines.extend(["", "Regards,", "Support"])
        return "\n".join(lines)

    if tone == "professional":
        lines = [
            "Hello,",
            "",
            f"Thank you for contacting us about {issue}.",
            (
                f"I have reviewed the details and logged {ticket_ref} as a {category.lower()} "
                f"request with {ticket['priority']} priority."
            ),
            f"Next, we will {next_step}.",
        ]
        if team_sentence:
            lines.append(team_sentence)
        lines.append(eta_sentence or "I will follow up once that review is complete.")
        lines.extend(["", "Regards,", "Support"])
        return "\n".join(lines)

    lines = [
        "Hi,",
        "",
        f"Thanks for reaching out about {issue}.",
        f"I have logged {ticket_ref} as a {category.lower()} request and we will {next_step}.",
    ]
    if team_sentence:
        lines.append(team_sentence)
    lines.append(eta_sentence or "If you have any screenshots or timestamps to add, feel free to reply with them.")
    lines.extend(["", "Best,", "Support"])
    return "\n".join(lines)


def _issue_summary(ticket: Mapping[str, str]) -> str:
    subject = _coerce_text(ticket.get("subject"))
    if subject:
        return subject

    body = _coerce_text(ticket.get("body"))
    if body:
        return _truncate(_first_sentence(body), limit=96)

    category = _coerce_text(ticket.get("category"))
    if category:
        return f"your {_pretty_label(category).lower()} issue"
    return "your request"


def _next_step(ticket: Mapping[str, str]) -> str:
    category = f"{ticket.get('category', '')} {ticket.get('body', '')}".lower()
    if any(word in category for word in ("billing", "refund", "invoice", "payment", "charge")):
        return "review the account history and confirm any billing correction"
    if any(word in category for word in ("outage", "incident", "downtime", "availability")):
        return "investigating the service impact and confirming the current mitigation"
    if any(word in category for word in ("login", "access", "auth", "password", "account")):
        return "check the account state and restore access if needed"
    if any(word in category for word in ("bug", "error", "crash", "exception", "defect")):
        return "reproduce the problem and confirm a fix or workaround"
    if any(word in category for word in ("feature", "request", "enhancement", "improvement")):
        return "log the request for product review and share any relevant guidance"
    if any(word in category for word in ("shipping", "delivery", "order", "tracking")):
        return "check the order status and confirm the latest shipping update"
    if any(word in category for word in ("security", "privacy", "compliance")):
        return "escalate this with the appropriate internal team immediately"
    return "review the details and confirm the best resolution path"


def _team_sentence(team: str) -> str:
    clean_team = _coerce_text(team)
    if not clean_team:
        return ""
    return f"This is currently with our {_pretty_label(clean_team).lower()} team."


def _eta_sentence(eta: str) -> str:
    clean_eta = _coerce_text(eta)
    if not clean_eta:
        return ""
    return f"Our current estimated next update is {clean_eta}."


def _first_sentence(text: str) -> str:
    parts = re.split(r"(?<=[.!?])\s+", text.strip(), maxsplit=1)
    return parts[0] if parts else text.strip()


def _truncate(text: str, *, limit: int) -> str:
    if len(text) <= limit:
        return text
    return text[: limit - 3].rstrip() + "..."


def _pretty_label(value: str) -> str:
    clean = _coerce_text(value)
    if not clean:
        return ""
    return re.sub(r"[_-]+", " ", clean).strip().title()


def _coerce_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return " ".join(value.split())
    return ""

