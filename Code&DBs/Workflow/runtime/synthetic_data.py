"""Synthetic Data authority domain primitives.

Synthetic Data owns generated dataset revisions and naming quality. It can
seed Workflow Context and Virtual Lab, but it never becomes Object Truth
evidence.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import hashlib
import json
import re
from typing import Any

from runtime.workflow_context import infer_scenario_pack_refs, scenario_pack_registry


DOMAIN_PACKS = {"saas_b2b", "support_ops", "finance_ops", "healthcare_ops", "logistics_ops"}
PRIVACY_MODES = {"synthetic_only", "schema_only", "anonymized_operational_seeded"}
DEFAULT_RECORDS_PER_OBJECT = 25
MAX_DATASET_RECORDS = 100_000

COMPANY_PREFIXES = [
    "Northstar", "Blue Harbor", "Cinder Ridge", "Silverline", "Evergreen",
    "Keystone", "Brightfield", "Summit", "Riverbend", "Juniper", "Marble",
    "Cobalt", "Redwood", "Ironwood", "Clearwater", "Vertex", "Alta",
    "Pioneer", "Sable", "Oakline", "Crescent", "Beacon", "Aster", "Canyon",
    "Horizon", "Wayfinder", "Fieldstone", "Lattice", "Onyx", "Cloudbreak",
    "Prairie", "Harborlight", "Granite", "Solace", "Ember", "Atlas",
    "Seabright", "Vista", "Helio", "Ridgeway", "Copper", "Windward",
    "Tandem", "Meridian", "Foresight", "Kestrel", "Rainier", "Arbor",
]
COMPANY_CORES = [
    "Analytics", "Supply", "Medical", "Systems", "Logistics", "Robotics",
    "Finance", "Energy", "Learning", "Security", "Manufacturing", "Retail",
    "Works", "Foods", "Studios", "Labs", "Networks", "Materials", "Health",
    "Cloud", "Payments", "Insurance", "BioSystems", "Distribution", "Media",
    "Signals", "Mobility", "Infrastructure", "Aviation", "Advisory",
    "Construction", "Pharma", "Ventures", "Platforms", "Operations",
    "Capital", "Data", "Freight", "Controls", "Research",
    "Commerce", "Care", "Devices", "Partners", "Markets", "Service",
]
COMPANY_SUFFIXES = [
    "Group", "Partners", "Co", "Labs", "Systems", "Works", "Collective",
    "Holdings", "Services", "Network", "Alliance", "Studio", "Foundry",
]

FIRST_NAMES = [
    "Avery", "Jordan", "Morgan", "Riley", "Taylor", "Casey", "Jamie",
    "Quinn", "Rowan", "Cameron", "Emerson", "Reese", "Parker", "Sage",
    "Drew", "Harper", "Skyler", "Alex", "Maya", "Nina", "Priya", "Iris",
    "Elena", "Sofia", "Nadia", "Mira", "Leah", "Amara", "Tessa", "Noor",
    "Lena", "Naomi", "Clara", "Mina", "Zara", "Ivy", "Kai", "Leo", "Owen",
    "Eli", "Noah", "Milo", "Theo", "Mateo", "Arjun", "Ravi", "Omar",
    "Jonah", "Felix", "Nolan", "Hugo", "Andre", "Julian", "Samir", "Evan",
]
LAST_NAMES = [
    "Chen", "Patel", "Rivera", "Nguyen", "Morgan", "Khan", "Garcia",
    "Okafor", "Kim", "Singh", "Bennett", "Brooks", "Shah", "Ibrahim",
    "Davis", "Miller", "Sato", "Lopez", "Martin", "Hayes", "Carter",
    "Reed", "Foster", "Murphy", "Cooper", "Bailey", "Hughes", "Myers",
    "Ross", "Ward", "Price", "Bell", "Cole", "Grant", "Diaz", "Hassan",
    "Lin", "Park", "Ali", "Stone", "Wright", "Torres", "Ramirez", "Young",
    "Evans", "Turner", "Scott", "Phillips", "Edwards", "Collins", "Stewart",
    "Morris", "Rogers", "Cook", "Powell", "Long", "Flores", "Jenkins",
    "Perry", "Russell", "Butler", "Barnes", "Fisher", "Mendoza",
]
MIDDLE_INITIALS = list("ABCDEFGHJKLMNPQRSTUVWXYZ")

ISSUE_AREAS = [
    "SAML login", "Invoice export", "Webhook retry", "Owner assignment",
    "Support sync", "Payment retry", "Renewal alert", "Duplicate merge",
    "Permission check", "Import watermark", "Slack approval", "Risk score",
    "Tax calculation", "Contract lookup", "Field mapping", "Identity match",
]
ISSUE_FAILURES = [
    "missing region", "stuck after retry", "using stale status",
    "dropping owner", "timing out", "creating duplicates", "blocked by scope",
    "missing lineage", "returning empty results", "skipping high priority",
    "losing audit trail", "failing validation", "waiting on approval",
]
ISSUE_CONTEXTS = [
    "after schema refresh", "during bulk import", "for enterprise accounts",
    "on renewal window", "after IdP rotation", "when webhook order changes",
    "for past-due invoices", "during sandbox replay", "with merged records",
]
ISSUE_QUALIFIERS = [
    "needs reversible fix", "requires owner review", "blocks promotion",
    "affects west region", "visible in audit log", "reproducible in lab",
    "detected by verifier", "ready for triage",
]

CHANNEL_TOPICS = [
    "billing", "renewal-risk", "support", "identity", "imports", "webhooks",
    "approvals", "security", "customer-health", "collections", "success",
]
CHANNEL_SUFFIXES = [
    "west", "east", "emea", "apac", "critical", "review", "ops", "alerts",
    "handoff", "exceptions", "daily",
]


class SyntheticDataError(ValueError):
    """Domain-level synthetic data failure with machine-readable detail."""

    def __init__(
        self,
        reason_code: str,
        message: str,
        *,
        details: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.reason_code = reason_code
        self.details = dict(details or {})


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def _digest(value: Any, *, length: int = 16) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()[:length]


def _slug(value: object) -> str:
    text = re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_")
    return text or "synthetic"


def _clean_text(value: object, *, field_name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise SyntheticDataError(
            "synthetic_data.invalid_input",
            f"{field_name} must be a non-empty string",
            details={"field_name": field_name},
        )
    return value.strip()


def _clean_optional_text(value: object) -> str | None:
    if value is None or value == "":
        return None
    if not isinstance(value, str) or not value.strip():
        raise SyntheticDataError(
            "synthetic_data.invalid_input",
            "optional text fields must be non-empty strings when supplied",
        )
    return value.strip()


def _clean_string_list(value: object) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes)):
        raise SyntheticDataError(
            "synthetic_data.invalid_input",
            "expected a list of strings",
        )
    return [str(item).strip() for item in value if str(item).strip()]


def _clean_object_counts(value: object) -> dict[str, int]:
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise SyntheticDataError(
            "synthetic_data.invalid_object_counts",
            "object_counts must be a JSON object",
        )
    counts: dict[str, int] = {}
    for key, raw_count in value.items():
        label = str(key).strip()
        if not label:
            continue
        count = int(raw_count)
        if count < 0:
            raise SyntheticDataError(
                "synthetic_data.invalid_object_counts",
                "object_counts cannot contain negative values",
                details={"object_label": label, "count": count},
            )
        counts[label] = count
    return counts


def _position(seed: str, category: str, object_label: str, index: int, capacity: int) -> int:
    if capacity <= 0:
        raise SyntheticDataError(
            "synthetic_data.naming_capacity_unavailable",
            "naming category has no capacity",
            details={"category": category, "object_label": object_label},
        )
    offset = int(_digest([seed, category, object_label], length=12), 16) % capacity
    # A fixed coprime stride keeps generation collision-free while avoiding
    # visually repetitive adjacent names such as many contacts with one surname.
    return (offset + (index * 7919)) % capacity


def _choice_by_position(position: int, values: Sequence[str], divisor: int = 1) -> str:
    return values[(position // divisor) % len(values)]


def _naming_category(object_label: str) -> str:
    normalized = _slug(object_label)
    if any(token in normalized for token in ("account", "customer", "company", "workspace", "organization")):
        return "company"
    if any(token in normalized for token in ("contact", "user", "approver", "principal", "owner", "actor")):
        return "person"
    if any(token in normalized for token in ("ticket", "case", "escalation", "failure", "attempt")):
        return "issue"
    if any(token in normalized for token in ("invoice", "payment", "subscription", "contract", "opportunity")):
        return "commercial"
    if "channel" in normalized or "slack" in normalized:
        return "channel"
    if any(token in normalized for token in ("webhook", "event", "automation", "batch", "snapshot")):
        return "event"
    return "generic"


def _category_capacity(category: str) -> int:
    if category in {"company", "generic"}:
        return len(COMPANY_PREFIXES) * len(COMPANY_CORES) * len(COMPANY_SUFFIXES)
    if category == "person":
        return len(FIRST_NAMES) * len(MIDDLE_INITIALS) * len(LAST_NAMES)
    if category == "issue":
        return len(ISSUE_AREAS) * len(ISSUE_FAILURES) * len(ISSUE_CONTEXTS) * len(ISSUE_QUALIFIERS)
    if category == "commercial":
        return len(COMPANY_PREFIXES) * 12_000
    if category == "channel":
        return len(CHANNEL_TOPICS) * len(CHANNEL_SUFFIXES) * len(COMPANY_PREFIXES)
    if category == "event":
        return len(ISSUE_AREAS) * len(CHANNEL_SUFFIXES) * 9_000
    return len(COMPANY_PREFIXES) * len(COMPANY_CORES) * len(COMPANY_SUFFIXES)


def _display_name_for(
    *,
    seed: str,
    namespace: str,
    object_label: str,
    object_index: int,
    reserved_terms: Sequence[str],
) -> tuple[str, dict[str, Any]]:
    category = _naming_category(object_label)
    capacity = _category_capacity(category)
    clean_reserved = [term.lower() for term in reserved_terms if term.strip()]
    for salt in range(0, 50):
        position = _position(seed, category, object_label, object_index + salt, capacity)
        display_name, components = _format_display_name(
            category=category,
            object_label=object_label,
            namespace=namespace,
            position=position,
        )
        lowered = display_name.lower()
        if not any(term in lowered for term in clean_reserved):
            components["reserved_term_retry_count"] = salt
            components["capacity"] = capacity
            return display_name, components
    raise SyntheticDataError(
        "synthetic_data.reserved_term_exhausted",
        "could not generate a display name outside reserved terms",
        details={"object_label": object_label, "reserved_terms": list(reserved_terms)},
    )


def _format_display_name(
    *,
    category: str,
    object_label: str,
    namespace: str,
    position: int,
) -> tuple[str, dict[str, Any]]:
    if category in {"company", "generic"}:
        prefix = _choice_by_position(position, COMPANY_PREFIXES)
        core = _choice_by_position(position, COMPANY_CORES, len(COMPANY_PREFIXES))
        suffix = _choice_by_position(position, COMPANY_SUFFIXES, len(COMPANY_PREFIXES) * len(COMPANY_CORES))
        return f"{prefix} {core} {suffix}", {
            "category": category,
            "pattern_ref": "synthetic_name.company.v1",
            "prefix": prefix,
            "core": core,
            "suffix": suffix,
        }
    if category == "person":
        first = _choice_by_position(position, FIRST_NAMES)
        middle = _choice_by_position(position, MIDDLE_INITIALS, len(FIRST_NAMES))
        last = _choice_by_position(position, LAST_NAMES, len(FIRST_NAMES) * len(MIDDLE_INITIALS))
        return f"{first} {middle}. {last}", {
            "category": category,
            "pattern_ref": "synthetic_name.person.v1",
            "first": first,
            "middle_initial": middle,
            "last": last,
        }
    if category == "issue":
        area = _choice_by_position(position, ISSUE_AREAS)
        failure = _choice_by_position(position, ISSUE_FAILURES, len(ISSUE_AREAS))
        context = _choice_by_position(position, ISSUE_CONTEXTS, len(ISSUE_AREAS) * len(ISSUE_FAILURES))
        qualifier = _choice_by_position(
            position,
            ISSUE_QUALIFIERS,
            len(ISSUE_AREAS) * len(ISSUE_FAILURES) * len(ISSUE_CONTEXTS),
        )
        return f"{area} {failure} {context}; {qualifier}", {
            "category": category,
            "pattern_ref": "synthetic_name.issue.v1",
            "area": area,
            "failure": failure,
            "context": context,
            "qualifier": qualifier,
        }
    if category == "commercial":
        company = _choice_by_position(position, COMPANY_PREFIXES)
        fiscal_year = 2026 + ((position // len(COMPANY_PREFIXES)) % 4)
        sequence = 1000 + ((position // (len(COMPANY_PREFIXES) * 4)) % 9000)
        label = _slug(object_label).replace("_", "-")
        return f"{label.upper()}-{fiscal_year}-{_slug(company).upper()}-{sequence}", {
            "category": category,
            "pattern_ref": "synthetic_name.commercial.v1",
            "company_token": company,
            "fiscal_year": fiscal_year,
            "sequence": sequence,
        }
    if category == "channel":
        topic = _choice_by_position(position, CHANNEL_TOPICS)
        suffix = _choice_by_position(position, CHANNEL_SUFFIXES, len(CHANNEL_TOPICS))
        team = _choice_by_position(position, COMPANY_PREFIXES, len(CHANNEL_TOPICS) * len(CHANNEL_SUFFIXES))
        return f"#{topic}-{_slug(team).replace('_', '-')}-{suffix}", {
            "category": category,
            "pattern_ref": "synthetic_name.channel.v1",
            "topic": topic,
            "team": team,
            "suffix": suffix,
        }
    area = _choice_by_position(position, ISSUE_AREAS)
    suffix = _choice_by_position(position, CHANNEL_SUFFIXES, len(ISSUE_AREAS))
    sequence = 10_000 + ((position // (len(ISSUE_AREAS) * len(CHANNEL_SUFFIXES))) % 90_000)
    return f"{object_label} {area} {suffix} event {sequence}", {
        "category": category,
        "pattern_ref": "synthetic_name.event.v1",
        "area": area,
        "suffix": suffix,
        "sequence": sequence,
    }


def build_synthetic_name_plan(
    *,
    namespace: str,
    seed: str,
    domain_pack: str,
    locale_ref: str,
    uniqueness_scope: str,
    object_counts: Mapping[str, int],
    reserved_terms: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Build an inspectable naming plan before records are generated."""

    reserved = sorted({_clean_text(term, field_name="reserved_terms") for term in reserved_terms or []})
    capacity_by_object = {
        label: _category_capacity(_naming_category(label))
        for label in sorted(object_counts)
    }
    exceeded = [
        {
            "object_label": label,
            "requested": int(object_counts[label]),
            "capacity": capacity_by_object[label],
        }
        for label in sorted(object_counts)
        if int(object_counts[label]) > capacity_by_object[label]
    ]
    if exceeded:
        raise SyntheticDataError(
            "synthetic_data.name_capacity_exceeded",
            "naming plan cannot produce enough unique display names",
            details={"exceeded": exceeded},
        )
    return {
        "plan_ref": f"synthetic_name_plan:{_slug(namespace)}:{_digest([namespace, seed, domain_pack, object_counts], length=20)}",
        "namespace": namespace,
        "domain_pack": domain_pack,
        "locale_ref": locale_ref,
        "uniqueness_scope": uniqueness_scope,
        "seed": seed,
        "reserved_terms": reserved,
        "pattern_sets": {
            "company": "synthetic_name.company.v1",
            "person": "synthetic_name.person.v1",
            "issue": "synthetic_name.issue.v1",
            "commercial": "synthetic_name.commercial.v1",
            "channel": "synthetic_name.channel.v1",
            "event": "synthetic_name.event.v1",
            "generic": "synthetic_name.company.v1",
        },
        "capacity_by_object": capacity_by_object,
        "quality_gates": {
            "collision_count": 0,
            "reserved_term_hits": 0,
            "placeholder_name_hits": 0,
            "min_unique_display_name_ratio": 0.995,
            "synthetic_labels_forbidden": True,
        },
    }


def generate_synthetic_dataset(
    *,
    intent: str,
    namespace: str = "default",
    workflow_ref: str | None = None,
    source_context_ref: str | None = None,
    source_object_truth_refs: Sequence[str] | None = None,
    scenario_pack_refs: Sequence[str] | None = None,
    object_counts: Mapping[str, int] | None = None,
    records_per_object: int = DEFAULT_RECORDS_PER_OBJECT,
    seed: str | None = None,
    domain_pack: str = "saas_b2b",
    locale_ref: str = "en-US",
    uniqueness_scope: str = "dataset",
    privacy_mode: str = "synthetic_only",
    reserved_terms: Sequence[str] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Generate a deterministic synthetic dataset with name-quality receipts."""

    clean_intent = _clean_text(intent, field_name="intent")
    clean_namespace = _clean_text(namespace, field_name="namespace")
    clean_domain_pack = str(domain_pack or "saas_b2b").strip()
    if clean_domain_pack not in DOMAIN_PACKS:
        raise SyntheticDataError(
            "synthetic_data.invalid_domain_pack",
            "domain_pack is not allowed",
            details={"domain_pack": clean_domain_pack, "allowed": sorted(DOMAIN_PACKS)},
        )
    clean_privacy_mode = str(privacy_mode or "synthetic_only").strip()
    if clean_privacy_mode not in PRIVACY_MODES:
        raise SyntheticDataError(
            "synthetic_data.invalid_privacy_mode",
            "privacy_mode is not allowed",
            details={"privacy_mode": clean_privacy_mode, "allowed": sorted(PRIVACY_MODES)},
        )
    clean_locale = str(locale_ref or "en-US").strip() or "en-US"
    clean_seed = seed.strip() if isinstance(seed, str) and seed.strip() else _digest(
        {
            "intent": clean_intent,
            "namespace": clean_namespace,
            "workflow_ref": workflow_ref,
            "scenario_pack_refs": list(scenario_pack_refs or []),
            "object_counts": dict(object_counts or {}),
        },
        length=14,
    )
    registry = scenario_pack_registry()
    requested_pack_refs = _clean_string_list(scenario_pack_refs)
    pack_refs = requested_pack_refs or infer_scenario_pack_refs(clean_intent, {})
    unknown_packs = [pack_ref for pack_ref in pack_refs if pack_ref not in registry]
    if unknown_packs:
        raise SyntheticDataError(
            "synthetic_data.unknown_scenario_pack",
            "unknown scenario pack requested",
            details={"unknown": unknown_packs, "allowed": sorted(registry)},
        )
    scenario_objects = _scenario_objects(pack_refs)
    counts = _clean_object_counts(object_counts)
    if not counts:
        if records_per_object < 1:
            raise SyntheticDataError(
                "synthetic_data.invalid_record_count",
                "records_per_object must be positive",
            )
        counts = {label: int(records_per_object) for label in scenario_objects}
    total_records = sum(counts.values())
    if total_records < 1 or total_records > MAX_DATASET_RECORDS:
        raise SyntheticDataError(
            "synthetic_data.invalid_record_count",
            "total synthetic records must be between 1 and MAX_DATASET_RECORDS",
            details={"total_records": total_records, "max": MAX_DATASET_RECORDS},
        )
    name_plan = build_synthetic_name_plan(
        namespace=clean_namespace,
        seed=clean_seed,
        domain_pack=clean_domain_pack,
        locale_ref=clean_locale,
        uniqueness_scope=uniqueness_scope,
        object_counts=counts,
        reserved_terms=reserved_terms,
    )
    generation_spec = {
        "intent": clean_intent,
        "namespace": clean_namespace,
        "workflow_ref": _clean_optional_text(workflow_ref),
        "source_context_ref": _clean_optional_text(source_context_ref),
        "source_object_truth_refs": _clean_string_list(source_object_truth_refs),
        "scenario_pack_refs": list(pack_refs),
        "object_counts": dict(counts),
        "records_per_object": int(records_per_object),
        "seed": clean_seed,
        "domain_pack": clean_domain_pack,
        "locale_ref": clean_locale,
        "privacy_mode": clean_privacy_mode,
    }
    dataset_ref = f"synthetic_dataset:{_slug(clean_namespace)}:{_digest(generation_spec, length=20)}"
    records = _generate_records(
        dataset_ref=dataset_ref,
        namespace=clean_namespace,
        seed=clean_seed,
        scenario_pack_refs=pack_refs,
        object_counts=counts,
        name_plan=name_plan,
    )
    quality_report = validate_synthetic_records(
        records=records,
        reserved_terms=name_plan["reserved_terms"],
        uniqueness_scope=uniqueness_scope,
    )
    return {
        "dataset_ref": dataset_ref,
        "namespace": clean_namespace,
        "workflow_ref": _clean_optional_text(workflow_ref),
        "source_context_ref": _clean_optional_text(source_context_ref),
        "source_object_truth_refs": _clean_string_list(source_object_truth_refs),
        "generator_ref": "authority.synthetic_data.internal_deterministic.v1",
        "generator_version": "synthetic_data.internal_deterministic.v1",
        "seed": clean_seed,
        "domain_pack": clean_domain_pack,
        "locale_ref": clean_locale,
        "privacy_mode": clean_privacy_mode,
        "evidence_tier": "synthetic",
        "scenario_pack_refs": list(pack_refs),
        "object_counts": dict(counts),
        "record_count": len(records),
        "name_plan": name_plan,
        "generation_spec": generation_spec,
        "schema_contract": _schema_contract(pack_refs, counts),
        "quality_report": quality_report,
        "quality_state": quality_report["quality_state"],
        "quality_score": quality_report["quality_score"],
        "permissions": {
            "live_writes_allowed": False,
            "customer_data_allowed": clean_privacy_mode == "anonymized_operational_seeded",
            "promotion_evidence_allowed": False,
            "object_truth_promotion_allowed": False,
        },
        "records": records,
        "metadata": dict(metadata or {}),
    }


def _scenario_objects(pack_refs: Sequence[str]) -> list[str]:
    registry = scenario_pack_registry()
    objects: list[str] = []
    for pack_ref in pack_refs:
        for label in registry[pack_ref]["objects"]:
            if label not in objects:
                objects.append(label)
    return objects or ["SyntheticObject"]


def _generate_records(
    *,
    dataset_ref: str,
    namespace: str,
    seed: str,
    scenario_pack_refs: Sequence[str],
    object_counts: Mapping[str, int],
    name_plan: Mapping[str, Any],
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    reserved_terms = list(name_plan.get("reserved_terms") or [])
    ordinal = 0
    for object_label in sorted(object_counts):
        count = int(object_counts[object_label])
        object_slug = _slug(object_label)
        for object_index in range(count):
            display_name, name_components = _display_name_for(
                seed=seed,
                namespace=namespace,
                object_label=object_label,
                object_index=object_index,
                reserved_terms=reserved_terms,
            )
            name_ref = (
                f"synthetic_name:{_slug(namespace)}:{object_slug}:"
                f"{_digest([seed, object_label, object_index, display_name], length=16)}"
            )
            record_ref = (
                f"synthetic_record:{_slug(dataset_ref)}:{object_slug}:"
                f"{_digest([dataset_ref, object_label, object_index], length=16)}"
            )
            fields = _record_fields(
                seed=seed,
                object_label=object_label,
                object_index=object_index,
                display_name=display_name,
                name_ref=name_ref,
            )
            records.append(
                {
                    "record_ref": record_ref,
                    "dataset_ref": dataset_ref,
                    "object_kind": object_label,
                    "object_slug": object_slug,
                    "ordinal": ordinal,
                    "display_name": display_name,
                    "name_ref": name_ref,
                    "name_components": name_components,
                    "fields": fields,
                    "quality_flags": [],
                    "lineage": {
                        "authority_domain_ref": "authority.synthetic_data",
                        "truth_state": "synthetic",
                        "evidence_tier": "synthetic",
                        "scenario_pack_refs": list(scenario_pack_refs),
                        "promotion_evidence_allowed": False,
                    },
                }
            )
            ordinal += 1
    return records


def _record_fields(
    *,
    seed: str,
    object_label: str,
    object_index: int,
    display_name: str,
    name_ref: str,
) -> dict[str, Any]:
    status_choices = ["new", "active", "at_risk", "paused", "ready_for_review"]
    priority_choices = ["low", "medium", "high", "critical"]
    category = _naming_category(object_label)
    return {
        "display_name": display_name,
        "synthetic_name_ref": name_ref,
        "synthetic_external_id": f"syn-{_slug(object_label)}-{_digest([seed, object_label, object_index], length=10)}",
        "status": status_choices[int(_digest([seed, object_label, object_index, "status"], length=8), 16) % len(status_choices)],
        "priority": priority_choices[int(_digest([seed, object_label, object_index, "priority"], length=8), 16) % len(priority_choices)],
        "object_category": category,
        "synthetic": True,
    }


def _schema_contract(pack_refs: Sequence[str], counts: Mapping[str, int]) -> dict[str, Any]:
    registry = scenario_pack_registry()
    fields: list[str] = []
    for pack_ref in pack_refs:
        for field in registry[pack_ref]["fields"]:
            if field not in fields:
                fields.append(field)
    return {
        "contract_ref": f"synthetic_schema_contract:{_digest([pack_refs, counts, fields], length=20)}",
        "object_kinds": sorted(counts),
        "common_fields": [
            "display_name",
            "synthetic_name_ref",
            "synthetic_external_id",
            "status",
            "priority",
            "object_category",
            "synthetic",
        ],
        "scenario_fields": fields,
        "all_records_synthetic": True,
    }


def validate_synthetic_records(
    *,
    records: Sequence[Mapping[str, Any]],
    reserved_terms: Sequence[str] | None = None,
    uniqueness_scope: str = "dataset",
) -> dict[str, Any]:
    """Validate generated records before persistence or export."""

    lowered_reserved = [term.lower() for term in reserved_terms or [] if term.strip()]
    display_names = [str(record.get("display_name") or "") for record in records]
    name_refs = [str(record.get("name_ref") or "") for record in records]
    duplicate_display_names = _duplicates(display_names)
    duplicate_name_refs = _duplicates(name_refs)
    reserved_hits = [
        name for name in display_names
        if any(term in name.lower() for term in lowered_reserved)
    ]
    placeholder_hits = [
        name for name in display_names
        if name.lower().startswith("synthetic ") or re.search(r"\b(test|fake)\b", name.lower())
    ]
    total = len(records)
    unique_ratio = (len(set(display_names)) / total) if total else 0.0
    object_counts: dict[str, int] = {}
    for record in records:
        object_kind = str(record.get("object_kind") or "unknown")
        object_counts[object_kind] = object_counts.get(object_kind, 0) + 1
    accepted = (
        total > 0
        and not duplicate_display_names
        and not duplicate_name_refs
        and not reserved_hits
        and not placeholder_hits
        and unique_ratio >= 0.995
    )
    penalties = (
        len(duplicate_display_names) * 0.08
        + len(duplicate_name_refs) * 0.12
        + len(reserved_hits) * 0.10
        + len(placeholder_hits) * 0.15
        + max(0.0, 0.995 - unique_ratio)
    )
    return {
        "quality_state": "accepted" if accepted else "rejected",
        "quality_score": max(0.0, min(1.0, round(1.0 - penalties, 4))),
        "record_count": total,
        "object_counts": object_counts,
        "uniqueness_scope": uniqueness_scope,
        "unique_display_name_ratio": round(unique_ratio, 6),
        "collision_count": len(duplicate_display_names),
        "duplicate_display_names": duplicate_display_names[:20],
        "duplicate_name_ref_count": len(duplicate_name_refs),
        "reserved_term_hits": reserved_hits[:20],
        "placeholder_name_hits": placeholder_hits[:20],
        "stable_identity_fields": ["record_ref", "name_ref"],
        "display_name_mutable": True,
        "identity_mutable": False,
    }


def _duplicates(values: Sequence[str]) -> list[str]:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for value in values:
        key = value.lower()
        if key in seen:
            duplicates.add(value)
        seen.add(key)
    return sorted(duplicates)


__all__ = [
    "DOMAIN_PACKS",
    "PRIVACY_MODES",
    "SyntheticDataError",
    "build_synthetic_name_plan",
    "generate_synthetic_dataset",
    "validate_synthetic_records",
]
