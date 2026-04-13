"""Session carry-forward and compaction for multi-session workflow continuity."""

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple


@dataclass(frozen=True)
class CarryForwardPack:
    pack_id: str
    objective: str
    decisions: Tuple[str, ...]
    open_questions: Tuple[str, ...]
    constraints: Tuple[str, ...]
    risks: Tuple[str, ...]
    artifacts: Tuple[str, ...]
    next_actions: Tuple[str, ...]
    created_at: datetime
    token_estimate: int


_BULLET_RE = re.compile(r"^\s*(?:[-*]|\d+\.)\s+(?P<item>.+?)\s*$")
_ARTIFACT_PATH_RE = re.compile(
    r"(?:/[\w .:+@/-]+\.(?:json|md|py|sql|txt|yaml|yml)|artifacts/[\w .:+@/-]+)"
)
_ARTIFACT_KEYS = {
    "artifact_id",
    "file_path",
    "pack_id",
    "run_id",
    "job_id",
    "workflow_id",
    "sandbox_id",
    "spec_path",
    "url",
}
_HEADING_TARGETS: tuple[tuple[tuple[str, ...], str], ...] = (
    (("must-do actions", "should-do actions", "next moves", "next steps", "next actions"), "next_actions"),
    (("build decisions", "decisions"), "decisions"),
    (("constraints", "learned constraints"), "constraints"),
    (("open questions", "questions"), "open_questions"),
    (("risks", "failure modes"), "risks"),
)


class SessionCompactor:
    """Reduces a CarryForwardPack to fit within a token budget."""

    # Priority order: higher index = lower priority = trimmed first
    _TRIMMABLE_FIELDS = [
        "artifacts",
        "open_questions",
        "next_actions",
        "risks",
        "decisions",
        "constraints",
    ]

    def estimate_tokens(self, pack: CarryForwardPack) -> int:
        return len(str(pack)) // 4

    def compact(
        self, pack: CarryForwardPack, max_tokens: int = 2000
    ) -> CarryForwardPack:
        if self.estimate_tokens(pack) <= max_tokens:
            return pack

        fields: Dict[str, List[str]] = {
            f: list(getattr(pack, f)) for f in self._TRIMMABLE_FIELDS
        }

        # Phase 1: trim lowest-priority lists first
        for field_name in self._TRIMMABLE_FIELDS:
            while fields[field_name] and self._estimate_with(pack, fields) > max_tokens:
                fields[field_name].pop()

        # Phase 2: summarize long strings if still over budget
        if self._estimate_with(pack, fields) > max_tokens:
            for field_name in self._TRIMMABLE_FIELDS:
                for i, val in enumerate(fields[field_name]):
                    if len(val) > 103 and self._estimate_with(pack, fields) > max_tokens:
                        fields[field_name][i] = val[:100] + "..."

        # Summarize objective only as last resort (never cut entirely)
        objective = pack.objective
        if self._estimate_with_obj(pack, fields, objective) > max_tokens and len(objective) > 103:
            objective = objective[:100] + "..."

        result = CarryForwardPack(
            pack_id=pack.pack_id,
            objective=objective,
            decisions=tuple(fields["decisions"]),
            open_questions=tuple(fields["open_questions"]),
            constraints=tuple(fields["constraints"]),
            risks=tuple(fields["risks"]),
            artifacts=tuple(fields["artifacts"]),
            next_actions=tuple(fields["next_actions"]),
            created_at=pack.created_at,
            token_estimate=0,
        )
        # Stabilize: token_estimate affects its own str() length, iterate to fixpoint
        est = self.estimate_tokens(result)
        for _ in range(5):
            candidate = CarryForwardPack(
                pack_id=result.pack_id,
                objective=result.objective,
                decisions=result.decisions,
                open_questions=result.open_questions,
                constraints=result.constraints,
                risks=result.risks,
                artifacts=result.artifacts,
                next_actions=result.next_actions,
                created_at=result.created_at,
                token_estimate=est,
            )
            new_est = self.estimate_tokens(candidate)
            if new_est == est:
                return candidate
            est = new_est
        return candidate

    def _estimate_with(
        self, pack: CarryForwardPack, fields: Dict[str, List[str]]
    ) -> int:
        return self._estimate_with_obj(pack, fields, pack.objective)

    def _estimate_with_obj(
        self, pack: CarryForwardPack, fields: Dict[str, List[str]], objective: str
    ) -> int:
        tmp = CarryForwardPack(
            pack_id=pack.pack_id,
            objective=objective,
            decisions=tuple(fields["decisions"]),
            open_questions=tuple(fields["open_questions"]),
            constraints=tuple(fields["constraints"]),
            risks=tuple(fields["risks"]),
            artifacts=tuple(fields["artifacts"]),
            next_actions=tuple(fields["next_actions"]),
            created_at=pack.created_at,
            token_estimate=0,
        )
        return self.estimate_tokens(tmp)


class CarryForwardManager:
    """Manages persistence and lifecycle of CarryForwardPacks."""

    def __init__(self, storage_dir: str) -> None:
        self.storage_dir = Path(storage_dir)
        self.storage_dir.mkdir(parents=True, exist_ok=True)

    def build(
        self,
        objective: str,
        decisions: Tuple[str, ...] = (),
        open_questions: Tuple[str, ...] = (),
        constraints: Tuple[str, ...] = (),
        risks: Tuple[str, ...] = (),
        artifacts: Tuple[str, ...] = (),
        next_actions: Tuple[str, ...] = (),
    ) -> CarryForwardPack:
        pack = CarryForwardPack(
            pack_id=uuid.uuid4().hex[:12],
            objective=objective,
            decisions=decisions,
            open_questions=open_questions,
            constraints=constraints,
            risks=risks,
            artifacts=artifacts,
            next_actions=next_actions,
            created_at=datetime.now(timezone.utc),
            token_estimate=0,
        )
        compactor = SessionCompactor()
        return CarryForwardPack(
            pack_id=pack.pack_id,
            objective=pack.objective,
            decisions=pack.decisions,
            open_questions=pack.open_questions,
            constraints=pack.constraints,
            risks=pack.risks,
            artifacts=pack.artifacts,
            next_actions=pack.next_actions,
            created_at=pack.created_at,
            token_estimate=compactor.estimate_tokens(pack),
        )

    def save(self, pack: CarryForwardPack) -> None:
        data = {
            "pack_id": pack.pack_id,
            "objective": pack.objective,
            "decisions": list(pack.decisions),
            "open_questions": list(pack.open_questions),
            "constraints": list(pack.constraints),
            "risks": list(pack.risks),
            "artifacts": list(pack.artifacts),
            "next_actions": list(pack.next_actions),
            "created_at": pack.created_at.isoformat(),
            "token_estimate": pack.token_estimate,
        }
        path = self.storage_dir / f"{pack.pack_id}.json"
        path.write_text(json.dumps(data, indent=2))

    def load(self, pack_id: str) -> Optional[CarryForwardPack]:
        path = self.storage_dir / f"{pack_id}.json"
        if not path.exists():
            return None
        data = json.loads(path.read_text())
        return CarryForwardPack(
            pack_id=data["pack_id"],
            objective=data["objective"],
            decisions=tuple(data["decisions"]),
            open_questions=tuple(data["open_questions"]),
            constraints=tuple(data["constraints"]),
            risks=tuple(data["risks"]),
            artifacts=tuple(data["artifacts"]),
            next_actions=tuple(data["next_actions"]),
            created_at=datetime.fromisoformat(data["created_at"]),
            token_estimate=data["token_estimate"],
        )

    def latest(self) -> Optional[CarryForwardPack]:
        packs: List[CarryForwardPack] = []
        for f in self.storage_dir.glob("*.json"):
            try:
                data = json.loads(f.read_text())
                packs.append(
                    CarryForwardPack(
                        pack_id=data["pack_id"],
                        objective=data["objective"],
                        decisions=tuple(data["decisions"]),
                        open_questions=tuple(data["open_questions"]),
                        constraints=tuple(data["constraints"]),
                        risks=tuple(data["risks"]),
                        artifacts=tuple(data["artifacts"]),
                        next_actions=tuple(data["next_actions"]),
                        created_at=datetime.fromisoformat(data["created_at"]),
                        token_estimate=data["token_estimate"],
                    )
                )
            except (json.JSONDecodeError, KeyError):
                continue
        if not packs:
            return None
        return max(packs, key=lambda p: p.created_at)

    def validate(self, pack: CarryForwardPack) -> List[str]:
        errors: List[str] = []
        if not pack.objective or not pack.objective.strip():
            errors.append("objective must not be empty")
        if not pack.pack_id or not pack.pack_id.strip():
            errors.append("pack_id must not be empty")
        if pack.token_estimate < 0:
            errors.append("token_estimate must not be negative")
        return errors


def pack_to_summary_dict(
    pack: CarryForwardPack,
    *,
    include_items: bool = True,
) -> dict[str, Any]:
    summary: dict[str, Any] = {
        "pack_id": pack.pack_id,
        "objective": pack.objective,
        "created_at": pack.created_at.isoformat(),
        "token_estimate": pack.token_estimate,
        "counts": {
            "decisions": len(pack.decisions),
            "open_questions": len(pack.open_questions),
            "constraints": len(pack.constraints),
            "risks": len(pack.risks),
            "artifacts": len(pack.artifacts),
            "next_actions": len(pack.next_actions),
        },
    }
    if include_items:
        summary.update(
            {
                "decisions": list(pack.decisions),
                "open_questions": list(pack.open_questions),
                "constraints": list(pack.constraints),
                "risks": list(pack.risks),
                "artifacts": list(pack.artifacts),
                "next_actions": list(pack.next_actions),
            }
        )
    return summary


def build_interaction_pack(
    manager: CarryForwardManager,
    *,
    objective: str,
    assistant_content: str,
    tool_results: Sequence[Mapping[str, Any] | Any] = (),
    max_items: int = 5,
    token_budget: int = 2000,
) -> Optional[CarryForwardPack]:
    clean_objective = (objective or "").strip()
    clean_content = (assistant_content or "").strip()
    if not clean_objective or not clean_content:
        return None

    sections = _extract_interaction_sections(
        assistant_content=clean_content,
        tool_results=tool_results,
        max_items=max_items,
    )
    if not any(sections.values()):
        return None

    pack = manager.build(
        clean_objective,
        decisions=sections["decisions"],
        open_questions=sections["open_questions"],
        constraints=sections["constraints"],
        risks=sections["risks"],
        artifacts=sections["artifacts"],
        next_actions=sections["next_actions"],
    )
    return SessionCompactor().compact(pack, max_tokens=token_budget)


def _extract_interaction_sections(
    *,
    assistant_content: str,
    tool_results: Sequence[Mapping[str, Any] | Any],
    max_items: int,
) -> dict[str, Tuple[str, ...]]:
    sections: dict[str, list[str]] = {
        "decisions": [],
        "open_questions": [],
        "constraints": [],
        "risks": [],
        "artifacts": [],
        "next_actions": [],
    }

    current_target: str | None = None
    bullet_fallbacks: list[str] = []
    for raw_line in assistant_content.splitlines():
        line = raw_line.strip()
        if not line:
            continue

        heading_target = _heading_target(line)
        if heading_target is not None:
            current_target = heading_target
            continue

        bullet_match = _BULLET_RE.match(raw_line)
        if bullet_match:
            item = bullet_match.group("item").strip()
            if current_target is not None:
                sections[current_target].append(item)
            else:
                bullet_fallbacks.append(item)
            continue

        if line.endswith("?"):
            sections["open_questions"].append(line)
        elif line.lower().startswith(("decision:", "decided:", "approved:", "rejected:")):
            sections["decisions"].append(line)
        elif "risk" in line.lower() or "failure mode" in line.lower():
            sections["risks"].append(line)

    if not sections["next_actions"] and bullet_fallbacks:
        sections["next_actions"].extend(bullet_fallbacks[:max_items])

    for question in _extract_questions(assistant_content):
        sections["open_questions"].append(question)

    tool_sections = _extract_tool_result_sections(tool_results)
    for key, values in tool_sections.items():
        sections[key].extend(values)

    return {
        key: _dedupe_trim(values, max_items=max_items)
        for key, values in sections.items()
    }


def _heading_target(line: str) -> str | None:
    normalized = line.strip().lower().rstrip(":")
    for headings, target in _HEADING_TARGETS:
        if normalized in headings:
            return target
    return None


def _extract_questions(text: str) -> list[str]:
    questions: list[str] = []
    for raw_line in text.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        bullet_match = _BULLET_RE.match(raw_line)
        if bullet_match:
            line = bullet_match.group("item").strip()
        if line.endswith("?") and len(line) > 4:
            questions.append(line)
            continue
        for part in re.findall(r"[^?.!]*\?", line):
            question = part.strip()
            if len(question) > 4:
                questions.append(question)
    return questions


def _extract_tool_result_sections(
    tool_results: Sequence[Mapping[str, Any] | Any],
) -> dict[str, list[str]]:
    sections: dict[str, list[str]] = {
        "constraints": [],
        "artifacts": [],
        "risks": [],
    }
    for result in tool_results:
        payload = result.get("result") if isinstance(result, Mapping) and "result" in result else result
        if isinstance(payload, Mapping):
            for entry in payload.get("constraints", []):
                if isinstance(entry, Mapping):
                    pattern = str(entry.get("pattern", "")).strip()
                    text = str(entry.get("text", "") or entry.get("constraint_text", "")).strip()
                    if pattern and text:
                        sections["constraints"].append(f"[{pattern}] {text}")
                    elif text:
                        sections["constraints"].append(text)
                elif entry:
                    sections["constraints"].append(str(entry))

            for entry in payload.get("artifacts", []):
                if isinstance(entry, Mapping):
                    for key in ("artifact_id", "file_path", "sandbox_id"):
                        value = entry.get(key)
                        if value:
                            sections["artifacts"].append(str(value))
                elif entry:
                    sections["artifacts"].append(str(entry))

            for key in ("error", "failure_code", "message"):
                value = payload.get(key)
                if value and key != "message":
                    sections["risks"].append(str(value))

        for path in _ARTIFACT_PATH_RE.findall(_stringify_payload(payload)):
            sections["artifacts"].append(path)
        for artifact_value in _walk_artifact_values(payload):
            sections["artifacts"].append(artifact_value)

    return sections


def _walk_artifact_values(value: Any) -> Iterable[str]:
    if isinstance(value, Mapping):
        for key, nested in value.items():
            if key in _ARTIFACT_KEYS and nested:
                yield str(nested)
            yield from _walk_artifact_values(nested)
    elif isinstance(value, (list, tuple, set)):
        for item in value:
            yield from _walk_artifact_values(item)


def _stringify_payload(value: Any) -> str:
    if isinstance(value, str):
        return value
    try:
        return json.dumps(value, default=str)
    except TypeError:
        return str(value)


def _dedupe_trim(values: Sequence[str], *, max_items: int) -> Tuple[str, ...]:
    trimmed: list[str] = []
    seen: set[str] = set()
    for value in values:
        clean = " ".join(str(value).strip().split())
        if not clean or clean in seen:
            continue
        seen.add(clean)
        trimmed.append(clean)
        if len(trimmed) >= max_items:
            break
    return tuple(trimmed)
