"""Governance filters for dispatch prompts, file writes, and scope validation.

Provides secret detection, prompt sanitization, write-path scope enforcement,
and file-write auditing with SHA256 hashing.
"""

import hashlib
import re
import time
from dataclasses import dataclass
from typing import List, Optional, Tuple


# ---------------------------------------------------------------------------
# Data classes (all frozen)
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Finding:
    pattern_name: str
    line_number: int
    redacted_match: str
    severity: str  # 'critical' | 'warning'

    def __post_init__(self) -> None:
        if self.severity not in ("critical", "warning"):
            raise ValueError(f"severity must be 'critical' or 'warning', got {self.severity!r}")


@dataclass(frozen=True)
class GovernanceScanResult:
    passed: bool
    findings: Tuple["Finding", ...]
    blocked_reason: Optional[str]


@dataclass(frozen=True)
class ScopeScanResult:
    passed: bool
    out_of_scope_paths: Tuple[str, ...]
    blocked_reason: Optional[str]


@dataclass(frozen=True)
class WriteAuditRecord:
    file_path: str
    sha256: str
    byte_count: int
    line_count: int
    timestamp: float


@dataclass(frozen=True)
class WriteDiffStats:
    lines_added: int
    lines_removed: int
    lines_changed: int


# ---------------------------------------------------------------------------
# Secret patterns
# ---------------------------------------------------------------------------

class SecretPattern:
    """Registry of compiled regex patterns that detect secrets/credentials."""

    _PATTERNS: List[Tuple[str, "re.Pattern[str]", str]] = []

    @classmethod
    def _build_patterns(cls) -> List[Tuple[str, "re.Pattern[str]", str]]:
        if cls._PATTERNS:
            return cls._PATTERNS

        raw: List[Tuple[str, str, str]] = [
            # API keys -- generic prefixes
            ("generic_api_key_sk", r"sk-[A-Za-z0-9]{20,}", "critical"),
            ("generic_api_key_prefix", r"key-[A-Za-z0-9]{20,}", "critical"),

            # Bearer tokens
            ("bearer_token", r"Bearer\s+[A-Za-z0-9\-._~+/]+=*", "critical"),

            # AWS
            ("aws_access_key", r"AKIA[0-9A-Z]{16}", "critical"),
            ("aws_secret_key", r"(?i)aws[_\-]?secret[_\-]?access[_\-]?key\s*[=:]\s*[A-Za-z0-9/+=]{30,}", "critical"),

            # GitHub PATs
            ("github_pat", r"gh[posta]_[A-Za-z0-9]{36,}", "critical"),

            # Stripe
            ("stripe_secret_key", r"sk_live_[A-Za-z0-9]{20,}", "critical"),
            ("stripe_publishable_key", r"pk_live_[A-Za-z0-9]{20,}", "warning"),

            # Generic password / secret in key=value
            ("generic_password_kv", r'(?i)(?:password|secret|token|api_key|apikey)\s*[=:]\s*["\']?[^\s"\']{8,}', "warning"),

            # Base64 blobs that look like tokens (40+ chars, exclude paths with / and .)
            ("base64_token_blob", r"(?<![A-Za-z0-9+/=._\-])[A-Za-z0-9+]{40,}={0,2}(?![A-Za-z0-9+/=._\-])", "warning"),
        ]

        cls._PATTERNS = [
            (name, re.compile(pattern), severity)
            for name, pattern, severity in raw
        ]
        return cls._PATTERNS

    @classmethod
    def patterns(cls) -> List[Tuple[str, "re.Pattern[str]", str]]:
        return cls._build_patterns()

    @classmethod
    def scan_text(cls, text: str) -> List[Finding]:
        """Scan *text* line-by-line and return all findings."""
        findings: List[Finding] = []
        for line_number, line in enumerate(text.splitlines(), start=1):
            for name, pattern, severity in cls.patterns():
                for match in pattern.finditer(line):
                    matched = match.group(0)
                    # Redact: show first 4 and last 2 chars, mask the middle
                    if len(matched) > 8:
                        redacted = matched[:4] + "*" * (len(matched) - 6) + matched[-2:]
                    else:
                        redacted = matched[:2] + "*" * (len(matched) - 2)
                    findings.append(Finding(
                        pattern_name=name,
                        line_number=line_number,
                        redacted_match=redacted,
                        severity=severity,
                    ))
        return findings


# ---------------------------------------------------------------------------
# GovernanceFilter
# ---------------------------------------------------------------------------

class GovernanceFilter:
    """Scans prompts for leaked secrets and validates write scope."""

    # ---- prompt scanning ---------------------------------------------------

    def scan_prompt(self, text: str) -> GovernanceScanResult:
        """Scan a dispatch prompt for leaked secrets. Fail-closed on error."""
        try:
            findings = SecretPattern.scan_text(text)
            if findings:
                return GovernanceScanResult(
                    passed=False,
                    findings=tuple(findings),
                    blocked_reason="Secrets detected in prompt",
                )
            return GovernanceScanResult(passed=True, findings=(), blocked_reason=None)
        except Exception as exc:
            return GovernanceScanResult(
                passed=False,
                findings=(),
                blocked_reason=f"Scan failed (fail-closed): {exc}",
            )

    # ---- scope scanning ----------------------------------------------------

    def scan_scope(
        self,
        write_paths: List[str],
        allowed_paths: Optional[List[str]],
    ) -> ScopeScanResult:
        """Validate that all *write_paths* fall within *allowed_paths*.

        If *allowed_paths* is ``None`` every path is allowed (open scope).
        """
        if allowed_paths is None:
            return ScopeScanResult(passed=True, out_of_scope_paths=(), blocked_reason=None)

        out: List[str] = []
        for wp in write_paths:
            if not any(wp.startswith(ap) for ap in allowed_paths):
                out.append(wp)

        if out:
            return ScopeScanResult(
                passed=False,
                out_of_scope_paths=tuple(out),
                blocked_reason="Write paths outside allowed scope",
            )
        return ScopeScanResult(passed=True, out_of_scope_paths=(), blocked_reason=None)

    # ---- prompt filtering --------------------------------------------------

    def filter_prompt(self, text: str) -> Tuple[str, List[str]]:
        """Return (sanitized_text, list_of_descriptions) with secrets redacted."""
        descriptions: List[str] = []
        sanitized = text

        for name, pattern, severity in SecretPattern.patterns():
            def _replace(m: re.Match, _name: str = name) -> str:
                descriptions.append(f"{_name}: {m.group(0)[:4]}...")
                return "[REDACTED_SECRET]"
            sanitized = pattern.sub(_replace, sanitized)

        return sanitized, descriptions


# ---------------------------------------------------------------------------
# FileWriteAudit
# ---------------------------------------------------------------------------

class FileWriteAudit:
    """Records and diffs file-write operations."""

    def __init__(self) -> None:
        self._log: List[WriteAuditRecord] = []

    def record_write(self, file_path: str, content: str) -> WriteAuditRecord:
        encoded = content.encode("utf-8")
        record = WriteAuditRecord(
            file_path=file_path,
            sha256=hashlib.sha256(encoded).hexdigest(),
            byte_count=len(encoded),
            line_count=content.count("\n") + (1 if content and not content.endswith("\n") else 0),
            timestamp=time.time(),
        )
        self._log.append(record)
        return record

    def diff_against(self, record: WriteAuditRecord, new_content: str) -> WriteDiffStats:
        """Compute a simple line-level diff between the content that produced
        *record* and *new_content*.

        Since the audit record stores a hash (not original text), we compare
        line counts and SHA for an approximation.
        """
        new_lines = new_content.splitlines()
        new_line_count = len(new_lines)
        old_line_count = record.line_count

        new_sha = hashlib.sha256(new_content.encode("utf-8")).hexdigest()
        if new_sha == record.sha256:
            return WriteDiffStats(lines_added=0, lines_removed=0, lines_changed=0)

        if new_line_count >= old_line_count:
            added = new_line_count - old_line_count
            removed = 0
        else:
            added = 0
            removed = old_line_count - new_line_count

        # Estimate changed lines as the minimum of old/new (worst-case overlap)
        changed = min(old_line_count, new_line_count)
        return WriteDiffStats(lines_added=added, lines_removed=removed, lines_changed=changed)

    @property
    def audit_log(self) -> Tuple[WriteAuditRecord, ...]:
        return tuple(self._log)
