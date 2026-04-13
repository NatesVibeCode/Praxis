"""Tests for runtime.governance -- secret detection, scope validation, file audit."""

import hashlib
import importlib.util
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

# Import governance directly to avoid runtime/__init__.py pulling in modules
# that require Python 3.10+ dataclass features.
_gov_path = Path(__file__).resolve().parents[2] / "runtime" / "governance.py"
_spec = importlib.util.spec_from_file_location("governance", str(_gov_path))
_mod = importlib.util.module_from_spec(_spec)
sys.modules["governance"] = _mod
_spec.loader.exec_module(_mod)

FileWriteAudit = _mod.FileWriteAudit
Finding = _mod.Finding
GovernanceFilter = _mod.GovernanceFilter
GovernanceScanResult = _mod.GovernanceScanResult
SecretPattern = _mod.SecretPattern
ScopeScanResult = _mod.ScopeScanResult
WriteAuditRecord = _mod.WriteAuditRecord
WriteDiffStats = _mod.WriteDiffStats


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_gf = GovernanceFilter()


# ---------------------------------------------------------------------------
# Secret detection -- each pattern type
# ---------------------------------------------------------------------------

class TestSecretDetection:

    def test_generic_api_key_sk(self):
        text = "my key is sk-abc123def456ghi789jkl012"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "generic_api_key_sk" for f in findings)

    def test_generic_api_key_prefix(self):
        text = "use key-ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "generic_api_key_prefix" for f in findings)

    def test_bearer_token(self):
        text = "Authorization: Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.payload.sig"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "bearer_token" for f in findings)

    def test_aws_access_key(self):
        text = "aws_access_key_id = AKIAIOSFODNN7EXAMPLE"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "aws_access_key" for f in findings)

    def test_aws_secret_key(self):
        text = "aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "aws_secret_key" for f in findings)

    def test_github_pat_ghp(self):
        text = "token = ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijkl"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "github_pat" for f in findings)

    def test_github_pat_gho(self):
        text = "GITHUB_TOKEN=gho_abcdefghijklmnopqrstuvwxyz1234567890"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "github_pat" for f in findings)

    def test_github_pat_ghs(self):
        text = "ghs_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijkl"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "github_pat" for f in findings)

    def test_stripe_secret_key(self):
        prefix, suffix = "sk_live_", "4eC39HqLyjWDarjtT1zdp7dc"
        text = f"STRIPE_KEY={prefix}{suffix}"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "stripe_secret_key" for f in findings)

    def test_stripe_publishable_key(self):
        prefix, suffix = "pk_live_", "TYooMQauvdEDq54NiTphI7jx"
        text = f"{prefix}{suffix}"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "stripe_publishable_key" for f in findings)

    def test_generic_password_kv(self):
        text = 'password = "supersecretpassword123"'
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "generic_password_kv" for f in findings)

    def test_generic_secret_kv(self):
        text = "secret: my_long_secret_value_here"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "generic_password_kv" for f in findings)

    def test_base64_token_blob(self):
        blob = "A" * 50
        text = f"token = {blob}"
        findings = SecretPattern.scan_text(text)
        assert any(f.pattern_name == "base64_token_blob" for f in findings)

    def test_finding_severity_validation(self):
        with pytest.raises(ValueError):
            Finding(pattern_name="x", line_number=1, redacted_match="x", severity="high")

    def test_redacted_match_is_masked(self):
        text = "sk-abcdefghijklmnopqrstuvwxyz"
        findings = SecretPattern.scan_text(text)
        f = next(f for f in findings if f.pattern_name == "generic_api_key_sk")
        assert "*" in f.redacted_match

    def test_line_number_is_correct(self):
        text = "line one\nline two\nsk-abcdefghijklmnopqrstuvwxyz"
        findings = SecretPattern.scan_text(text)
        assert any(f.line_number == 3 for f in findings)


# ---------------------------------------------------------------------------
# Clean prompt passes scan
# ---------------------------------------------------------------------------

class TestCleanPrompt:

    def test_clean_prompt_passes(self):
        result = _gf.scan_prompt("Please build a REST API for the widget service.")
        assert result.passed is True
        assert result.findings == ()
        assert result.blocked_reason is None

    def test_clean_prompt_with_short_tokens(self):
        # Short strings that look like key prefixes but are too short
        result = _gf.scan_prompt("Use sk-short or key-tiny as names.")
        assert result.passed is True


# ---------------------------------------------------------------------------
# Prompt filtering redacts secrets
# ---------------------------------------------------------------------------

class TestPromptFiltering:

    def test_filter_redacts_api_key(self):
        text = "Use sk-abcdefghijklmnopqrstuvwxyz to auth."
        sanitized, descriptions = _gf.filter_prompt(text)
        assert "[REDACTED_SECRET]" in sanitized
        assert len(descriptions) >= 1

    def test_filter_redacts_bearer(self):
        text = "Authorization: Bearer eyJhbGciOiJIUzI1NiJ9.payload.sig"
        sanitized, _ = _gf.filter_prompt(text)
        assert "[REDACTED_SECRET]" in sanitized

    def test_filter_returns_clean_text_unchanged(self):
        text = "Just a normal prompt with no secrets."
        sanitized, descriptions = _gf.filter_prompt(text)
        assert sanitized == text
        assert descriptions == []

    def test_filter_multiple_secrets(self):
        text = "key1=sk-aaaabbbbccccddddeeeeffffgggg\nkey2=ghp_ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijkl"
        sanitized, descriptions = _gf.filter_prompt(text)
        assert sanitized.count("[REDACTED_SECRET]") >= 2


# ---------------------------------------------------------------------------
# Scope validation
# ---------------------------------------------------------------------------

class TestScopeValidation:

    def test_within_bounds_passes(self):
        result = _gf.scan_scope(
            write_paths=["/project/src/app.py", "/project/src/lib.py"],
            allowed_paths=["/project/src/"],
        )
        assert result.passed is True
        assert result.out_of_scope_paths == ()

    def test_outside_bounds_fails(self):
        result = _gf.scan_scope(
            write_paths=["/project/src/app.py", "/etc/passwd"],
            allowed_paths=["/project/src/"],
        )
        assert result.passed is False
        assert "/etc/passwd" in result.out_of_scope_paths
        assert result.blocked_reason is not None

    def test_none_allowed_paths_is_open_scope(self):
        result = _gf.scan_scope(
            write_paths=["/anywhere/at/all.txt"],
            allowed_paths=None,
        )
        assert result.passed is True

    def test_empty_write_paths_passes(self):
        result = _gf.scan_scope(write_paths=[], allowed_paths=["/project/"])
        assert result.passed is True

    def test_multiple_allowed_paths(self):
        result = _gf.scan_scope(
            write_paths=["/a/file.py", "/b/file.py"],
            allowed_paths=["/a/", "/b/"],
        )
        assert result.passed is True


# ---------------------------------------------------------------------------
# File write audit
# ---------------------------------------------------------------------------

class TestFileWriteAudit:

    def test_records_sha256_correctly(self):
        audit = FileWriteAudit()
        content = "hello world\n"
        record = audit.record_write("/tmp/test.txt", content)
        expected_sha = hashlib.sha256(content.encode("utf-8")).hexdigest()
        assert record.sha256 == expected_sha
        assert record.byte_count == len(content.encode("utf-8"))
        assert record.line_count == 1

    def test_line_count_no_trailing_newline(self):
        audit = FileWriteAudit()
        record = audit.record_write("/f.txt", "line1\nline2\nline3")
        assert record.line_count == 3

    def test_line_count_with_trailing_newline(self):
        audit = FileWriteAudit()
        record = audit.record_write("/f.txt", "line1\nline2\n")
        assert record.line_count == 2

    def test_audit_log_returns_frozen(self):
        audit = FileWriteAudit()
        audit.record_write("/a.txt", "aaa")
        audit.record_write("/b.txt", "bbb")
        log = audit.audit_log
        assert isinstance(log, tuple)
        assert len(log) == 2

    def test_audit_log_immutable(self):
        audit = FileWriteAudit()
        audit.record_write("/a.txt", "aaa")
        log = audit.audit_log
        with pytest.raises(AttributeError):
            log.append("x")  # type: ignore[attr-defined]

    def test_record_has_timestamp(self):
        audit = FileWriteAudit()
        record = audit.record_write("/a.txt", "data")
        assert record.timestamp > 0


# ---------------------------------------------------------------------------
# Diff stats
# ---------------------------------------------------------------------------

class TestDiffStats:

    def test_identical_content_zero_diff(self):
        audit = FileWriteAudit()
        content = "line1\nline2\n"
        record = audit.record_write("/f.txt", content)
        stats = audit.diff_against(record, content)
        assert stats.lines_added == 0
        assert stats.lines_removed == 0
        assert stats.lines_changed == 0

    def test_lines_added(self):
        audit = FileWriteAudit()
        old = "line1\nline2\n"
        record = audit.record_write("/f.txt", old)
        new = "line1\nline2\nline3\nline4\n"
        stats = audit.diff_against(record, new)
        assert stats.lines_added > 0

    def test_lines_removed(self):
        audit = FileWriteAudit()
        old = "line1\nline2\nline3\n"
        record = audit.record_write("/f.txt", old)
        new = "line1\n"
        stats = audit.diff_against(record, new)
        assert stats.lines_removed > 0


# ---------------------------------------------------------------------------
# Fail-closed behavior
# ---------------------------------------------------------------------------

class TestFailClosed:

    def test_scan_exception_blocks(self):
        with patch.object(SecretPattern, "scan_text", side_effect=RuntimeError("boom")):
            result = _gf.scan_prompt("anything")
        assert result.passed is False
        assert "fail-closed" in (result.blocked_reason or "").lower()

    def test_scan_exception_includes_reason(self):
        with patch.object(SecretPattern, "scan_text", side_effect=ValueError("bad input")):
            result = _gf.scan_prompt("test")
        assert result.passed is False
        assert "bad input" in (result.blocked_reason or "")


# ---------------------------------------------------------------------------
# Dataclass frozen enforcement
# ---------------------------------------------------------------------------

class TestFrozenDataclasses:

    def test_finding_is_frozen(self):
        f = Finding(pattern_name="x", line_number=1, redacted_match="x", severity="critical")
        with pytest.raises(AttributeError):
            f.pattern_name = "y"  # type: ignore[misc]

    def test_scan_result_is_frozen(self):
        r = GovernanceScanResult(passed=True, findings=(), blocked_reason=None)
        with pytest.raises(AttributeError):
            r.passed = False  # type: ignore[misc]

    def test_write_audit_record_is_frozen(self):
        r = WriteAuditRecord(file_path="/a", sha256="abc", byte_count=3, line_count=1, timestamp=0.0)
        with pytest.raises(AttributeError):
            r.file_path = "/b"  # type: ignore[misc]

    def test_write_diff_stats_is_frozen(self):
        d = WriteDiffStats(lines_added=1, lines_removed=0, lines_changed=0)
        with pytest.raises(AttributeError):
            d.lines_added = 5  # type: ignore[misc]
