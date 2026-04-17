from __future__ import annotations

import io
import json
from types import SimpleNamespace

from runtime import receipt_store
import runtime.verifier_authority as verifier_authority
from surfaces.cli.commands import workflow as workflow_commands


class _AuthorityConn:
    def __init__(self) -> None:
        self.verification_inserts: list[tuple[object, ...]] = []
        self.healing_inserts: list[tuple[object, ...]] = []

    def execute(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith(
            "SELECT verifier_ref, display_name, description, verifier_kind, verification_ref, builtin_ref, default_inputs, enabled, decision_ref FROM verifier_registry WHERE verifier_ref = $1"
        ):
            verifier_ref = args[0]
            rows = {
                "verifier.platform.receipt_provenance": {
                    "verifier_ref": "verifier.platform.receipt_provenance",
                    "display_name": "Receipt Provenance",
                    "description": "Check receipt provenance",
                    "verifier_kind": "builtin",
                    "verification_ref": None,
                    "builtin_ref": "receipt_provenance",
                    "default_inputs": {},
                    "enabled": True,
                    "decision_ref": "decision.verifier",
                },
                "verifier.job.python.py_compile": {
                    "verifier_ref": "verifier.job.python.py_compile",
                    "display_name": "Py Compile",
                    "description": "Compile Python files",
                    "verifier_kind": "verification_ref",
                    "verification_ref": "verification.python.py_compile",
                    "builtin_ref": None,
                    "default_inputs": {},
                    "enabled": True,
                    "decision_ref": "decision.verifier.job",
                },
            }
            row = rows.get(verifier_ref)
            return [row] if row else []
        if normalized.startswith(
            "SELECT healer_ref FROM verifier_healer_bindings WHERE verifier_ref = $1 AND enabled = TRUE ORDER BY healer_ref ASC LIMIT 1"
        ):
            return [{"healer_ref": "healer.platform.receipt_provenance_backfill"}]
        if normalized.startswith(
            "SELECT healer_ref FROM verifier_healer_bindings WHERE verifier_ref = $1 AND enabled = TRUE ORDER BY healer_ref ASC"
        ):
            return [{"healer_ref": "healer.platform.receipt_provenance_backfill"}]
        if normalized.startswith(
            "SELECT healer_ref, display_name, description, executor_kind, action_ref, auto_mode, safety_mode, enabled, decision_ref FROM healer_registry WHERE healer_ref = $1"
        ):
            return [
                {
                    "healer_ref": "healer.platform.receipt_provenance_backfill",
                    "display_name": "Receipt Provenance Backfill",
                    "description": "Repair receipt provenance",
                    "executor_kind": "builtin",
                    "action_ref": "receipt_provenance_backfill",
                    "auto_mode": "assisted",
                    "safety_mode": "guarded",
                    "enabled": True,
                    "decision_ref": "decision.healer",
                }
            ]
        if normalized.startswith(
            "SELECT verifier_ref, display_name, description, verifier_kind, verification_ref, builtin_ref, default_inputs, enabled, decision_ref FROM verifier_registry ORDER BY verifier_ref ASC"
        ):
            return [
                {
                    "verifier_ref": "verifier.job.python.py_compile",
                    "display_name": "Py Compile",
                    "description": "Compile Python files",
                    "verifier_kind": "verification_ref",
                    "verification_ref": "verification.python.py_compile",
                    "builtin_ref": None,
                    "default_inputs": {},
                    "enabled": True,
                    "decision_ref": "decision.verifier.job",
                },
                {
                    "verifier_ref": "verifier.platform.receipt_provenance",
                    "display_name": "Receipt Provenance",
                    "description": "Check receipt provenance",
                    "verifier_kind": "builtin",
                    "verification_ref": None,
                    "builtin_ref": "receipt_provenance",
                    "default_inputs": {},
                    "enabled": True,
                    "decision_ref": "decision.verifier",
                },
            ]
        if normalized.startswith(
            "SELECT verification_ref, display_name, executor_kind, argv_template, template_inputs, default_timeout_seconds, enabled FROM verification_registry WHERE verification_ref = ANY($1::text[])"
        ):
            refs = {str(item) for item in args[0]}
            rows = []
            if "verification.python.py_compile" in refs:
                rows.append(
                    {
                        "verification_ref": "verification.python.py_compile",
                        "display_name": "Python Bytecode Compile",
                        "executor_kind": "argv",
                        "argv_template": ["python3", "-m", "py_compile", "{path}"],
                        "template_inputs": ["path"],
                        "default_timeout_seconds": 60,
                        "enabled": True,
                    }
                )
            return rows
        if normalized.startswith(
            "SELECT healer_ref, display_name, description, executor_kind, action_ref, auto_mode, safety_mode, enabled, decision_ref FROM healer_registry ORDER BY healer_ref ASC"
        ):
            return [
                {
                    "healer_ref": "healer.platform.receipt_provenance_backfill",
                    "display_name": "Receipt Provenance Backfill",
                    "description": "Repair receipt provenance",
                    "executor_kind": "builtin",
                    "action_ref": "receipt_provenance_backfill",
                    "auto_mode": "assisted",
                    "safety_mode": "guarded",
                    "enabled": True,
                    "decision_ref": "decision.healer",
                }
            ]
        if normalized.startswith(
            "SELECT binding_ref, verifier_ref, healer_ref, enabled, binding_revision, decision_ref FROM verifier_healer_bindings ORDER BY verifier_ref ASC, healer_ref ASC"
        ):
            return [
                {
                    "binding_ref": "binding.one",
                    "verifier_ref": "verifier.platform.receipt_provenance",
                    "healer_ref": "healer.platform.receipt_provenance_backfill",
                    "enabled": True,
                    "binding_revision": "binding.rev",
                    "decision_ref": "decision.binding",
                }
            ]
        if normalized.startswith("INSERT INTO verification_runs"):
            self.verification_inserts.append(args)
            return []
        if normalized.startswith("INSERT INTO healing_runs"):
            self.healing_inserts.append(args)
            return []
        raise AssertionError(query)

    def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT COUNT(*) AS recent_failures FROM verification_runs"):
            return {"recent_failures": 0}
        if normalized.startswith("SELECT COUNT(*) AS recent_failures FROM healing_runs"):
            return {"recent_failures": 0}
        if normalized.startswith("SELECT COALESCE(outputs->>'control_plane_bug_fingerprint', '') AS fingerprint FROM verification_runs"):
            return {}
        raise AssertionError(query)


def test_registry_snapshot_lists_verifiers_healers_and_bindings() -> None:
    snapshot = verifier_authority.registry_snapshot(conn=_AuthorityConn())

    assert [item["verifier_ref"] for item in snapshot["verifiers"]] == [
        "verifier.job.python.py_compile",
        "verifier.platform.receipt_provenance",
    ]
    assert snapshot["healers"][0]["healer_ref"] == "healer.platform.receipt_provenance_backfill"
    assert snapshot["bindings"][0]["verifier_ref"] == "verifier.platform.receipt_provenance"


def test_run_registered_verifier_records_failed_builtin_and_suggests_healer(
    monkeypatch,
) -> None:
    conn = _AuthorityConn()
    monkeypatch.setattr(
        verifier_authority,
        "_run_builtin_verifier",
        lambda builtin_ref, *, inputs, conn=None: ("failed", {"summary": {"builtin_ref": builtin_ref}}),
    )
    monkeypatch.setattr(verifier_authority, "_maybe_promote_verifier_bug", lambda **_kwargs: None)

    payload = verifier_authority.run_registered_verifier(
        "verifier.platform.receipt_provenance",
        conn=conn,
    )

    assert payload["status"] == "failed"
    assert payload["suggested_healer_ref"] == "healer.platform.receipt_provenance_backfill"
    assert len(conn.verification_inserts) == 1


def test_run_registered_verifier_wraps_verification_registry_executor(
    monkeypatch,
) -> None:
    import runtime.verification as verification

    conn = _AuthorityConn()
    observed_commands = []
    summary = SimpleNamespace(
        all_passed=True,
        to_json=lambda: {"total": 1, "passed": 1, "failed": 0, "all_passed": True, "results": []},
    )
    monkeypatch.setattr(
        verification,
        "run_verify",
        lambda commands, **_kwargs: observed_commands.extend(commands) or ("result",),
    )
    monkeypatch.setattr(verification, "summarize_verification", lambda *_args, **_kwargs: summary)
    monkeypatch.setattr(verifier_authority, "_maybe_promote_verifier_bug", lambda **_kwargs: None)

    payload = verifier_authority.run_registered_verifier(
        "verifier.job.python.py_compile",
        inputs={"path": "sample.py"},
        conn=conn,
    )

    assert payload["status"] == "passed"
    assert payload["target_ref"] == "verifier.job.python.py_compile"
    assert payload["outputs"]["verification_ref"] == "verification.python.py_compile"
    assert observed_commands[0].argv == ("python3", "-m", "py_compile", "sample.py")
    assert len(conn.verification_inserts) == 1


def test_run_registered_verifier_can_skip_control_plane_bug_promotion(
    monkeypatch,
) -> None:
    import runtime.verification as verification

    conn = _AuthorityConn()
    summary = SimpleNamespace(
        all_passed=False,
        to_json=lambda: {"total": 1, "passed": 0, "failed": 1, "all_passed": False, "results": []},
    )
    monkeypatch.setattr(verification, "run_verify", lambda *_args, **_kwargs: ("result",))
    monkeypatch.setattr(verification, "summarize_verification", lambda *_args, **_kwargs: summary)
    promoted: list[dict[str, object]] = []
    monkeypatch.setattr(
        verifier_authority,
        "_maybe_promote_verifier_bug",
        lambda **kwargs: promoted.append(kwargs) or "BUG-PROMOTED",
    )

    payload = verifier_authority.run_registered_verifier(
        "verifier.job.python.py_compile",
        inputs={"path": "sample.py"},
        conn=conn,
        promote_bug=False,
    )

    assert payload["status"] == "failed"
    assert payload["bug_id"] is None
    assert promoted == []


def test_run_registered_healer_reruns_target_verifier_and_records_run(
    monkeypatch,
) -> None:
    conn = _AuthorityConn()
    monkeypatch.setattr(
        verifier_authority,
        "_run_builtin_healer",
        lambda action_ref, *, inputs, conn=None: ("succeeded", {"action_ref": action_ref}),
    )
    monkeypatch.setattr(
        verifier_authority,
        "run_registered_verifier",
        lambda verifier_ref, **_kwargs: {
            "verification_run_id": "verification_run:test",
            "status": "passed",
            "verifier": {"verifier_ref": verifier_ref},
        },
    )
    monkeypatch.setattr(verifier_authority, "_maybe_resolve_verifier_bug", lambda **_kwargs: None)

    payload = verifier_authority.run_registered_healer(
        verifier_ref="verifier.platform.receipt_provenance",
        conn=conn,
    )

    assert payload["status"] == "succeeded"
    assert payload["outputs"]["post_verification"]["status"] == "passed"
    assert len(conn.healing_inserts) == 1


def test_verify_platform_command_lists_registry(monkeypatch) -> None:
    monkeypatch.setattr(
        workflow_commands,
        "registry_snapshot",
        lambda: {"verifiers": [{"verifier_ref": "verifier.platform.schema_authority"}]},
        raising=False,
    )
    monkeypatch.setattr(
        __import__("runtime.verifier_authority", fromlist=["registry_snapshot"]),
        "registry_snapshot",
        lambda: {"verifiers": [{"verifier_ref": "verifier.platform.schema_authority"}]},
    )
    buf = io.StringIO()

    exit_code = workflow_commands._verify_platform_command([], stdout=buf)

    assert exit_code == 0
    payload = json.loads(buf.getvalue())
    assert payload["verifiers"][0]["verifier_ref"] == "verifier.platform.schema_authority"


def test_verify_command_rejects_legacy_verify_receipt(monkeypatch) -> None:
    import runtime.verification as verification

    class _Receipt:
        def to_dict(self) -> dict[str, object]:
            return {
                "run_id": "run.legacy",
                "outputs": {},
                "workdir": "/tmp",
                "verify": [
                    {
                        "verification_ref": "verification.python.py_compile",
                        "inputs": {"path": "sample.py"},
                    }
                ],
            }

    monkeypatch.setattr(receipt_store, "load_receipt", lambda _receipt_ref: _Receipt())
    monkeypatch.setattr(receipt_store, "find_receipt_by_run_id", lambda _receipt_ref: None)
    monkeypatch.setattr(
        verification,
        "resolve_verify_commands",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("legacy verify bindings should not be resolved")),
    )
    buf = io.StringIO()

    exit_code = workflow_commands._verify_command(["receipt.legacy"], stdout=buf)

    assert exit_code == 1
    assert "no verify refs found in receipt" in buf.getvalue()


def test_heal_command_runs_registered_healer(monkeypatch) -> None:
    monkeypatch.setattr(
        __import__("runtime.verifier_authority", fromlist=["run_registered_healer"]),
        "run_registered_healer",
        lambda **_kwargs: {"status": "succeeded", "healing_run_id": "healing_run:test"},
    )
    buf = io.StringIO()

    exit_code = workflow_commands._heal_command(
        ["--verifier-ref", "verifier.platform.receipt_provenance"],
        stdout=buf,
    )

    assert exit_code == 0
    payload = json.loads(buf.getvalue())
    assert payload["healing_run_id"] == "healing_run:test"


class _ReceiptProvenanceConn:
    def fetchrow(self, query: str, *args):
        normalized = " ".join(query.split())
        if normalized.startswith("SELECT COUNT(*) AS receipts_total, COUNT(*) FILTER ( WHERE outputs ? 'git_provenance' ) AS receipts_with_git_provenance"):
            return {
                "receipts_total": 3,
                "receipts_with_git_provenance": 2,
                "receipts_with_repo_snapshot_ref": 2,
                "duplicated_git_fields": 0,
                "unavailable_git_provenance": 0,
            }
        raise AssertionError(query)


def test_builtin_verify_receipt_provenance_fails_when_receipts_lack_git_provenance(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        receipt_store,
        "proof_metrics",
        lambda conn=None: {
            "receipts": {
                "total": 3,
                "with_git_provenance": 2,
                "with_repo_snapshot_ref": 2,
            }
        },
        raising=False,
    )

    status, outputs = verifier_authority._builtin_verify_receipt_provenance(
        inputs={},
        conn=_ReceiptProvenanceConn(),
    )

    assert status == "failed"
    assert outputs["summary"]["missing_git_receipts"] == 1


def test_run_registered_verifier_records_error_runs_when_execution_crashes(
    monkeypatch,
) -> None:
    conn = _AuthorityConn()
    monkeypatch.setattr(
        verifier_authority,
        "_run_builtin_verifier",
        lambda builtin_ref, *, inputs, conn=None: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(verifier_authority, "_maybe_promote_verifier_bug", lambda **_kwargs: None)

    payload = verifier_authority.run_registered_verifier(
        "verifier.platform.receipt_provenance",
        conn=conn,
    )

    assert payload["status"] == "error"
    assert payload["outputs"]["exception_type"] == "RuntimeError"
    assert len(conn.verification_inserts) == 1
    assert conn.verification_inserts[0][4] == "error"


def test_run_registered_healer_records_error_runs_when_execution_crashes(
    monkeypatch,
) -> None:
    conn = _AuthorityConn()
    monkeypatch.setattr(
        verifier_authority,
        "_run_builtin_healer",
        lambda action_ref, *, inputs, conn=None: (_ for _ in ()).throw(RuntimeError("boom")),
    )
    monkeypatch.setattr(verifier_authority, "_maybe_promote_healer_bug", lambda **_kwargs: None)

    payload = verifier_authority.run_registered_healer(
        verifier_ref="verifier.platform.receipt_provenance",
        conn=conn,
    )

    assert payload["status"] == "error"
    assert payload["outputs"]["action_outputs"]["exception_type"] == "RuntimeError"
    assert len(conn.healing_inserts) == 1
    assert conn.healing_inserts[0][5] == "error"


def test_run_registered_verifier_promotes_repeated_failures_into_bug(
    monkeypatch,
) -> None:
    conn = _AuthorityConn()
    evidence_links: list[tuple[str, str, str, str]] = []
    monkeypatch.setattr(
        verifier_authority,
        "_run_builtin_verifier",
        lambda builtin_ref, *, inputs, conn=None: ("failed", {"summary": {"builtin_ref": builtin_ref}}),
    )
    monkeypatch.setattr(
        verifier_authority,
        "_load_open_bug_by_fingerprint",
        lambda **_kwargs: None,
    )
    monkeypatch.setattr(
        verifier_authority,
        "_recent_verification_failure_count",
        lambda **_kwargs: 3,
    )
    monkeypatch.setattr(
        verifier_authority,
        "_file_control_plane_bug",
        lambda **_kwargs: SimpleNamespace(bug_id="BUG-VERIFY"),
    )
    monkeypatch.setattr(
        verifier_authority,
        "_link_bug_evidence",
        lambda **kwargs: evidence_links.append(
            (
                kwargs["bug_id"],
                kwargs["evidence_kind"],
                kwargs["evidence_ref"],
                kwargs["evidence_role"],
            )
        ),
    )

    payload = verifier_authority.run_registered_verifier(
        "verifier.platform.receipt_provenance",
        conn=conn,
    )

    assert payload["bug_id"] == "BUG-VERIFY"
    assert evidence_links[0][0] == "BUG-VERIFY"
    assert evidence_links[0][1] == "verification_run"
    assert evidence_links[0][3] == "observed_in"


def test_run_registered_healer_resolves_bug_after_post_verification_passes(
    monkeypatch,
) -> None:
    conn = _AuthorityConn()
    monkeypatch.setattr(
        verifier_authority,
        "_run_builtin_healer",
        lambda action_ref, *, inputs, conn=None: ("succeeded", {"action_ref": action_ref}),
    )
    monkeypatch.setattr(
        verifier_authority,
        "run_registered_verifier",
        lambda verifier_ref, **_kwargs: {
            "verification_run_id": "verification_run:post",
            "status": "passed",
            "verifier": {"verifier_ref": verifier_ref},
        },
    )
    monkeypatch.setattr(
        verifier_authority,
        "_maybe_resolve_verifier_bug",
        lambda **_kwargs: "BUG-VERIFY",
    )

    payload = verifier_authority.run_registered_healer(
        verifier_ref="verifier.platform.receipt_provenance",
        conn=conn,
    )

    assert payload["status"] == "succeeded"
    assert payload["resolved_bug_id"] == "BUG-VERIFY"
