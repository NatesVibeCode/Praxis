from __future__ import annotations

from surfaces.api.handlers import workflow_admin
from surfaces.mcp.tools import bugs as mcp_bugs


class _FakeReceiptIngester:
    def load_recent(self, since_hours: int = 24):
        del since_hours
        return [{"run_id": "run-1"}]

    def compute_pass_rate(self, receipts):
        del receipts
        return 1.0

    def top_failure_codes(self, receipts):
        del receipts
        return []


class _OrientBugTracker:
    def __init__(self, database_url: str) -> None:
        self._conn = type("_Conn", (), {"_database_url": database_url})()


class _OrientSubsystems:
    def __init__(self, database_url: str) -> None:
        self._database_url = database_url

    def get_receipt_ingester(self):
        return _FakeReceiptIngester()

    def get_bug_tracker(self):
        return _OrientBugTracker(self._database_url)


def test_orient_projects_database_authority_identity_as_first_class_truth(monkeypatch) -> None:
    database_url = "postgresql://nate:secret@repo.test:5432/praxis"
    fake_native_instance = {
        "praxis_instance_name": "praxis",
        "praxis_runtime_profile": "praxis",
        "repo_root": "/repo",
        "workdir": "/repo",
    }

    monkeypatch.setattr(workflow_admin, "dependency_truth_report", lambda scope="all": {"ok": True})
    monkeypatch.setattr(
        workflow_admin,
        "_handle_health",
        lambda subs, body: {
            "preflight": {"overall": "healthy"},
            "operator_snapshot": {},
            "proof_metrics": {},
            "schema_authority": {},
            "lane_recommendation": {"recommended_posture": "build"},
        },
    )
    monkeypatch.setattr(
        workflow_admin,
        "_build_standing_orders",
        lambda subs: [{"title": "orient authority"}],
    )
    monkeypatch.setattr(
        workflow_admin,
        "_workflow_env",
        lambda subs: {
            "WORKFLOW_DATABASE_URL": database_url,
            "WORKFLOW_DATABASE_AUTHORITY_SOURCE": "repo_env:/repo/.env",
            "PRAXIS_API_BASE_URL": "http://praxis.test:8420",
        },
    )
    monkeypatch.setattr(
        workflow_admin,
        "native_instance_contract",
        lambda env=None: fake_native_instance,
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_code_hotspots",
        lambda **kwargs: {"authority": "code_hotspots", "kwargs": kwargs},
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_bug_scoreboard",
        lambda **kwargs: {"authority": "bug_scoreboard", "kwargs": kwargs},
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_bug_triage_packet",
        lambda **kwargs: {
            "authority": "bug_triage_packet",
            "observability_state": "complete",
            "summary": {"live_defect": 1},
        },
    )
    monkeypatch.setattr(
        workflow_admin,
        "build_platform_observability",
        lambda **kwargs: {"authority": "platform_observability", "kwargs": kwargs},
    )

    result = workflow_admin._handle_orient(_OrientSubsystems(database_url), {})

    assert result["instruction_authority"]["packet_read_order"][:3] == [
        "standing_orders",
        "authority_envelope",
        "database_authority",
    ]
    assert result["authority_envelope"]["database_authority_ref"] == "/orient#database_authority"
    database_authority = result["database_authority"]
    assert database_authority["kind"] == "workflow_database_authority"
    assert database_authority["status"] == "ready"
    assert database_authority["authority_source"] == "repo_env:/repo/.env"
    assert database_authority["fingerprint"].startswith("workflow_pool:")
    assert database_authority["fingerprint"] == database_authority["observed_fingerprint"]
    assert database_authority["comparison_field"] == "fingerprint"
    runtime_binding = result["primitive_contracts"]["runtime_binding"]["database"]
    assert runtime_binding["status"] == "ready"
    assert runtime_binding["fingerprint"] == database_authority["fingerprint"]
    assert runtime_binding["comparison_field"] == "fingerprint"


def test_praxis_bugs_degrades_visibly_when_live_bug_connection_drifts(monkeypatch) -> None:
    class _Tracker:
        def __init__(self) -> None:
            self._conn = type(
                "_Conn",
                (),
                {"_database_url": "postgresql://stale.example:5432/praxis"},
            )()

        def stats(self):
            return {"total": 1, "open_count": 1}

    class _Subs:
        def _postgres_env(self):
            return {
                "WORKFLOW_DATABASE_URL": "postgresql://live.example:5432/praxis",
                "WORKFLOW_DATABASE_AUTHORITY_SOURCE": "repo_env:/repo/.env",
            }

        def get_bug_tracker(self):
            return _Tracker()

        def get_bug_tracker_mod(self):
            return type(
                "_BugTrackerModule",
                (),
                {
                    "BugStatus": type(
                        "_BugStatus",
                        (),
                        {
                            "FIXED": "FIXED",
                            "WONT_FIX": "WONT_FIX",
                            "DEFERRED": "DEFERRED",
                        },
                    )()
                },
            )()

    monkeypatch.setattr(mcp_bugs, "_subs", _Subs())

    payload = mcp_bugs.tool_praxis_bugs({"action": "stats"})

    assert payload["observability_state"] == "degraded"
    assert payload["database_authority"]["status"] == "degraded"
    assert payload["database_authority"]["authority_source"] == "repo_env:/repo/.env"
    assert payload["database_authority"]["fingerprint"].startswith("workflow_pool:")
    assert payload["database_authority"]["observed_fingerprint"].startswith("workflow_pool:")
    assert (
        payload["database_authority"]["fingerprint"]
        != payload["database_authority"]["observed_fingerprint"]
    )
    assert payload["warnings"] == [
        "Resolved workflow DB authority does not match the live surface connection "
        "fingerprint; treat results as degraded until the surface is rebound to "
        "the canonical authority."
    ]
