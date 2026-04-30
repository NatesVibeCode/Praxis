from __future__ import annotations

from storage.postgres import object_truth_repository as repo


class _RecordingConn:
    def __init__(self) -> None:
        self.fetchrow_calls: list[tuple[str, tuple[object, ...]]] = []
        self.fetch_calls: list[tuple[str, tuple[object, ...]]] = []
        self.execute_calls: list[tuple[str, tuple[object, ...]]] = []
        self.batch_calls: list[tuple[str, list[tuple[object, ...]]]] = []

    def fetchrow(self, sql: str, *args):
        self.fetchrow_calls.append((sql, args))
        if "object_truth_mdm_resolution_packets" in sql:
            return {
                "packet_ref": args[0],
                "resolution_packet_digest": args[1],
                "client_ref": args[2],
                "entity_type": args[3],
                "identity_cluster_count": args[5],
                "field_comparison_count": args[6],
                "packet_json": args[11],
            }
        return None

    def fetch(self, sql: str, *args):
        self.fetch_calls.append((sql, args))
        return []

    def execute(self, sql: str, *args):
        self.execute_calls.append((sql, args))
        return []

    def execute_many(self, sql: str, rows: list[tuple[object, ...]]) -> None:
        self.batch_calls.append((sql, rows))


def _packet() -> dict[str, object]:
    return {
        "packet_ref": "packet.1",
        "resolution_packet_digest": "packet.digest",
        "client_ref": "client.acme",
        "entity_type": "organization",
        "as_of": "2026-04-30T16:00:00Z",
        "identity_clusters": [
            {
                "cluster_id": "cluster.1",
                "identity_cluster_digest": "cluster.digest",
                "entity_type": "organization",
                "review_status": "auto-accepted",
                "cluster_confidence": 0.9,
                "member_records": [{}, {}],
            }
        ],
        "field_comparisons": [
            {
                "field_comparison_digest": "comparison.digest",
                "cluster_id": "cluster.1",
                "canonical_record_id": "canonical.organization.acme",
                "canonical_field": "legal_name",
                "entity_type": "organization",
                "selection_state": "selected",
                "conflict_flag": False,
                "consensus_flag": True,
                "typed_gaps": [],
            }
        ],
        "normalization_rules": [
            {
                "rule_ref": "rule.1",
                "normalization_rule_digest": "rule.digest",
                "entity_type": "organization",
                "field_name": "legal_name",
                "reversible": True,
                "loss_risk": "lossy-source-preserved",
            }
        ],
        "authority_evidence": [
            {
                "authority_evidence_digest": "authority.digest",
                "entity_type": "organization",
                "field_name": "legal_name",
                "source_system": "hubspot",
                "authority_rank": 1,
                "evidence_type": "system_stewardship_assignment",
                "evidence_reference": "policy.hubspot.legal_name",
            }
        ],
        "hierarchy_signals": [
            {
                "hierarchy_signal_digest": "hierarchy.digest",
                "entity_type": "organization",
                "signal_type": "parent-child",
                "source_system": "hubspot",
                "source_record_id": "hs-001",
                "authoritative": True,
            }
        ],
        "typed_gaps": [
            {
                "gap_id": "gap.1",
                "gap_digest": "gap.digest",
                "entity_type": "organization",
                "field_name": "legal_name",
                "gap_type": "manual-review-pending",
                "severity": "medium",
            }
        ],
    }


def test_persist_mdm_resolution_packet_writes_packet_and_child_records() -> None:
    conn = _RecordingConn()

    result = repo.persist_mdm_resolution_packet(
        conn,
        packet=_packet(),
        observed_by_ref="operator:nate",
        source_ref="phase_03_test",
    )

    assert "INSERT INTO object_truth_mdm_resolution_packets" in conn.fetchrow_calls[0][0]
    assert len(conn.execute_calls) == 6
    assert len(conn.batch_calls) == 6
    assert any("object_truth_mdm_identity_clusters" in call[0] for call in conn.batch_calls)
    assert any("object_truth_mdm_typed_gaps" in call[0] for call in conn.batch_calls)
    assert result["packet"]["packet_ref"] == "packet.1"
    assert result["identity_cluster_count"] == 1
    assert result["field_comparison_count"] == 1
    assert result["typed_gap_count"] == 1
