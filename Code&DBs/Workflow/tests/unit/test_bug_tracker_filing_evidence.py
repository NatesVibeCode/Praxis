from __future__ import annotations

import uuid
from datetime import datetime, timezone
from runtime.bug_tracker import BugTracker, BugSeverity, BugCategory, BugStatus
from runtime.bug_evidence import EVIDENCE_ROLE_DISCOVERED_BY

def test_file_bug_automatically_links_evidence(monkeypatch) -> None:
    observed_inserts = []
    
    class FakeConn:
        def execute(self, query, *args):
            if "INSERT INTO bugs" in query:
                return []
            if "INSERT INTO bug_evidence_links" in query:
                observed_inserts.append(args)
                return []
            return []
            
        def fetchrow(self, query, *args):
            # Minimal row for _row_to_bug
            return {
                "bug_id": args[0],
                "bug_key": "test_bug",
                "title": "test",
                "severity": "P2",
                "status": "OPEN",
                "priority": "P2",
                "category": "OTHER",
                "description": "test",
                "summary": "test",
                "filed_at": datetime.now(timezone.utc),
                "updated_at": datetime.now(timezone.utc),
                "resolved_at": None,
                "created_at": datetime.now(timezone.utc),
                "filed_by": "alice",
                "tags": "",
                "source_kind": "manual",
                "discovered_in_run_id": "run123",
                "discovered_in_receipt_id": "receipt456",
                "owner_ref": None,
                "source_issue_id": None,
                "decision_ref": "dec1",
                "resolution_summary": None,
                "resume_context": "{}",
            }
            
        def fetchval(self, query, *args):
            return True # Pretend run/receipt exist

    tracker = BugTracker(FakeConn())
    
    bug, _ = tracker.file_bug(
        title="Test Bug",
        severity=BugSeverity.P2,
        category=BugCategory.OTHER,
        description="Test description",
        filed_by="alice",
        discovered_in_run_id="run123",
        discovered_in_receipt_id="receipt456",
    )
    
    # Check that two evidence links were created
    assert len(observed_inserts) == 2
    
    # Check discovery receipt link
    receipt_link = next(arg for arg in observed_inserts if arg[2] == "receipt")
    assert receipt_link[3] == "receipt456"
    assert receipt_link[4] == EVIDENCE_ROLE_DISCOVERED_BY
    
    # Check discovery run link
    run_link = next(arg for arg in observed_inserts if arg[2] == "run")
    assert run_link[3] == "run123"
    assert run_link[4] == EVIDENCE_ROLE_DISCOVERED_BY
