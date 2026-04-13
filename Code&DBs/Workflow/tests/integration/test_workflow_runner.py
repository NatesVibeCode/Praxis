"""Integration tests for the self-hosting dispatch runner."""

from __future__ import annotations

import concurrent.futures
import itertools
import json
import os
import sys
import tempfile
import threading
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Path setup: add the Workflow root so sibling imports work
# ---------------------------------------------------------------------------

_WORKFLOW_ROOT = str(Path(__file__).resolve().parent.parent.parent)
if _WORKFLOW_ROOT not in sys.path:
    sys.path.insert(0, _WORKFLOW_ROOT)

from _pg_test_conn import get_test_conn

# Direct file import to avoid triggering surfaces/cli/__init__.py
# which chains into observability modules requiring Python 3.10+
import importlib.util

def _direct_import(name: str, filepath: str):
    spec = importlib.util.spec_from_file_location(name, filepath)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod

_workflow_runner = _direct_import(
    "workflow_runner",
    os.path.join(_WORKFLOW_ROOT, "surfaces", "cli", "workflow_runner.py"),
)

WorkflowSpec = _workflow_runner.WorkflowSpec
WorkflowSpecError = _workflow_runner.WorkflowSpecError
WorkflowRunner = _workflow_runner.WorkflowRunner
RunResult = _workflow_runner.RunResult
JobExecution = _workflow_runner.JobExecution
VerifyResult = _workflow_runner.VerifyResult
Telemetry = _workflow_runner.Telemetry

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

_REPO_ROOT = str(Path(__file__).resolve().parent.parent.parent.parent.parent)
_AGENTS_JSON = os.path.join(_REPO_ROOT, "config", "agents.json")
_SAMPLE_SPEC = os.path.join(
    _REPO_ROOT,
    "artifacts", "dispatch",
    "DAGW35A_foundation_composite_scorer.queue.json",
)


@pytest.fixture
def tmp_receipts(tmp_path):
    """Return a temporary directory for receipts."""
    d = tmp_path / "receipts"
    d.mkdir()
    return str(d)


@pytest.fixture
def tmp_db(tmp_path):
    """Return a temporary DB path."""
    return str(tmp_path / "test.db")


@pytest.fixture
def tmp_constraints_db(tmp_path):
    """Return a temporary constraints DB path."""
    return str(tmp_path / "constraints.db")


def _write_spec(tmp_path: Path, spec_dict: dict) -> str:
    """Write a spec dict to a temp .queue.json file and return the path."""
    p = tmp_path / "test.queue.json"
    p.write_text(json.dumps(spec_dict), encoding="utf-8")
    return str(p)


def _minimal_spec() -> dict:
    """Return a minimal valid spec dict."""
    return {
        "name": "Test Spec",
        "workflow_id": "test_spec_001",
        "phase": "TEST",
        "outcome_goal": "Verify the runner works",
        "anti_requirements": ["no production changes"],
        "jobs": [
            {
                "label": "test_job_1",
                "stage": "build",
                "agent": "anthropic/claude-sonnet-4",
                "prompt": "Echo hello world",
                "workdir": "/tmp",
                "scope": {
                    "read": [],
                    "write": ["test_output.py"],
                },
                "verify_refs": ["verify_ref.python.py_compile.test_output.py"],
            }
        ],
        "verify_refs": ["verify_ref.python.py_compile.test_output.py"],
    }


# ---------------------------------------------------------------------------
# Tests: WorkflowSpec.load
# ---------------------------------------------------------------------------


class TestWorkflowSpecLoad:
    """Tests for WorkflowSpec parsing and validation."""

    def test_load_real_spec(self):
        """WorkflowSpec.load parses a real queue.json from artifacts/workflow/."""
        if not os.path.exists(_SAMPLE_SPEC):
            pytest.skip("Sample spec not found")
        spec = WorkflowSpec.load(_SAMPLE_SPEC)
        assert spec.name
        assert spec.workflow_id
        assert spec.phase
        assert len(spec.jobs) > 0
        assert spec.jobs[0]["label"]

    def test_load_minimal_spec(self, tmp_path):
        """WorkflowSpec.load handles a minimal valid spec."""
        path = _write_spec(tmp_path, _minimal_spec())
        spec = WorkflowSpec.load(path)
        assert spec.name == "Test Spec"
        assert spec.workflow_id == "test_spec_001"
        assert len(spec.jobs) == 1

    def test_missing_file_raises(self):
        """WorkflowSpec.load raises on missing file."""
        with pytest.raises(WorkflowSpecError, match="not found"):
            WorkflowSpec.load("/nonexistent/path.json")

    def test_missing_required_fields(self, tmp_path):
        """WorkflowSpec.load raises when required fields are absent."""
        path = _write_spec(tmp_path, {"name": "incomplete"})
        with pytest.raises(WorkflowSpecError, match="Missing required fields"):
            WorkflowSpec.load(path)

    def test_empty_jobs_raises(self, tmp_path):
        """WorkflowSpec.load raises when jobs list is empty."""
        data = _minimal_spec()
        data["jobs"] = []
        path = _write_spec(tmp_path, data)
        with pytest.raises(WorkflowSpecError, match="non-empty"):
            WorkflowSpec.load(path)

            WorkflowSpec.load(path)

    def test_job_missing_prompt_raises(self, tmp_path):
        """WorkflowSpec.load raises when a job is missing its prompt."""
        data = _minimal_spec()
        data["jobs"][0].pop("prompt")
        path = _write_spec(tmp_path, data)
        with pytest.raises(WorkflowSpecError, match="missing 'prompt'"):
            WorkflowSpec.load(path)

    def test_route_id_is_not_accepted_as_job_agent(self, tmp_path):
        """WorkflowSpec.load rejects legacy route_id job routing."""
        data = _minimal_spec()
        data["jobs"][0].pop("agent")
        data["jobs"][0]["route_id"] = "legacy/review"
        path = _write_spec(tmp_path, data)
        with pytest.raises(WorkflowSpecError, match="legacy 'route_id'"):
            WorkflowSpec.load(path)

    def test_verify_is_not_accepted_at_spec_level(self, tmp_path):
        """WorkflowSpec.load rejects legacy top-level verify bindings."""
        data = _minimal_spec()
        data["verify"] = [
            {
                "verification_ref": "verification.python.py_compile",
                "inputs": {"path": "sample.py"},
            }
        ]
        path = _write_spec(tmp_path, data)
        with pytest.raises(WorkflowSpecError, match="legacy 'verify'"):
            WorkflowSpec.load(path)

    def test_verify_is_not_accepted_on_jobs(self, tmp_path):
        """WorkflowSpec.load rejects legacy per-job verify bindings."""
        data = _minimal_spec()
        data["jobs"][0]["verify"] = [
            {
                "verification_ref": "verification.python.py_compile",
                "inputs": {"path": "sample.py"},
            }
        ]
        path = _write_spec(tmp_path, data)
        with pytest.raises(WorkflowSpecError, match="legacy 'verify'"):
            WorkflowSpec.load(path)

    def test_summary(self, tmp_path):
        """WorkflowSpec.summary returns structured data."""
        path = _write_spec(tmp_path, _minimal_spec())
        spec = WorkflowSpec.load(path)
        s = spec.summary()
        assert s["name"] == "Test Spec"
        assert s["job_count"] == 1
        assert s["job_labels"] == ["test_job_1"]


# ---------------------------------------------------------------------------
# Tests: WorkflowRunner initialization
# ---------------------------------------------------------------------------


class TestWorkflowRunnerInit:
    """Tests for WorkflowRunner initialization with real config."""

    def test_init_with_real_config(self, tmp_receipts, tmp_db):
        """WorkflowRunner initializes with the real agents.json."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")
        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=tmp_receipts,
            db_path=tmp_db,
        )
        assert runner._agent_registry is not None

    def test_init_creates_receipts_dir(self, tmp_path):
        """WorkflowRunner creates the receipts directory if it does not exist."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")
        receipts = str(tmp_path / "new_receipts")
        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=receipts,
            db_path=str(tmp_path / "test.db"),
        )
        assert os.path.isdir(receipts)


# ---------------------------------------------------------------------------
# Tests: dry_run
# ---------------------------------------------------------------------------


class TestDryRun:
    """Tests for dry-run mode."""

    def test_dry_run_returns_results(self, tmp_path):
        """Dry run returns RunResult without executing."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")
        spec_path = _write_spec(tmp_path, _minimal_spec())
        spec = WorkflowSpec.load(spec_path)

        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=str(tmp_path / "receipts"),
            db_path=str(tmp_path / "test.db"),
        )

        result = runner.run_workflow(spec, dry_run=True)
        assert isinstance(result, RunResult)
        assert result.total_jobs == 1
        assert result.succeeded == 1
        assert result.failed == 0
        assert result.blocked == 0

    def test_dry_run_job_status(self, tmp_path):
        """Each job in dry run reports succeeded status."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")
        spec_path = _write_spec(tmp_path, _minimal_spec())
        spec = WorkflowSpec.load(spec_path)

        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=str(tmp_path / "receipts"),
            db_path=str(tmp_path / "test.db"),
        )

        result = runner.run_workflow(spec, dry_run=True)
        jr = result.job_results[0]
        assert jr.status == "succeeded"
        assert jr.exit_code == 0
        assert "dry-run" in jr.stdout.lower()

    def test_execute_job_does_not_fall_back_to_route_id(self, tmp_path):
        """Legacy route_id should not be accepted as a job agent slug."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")
        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=str(tmp_path / "receipts"),
            db_path=str(tmp_path / "test.db"),
        )

        with pytest.raises(KeyError):
            runner._execute_job(
                {"label": "legacy_job", "route_id": "legacy/review"},
                agent_config=None,
                prompt="Echo hello world",
                workdir="/tmp",
                timeout=1,
            )


class TestLiveExecutionGuard:
    """Tests that the legacy runner no longer spawns live CLI subprocesses."""

    def test_cli_transport_is_blocked_in_legacy_runner(self, monkeypatch):
        runner = WorkflowRunner.__new__(WorkflowRunner)
        job = {"label": "job-1", "agent": "anthropic/claude-sonnet-4"}
        agent_config = type(
            "AgentConfig",
            (),
            {
                "provider": "anthropic",
                "model": "claude-sonnet-4",
                "wrapper_command": None,
                "timeout_seconds": 60,
            },
        )()

        monkeypatch.setattr(
            _workflow_runner,
            "resolve_execution_transport",
            lambda _config: type("Transport", (), {"transport_kind": "cli"})(),
        )
        result = runner._execute_job(job, agent_config, "hello", "/tmp", 30)

        assert result.status == "blocked"
        assert result.exit_code is None
        assert "runtime.workflow" in result.stderr


# ---------------------------------------------------------------------------
# Tests: verify commands
# ---------------------------------------------------------------------------


class TestVerifyCommands:
    """Tests for _run_verify."""

    def test_passing_verify(self, tmp_path):
        """Verify commands that succeed return passed=True."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")
        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=str(tmp_path / "receipts"),
            db_path=str(tmp_path / "test.db"),
            pg_conn=get_test_conn(),
        )
        results = runner._run_verify(
            [
                {"verification_ref": "verification.python.py_compile", "inputs": {"path": __file__}},
                {"verification_ref": "verification.python.py_compile", "inputs": {"path": _workflow_runner.__file__}},
            ],
            workdir="/tmp",
        )
        assert len(results) == 2
        assert all(r.passed for r in results)

    def test_failing_verify(self, tmp_path):
        """Verify commands that fail return passed=False."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")
        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=str(tmp_path / "receipts"),
            db_path=str(tmp_path / "test.db"),
            pg_conn=get_test_conn(),
        )
        results = runner._run_verify(
            [{"verification_ref": "verification.python.py_compile", "inputs": {"path": "/tmp/does-not-exist.py"}}],
            workdir="/tmp",
        )
        assert len(results) == 1
        assert not results[0].passed


# ---------------------------------------------------------------------------
# Tests: receipt writing
# ---------------------------------------------------------------------------


class TestReceiptWriting:
    """Tests for receipt writing to temp directory."""

    def test_receipt_storage_is_postgres_only(self, tmp_path):
        """Dry run returns receipt refs without writing JSON receipt files."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")
        receipts_dir = str(tmp_path / "receipts")
        spec_path = _write_spec(tmp_path, _minimal_spec())
        spec = WorkflowSpec.load(spec_path)

        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=receipts_dir,
            db_path=str(tmp_path / "test.db"),
        )
        result = runner.run_workflow(spec, dry_run=True)

        receipt_files = list(Path(receipts_dir).glob("*.json"))
        assert receipt_files == []
        assert len(result.receipts_written) == result.total_jobs
        assert all(receipt_ref for receipt_ref in result.receipts_written)


# ---------------------------------------------------------------------------
# Tests: evidence sequencing
# ---------------------------------------------------------------------------


class _RecordingPgConn:
    def __init__(self):
        self.receipt_rows: list[dict[str, object]] = []
        self._lock = threading.Lock()

    def execute(self, query, *args):
        if "FROM failure_category_zones" in query:
            return [
                {"category": "timeout", "zone": "external", "is_transient": True},
                {"category": "rate_limit", "zone": "external", "is_transient": True},
                {"category": "verification_failed", "zone": "internal", "is_transient": False},
            ]
        if "INSERT INTO receipts" in query:
            with self._lock:
                inputs = json.loads(args[11])
                outputs = json.loads(args[12])
                self.receipt_rows.append({
                    "receipt_id": args[0],
                    "status": args[10],
                    "evidence_seq": args[8],
                    "transition_seq": inputs["transition_seq"],
                    "failure_code": args[15],
                    "inputs": inputs,
                    "outputs": outputs,
                })
        return []


class _RaceyEvidenceSeqRunner(WorkflowRunner):
    """Turns legacy _evidence_seq reads into a deterministic race."""

    def __init__(self, parties: int, pg_conn: _RecordingPgConn):
        self._pg_conn = pg_conn
        self._run_id = "dispatch_test_run"
        self._workflow_id = "dispatch.TEST"
        self._request_id = "req_test"
        self._receipts_dir = tempfile.mkdtemp(prefix="dispatch-runner-receipts-")
        self._evidence_seq = 0
        self._evidence_counter = itertools.count(1)
        self._evidence_seq_barrier = threading.Barrier(parties)

    def __getattribute__(self, name: str):
        if name == "_evidence_seq":
            value = object.__getattribute__(self, name)
            barrier = object.__getattribute__(self, "_evidence_seq_barrier")
            barrier.wait(timeout=5)
            return value
        return object.__getattribute__(self, name)


class TestEvidenceSequencing:
    def test_write_receipt_populates_canonical_receipts(self):
        pg_conn = _RecordingPgConn()
        runner = _RaceyEvidenceSeqRunner(1, pg_conn)
        spec = WorkflowSpec(
            name="Test Spec",
            workflow_id="test_spec_001",
            phase="TEST",
            jobs=[],
            verify_refs=[],
            outcome_goal="",
            anti_requirements=[],
            raw={},
        )

        receipt_ref = runner._write_receipt(
            JobExecution(
                job_label="job_1",
                agent_slug="anthropic/claude-sonnet-4",
                status="failed",
                exit_code=124,
                stdout="",
                stderr="Timed out after 60s",
                duration_seconds=60.0,
                verify_passed=False,
                retry_count=2,
                telemetry=Telemetry(
                    input_tokens=11,
                    output_tokens=7,
                    cache_read_tokens=3,
                    cache_creation_tokens=2,
                    cost_usd=0.42,
                    model="claude-sonnet-4",
                    duration_api_ms=3913,
                    num_turns=4,
                    tool_use={"web_search_requests": 1},
                ),
            ),
            spec,
        )

        assert receipt_ref.startswith("rcpt_")
        assert len(pg_conn.receipt_rows) == 1
        assert pg_conn.receipt_rows[0]["receipt_id"].startswith("rcpt_")
        assert pg_conn.receipt_rows[0]["status"] == "failed"
        assert pg_conn.receipt_rows[0]["failure_code"] == "timeout"
        assert pg_conn.receipt_rows[0]["inputs"]["job_label"] == "job_1"
        assert pg_conn.receipt_rows[0]["inputs"]["transition_seq"] == 1
        assert pg_conn.receipt_rows[0]["outputs"]["cost_usd"] == pytest.approx(0.42)
        assert pg_conn.receipt_rows[0]["outputs"]["tool_use"] == {"web_search_requests": 1}

    def test_write_receipt_uses_atomic_counter_for_parallel_receipts(self):
        pg_conn = _RecordingPgConn()
        worker_count = 8
        runner = _RaceyEvidenceSeqRunner(worker_count, pg_conn)
        spec = WorkflowSpec(
            name="Test Spec",
            workflow_id="test_spec_001",
            phase="TEST",
            jobs=[],
            verify_refs=[],
            outcome_goal="",
            anti_requirements=[],
            raw={},
        )

        def _write_one(job_index: int) -> None:
            runner._write_receipt(
                JobExecution(
                    job_label=f"job_{job_index}",
                    agent_slug="anthropic/claude-sonnet-4",
                    status="succeeded",
                    exit_code=0,
                    stdout="",
                    stderr="",
                    duration_seconds=0.01,
                    verify_passed=True,
                    retry_count=0,
                ),
                spec,
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=worker_count) as pool:
            list(pool.map(_write_one, range(worker_count)))

        evidence_seqs = sorted(row["evidence_seq"] for row in pg_conn.receipt_rows)
        transition_seqs = sorted(row["transition_seq"] for row in pg_conn.receipt_rows)

        assert evidence_seqs == list(range(1, worker_count + 1))
        assert transition_seqs == list(range(1, worker_count + 1))


# ---------------------------------------------------------------------------
# Tests: blocked jobs
# ---------------------------------------------------------------------------


class TestBlockedJobs:
    """Tests for governance-blocked jobs."""

    def test_secret_in_prompt_blocks_job(self, tmp_path):
        """A job with a secret in its prompt is blocked by governance."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")

        data = _minimal_spec()
        # Inject a fake secret that triggers the governance filter
        data["jobs"][0]["prompt"] = "Use this key: sk-AAAAAAAAAAAAAAAAAAAAAAAAAAAA to authenticate"

        spec_path = _write_spec(tmp_path, data)
        spec = WorkflowSpec.load(spec_path)

        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=str(tmp_path / "receipts"),
            db_path=str(tmp_path / "test.db"),
        )

        result = runner.run_workflow(spec, dry_run=True)
        assert result.blocked == 1
        assert result.job_results[0].status == "blocked"


# ---------------------------------------------------------------------------
# Tests: full lifecycle with dry_run on a real spec
# ---------------------------------------------------------------------------


class TestFullLifecycle:
    """End-to-end lifecycle test with a real spec in dry-run mode."""

    def test_real_spec_dry_run(self, tmp_path):
        """Load a real spec from artifacts and dry-run it end to end."""
        if not os.path.exists(_SAMPLE_SPEC):
            pytest.skip("Sample spec not found")
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")

        spec = WorkflowSpec.load(_SAMPLE_SPEC)

        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=str(tmp_path / "receipts"),
            db_path=str(tmp_path / "test.db"),
        )

        result = runner.run_workflow(spec, dry_run=True)

        assert isinstance(result, RunResult)
        assert result.spec_name == spec.name
        assert result.total_jobs == len(spec.jobs)
        # In dry-run, all non-blocked jobs should succeed
        assert result.succeeded + result.blocked == result.total_jobs
        assert result.duration_seconds >= 0
        assert len(result.receipts_written) == result.total_jobs


# ---------------------------------------------------------------------------
# Tests: multi-job spec
# ---------------------------------------------------------------------------


class TestMultiJobSpec:
    """Tests for specs with multiple jobs."""

    def test_multi_job_dry_run(self, tmp_path):
        """A spec with multiple jobs runs all of them in dry-run."""
        if not os.path.exists(_AGENTS_JSON):
            pytest.skip("agents.json not found")

        data = _minimal_spec()
        data["jobs"].append({
            "label": "test_job_2",
            "stage": "test",
            "agent": "anthropic/claude-sonnet-4",
            "prompt": "Run tests",
            "workdir": "/tmp",
            "scope": {"read": [], "write": ["test_output_2.py"]},
        })

        spec_path = _write_spec(tmp_path, data)
        spec = WorkflowSpec.load(spec_path)

        runner = WorkflowRunner(
            config_root=os.path.dirname(_AGENTS_JSON),
            receipts_dir=str(tmp_path / "receipts"),
            db_path=str(tmp_path / "test.db"),
        )

        result = runner.run_workflow(spec, dry_run=True)
        assert result.total_jobs == 2
        assert result.succeeded == 2
        assert len(result.job_results) == 2


# ---------------------------------------------------------------------------
# Tests: telemetry parsing
# ---------------------------------------------------------------------------


class TestTelemetryParsing:
    """Tests for _parse_cli_telemetry."""

    def test_parse_anthropic_json(self):
        """Parses Claude --output-format json output correctly."""
        raw = json.dumps({
            "type": "result",
            "subtype": "success",
            "duration_ms": 4174,
            "duration_api_ms": 3913,
            "num_turns": 2,
            "result": "Hello, world!",
            "total_cost_usd": 0.050855,
            "usage": {
                "input_tokens": 3,
                "cache_creation_input_tokens": 6496,
                "cache_read_input_tokens": 19930,
                "output_tokens": 11,
                "server_tool_use": {
                    "web_search_requests": 1,
                    "web_fetch_requests": 2,
                },
            },
            "modelUsage": {
                "claude-sonnet-4-6": {
                    "inputTokens": 3,
                    "outputTokens": 11,
                    "cacheReadInputTokens": 19930,
                    "cacheCreationInputTokens": 6496,
                    "costUSD": 0.050855,
                },
            },
        })

        telemetry, result_text = WorkflowRunner._parse_cli_telemetry(raw, "anthropic")

        assert result_text == "Hello, world!"
        assert telemetry is not None
        assert telemetry.input_tokens == 3
        assert telemetry.output_tokens == 11
        assert telemetry.cache_read_tokens == 19930
        assert telemetry.cache_creation_tokens == 6496
        assert telemetry.cost_usd == 0.050855
        assert telemetry.model == "claude-sonnet-4-6"
        assert telemetry.duration_api_ms == 3913
        assert telemetry.num_turns == 2
        assert telemetry.tool_use["web_search_requests"] == 1
        assert telemetry.tool_use["web_fetch_requests"] == 2

    def test_parse_non_json_returns_raw(self):
        """Non-JSON stdout returns None telemetry and original text."""
        raw = "Just plain text output from the agent"
        telemetry, result_text = WorkflowRunner._parse_cli_telemetry(raw, "anthropic")

        assert telemetry is None
        assert result_text == raw

    def test_parse_google_json(self):
        """Parses Gemini --output-format json output."""
        raw = json.dumps({
            "result": "Gemini says hello",
            "total_cost_usd": 0.002,
            "model": "gemini-2.5-flash",
            "num_turns": 1,
            "usage": {
                "input_tokens": 100,
                "output_tokens": 50,
            },
        })

        telemetry, result_text = WorkflowRunner._parse_cli_telemetry(raw, "google")

        assert result_text == "Gemini says hello"
        assert telemetry is not None
        assert telemetry.input_tokens == 100
        assert telemetry.output_tokens == 50
        assert telemetry.cost_usd == 0.002
        assert telemetry.model == "gemini-2.5-flash"

    def test_telemetry_dataclass_defaults(self):
        """Telemetry defaults to zeros and empty dict."""
        t = Telemetry()
        assert t.input_tokens == 0
        assert t.output_tokens == 0
        assert t.cost_usd == 0.0
        assert t.tool_use == {}

    def test_job_execution_includes_telemetry(self):
        """JobExecution can carry telemetry."""
        t = Telemetry(input_tokens=100, output_tokens=50, cost_usd=0.01)
        je = JobExecution(
            job_label="test",
            agent_slug="test-agent",
            status="succeeded",
            exit_code=0,
            stdout="ok",
            stderr="",
            duration_seconds=1.0,
            verify_passed=None,
            retry_count=0,
            telemetry=t,
        )
        assert je.telemetry.input_tokens == 100
        assert je.telemetry.cost_usd == 0.01
