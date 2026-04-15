#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import sys
from pathlib import Path

WORKFLOW_ROOT = Path(__file__).resolve().parents[1]
DEPENDENCY_CONTRACT_PATH = WORKFLOW_ROOT / "runtime" / "dependency_contract.py"


def _load_requirements_for_scope():
    spec = importlib.util.spec_from_file_location("praxis_dependency_contract", DEPENDENCY_CONTRACT_PATH)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"unable to load dependency contract from {DEPENDENCY_CONTRACT_PATH}")
    module = importlib.util.module_from_spec(spec)
    sys.modules.setdefault("praxis_dependency_contract", module)
    spec.loader.exec_module(module)
    return module.requirements_for_scope


requirements_for_scope = _load_requirements_for_scope()


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export one dependency scope from requirements.runtime.txt")
    parser.add_argument("--scope", required=True, help="Declared dependency scope to export")
    parser.add_argument("--manifest", type=Path, required=True, help="Path to requirements.runtime.txt")
    parser.add_argument("--output", type=Path, required=True, help="Output requirements file path")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    requirements = requirements_for_scope(scope=args.scope, manifest_path=args.manifest)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    payload = "\n".join(requirements).strip()
    if payload:
        payload += "\n"
    args.output.write_text(payload, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
