"""Plan an operation-field evolution — introspect the CQRS chain.

Adding a new field to an existing CQRS operation's input shape today
touches 6-10 files (Pydantic input model → handler → frontdoor sync +
async + private builder → repository SQL + row loader → MCP tool wrapper
→ tool catalog inputSchema → cli_metadata example). There is no wizard
for that today — `praxis_register_operation` covers NEW ops only.

This query operation closes the introspection half of the wizard:

  Input:
    operation_name           — existing op (e.g. 'operator.architecture_policy_record')
    field_name               — new field on the input shape (e.g. 'decision_provenance')
    field_type_annotation    — Python type, e.g. 'str | None'
    field_default_repr       — literal default, e.g. 'None' or "'inferred'"
    field_description        — for the MCP tool's inputSchema
    db_column                — optional; the matching DB column when the
                               field is column-backed
    db_table                 — optional; the table that owns db_column

  Output (deterministic, receipt-backed):
    plan[]                   — ordered list of (file, hop_kind, search_anchor,
                               suggested_insert) tuples covering every
                               site that needs an edit
    existing_input_fields[]  — current Pydantic class fields (introspected)
    db_column_present        — boolean — whether (db_table, db_column)
                               already exists in information_schema
    discovery_warnings[]     — sites the tool couldn't auto-locate; the
                               operator must find them

v1 is plan-only. v2 (deferred packet) gets a sibling command op
'evolve_operation_field.apply' that takes the same plan and edits via AST.
"""

from __future__ import annotations

import ast
import os
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field


# File lives at <repo>/Code&DBs/Workflow/runtime/operations/queries/evolve_operation_field.py
# parents[3] = Workflow, parents[4] = Code&DBs, parents[5] = repo root.
WORKFLOW_ROOT = Path(__file__).resolve().parents[3]
REPO_ROOT = WORKFLOW_ROOT.parents[1]


class QueryEvolveOperationField(BaseModel):
    """Plan-only query: surface every file/site that needs editing to add
    a new optional field to an existing CQRS operation's input shape."""

    operation_name: str = Field(
        ...,
        description="Existing operation_name (e.g. 'operator.architecture_policy_record').",
    )
    field_name: str = Field(
        ...,
        description="New field name to add (e.g. 'decision_provenance').",
    )
    field_type_annotation: str = Field(
        default="str | None",
        description="Python type annotation for the new field, e.g. 'str | None' or 'int'.",
    )
    field_default_repr: str = Field(
        default="None",
        description="Python literal for the default value, e.g. 'None' or \"'inferred'\".",
    )
    field_description: str = Field(
        default="",
        description="Human description; used in the MCP tool's inputSchema.",
    )
    db_column: str | None = Field(
        default=None,
        description="Optional matching DB column name when the field is column-backed.",
    )
    db_table: str | None = Field(
        default=None,
        description="Optional table that owns db_column.",
    )


def _read_input_model_ref(subsystems: Any, operation_name: str) -> tuple[str | None, str | None]:
    """Look up input_model_ref + handler_ref from operation_catalog_registry."""
    pg = subsystems.get_pg_conn()
    rows = pg.execute(
        """
        SELECT input_model_ref, handler_ref
          FROM operation_catalog_registry
         WHERE operation_name = $1
           AND enabled = TRUE
         LIMIT 1
        """,
        operation_name,
    )
    if not rows:
        return None, None
    return rows[0]["input_model_ref"], rows[0]["handler_ref"]


def _resolve_module_path(dotted: str) -> Path | None:
    """Convert dotted path to file path. Handles both class-style refs
    ('runtime.foo.FooCommand') and function-style refs
    ('runtime.foo.handle_foo') — strips trailing identifiers progressively
    until we find a file that exists."""
    if not dotted:
        return None
    parts = dotted.split(".")
    # Try the full path first, then progressively shorter — covers both
    # class names (capitalized) and function names (lowercase).
    while parts:
        rel = Path(*parts).with_suffix(".py")
        candidate = WORKFLOW_ROOT / rel
        if candidate.exists():
            return candidate
        parts.pop()
    return None


def _existing_pydantic_fields(file_path: Path, class_name: str) -> list[dict[str, str]]:
    """AST-walk the file to extract field annotations on a BaseModel subclass."""
    try:
        text = file_path.read_text(encoding="utf-8")
        tree = ast.parse(text)
    except (OSError, SyntaxError):
        return []
    for node in ast.walk(tree):
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            fields: list[dict[str, str]] = []
            for item in node.body:
                if isinstance(item, ast.AnnAssign) and isinstance(item.target, ast.Name):
                    annotation_src = ast.unparse(item.annotation) if item.annotation else ""
                    default_src = ast.unparse(item.value) if item.value else ""
                    fields.append({
                        "name": item.target.id,
                        "annotation": annotation_src,
                        "default": default_src,
                    })
            return fields
    return []


def _grep_files(root: Path, pattern: str, glob: str = "**/*.py") -> list[Path]:
    """Find files under root matching the literal pattern."""
    rgx = re.compile(re.escape(pattern))
    hits: list[Path] = []
    for path in root.rglob(glob):
        if "__pycache__" in path.parts:
            continue
        try:
            if rgx.search(path.read_text(encoding="utf-8", errors="ignore")):
                hits.append(path)
        except OSError:
            continue
    return hits


def _check_db_column(subsystems: Any, table: str, column: str) -> bool:
    pg = subsystems.get_pg_conn()
    rows = pg.execute(
        """
        SELECT 1 FROM information_schema.columns
         WHERE table_schema = 'public'
           AND table_name = $1 AND column_name = $2
         LIMIT 1
        """,
        table,
        column,
    )
    return bool(rows)


_PY_TO_JSON_TYPE = {
    "str": "string",
    "int": "integer",
    "float": "number",
    "bool": "boolean",
    "dict": "object",
    "list": "array",
    "tuple": "array",
    "datetime": "string",
}


def _python_type_to_json_schema_type(annotation: str) -> str:
    """Map a Python type annotation to its JSON Schema type. Strips
    ' | None' / Optional[…] markers so callers get the inner type."""
    raw = annotation.strip()
    # Strip ' | None' or 'None | …'
    parts = [p.strip() for p in raw.split("|") if p.strip().lower() != "none"]
    inner = parts[0] if parts else raw
    # Strip generic parameters: dict[str, Any] → dict
    inner = inner.split("[", 1)[0].strip()
    return _PY_TO_JSON_TYPE.get(inner, "string")


def _suggested_insert(
    field_name: str, field_type: str, field_default: str, field_description: str
) -> dict[str, str]:
    """Per-hop suggested code or pattern. Operator applies these manually
    in v1; v2 will AST-insert them automatically."""
    json_type = _python_type_to_json_schema_type(field_type)
    return {
        "pydantic_field": (
            f"    # Operator-evolved field (plan-only v1). Provide a working default so existing\n"
            f"    # callers don't break.\n"
            f"    {field_name}: {field_type} = {field_default}"
        ),
        "handler_passthrough": f"        {field_name}=command.{field_name},",
        "frontdoor_signature_param": f"        {field_name}: {field_type} = {field_default},",
        "frontdoor_passthrough_kw": f"                {field_name}={field_name},",
        "repository_insert_column": f"                    {field_name},",
        "repository_insert_placeholder": "                    $<NEXT_INDEX>",
        "repository_on_conflict_set": (
            f"                    {field_name} = COALESCE(EXCLUDED.{field_name}, "
            f"operator_decisions.{field_name})"
        ),
        "repository_returning_column": f"                    {field_name},",
        "repository_execute_arg": (
            f"                normalized_operator_decision.{field_name},  # bind to $<NEXT_INDEX>"
        ),
        "repository_loader_field": (
            f'        {field_name}=(\n'
            f'            row["{field_name}"]\n'
            f'            if "{field_name}" in row.keys() and row["{field_name}"] is not None\n'
            f"            else {field_default}\n"
            f"        ),"
        ),
        "mcp_tool_payload_kv": f'            "{field_name}": params.get("{field_name}"),',
        "mcp_tool_inputSchema_property": (
            f'                    "{field_name}": {{\n'
            f'                        "type": "{json_type}",\n'
            f'                        "description": "{field_description}",\n'
            f"                    }},"
        ),
        "cli_metadata_example": (
            f'# Add an example with "{field_name}" populated to demonstrate the new field.'
        ),
    }


def handle_query_evolve_operation_field(
    query: QueryEvolveOperationField,
    subsystems: Any,
) -> dict[str, Any]:
    plan: list[dict[str, str]] = []
    warnings: list[str] = []

    # 1. Resolve input_model_ref + handler_ref via the catalog.
    input_model_ref, handler_ref = _read_input_model_ref(subsystems, query.operation_name)
    if input_model_ref is None:
        return {
            "ok": False,
            "error": f"operation '{query.operation_name}' not found in operation_catalog_registry "
                     f"(or disabled).",
            "discovery_warnings": [],
            "plan": [],
        }

    # 2. Pydantic input class — primary edit site.
    pydantic_class_name = input_model_ref.rsplit(".", 1)[-1]
    pydantic_path = _resolve_module_path(input_model_ref)
    existing_fields: list[dict[str, str]] = []
    if pydantic_path is None:
        warnings.append(
            f"could not resolve input_model_ref '{input_model_ref}' to a file under "
            f"{WORKFLOW_ROOT}; check the dotted path"
        )
    else:
        existing_fields = _existing_pydantic_fields(pydantic_path, pydantic_class_name)
        if any(f["name"] == query.field_name for f in existing_fields):
            return {
                "ok": False,
                "error": f"field '{query.field_name}' already exists on {pydantic_class_name}; "
                         f"this tool only handles ADDING new fields. Use praxis_data_dictionary "
                         f"set_override for metadata changes on existing fields.",
                "existing_input_fields": existing_fields,
                "plan": [],
            }
        plan.append({
            "hop_kind": "pydantic_input_model",
            "file": str(pydantic_path.relative_to(REPO_ROOT)),
            "search_anchor": f"class {pydantic_class_name}(",
            "insert_after": "the last field declaration inside the class",
            "snippet": _suggested_insert(
                query.field_name,
                query.field_type_annotation,
                query.field_default_repr,
                query.field_description,
            )["pydantic_field"],
        })

    # 3. Handler — pass field through to frontdoor.
    if handler_ref:
        handler_fn = handler_ref.rsplit(".", 1)[-1]
        handler_path = _resolve_module_path(handler_ref)
        if handler_path:
            plan.append({
                "hop_kind": "command_handler_passthrough",
                "file": str(handler_path.relative_to(REPO_ROOT)),
                "search_anchor": f"def {handler_fn}(",
                "insert_after": "existing kwargs at the frontdoor call site",
                "snippet": _suggested_insert(
                    query.field_name,
                    query.field_type_annotation,
                    query.field_default_repr,
                    query.field_description,
                )["handler_passthrough"],
            })

    # 4. Frontdoor methods (sync + async + private builder).
    # Heuristic: search surfaces/api/operator_write.py and friends for the handler's
    # call signature.
    frontdoor_root = WORKFLOW_ROOT / "surfaces" / "api"
    if frontdoor_root.exists():
        # Find the operation's matching frontdoor method by searching for one of
        # the existing kwargs (decision_source is in every operator op).
        anchor_kwarg = "decision_source"
        if not existing_fields or not any(f["name"] == anchor_kwarg for f in existing_fields):
            anchor_kwarg = (existing_fields[0]["name"] if existing_fields else "")
        if anchor_kwarg:
            hits = _grep_files(frontdoor_root, f"{anchor_kwarg}=")
            for hit in hits[:3]:
                plan.append({
                    "hop_kind": "frontdoor_method",
                    "file": str(hit.relative_to(REPO_ROOT)),
                    "search_anchor": f"{anchor_kwarg}=",
                    "insert_after": "the existing kwarg list (signature + call passthrough)",
                    "snippet": _suggested_insert(
                        query.field_name,
                        query.field_type_annotation,
                        query.field_default_repr,
                        query.field_description,
                    )["frontdoor_signature_param"],
                })

    # 5. Repository SQL — only when db_table + db_column are given.
    # The repository hop is unique: a single SQL function touches the new
    # column in 5+ places (column list, $-placeholders, ON CONFLICT SET,
    # RETURNING, execute() args, row-loader). Surfacing structured sub-
    # snippets instead of one paste-able blob, since the operator inserts
    # them in different parts of the same INSERT statement.
    if query.db_table and query.db_column:
        column_present = _check_db_column(subsystems, query.db_table, query.db_column)
        if not column_present:
            warnings.append(
                f"DB column '{query.db_table}.{query.db_column}' is not present in "
                f"information_schema. Add it via a migration first; this wizard plans "
                f"the application-side wiring, not the schema."
            )
        suggested = _suggested_insert(
            query.field_name,
            query.field_type_annotation,
            query.field_default_repr,
            query.field_description,
        )
        repo_root_dir = WORKFLOW_ROOT / "storage" / "postgres"
        repo_hits = _grep_files(repo_root_dir, f"INTO {query.db_table}")
        for hit in repo_hits[:2]:
            plan.append({
                "hop_kind": "repository_sql",
                "file": str(hit.relative_to(REPO_ROOT)),
                "search_anchor": f"INSERT INTO {query.db_table}",
                "insert_after": "5 sub-sites in the same INSERT statement (see sub_snippets)",
                "sub_snippets": {
                    "1_column_list": suggested["repository_insert_column"],
                    "2_values_placeholder": suggested["repository_insert_placeholder"],
                    "3_on_conflict_set": suggested["repository_on_conflict_set"],
                    "4_returning_column": suggested["repository_returning_column"],
                    "5_execute_arg": suggested["repository_execute_arg"],
                    "6_row_loader_in_dataclass": suggested["repository_loader_field"],
                },
                "snippet": (
                    "Multi-site insertion — see sub_snippets keys 1-6. Tip: search for "
                    f"'INSERT INTO {query.db_table}' in the file, then walk the structure "
                    "(columns, VALUES, ON CONFLICT SET, RETURNING, execute() args list). "
                    "The row-loader (#6) lives in _decision_record_from_row or equivalent."
                ),
            })

    # 5b. Authority dataclass — the row-loader returns into one of these.
    # Heuristic: scan authority/ for `@dataclass` classes whose name maps
    # to the target table (TitleCaseAuthorityRecord), or for any class
    # whose ast.unparse(annotation) lists the table's primary key. The
    # wizard surfaces a warning rather than a precise hop in v1 because
    # naming conventions vary; v2 should walk the row-loader call site
    # to find the exact dataclass.
    if query.db_table:
        authority_root = WORKFLOW_ROOT / "authority"
        if authority_root.exists():
            authority_hits = _grep_files(authority_root, query.db_table)
            for hit in authority_hits[:1]:
                plan.append({
                    "hop_kind": "authority_dataclass",
                    "file": str(hit.relative_to(REPO_ROOT)),
                    "search_anchor": "@dataclass",
                    "insert_after": (
                        "the matching record dataclass (e.g. OperatorDecisionAuthorityRecord). "
                        "The row-loader returns into this dataclass, so it needs the field with "
                        "a default — otherwise the loader breaks for old rows."
                    ),
                    "snippet": (
                        f"    # Migration-302-style — added because the row-loader builds this dataclass.\n"
                        f"    {query.field_name}: {query.field_type_annotation} = {query.field_default_repr}"
                    ),
                })

    # 6. MCP tool wrapper — heuristic search.
    mcp_root = WORKFLOW_ROOT / "surfaces" / "mcp" / "tools"
    if mcp_root.exists():
        op_short = query.operation_name.replace(".", "_").replace("__", "_")
        tool_hits = _grep_files(mcp_root, f'operation_name="{query.operation_name}"')
        for hit in tool_hits[:1]:
            plan.append({
                "hop_kind": "mcp_tool_wrapper",
                "file": str(hit.relative_to(REPO_ROOT)),
                "search_anchor": f'operation_name="{query.operation_name}"',
                "insert_after": "the payload dict (add a key for the new field)",
                "snippet": _suggested_insert(
                    query.field_name,
                    query.field_type_annotation,
                    query.field_default_repr,
                    query.field_description,
                )["mcp_tool_payload_kv"],
            })
        # Tool catalog inputSchema lives in the same file.
        for hit in tool_hits[:1]:
            plan.append({
                "hop_kind": "mcp_tool_inputSchema",
                "file": str(hit.relative_to(REPO_ROOT)),
                "search_anchor": '"inputSchema":',
                "insert_after": "existing properties dict; required[] does NOT need updating "
                                "(field is optional with a default)",
                "snippet": _suggested_insert(
                    query.field_name,
                    query.field_type_annotation,
                    query.field_default_repr,
                    query.field_description,
                )["mcp_tool_inputSchema_property"],
            })

    # 7. cli_metadata example update.
    cli_metadata_path = WORKFLOW_ROOT / "surfaces" / "mcp" / "cli_metadata.py"
    if cli_metadata_path.exists():
        plan.append({
            "hop_kind": "cli_metadata_example",
            "file": str(cli_metadata_path.relative_to(REPO_ROOT)),
            "search_anchor": "praxis_<tool_name>",
            "insert_after": "an existing example block; show the new field populated for discoverability",
            "snippet": _suggested_insert(
                query.field_name,
                query.field_type_annotation,
                query.field_default_repr,
                query.field_description,
            )["cli_metadata_example"],
        })

    return {
        "ok": True,
        "operation_name": query.operation_name,
        "input_model_ref": input_model_ref,
        "handler_ref": handler_ref,
        "field_name": query.field_name,
        "field_type_annotation": query.field_type_annotation,
        "existing_input_fields": existing_fields,
        "db_column_present": (
            _check_db_column(subsystems, query.db_table, query.db_column)
            if query.db_table and query.db_column
            else None
        ),
        "plan": plan,
        "discovery_warnings": warnings,
        "next_step_for_operator": (
            "v1 plan-only — apply each hop in order with Edit/Write. Re-run after each "
            "hop to confirm the field is present and the chain still parses. v2 (deferred) "
            "will offer auto-apply via AST patching once the v1 introspection is stable."
        ),
    }


__all__ = [
    "QueryEvolveOperationField",
    "handle_query_evolve_operation_field",
]
