#!/usr/bin/env bash
# setup-dblink-for-policy-authority — one-time superuser provisioning for
# the autonomous-write helper in policy_authority_record_compliance_receipt.
#
# What this does
#   1. CREATE EXTENSION IF NOT EXISTS dblink   (extension install)
#   2. GRANT EXECUTE ON FUNCTION dblink_connect_u(text, text) TO praxis
#      (the unsafe dblink form is restricted to superusers by default;
#      our helper uses it inside SECURITY DEFINER, but the executing
#      role still needs EXECUTE — SECURITY DEFINER scopes argument
#      authority, not capability authority)
#
# Why a separate script
#   Migration 298 ships the helper function but assumes dblink is
#   already installed and granted to the praxis role. Both steps require
#   superuser; the migration runs as praxis. Splitting them out keeps
#   the migration safe to re-run while making the one-time provisioning
#   explicit.
#
# When to run
#   - Once per database, after first checkout / before first run of
#     migration 298.
#   - After dropping/recreating the DB.
#
# Idempotent: safe to re-run.
#
# Usage:
#   scripts/setup-dblink-for-policy-authority.sh                # uses default DSN below
#   PG_SUPER_DSN=postgresql://nate@127.0.0.1/praxis scripts/setup-dblink-for-policy-authority.sh
#
# Requires: psql on PATH; DSN with superuser rights.

set -euo pipefail

PG_SUPER_DSN="${PG_SUPER_DSN:-postgresql://nate@127.0.0.1:5432/praxis}"
TARGET_ROLE="${PRAXIS_DB_ROLE:-praxis}"

echo "→ Installing dblink + granting to $TARGET_ROLE on $PG_SUPER_DSN"

psql "$PG_SUPER_DSN" -v ON_ERROR_STOP=1 <<SQL
-- 1. Extension install (no-op if already present).
CREATE EXTENSION IF NOT EXISTS dblink;

-- 2. Grant unsafe dblink form to the application role. dblink_connect_u
--    skips libpq's auth-method check, which is what lets the helper
--    open same-DB connections without explicit credentials. Restricted
--    to superusers by default; we widen to the praxis role so the
--    SECURITY DEFINER helper can execute it.
GRANT EXECUTE ON FUNCTION dblink_connect_u(text, text) TO $TARGET_ROLE;
GRANT EXECUTE ON FUNCTION dblink_disconnect(text) TO $TARGET_ROLE;
GRANT EXECUTE ON FUNCTION dblink_exec(text, text) TO $TARGET_ROLE;

-- Sanity: confirm role can resolve the function.
SELECT has_function_privilege('$TARGET_ROLE', 'dblink_connect_u(text, text)', 'execute')
    AS dblink_connect_u_grantable;
SQL

echo "→ done"
