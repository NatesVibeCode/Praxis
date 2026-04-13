"""Stdlib-only API transport worker executed inside a sandbox.

Looks up a handler from the transport registry by protocol family.
Zero provider awareness — the caller passes protocol, endpoint, and key env.
"""

from __future__ import annotations

import argparse
import sys

from runtime.http_transport import get_handler


def main() -> int:
    parser = argparse.ArgumentParser(description="Run one API call inside a sandbox")
    parser.add_argument("--api-protocol", required=True)
    parser.add_argument("--api-endpoint", required=True)
    parser.add_argument("--api-key-env", required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--max-output-tokens", type=int, default=4096)
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument("--reasoning-effort", default=None)
    args = parser.parse_args()

    handler = get_handler(args.api_protocol.strip().lower())
    kwargs = dict(
        model=args.model,
        max_tokens=args.max_output_tokens,
        timeout=args.timeout_seconds,
        api_endpoint=args.api_endpoint,
        api_key_env=args.api_key_env,
    )
    if args.reasoning_effort:
        kwargs["reasoning_effort"] = args.reasoning_effort
    text = handler(sys.stdin.read(), **kwargs)
    sys.stdout.write(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
