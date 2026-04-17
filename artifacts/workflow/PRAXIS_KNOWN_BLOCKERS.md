# Known Blockers

- No active blocker is accepted if smoke reaches terminal success with canonical receipt proof.
- Known failure classes to watch:
  - missing workflow definition seed row
  - unbootstrapped workflow schema
  - runtime profile drift between checked-in config and DB authority
  - execution proof ending without the canonical completion receipt
