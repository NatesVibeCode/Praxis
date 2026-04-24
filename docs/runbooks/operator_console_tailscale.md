# Operator Console over Tailscale

Dev-only god-mode console reachable only from the operator's tailnet. No
public-internet exposure, no WebAuthn ceremony — Tailscale device presence
is the auth primitive. This runbook is the "just me and my phone" path.

Prerequisites:
- A VPS (or home rig) that already runs the Praxis API via `scripts/bootstrap`.
- An iOS / Android phone you'll use on the road.
- A Tailscale account (free tier is enough).

## 1. Install Tailscale on the server

**macOS / Linux VPS**
```sh
# macOS
brew install tailscale
sudo tailscaled install-system-daemon   # enables auto-start
sudo tailscale up

# Debian / Ubuntu
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up
```

`tailscale up` prints a URL. Open it in a browser and sign in. The machine
appears in your tailnet device list.

**Verify:**
```sh
tailscale status          # shows this node + tailnet peers
tailscale ip -4           # prints the 100.x.y.z tailnet IP
```

## 2. Install Tailscale on your phone

- iOS: App Store → Tailscale. Sign in with the same account. The phone
  shows up in `tailscale status` on the VPS.
- Android: Play Store → same flow.

From the phone, open Safari/Chrome → `http://<vps-tailnet-name>:8420/api/health`.
Should return `{"ok": true, ...}`. If it does, networking is done.

## 3. Start the API bound to the tailnet interface only

The operator console is gated on `PRAXIS_OPERATOR_DEV_MODE=1`. To keep the
API reachable from the tailnet but nothing else, bind it to the Tailscale IP
rather than `0.0.0.0`:

```sh
./scripts/operator-console-up
```

That script:
- Checks `tailscale` is installed and authenticated.
- Reads the tailnet IP from `tailscale ip -4`.
- Starts the API with `PRAXIS_API_HOST=<tailnet-ip>` and
  `PRAXIS_OPERATOR_DEV_MODE=1`.
- Prints the URL to load in the phone's browser.

### What if I want public-internet reachability too?

Don't. The operator console is dev-only. Public exposure brings back the
multi-user threat model that mobile v1 tried and failed to solve — see
`docs/archive/mobile-v1.md` and
`decision.2026-04-24.operator-console-anti-patterns`.

If you genuinely need external-user mobile access later, design it against
the actual external user, not in abstract. The console stays private.

## 4. Load the console on the phone

On the phone, with Tailscale active:

```
http://<vps-tailnet-name>:8420/console
```

Paste the same `PRAXIS_API_TOKEN` you start the server with. Pick a CLI and
a permission mode. Type a prompt. Send.

## 5. Lock it down (optional tightening)

- Rotate `PRAXIS_API_TOKEN` regularly — it's the only secret the console
  asks for beyond tailnet membership.
- In Tailscale ACL, restrict `:8420` to the specific device tags you use
  (phone, laptop). Keeps a compromised tailnet peer from hitting the API.
- Run the API under a systemd unit or launchd plist so restart is automatic.

## Troubleshooting

**`tailscale: command not found`** — install Tailscale (step 1).

**`tailscale status` says "Stopped"** — run `sudo tailscale up` and sign in.

**Phone browser can't reach the VPS** — check `tailscale status` on both
devices. Both must show `active` peers. If the VPS uses a non-default
MagicDNS name, use the literal IP from `tailscale ip -4` instead.

**`/console` returns 404** — `PRAXIS_OPERATOR_DEV_MODE` is not `1`. Use
`scripts/operator-console-up` or set the env var explicitly.

**API binds but phone sees "connection refused"** — the API is bound to
`127.0.0.1` instead of the tailnet IP. `operator-console-up` sets
`PRAXIS_API_HOST` correctly; if you start the API another way, pass
`--host <tailnet-ip>` explicitly.
