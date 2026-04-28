"""Host-side secure credential capture for macOS Keychain.

The LLM/MCP surface may request credential capture, but the raw secret must
enter through a host-controlled UI and must never be returned to the caller.
"""

from __future__ import annotations

import re
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass

from .keychain import keychain_get, keychain_set

_ENV_VAR_RE = re.compile(r"^[A-Z][A-Z0-9_]*$")


@dataclass(frozen=True, slots=True)
class CredentialCaptureResult:
    env_var_name: str
    status: str
    message: str
    stored: bool = False
    verified: bool = False
    source: str | None = None
    capture_ui: str = "macos_secure_window"
    error_code: str | None = None

    def to_redacted_dict(self) -> dict[str, object]:
        return {
            "env_var_name": self.env_var_name,
            "status": self.status,
            "stored": self.stored,
            "verified": self.verified,
            "source": self.source,
            "capture_ui": self.capture_ui,
            "error_code": self.error_code,
            "message": self.message,
        }


def secure_entry_action(env_var_name: str, provider_label: str) -> dict[str, object]:
    """Return a redacted action descriptor a wizard can expose to an LLM/UI."""
    return {
        "kind": "secure_key_entry",
        "env_var_name": env_var_name,
        "provider_label": provider_label,
        "authority": "macos_keychain",
        "account": "praxis",
        "service": env_var_name,
        "standard_path": "wizard_opens_independent_secure_window",
        "fallback_path": "host_cli_hidden_prompt",
        "raw_secret_policy": "never_return_to_llm_mcp_logs_or_receipts",
    }


def capture_api_key_to_keychain(
    env_var_name: str,
    *,
    provider_label: str,
) -> CredentialCaptureResult:
    """Open a macOS secure entry dialog, store the secret, and verify presence.

    The returned object is intentionally redacted. It never contains the API
    key, even on failure.
    """
    env_var_name = str(env_var_name or "").strip()
    provider_label = str(provider_label or env_var_name or "provider").strip()
    if not _ENV_VAR_RE.match(env_var_name):
        return CredentialCaptureResult(
            env_var_name=env_var_name,
            status="blocked",
            message="Credential service name must be an uppercase env-var style name.",
            error_code="credential_capture.invalid_env_var_name",
        )
    if sys.platform != "darwin":
        return CredentialCaptureResult(
            env_var_name=env_var_name,
            status="blocked",
            message="Secure Keychain capture requires the macOS host.",
            error_code="credential_capture.host_not_macos",
        )
    if shutil.which("swift"):
        return _capture_with_swift_keychain(env_var_name, provider_label)

    if not shutil.which("osascript"):
        return CredentialCaptureResult(
            env_var_name=env_var_name,
            status="blocked",
            message="macOS secure window transport is unavailable.",
            error_code="credential_capture.osascript_missing",
        )

    secret_result = _prompt_macos_hidden_answer(env_var_name, provider_label)
    if secret_result.returncode != 0:
        return CredentialCaptureResult(
            env_var_name=env_var_name,
            status="canceled",
            message="Credential entry was canceled or the secure window could not complete.",
            error_code="credential_capture.canceled",
        )
    secret = (secret_result.stdout or "").strip()
    if not secret:
        return CredentialCaptureResult(
            env_var_name=env_var_name,
            status="missing",
            message="No API key was entered.",
            error_code="credential_capture.empty_secret",
        )

    if not keychain_set(env_var_name, secret):
        return CredentialCaptureResult(
            env_var_name=env_var_name,
            status="blocked",
            message="Could not write the credential to macOS Keychain.",
            error_code="credential_capture.keychain_write_failed",
        )

    verified = bool(keychain_get(env_var_name))
    return CredentialCaptureResult(
        env_var_name=env_var_name,
        status="ok" if verified else "blocked",
        stored=True,
        verified=verified,
        source="keychain" if verified else None,
        message=(
            f"{env_var_name} stored in macOS Keychain."
            if verified
            else f"{env_var_name} was written but could not be verified."
        ),
        error_code=None if verified else "credential_capture.verify_failed",
    )


def _capture_with_swift_keychain(
    env_var_name: str,
    provider_label: str,
) -> CredentialCaptureResult:
    result = subprocess.run(
        ["swift", "-", env_var_name, provider_label],
        input=_SWIFT_CAPTURE_SOURCE,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        return CredentialCaptureResult(
            env_var_name=env_var_name,
            status="blocked",
            message="Secure Keychain capture helper failed.",
            error_code="credential_capture.swift_failed",
        )
    try:
        payload = json.loads(result.stdout or "{}")
    except json.JSONDecodeError:
        return CredentialCaptureResult(
            env_var_name=env_var_name,
            status="blocked",
            message="Secure Keychain capture helper returned invalid status.",
            error_code="credential_capture.swift_invalid_json",
        )
    return CredentialCaptureResult(
        env_var_name=env_var_name,
        status=str(payload.get("status") or "blocked"),
        message=str(payload.get("message") or "Secure Keychain capture finished."),
        stored=bool(payload.get("stored")),
        verified=bool(payload.get("verified")),
        source=str(payload.get("source") or "") or None,
        capture_ui="macos_secure_window",
        error_code=str(payload.get("error_code") or "") or None,
    )


def _prompt_macos_hidden_answer(
    env_var_name: str,
    provider_label: str,
) -> subprocess.CompletedProcess[str]:
    script = """
on run argv
  set envVarName to item 1 of argv
  set providerLabel to item 2 of argv
  set promptText to "Enter the " & providerLabel & " API key for " & envVarName & ". It will be stored in macOS Keychain."
  set dialogResult to display dialog promptText default answer "" with hidden answer buttons {"Cancel", "Save"} default button "Save" cancel button "Cancel" with title "Praxis API Key"
  return text returned of dialogResult
end run
"""
    return subprocess.run(
        ["osascript", "-e", script, env_var_name, provider_label],
        capture_output=True,
        text=True,
        timeout=300,
    )


_SWIFT_CAPTURE_SOURCE = r'''
import AppKit
import Foundation
import Security

func emit(_ payload: [String: Any]) {
    let data = try! JSONSerialization.data(withJSONObject: payload, options: [.sortedKeys])
    FileHandle.standardOutput.write(data)
    FileHandle.standardOutput.write("\n".data(using: .utf8)!)
}

let args = CommandLine.arguments
let service = args.count > 1 ? args[1] : ""
let providerLabel = args.count > 2 ? args[2] : "provider"
let account = "praxis"

if service.isEmpty {
    emit([
        "status": "blocked",
        "stored": false,
        "verified": false,
        "error_code": "credential_capture.invalid_env_var_name",
        "message": "Missing credential service name."
    ])
    exit(1)
}

let app = NSApplication.shared
app.setActivationPolicy(.accessory)

let alert = NSAlert()
alert.messageText = "Praxis API Key"
alert.informativeText = "Enter the \(providerLabel) API key for \(service). It will be stored in macOS Keychain."
alert.addButton(withTitle: "Save")
alert.addButton(withTitle: "Cancel")
let input = NSSecureTextField(frame: NSRect(x: 0, y: 0, width: 420, height: 24))
input.placeholderString = service
alert.accessoryView = input
app.activate(ignoringOtherApps: true)

let response = alert.runModal()
if response != .alertFirstButtonReturn {
    emit([
        "status": "canceled",
        "stored": false,
        "verified": false,
        "error_code": "credential_capture.canceled",
        "message": "Credential entry was canceled."
    ])
    exit(0)
}

let secret = input.stringValue
if secret.trimmingCharacters(in: .whitespacesAndNewlines).isEmpty {
    emit([
        "status": "missing",
        "stored": false,
        "verified": false,
        "error_code": "credential_capture.empty_secret",
        "message": "No API key was entered."
    ])
    exit(0)
}

let secretData = secret.data(using: .utf8)!
let query: [String: Any] = [
    kSecClass as String: kSecClassGenericPassword,
    kSecAttrAccount as String: account,
    kSecAttrService as String: service
]

var attributes = query
attributes[kSecValueData as String] = secretData
attributes[kSecAttrLabel as String] = "Praxis \(service)"

var status = SecItemAdd(attributes as CFDictionary, nil)
if status == errSecDuplicateItem {
    status = SecItemUpdate(query as CFDictionary, [kSecValueData as String: secretData] as CFDictionary)
}

if status != errSecSuccess {
    emit([
        "status": "blocked",
        "stored": false,
        "verified": false,
        "error_code": "credential_capture.keychain_write_failed",
        "message": "Could not write the credential to macOS Keychain."
    ])
    exit(0)
}

var verifyQuery = query
verifyQuery[kSecReturnData as String] = true
verifyQuery[kSecMatchLimit as String] = kSecMatchLimitOne
var item: CFTypeRef?
let verifyStatus = SecItemCopyMatching(verifyQuery as CFDictionary, &item)
let verified = verifyStatus == errSecSuccess

emit([
    "status": verified ? "ok" : "blocked",
    "stored": true,
    "verified": verified,
    "source": verified ? "keychain" : "",
    "error_code": verified ? "" : "credential_capture.verify_failed",
    "message": verified ? "\(service) stored in macOS Keychain." : "\(service) was written but could not be verified."
])
'''
