"""GitHub integration for dispatch-driven PR reviews and webhooks.

Provides:
- GitHubClient: Low-level REST API client using urllib (no SDK)
- dispatch_pr_review: Get PR diff and dispatch a review task
- post_review_to_pr: Post review findings as a PR comment
- dispatch_and_comment: One-shot PR review with auto-posting
"""

from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from typing import Any

from .workflow import WorkflowSpec, WorkflowResult, dispatch


class GitHubClient:
    """Low-level GitHub REST API client using urllib.request.

    No external SDK — minimal dependencies, direct control over HTTP requests.
    """

    __slots__ = ("token",)

    def __init__(self, token: str | None = None):
        """Initialize with GitHub token.

        Args:
            token: GitHub personal access token. If None, reads from GITHUB_TOKEN env var.

        Raises:
            ValueError: If no token is provided and GITHUB_TOKEN env var is not set.
        """
        if token is None:
            token = os.environ.get("GITHUB_TOKEN")
        if not token:
            raise ValueError(
                "GitHub token required. Pass token= or set GITHUB_TOKEN env var."
            )
        self.token = token

    def _request(
        self,
        method: str,
        path: str,
        body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Make a raw HTTP request to GitHub REST API.

        Args:
            method: HTTP method (GET, POST, PATCH, etc.)
            path: API path (e.g., "/repos/owner/repo/issues/123")
            body: Request body (will be JSON-encoded)
            headers: Additional headers

        Returns:
            Parsed JSON response

        Raises:
            urllib.error.HTTPError: On HTTP errors (4xx, 5xx)
            urllib.error.URLError: On network errors
        """
        url = f"https://api.github.com{path}"

        # Prepare headers
        req_headers = {
            "Authorization": f"token {self.token}",
            "Accept": "application/vnd.github.v3+json",
            "User-Agent": "dag-workflow-github-integration/1.0",
        }
        if headers:
            req_headers.update(headers)

        # Prepare body
        req_body = None
        if body is not None:
            req_body = json.dumps(body).encode("utf-8")
            req_headers["Content-Type"] = "application/json"

        # Make request
        req = urllib.request.Request(
            url,
            data=req_body,
            headers=req_headers,
            method=method,
        )

        try:
            with urllib.request.urlopen(req) as response:
                response_body = response.read().decode("utf-8")
                return json.loads(response_body) if response_body else {}
        except urllib.error.HTTPError as e:
            error_body = e.read().decode("utf-8")
            try:
                error_data = json.loads(error_body)
            except json.JSONDecodeError:
                error_data = {"message": error_body}
            raise RuntimeError(
                f"GitHub API error {e.code}: {error_data.get('message', error_body)}"
            ) from e

    def post_pr_comment(
        self, owner: str, repo: str, pr_number: int, body: str
    ) -> dict[str, Any]:
        """Post a comment on a PR.

        Args:
            owner: Repository owner (e.g., "anthropic")
            repo: Repository name (e.g., "anthropic-sdk-python")
            pr_number: PR number
            body: Comment body (markdown)

        Returns:
            GitHub API response (comment object)
        """
        path = f"/repos/{owner}/{repo}/issues/{pr_number}/comments"
        return self._request("POST", path, body={"body": body})

    def get_pr_files(
        self, owner: str, repo: str, pr_number: int
    ) -> list[dict[str, Any]]:
        """Get list of changed files in a PR.

        Args:
            owner: Repository owner
            repo: Repository name
            pr_number: PR number

        Returns:
            List of file objects with keys:
            - filename: Path in repo
            - status: "added", "removed", "modified", "renamed", "copied"
            - additions, deletions, changes: Counts
            - patch: Unified diff (if changes < 1000 lines)
        """
        path = f"/repos/{owner}/{repo}/pulls/{pr_number}/files"
        # GitHub paginates with max 100 per page; get up to 3 pages (300 files)
        all_files = []
        for page in range(1, 4):
            params = {"page": str(page), "per_page": "100"}
            url = f"{path}?{urllib.parse.urlencode(params)}"
            files = self._request("GET", url)
            if isinstance(files, list):
                all_files.extend(files)
                if len(files) < 100:
                    break
            else:
                break
        return all_files

    def get_pr_diff(self, owner: str, repo: str, pr_number: int) -> str:
        """Get the raw unified diff for a PR.

        Args:
            owner: Repository owner
            repo: Repository name
            pr_number: PR number

        Returns:
            Unified diff as string
        """
        path = f"/repos/{owner}/{repo}/pulls/{pr_number}"
        # Request as patch/diff format
        req_headers = {"Accept": "application/vnd.github.v3.patch"}

        url = f"https://api.github.com{path}"
        req_headers_full = {
            "Authorization": f"token {self.token}",
            "User-Agent": "dag-workflow-github-integration/1.0",
        }
        req_headers_full.update(req_headers)

        req = urllib.request.Request(url, headers=req_headers_full, method="GET")
        try:
            with urllib.request.urlopen(req) as response:
                return response.read().decode("utf-8")
        except urllib.error.HTTPError as e:
            raise RuntimeError(
                f"Failed to fetch PR diff: {e.code} {e.reason}"
            ) from e

    def get_pr(self, owner: str, repo: str, pr_number: int) -> dict[str, Any]:
        """Get PR metadata.

        Args:
            owner: Repository owner
            repo: Repository name
            pr_number: PR number

        Returns:
            GitHub API response (PR object) with keys like title, body, state, head, base
        """
        path = f"/repos/{owner}/{repo}/pulls/{pr_number}"
        return self._request("GET", path)


def dispatch_pr_review(
    owner: str,
    repo: str,
    pr_number: int,
    *,
    tier: str = "mid",
    max_retries: int = 0,
) -> WorkflowResult:
    """Dispatch a PR review task.

    Fetches the PR diff and dispatches it as a review spec.

    Args:
        owner: Repository owner
        repo: Repository name
        pr_number: PR number
        tier: Model tier ("frontier", "mid", "economy", "auto")
        max_retries: Number of retries on transient failures

    Returns:
        WorkflowResult from the review dispatch

    Raises:
        ValueError: If GITHUB_TOKEN not set
        RuntimeError: If GitHub API call fails
    """
    client = GitHubClient()

    # Fetch PR metadata and diff
    pr_meta = client.get_pr(owner, repo, pr_number)
    pr_diff = client.get_pr_diff(owner, repo, pr_number)

    # Build review prompt
    prompt = f"""Review the following pull request and provide detailed feedback.

## PR Context
- **Repository**: {owner}/{repo}
- **PR Number**: #{pr_number}
- **Title**: {pr_meta.get('title', 'N/A')}
- **Author**: {pr_meta.get('user', {}).get('login', 'unknown')}
- **Description**: {pr_meta.get('body', '(no description)')}

## Changed Files
{_format_pr_files(client.get_pr_files(owner, repo, pr_number))}

## Unified Diff
```diff
{pr_diff}
```

## Review Task
Analyze the changes and provide:
1. **Summary**: What does this PR change?
2. **Code Quality Issues**: Any potential bugs, style, or architectural concerns?
3. **Testing**: Are there test coverage gaps?
4. **Security**: Any security implications?
5. **Performance**: Any performance considerations?
6. **Recommendations**: Specific actionable improvements.

Format as markdown suitable for posting as a GitHub PR comment.
"""

    # Dispatch the review
    spec = WorkflowSpec(
        prompt=prompt,
        tier=tier,
        max_retries=max_retries,
        label=f"pr-review-{owner}/{repo}#{pr_number}",
    )

    return run_workflow(spec)


def post_review_to_pr(
    owner: str,
    repo: str,
    pr_number: int,
    result: WorkflowResult,
) -> dict[str, Any]:
    """Post review findings as a PR comment.

    Args:
        owner: Repository owner
        repo: Repository name
        pr_number: PR number
        result: WorkflowResult from dispatch_pr_review

    Returns:
        GitHub API response (comment object)

    Raises:
        ValueError: If GITHUB_TOKEN not set or result.status != "succeeded"
        RuntimeError: If GitHub API call fails
    """
    if result.status != "succeeded":
        raise ValueError(
            f"Cannot post failed review (status={result.status}, "
            f"code={result.reason_code}). Review dispatch must succeed."
        )

    client = GitHubClient()

    # Extract the completion text
    completion = result.completion or ""

    # Format as a GitHub PR comment with metadata
    comment_body = f"""## Review Analysis
**Dispatch Run**: `{result.run_id}`
**Model**: {result.author_model or f"{result.provider_slug}/{result.model_slug}"}
**Duration**: {result.latency_ms}ms

---

{completion}

---
<sub>Review generated by dag-workflow workflow platform</sub>
"""

    return client.post_pr_comment(owner, repo, pr_number, comment_body)


def dispatch_and_comment(
    owner: str,
    repo: str,
    pr_number: int,
    **kwargs,
) -> WorkflowResult:
    """One-shot PR review: dispatch + post comment.

    Convenience function that:
    1. Dispatches a PR review task
    2. On success, posts the findings as a PR comment
    3. Returns the workflow result

    Args:
        owner: Repository owner
        repo: Repository name
        pr_number: PR number
        **kwargs: Passed to dispatch_pr_review (tier, max_retries, etc.)

    Returns:
        WorkflowResult from the review

    Raises:
        ValueError: If no GitHub token or dispatch fails
        RuntimeError: If GitHub API calls fail
    """
    result = dispatch_pr_review(owner, repo, pr_number, **kwargs)

    if result.status == "succeeded":
        try:
            comment = post_review_to_pr(owner, repo, pr_number, result)
            # Augment result with comment info for caller visibility
            print(
                f"✓ Review posted as comment {comment.get('id')} on "
                f"{owner}/{repo}#{pr_number}"
            )
        except Exception as e:
            print(f"⚠ Review dispatch succeeded but comment post failed: {e}")

    return result


def _format_pr_files(files: list[dict[str, Any]]) -> str:
    """Format PR file list for inclusion in prompt."""
    if not files:
        return "(no files changed)"

    lines = []
    for f in files[:20]:  # Limit to first 20 files in prompt
        filename = f.get("filename", "?")
        status = f.get("status", "?")
        changes = f.get("changes", 0)
        lines.append(f"- `{filename}` ({status}, {changes} changes)")

    if len(files) > 20:
        lines.append(f"- ... and {len(files) - 20} more files")

    return "\n".join(lines)


__all__ = [
    "GitHubClient",
    "dispatch_pr_review",
    "post_review_to_pr",
    "dispatch_and_comment",
]
