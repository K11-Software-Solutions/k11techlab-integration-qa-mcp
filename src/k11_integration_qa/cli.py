"""Command line: start the MCP server (default), or run integration flows in CI."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from . import __version__

USAGE = f"""k11-integration-qa-mcp {__version__}

Usage:
  k11-integration-qa-mcp                   start the MCP server on stdio
  k11-integration-qa-mcp run [opts] [paths]  run flows (default tests/integration/), write the report
  k11-integration-qa-mcp init              write a starter .qa-mcp/integration.yaml and an example flow
  k11-integration-qa-mcp --version

run options:
  --env NAME            environment from .qa-mcp/api-test.yaml
  --tags a,b            only flows with one of these tags
  --report-dir PATH     default .qa-mcp/integration-report
  --fail-on never       exit 0 even when flows fail (the Release Gate reads the report)

The project is QA_MCP_ROOT or the current directory.
"""


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(errors="replace")
        except (AttributeError, ValueError):
            pass
    if not argv:
        from .server import serve

        serve()
        return 0
    cmd = argv[0]
    if cmd in ("--version", "-v"):
        print(__version__)
        return 0
    if cmd in ("--help", "-h", "help"):
        print(USAGE)
        return 0
    root = Path(os.environ.get("QA_MCP_ROOT") or os.getcwd()).resolve()
    if cmd == "init":
        from .config import CONFIG
        from .run import EXAMPLE_FLOW, INIT

        p = root / CONFIG
        if p.exists():
            print(f"{CONFIG} already exists", file=sys.stderr)
            return 1
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(INIT, "utf8")
        ex = root / "tests/integration/example.yaml"
        if not ex.exists():
            ex.parent.mkdir(parents=True, exist_ok=True)
            ex.write_text(EXAMPLE_FLOW, "utf8")
        print(f"Wrote {CONFIG} and tests/integration/example.yaml")
        return 0
    if cmd != "run":
        print(f"Unknown command: {cmd}\n\n{USAGE}", file=sys.stderr)
        return 2
    p = argparse.ArgumentParser(prog="k11-integration-qa-mcp run")
    p.add_argument("paths", nargs="*")
    p.add_argument("--env")
    p.add_argument("--tags")
    p.add_argument("--report-dir", default=".qa-mcp/integration-report")
    p.add_argument("--fail-on", default="failure", choices=["failure", "never"])
    a = p.parse_args(argv[1:])
    from .run import run

    try:
        out = run(root, a.env, a.paths or None, a.tags.split(",") if a.tags else None, a.report_dir)
    except (ValueError, OSError) as e:
        print(f"run failed: {e}", file=sys.stderr)
        return 1
    doc, s = out["report"], out["report"]["summary"]
    print(f"{doc['reason']}: {s['passed']} passed, {s['failed']} failed, {s['blocked']} blocked, {s['error']} errors "
          f"({s['messages_published']} published, {s['messages_expected']} message checks, {s['jobs_run']} job runs). "
          f"Report: {out['paths']['markdown']}")
    for r in out["results"]:
        if r.status != "passed":
            bad = next((x for x in r.steps if x.status not in ("passed", "skipped")), None)
            why = r.error or (bad and (bad.error or "; ".join(f"{c['check']}: {c.get('detail', '')}" for c in bad.assertions if not c["ok"])))
            print(f"  - {r.status}: {r.name}" + (f" — {bad.name}: {why}" if bad else f" — {why}"))
    if os.environ.get("GITHUB_STEP_SUMMARY"):
        with open(os.environ["GITHUB_STEP_SUMMARY"], "a", encoding="utf8") as fh:
            fh.write((root / out["paths"]["markdown"]).read_text("utf8") + "\n")
    if a.fail_on == "never":
        return 0
    return 0 if doc["status"] == "completed" and s["passed"] == s["flows"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
