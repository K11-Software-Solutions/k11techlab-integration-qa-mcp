"""MCP server: integration tests across APIs, message brokers (Kafka), batch jobs and databases."""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Optional

from mcp.server.fastmcp import FastMCP

from . import __version__, config, report


def project_root() -> Path:
    return Path(os.environ.get("QA_MCP_ROOT") or os.getcwd()).resolve()


def _brief(doc: dict, limit: int = 20) -> dict:
    failing = []
    for f in doc["flows"]:
        if f["status"] == "passed":
            continue
        steps = [{"name": s["name"], "kind": s["kind"], "status": s["status"], "error": s["error"],
                  "failed_checks": [a for a in s["assertions"] if not a["ok"]]}
                 for s in f["steps"] if s["status"] not in ("passed", "skipped")]
        failing.append({"flow": f["name"], "file": f["file"], "status": f["status"], "error": f["error"], "steps": steps})
    return {"status": doc["status"], "reason": doc["reason"], "environment": doc["environment"], "summary": doc["summary"],
            "failing_flows": failing[:limit]}


def create_server() -> FastMCP:
    mcp = FastMCP(
        "k11-integration-qa",
        instructions=(
            "Integration testing for event-driven and batch systems. Flows (tests/integration/*.yaml) combine HTTP calls "
            "and database checks (shared with k11 API Functional QA) with message steps: listen to a topic BEFORE the "
            "action, publish, expect_message / expect_no_message with JSON-path matching, a time limit and optional "
            "AsyncAPI schema checks, run_job (a command without a shell, or an HTTP job with polling) and wait_until "
            "(poll a SELECT or GET until it holds). Start with integration_config. Publishing and jobs are refused where "
            "writes are not allowed and on protected topics; never point flows at production brokers."
        ),
    )
    mcp._mcp_server.version = __version__

    @mcp.tool(title="Integration config")
    def integration_config() -> dict:
        """Brokers, jobs, AsyncAPI contract, protected topics, environments (and whether publishing and jobs may run
        there) and the flows found under tests/integration/."""
        from k11_api_qa import config as api_config
        from k11_api_qa.config import writes_allowed

        from .flows import flow_files

        root = project_root()
        cfg = config.load(root)
        api_cfg = api_config.load(root)
        envs = {}
        for name, env in api_cfg.environments.items():
            url = str(env.get("base_url", ""))
            ok, why = writes_allowed(name, env, url if not url.startswith("${") else "http://unresolved")
            envs[name] = {"publish_and_jobs": "allowed" if ok else f"refused ({why})"}
        return {"config": config.CONFIG if cfg.exists else None,
                "brokers": {n: {"type": b["type"], **({"bootstrap": b.get("bootstrap")} if b.get("bootstrap") else {})}
                            for n, b in cfg.brokers.items()},
                "jobs": {n: ("command: " + " ".join(j["command"])) if j.get("command") else f"http: {j['http'].get('call')}"
                         for n, j in cfg.jobs.items()},
                "asyncapi": cfg.asyncapi, "protected_topics": cfg.protected_topics,
                "environments": envs, "default_environment": api_cfg.default_environment,
                "databases": list(api_cfg.databases), "fixtures": list(api_cfg.fixtures),
                "flows": [p.relative_to(root).as_posix() for p in flow_files(root)]}

    @mcp.tool(title="Run integration tests")
    def run_integration_tests(environment: Optional[str] = None, paths: Optional[list[str]] = None,
                              tags: Optional[list[str]] = None) -> dict:
        """Run integration flows (default: all of tests/integration/) and write .qa-mcp/integration-report/ (JSON
        evidence for the Release Gate, Markdown for the PR, JUnit). Returns the summary and the failing steps with
        the closest message seen, query results or job output."""
        from .run import run

        out = run(project_root(), environment, paths, tags)
        return {**_brief(out["report"]), "paths": out["paths"]}

    @mcp.tool(title="Validate flow")
    def validate_flow(path: str) -> dict:
        """Check a flow file's structure (step kinds and their keys) without running it."""
        from k11_contract_qa.spec import inside

        from .flows import load_flow

        flow = load_flow(inside(project_root(), path))
        return {"valid": True, "name": flow["name"], "steps": len(flow["steps"]),
                "kinds": sorted({next(k for k in s if k != "name") for s in flow["steps"]})}

    @mcp.tool(title="Watch topic")
    def watch_topic(topic: str, broker: Optional[str] = None, seconds: float = 5, limit: int = 20) -> dict:
        """Listen to a topic for a few seconds (read-only, from now on) and return the messages seen, to learn their
        shape before writing expect_message steps. Values are shown as sent; don't use it on topics carrying real
        personal data."""
        from .brokers import open_broker

        cfg = config.load(project_root())
        name = broker or next(iter(cfg.brokers), None)
        if not name or name not in cfg.brokers:
            raise ValueError(f"Unknown broker '{name}'. Known: {sorted(cfg.brokers) or ['(none in integration.yaml)']}")
        b = open_broker(name, cfg.brokers[name])
        try:
            lst = b.listen(topic)
            time.sleep(max(0.1, min(float(seconds), 30)))
            msgs = lst.snapshot()
            lst.close()
        finally:
            b.close()
        return {"topic": topic, "broker": name, "seconds": seconds, "count": len(msgs),
                "messages": [m.as_dict() for m in msgs[:limit]], "error": lst.error}

    @mcp.tool(title="Get report")
    def get_report(flow: Optional[str] = None) -> dict:
        """The last integration report, or one flow's steps with full evidence (requests, messages, queries, job output)."""
        p = project_root() / report.REPORT_DIR / "integration-report.json"
        if not p.is_file():
            raise ValueError("No report yet; run run_integration_tests first")
        doc = json.loads(p.read_text("utf8"))
        if not flow:
            return _brief(doc)
        for f in doc["flows"]:
            if flow in (f["name"], f["file"]):
                return f
        raise ValueError(f"No flow '{flow}' in the report. Flows: {[f['name'] for f in doc['flows']]}")

    @mcp.tool(title="Create integration config")
    def init_integration_config(overwrite: bool = False, example_flow: bool = True) -> dict:
        """Write a starter .qa-mcp/integration.yaml and, optionally, tests/integration/example.yaml."""
        from .run import EXAMPLE_FLOW, INIT

        root = project_root()
        p = root / config.CONFIG
        if p.exists() and not overwrite:
            raise ValueError(f"{config.CONFIG} already exists (overwrite=true to replace it)")
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(INIT, "utf8")
        written = [config.CONFIG]
        ex = root / "tests/integration/example.yaml"
        if example_flow and not ex.exists():
            ex.parent.mkdir(parents=True, exist_ok=True)
            ex.write_text(EXAMPLE_FLOW, "utf8")
            written.append("tests/integration/example.yaml")
        return {"written": written}

    return mcp


def serve() -> None:
    create_server().run()
