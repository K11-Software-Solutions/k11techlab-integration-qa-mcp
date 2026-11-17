"""Run integration flows and write the report (shared by the MCP server and the CLI)."""

from __future__ import annotations

from pathlib import Path

from k11_api_qa import config as api_config

from . import config, report
from .flows import flow_files, run_files

INIT = """# k11 Integration QA: messaging, batch jobs and database flows.
# Environments, auth and databases come from .qa-mcp/api-test.yaml (shared with k11 API Functional QA).
brokers:
  main: { type: kafka, bootstrap: "${ENV:KAFKA_BOOTSTRAP}" }    # or { type: memory } for in-process tests
jobs:
  # nightly_export:
  #   command: [python, jobs/nightly_export.py]                 # a list: runs without a shell, in the project
  #   env: { EXPORT_DB: "${ENV:TEST_DB_URL}" }
  #   timeout: 120s
  # reindex:
  #   http: { call: POST /admin/jobs/reindex, save: { jobId: "$.id" } }
  #   poll: { call: "GET /admin/jobs/${jobId}", until: { "$.status": done }, fail_if: { "$.status": failed } }
# asyncapi: asyncapi.yaml                                       # check message payloads against the contract
protected_topics: ["prod.*", "*.prod.*", "*.prod", "*production*", "live.*"]   # never published to
"""

EXAMPLE_FLOW = """name: An order is published and processed
tags: [orders]
steps:
  - listen: { broker: main, topic: orders.events, as: events }   # subscribe BEFORE the action
  - call: POST /orders
    body: { sku: BAG-1, qty: 1 }
    expect: { status: 201 }
    save: { orderId: "$.id" }
  - expect_message:
      from: events
      match: { "$.type": OrderCreated, "$.orderId": "${orderId}" }
      within: 10s
  - wait_until:
      call: GET /orders/${orderId}
      expect: { json: { "$.status": reserved } }
      within: 15s
"""


def run(root: Path, environment: str | None = None, targets: list[str] | None = None, tags: list[str] | None = None,
        report_dir: str = report.REPORT_DIR) -> dict:
    api_cfg = api_config.load(root)
    cfg = config.load(root)
    files = flow_files(root, targets)
    if files:
        results, env_name = run_files(api_cfg, cfg, files, environment, tags)
    else:
        results, env_name = [], environment or api_cfg.default_environment
    paths = report.write(results, env_name, Path(root) / report_dir)
    return {"report": report.to_dict(results, env_name), "results": results,
            "paths": {k: p.relative_to(root).as_posix() for k, p in paths.items()}}
