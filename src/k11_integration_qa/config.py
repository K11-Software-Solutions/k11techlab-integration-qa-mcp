"""Integration config: ``.qa-mcp/integration.yaml`` (environments, auth and databases come from
``.qa-mcp/api-test.yaml``, shared with k11 API Functional QA).

    brokers:
      main: { type: kafka, bootstrap: "${ENV:KAFKA_BOOTSTRAP}" }    # or type: memory (tests, demos)
    jobs:
      daily_totals:
        command: [python, jobs/daily_totals.py]                     # no shell; runs in the project
        env: { SHOP_DB: shop.db }
        timeout: 60s
      reindex:
        http: { call: POST /admin/jobs/reindex, save: { jobId: "$.id" } }
        poll: { call: "GET /admin/jobs/${jobId}", until: { "$.status": done }, fail_if: { "$.status": failed } }
        timeout: 120s
    asyncapi: asyncapi.yaml                                         # validate message payloads (optional)
    protected_topics: ["prod.*", "*.prod.*", "*production*"]        # never published to

Publishing messages and running jobs change data, so they follow API Functional QA's write rules:
local hosts, test-named environments, or ``allow_writes: true``; never production-like names.
"""

from __future__ import annotations

import fnmatch
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

CONFIG = ".qa-mcp/integration.yaml"
DEFAULT_PROTECTED = ["prod.*", "*.prod.*", "*.prod", "*production*", "live.*"]
_DUR = re.compile(r"^\s*(\d+(?:\.\d+)?)\s*(ms|s|m|h)?\s*$", re.I)


def seconds(value: Any, default: float = 10.0) -> float:
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return float(value)
    m = _DUR.match(str(value))
    if not m:
        raise ValueError(f"Invalid duration {value!r} (use e.g. 10s, 2m, 500ms)")
    return float(m.group(1)) * {"ms": 0.001, "s": 1, "m": 60, "h": 3600}[(m.group(2) or "s").lower()]


def sub_env(v: Any) -> Any:
    if isinstance(v, str):
        def rep(m):
            if m.group(1) not in os.environ:
                raise ValueError(f"Environment variable {m.group(1)} is not set")
            return os.environ[m.group(1)]
        return re.sub(r"\$\{ENV:([A-Za-z_][A-Za-z0-9_]*)\}", rep, v)
    if isinstance(v, dict):
        return {k: sub_env(x) for k, x in v.items()}
    if isinstance(v, list):
        return [sub_env(x) for x in v]
    return v


@dataclass
class IntegrationConfig:
    root: Path
    brokers: dict[str, dict] = field(default_factory=dict)
    jobs: dict[str, dict] = field(default_factory=dict)
    asyncapi: str | None = None
    protected_topics: list[str] = field(default_factory=lambda: list(DEFAULT_PROTECTED))
    exists: bool = False

    def protected(self, topic: str) -> bool:
        return any(fnmatch.fnmatchcase(topic, p) for p in self.protected_topics)


def load(root: Path) -> IntegrationConfig:
    p = Path(root) / CONFIG
    data: dict[str, Any] = (yaml.safe_load(p.read_text("utf8")) or {}) if p.is_file() else {}
    unknown = set(data) - {"brokers", "jobs", "asyncapi", "protected_topics"}
    if unknown:
        raise ValueError(f"{CONFIG}: unknown key(s) {sorted(unknown)}")
    for name, b in (data.get("brokers") or {}).items():
        if (b or {}).get("type") not in ("kafka", "memory"):
            raise ValueError(f"{CONFIG}: broker '{name}' needs type: kafka or memory")
    for name, j in (data.get("jobs") or {}).items():
        if not isinstance(j, dict) or not (j.get("command") or j.get("http")):
            raise ValueError(f"{CONFIG}: job '{name}' needs 'command' (a list) or 'http'")
        if j.get("command") is not None and not (isinstance(j["command"], list) and all(isinstance(x, str) for x in j["command"])):
            raise ValueError(f"{CONFIG}: job '{name}' command must be a list of strings (no shell)")
    cfg = IntegrationConfig(root=Path(root), brokers=data.get("brokers") or {}, jobs=data.get("jobs") or {},
                            asyncapi=data.get("asyncapi"), exists=p.is_file())
    if "protected_topics" in data:
        cfg.protected_topics = list(data["protected_topics"] or [])
    return cfg
