"""Integration flows: HTTP calls, database checks, messages and batch jobs in one test.

A flow (``tests/integration/*.yaml``) is an API Functional QA flow with more step types:

    name: An order reserves stock through the inventory consumer
    fixtures: [customer]
    steps:
      - listen: { broker: main, topic: orders.events, as: events }       # subscribe BEFORE acting
      - call: POST /orders
        body: { customerId: "${fixture.customer.id}", items: [{ sku: BAG-1, qty: 2 }] }
        expect: { status: 201 }
        save: { orderId: "$.id" }
      - name: OrderCreated is published
        expect_message:
          from: events
          match: { "$.type": OrderCreated, "$.orderId": "${orderId}" }  # JSON paths on the payload; also key, header:Name
          within: 10s
          schema: OrderCreated                                          # AsyncAPI message (asyncapi: in integration.yaml)
          save: { eventId: "$.eventId" }
      - name: the consumer reserved the stock
        wait_until:
          db: SELECT reserved FROM inventory WHERE sku = 'BAG-1'
          expect: { row: { reserved: 2 } }
          within: 10s
      - publish: { broker: main, topic: orders.commands, key: "${orderId}", value: "not json" }   # a poison message
      - expect_message: { from: dlq, match: { key: "${orderId}" }, within: 10s }
      - run_job: { job: daily_totals }
      - expect_no_message: { from: events, match: { "$.type": OrderFailed }, within: 2s }

Rules: publishing and jobs are data-changing, so they are refused where API Functional QA refuses
writes, and publishing to protected (production-named) topics is always refused. Every wait has a
time limit; cleanup always runs; listeners are closed at the end of the flow.
"""

from __future__ import annotations

import json
import os
import subprocess
import time
from pathlib import Path
from typing import Any

import yaml
from k11_api_qa.config import Config as ApiConfig
from k11_api_qa.runner import FlowResult, Runner, StepResult
from k11_api_qa.values import Vars, check, json_path

from .brokers import Listener, Message, open_broker
from .config import IntegrationConfig, seconds, sub_env

STEP_KINDS = ("call", "db", "listen", "publish", "expect_message", "expect_no_message", "run_job", "wait_until")
API_KEYS = {"name", "call", "db", "database", "path", "query", "headers", "body", "form", "auth", "expect", "save", "params"}
LISTEN_KEYS = {"broker", "topic", "as"}
PUBLISH_KEYS = {"broker", "topic", "key", "value", "headers"}
EXPECT_KEYS = {"from", "match", "within", "count", "schema", "save"}
SPEC_KEYS = {"listen": LISTEN_KEYS, "publish": PUBLISH_KEYS, "expect_message": EXPECT_KEYS,
             "expect_no_message": {"from", "match", "within"}, "run_job": {"job", "timeout"},
             "wait_until": {"db", "call", "database", "params", "query", "headers", "expect", "save", "within"}}
FLOW_KEYS = {"name", "description", "auth", "fixtures", "vars", "steps", "cleanup", "tags", "environment"}


class FlowError(ValueError):
    pass


def validate(flow: Any, label: str) -> dict:
    if not isinstance(flow, dict) or not flow.get("name") or not flow.get("steps"):
        raise FlowError(f"{label}: a flow needs 'name' and 'steps'")
    bad = set(flow) - FLOW_KEYS
    if bad:
        raise FlowError(f"{label}: unknown key(s) {sorted(bad)}")
    for section in ("steps", "cleanup"):
        for i, s in enumerate(flow.get(section) or [], 1):
            kinds = [k for k in STEP_KINDS if k in (s or {})]
            if len(kinds) != 1:
                raise FlowError(f"{label} {section}[{i}]: needs exactly one of {', '.join(STEP_KINDS)}")
            k = kinds[0]
            if k in ("call", "db"):
                extra = set(s) - API_KEYS
            else:
                extra = set(s) - {"name", k}
                inner = s[k]
                if not isinstance(inner, dict):
                    raise FlowError(f"{label} {section}[{i}]: '{k}' needs a mapping")
                extra |= {f"{k}.{x}" for x in set(inner) - SPEC_KEYS[k]}
            if extra:
                raise FlowError(f"{label} {section}[{i}]: unknown key(s) {sorted(extra)}")
            need = {"listen": "topic", "publish": "topic", "expect_message": "from", "expect_no_message": "from", "run_job": "job"}.get(k)
            if need and not s[k].get(need):
                raise FlowError(f"{label} {section}[{i}]: '{k}' needs '{need}'")
    return flow


def load_flow(path: Path) -> dict:
    try:
        data = yaml.safe_load(Path(path).read_text("utf8"))
    except yaml.YAMLError as e:
        raise FlowError(f"{Path(path).name}: invalid YAML: {str(e).splitlines()[0]}") from None
    return validate(data, Path(path).name)


def _match(m: Message, match: dict, v: Vars) -> tuple[bool, list[str]]:
    why = []
    for k, expected in (match or {}).items():
        expected = v.sub(expected)
        if k == "key":
            found, actual = m.key is not None, m.key
        elif k == "topic":
            found, actual = True, m.topic
        elif k.lower().startswith("header:"):
            name = k.split(":", 1)[1].strip()
            found, actual = name in m.headers, m.headers.get(name)
        elif k in ("$", "value"):
            found, actual = True, m.value
        else:
            found, actual = json_path(m.value, k) if isinstance(m.value, (dict, list)) else (False, None)
        ok, detail = check(actual, expected, found)
        if not ok:
            why.append(f"{k}: {detail}")
    return not why, why


class IntegrationRunner:
    def __init__(self, api_cfg: ApiConfig, cfg: IntegrationConfig, environment: str | None):
        self.api_cfg, self.cfg = api_cfg, cfg
        self.http = Runner(api_cfg, environment, spec=None)
        self.env_name = self.http.env_name
        self.writes, self.writes_why = self.http.writes, self.http.writes_why
        self.brokers: dict[str, Any] = {}
        self.asyncapi = None

    def close(self) -> None:
        for b in self.brokers.values():
            try:
                b.close()
            except Exception:  # noqa: BLE001
                pass
        self.http.close()

    def broker(self, name: str):
        if name not in self.cfg.brokers:
            raise ValueError(f"Unknown broker '{name}'. Known: {sorted(self.cfg.brokers) or ['(none in integration.yaml)']}")
        if name not in self.brokers:
            self.brokers[name] = open_broker(name, self.cfg.brokers[name])
        return self.brokers[name]

    # ---- one flow ----------------------------------------------------------------------------
    def run_flow(self, flow: dict, file: str = "") -> FlowResult:
        res = FlowResult(flow["name"], file, self.env_name)
        v = Vars({"env": {"name": self.env_name, "base_url": self.http.base_url}})
        listeners: dict[str, Listener] = {}
        pending: list = []
        started = time.perf_counter()
        auth = flow.get("auth", self.http.env.get("auth", "default"))
        try:
            v.values.update(v.sub(flow.get("vars") or {}))
            for fx in flow.get("fixtures") or []:
                sr = self.http._fixture(fx, v, auth, pending)
                res.steps.append(sr)
                if sr.status != "passed":
                    res.status = "failed" if sr.status == "failed" else sr.status
                    return res
            for i, step in enumerate(flow["steps"], 1):
                sr = self._step(step, v, auth, i, listeners)
                res.steps.append(sr)
                if sr.status != "passed":
                    if res.status == "passed":
                        res.status = sr.status if sr.status in ("blocked", "error") else "failed"
                    break  # later steps usually depend on this one
        except (ValueError, FlowError) as e:
            res.status, res.error = "error", v.mask(str(e))
        finally:
            for i, step in enumerate(flow.get("cleanup") or [], 1):
                res.steps.append(self._step(step, v, auth, i, listeners, cleanup=True))
            for kind, spec in reversed(pending):
                sr = self.http._step(spec, v, auth, 0, cleanup=True)
                sr.name = f"fixture cleanup: {kind}"
                res.steps.append(sr)
            for lst in listeners.values():
                lst.close()
            res.ms = round((time.perf_counter() - started) * 1000)
        return res

    def _step(self, step: dict, v: Vars, auth: str, i: int, listeners: dict, cleanup: bool = False) -> StepResult:
        kind = next(k for k in STEP_KINDS if k in step)
        if kind in ("call", "db"):
            return self.http._step(step, v, auth, i, cleanup=cleanup)
        name = str(step.get("name") or kind.replace("_", " "))
        sr = StepResult(name, kind, cleanup=cleanup)
        t0 = time.perf_counter()
        try:
            getattr(self, f"_{kind}")(step[kind] or {}, v, sr, listeners, auth)
        except ValueError as e:
            sr.status, sr.error = "error", v.mask(str(e))
        except Exception as e:  # noqa: BLE001 - broker / driver errors are evidence, not crashes
            sr.status, sr.error = "error", v.mask(f"{type(e).__name__}: {e}")[:300]
        sr.ms = round((time.perf_counter() - t0) * 1000)
        if sr.status == "passed" and any(not a["ok"] for a in sr.assertions):
            sr.status = "failed"
        return sr

    # ---- messaging ---------------------------------------------------------------------------
    def _listen(self, spec: dict, v: Vars, sr: StepResult, listeners: dict, auth: str) -> None:
        topic = str(v.sub(spec["topic"]))
        alias = str(spec.get("as") or topic)
        listeners[alias] = self.broker(spec.get("broker") or next(iter(self.cfg.brokers), "")).listen(topic)
        sr.request = {"listen": topic, "as": alias}
        sr.assertions.append({"check": f"listening on {topic}", "ok": True, "detail": ""})

    def _publish(self, spec: dict, v: Vars, sr: StepResult, listeners: dict, auth: str) -> None:
        topic = str(v.sub(spec["topic"]))
        if self.cfg.protected(topic):
            sr.status, sr.error = "blocked", f"'{topic}' matches protected_topics; publishing to it is refused"
            return
        if not self.writes:
            sr.status, sr.error = "blocked", f"Publishing refused on '{self.env_name}': {self.writes_why}"
            return
        value = v.sub(spec.get("value"))
        key = v.sub(spec.get("key")) if spec.get("key") is not None else None
        headers = {k: str(x) for k, x in (v.sub(spec.get("headers") or {})).items()}
        m = self.broker(spec.get("broker") or next(iter(self.cfg.brokers), "")).publish(topic, value, str(key) if key is not None else None, headers)
        sr.request = {"publish": topic, "key": v.mask(key), "value": v.mask(value)}
        sr.response = {"partition": m.partition, "offset": m.offset}
        sr.assertions.append({"check": f"published to {topic}", "ok": True, "detail": ""})

    def _listener(self, spec: dict, listeners: dict) -> Listener:
        src = spec.get("from")
        if not src or src not in listeners:
            raise ValueError(f"expect_message needs 'from': a listener started earlier with listen (known: {sorted(listeners) or ['none']})")
        return listeners[src]

    def _expect_message(self, spec: dict, v: Vars, sr: StepResult, listeners: dict, auth: str) -> None:
        lst = self._listener(spec, listeners)
        within = seconds(spec.get("within"), 10)
        want = int(spec.get("count", 1))
        deadline = time.time() + within
        hits: list[Message] = []
        closest: tuple[int, Message | None, list[str]] = (10**9, None, [])
        while True:
            hits = []
            for m in lst.snapshot():
                ok, why = _match(m, spec.get("match") or {}, v)
                if ok:
                    hits.append(m)
                elif len(why) <= closest[0]:  # ties: the later message is usually the one meant
                    closest = (len(why), m, why)
            if len(hits) >= want or time.time() >= deadline or lst.error:
                break
            time.sleep(0.05)
        sr.request = {"from": spec.get("from"), "topic": lst.topic, "match": v.mask(v.sub(spec.get("match") or {})), "within_s": within}
        seen = len(lst.snapshot())
        if len(hits) < want:
            detail = f"{len(hits)} of {want} matching message(s) within {within:g}s ({seen} message(s) on {lst.topic})"
            if closest[1] is not None:
                detail += f"; closest: {'; '.join(closest[2])[:200]}"
            if lst.error:
                detail += f"; listener error: {lst.error}"
            sr.assertions.append({"check": f"message on {lst.topic}", "ok": False, "detail": v.mask(detail)})
            if closest[1] is not None:
                sr.response = {"closest": v.mask(closest[1].as_dict())}
            return
        m = hits[0]
        sr.response = {"message": v.mask(m.as_dict()), "matched": len(hits), "seen": seen}
        sr.assertions.append({"check": f"message on {lst.topic}", "ok": True, "detail": ""})
        if spec.get("schema"):
            sr.assertions.append(self._schema(spec["schema"], m))
        for var, src in (spec.get("save") or {}).items():
            src = str(src)
            if src == "key":
                v.values[var] = m.key
            elif src.lower().startswith("header:"):
                v.values[var] = m.headers.get(src.split(":", 1)[1].strip())
            else:
                found, val = json_path(m.value, src) if isinstance(m.value, (dict, list)) else (False, None)
                if not found:
                    sr.assertions.append({"check": f"save {var} from {src}", "ok": False, "detail": "not in the message"})
                    continue
                v.values[var] = val
            sr.saved[var] = v.mask(v.values.get(var))

    def _expect_no_message(self, spec: dict, v: Vars, sr: StepResult, listeners: dict, auth: str) -> None:
        lst = self._listener(spec, listeners)
        within = seconds(spec.get("within"), 3)
        deadline = time.time() + within
        while time.time() < deadline:
            bad = [m for m in lst.snapshot() if _match(m, spec.get("match") or {}, v)[0]]
            if bad:
                sr.response = {"unexpected": v.mask(bad[0].as_dict())}
                sr.assertions.append({"check": f"no matching message on {lst.topic}", "ok": False,
                                      "detail": f"{len(bad)} unexpected message(s)"})
                return
            time.sleep(0.05)
        sr.request = {"from": spec.get("from"), "match": v.mask(v.sub(spec.get("match") or {})), "within_s": within}
        sr.assertions.append({"check": f"no matching message on {lst.topic} for {within:g}s", "ok": True, "detail": ""})

    def _schema(self, name: str, m: Message) -> dict:
        from jsonschema import Draft202012Validator

        if not self.cfg.asyncapi:
            return {"check": f"matches schema {name}", "ok": False, "detail": "set asyncapi: in integration.yaml"}
        if self.asyncapi is None:
            self.asyncapi = yaml.safe_load((self.cfg.root / self.cfg.asyncapi).read_text("utf8"))
        msgs = ((self.asyncapi.get("components") or {}).get("messages") or {})
        msg = msgs.get(name)
        if msg is None:
            return {"check": f"matches schema {name}", "ok": False, "detail": f"no message '{name}' in {self.cfg.asyncapi}"}
        from k11_contract_qa.spec import resolve

        payload = resolve(self.asyncapi, (resolve(self.asyncapi, msg) or {}).get("payload") or {})
        errs = list(Draft202012Validator(payload).iter_errors(m.value))[:3]
        return {"check": f"matches AsyncAPI message {name}", "ok": not errs,
                "detail": "; ".join(f"{'.'.join(str(x) for x in e.absolute_path) or '(payload)'}: {e.message[:100]}" for e in errs)}

    # ---- jobs and waiting --------------------------------------------------------------------
    def _run_job(self, spec: dict, v: Vars, sr: StepResult, listeners: dict, auth: str) -> None:
        name = spec.get("job")
        job = self.cfg.jobs.get(name)
        if job is None:
            raise ValueError(f"Unknown job '{name}'. Known: {sorted(self.cfg.jobs) or ['(none in integration.yaml)']}")
        if not self.writes:
            sr.status, sr.error = "blocked", f"Running jobs is refused on '{self.env_name}': {self.writes_why}"
            return
        timeout = seconds(spec.get("timeout") or job.get("timeout"), 300)
        sr.request = {"job": name}
        if job.get("command"):
            cmd = [str(v.sub(sub_env(x))) for x in job["command"]]
            env = {**os.environ, **{k: str(v.sub(sub_env(x))) for k, x in (job.get("env") or {}).items()}}
            try:
                p = subprocess.run(cmd, cwd=self.cfg.root, env=env, capture_output=True, text=True, encoding="utf-8",
                                   errors="replace", timeout=timeout)
            except subprocess.TimeoutExpired:
                sr.assertions.append({"check": f"job {name} finishes within {timeout:g}s", "ok": False, "detail": "timed out"})
                return
            except FileNotFoundError as e:
                raise ValueError(f"Job '{name}': can't start {cmd[0]} ({e})") from None
            tail = (p.stdout + p.stderr).strip().splitlines()[-8:]
            sr.response = {"exit_code": p.returncode, "output": v.mask("\n".join(tail))[:1500]}
            sr.assertions.append({"check": f"job {name} exits 0", "ok": p.returncode == 0,
                                  "detail": "" if p.returncode == 0 else f"exit code {p.returncode}"})
            return
        start = self.http._step({**job["http"], "name": f"start {name}"}, v, auth, 0)
        sr.request, sr.response = start.request, start.response
        if start.status != "passed":
            sr.assertions += start.assertions or [{"check": f"start {name}", "ok": False, "detail": start.error or ""}]
            return
        poll = job.get("poll")
        if not poll:
            sr.assertions.append({"check": f"job {name} started", "ok": True, "detail": ""})
            return
        deadline = time.time() + timeout
        while True:
            r = self.http._step({"call": poll["call"], "name": f"poll {name}"}, v, auth, 0)
            body = (r.response or {}).get("body")
            if poll.get("fail_if") and all(_ok(body, k, v.sub(e)) for k, e in poll["fail_if"].items()):
                sr.response = r.response
                sr.assertions.append({"check": f"job {name} succeeds", "ok": False, "detail": "the job reported failure"})
                return
            if all(_ok(body, k, v.sub(e)) for k, e in (poll.get("until") or {}).items()):
                sr.response = r.response
                sr.assertions.append({"check": f"job {name} finishes", "ok": True, "detail": ""})
                return
            if time.time() >= deadline:
                sr.response = r.response
                sr.assertions.append({"check": f"job {name} finishes within {timeout:g}s", "ok": False, "detail": "timed out"})
                return
            time.sleep(float(poll.get("interval_s", 1)))

    def _wait_until(self, spec: dict, v: Vars, sr: StepResult, listeners: dict, auth: str) -> None:
        within = seconds(spec.get("within"), 10)
        inner = {k: x for k, x in spec.items() if k != "within"}
        if bool(inner.get("db")) == bool(inner.get("call")):
            raise ValueError("wait_until needs 'db' (a SELECT) or 'call' (a GET), with 'expect'")
        if inner.get("call") and str(inner["call"]).split(" ", 1)[0].upper() not in ("GET", "HEAD"):
            raise ValueError("wait_until only polls GET calls")
        deadline = time.time() + within
        tries = 0
        while True:
            tries += 1
            r = self.http._step(inner, v, auth, 0)
            if r.status == "passed" or time.time() >= deadline or r.status in ("blocked", "error"):
                break
            time.sleep(0.25)
        sr.request, sr.response, sr.query = r.request, r.response, r.query
        sr.saved = r.saved
        if r.status in ("blocked", "error"):
            sr.status, sr.error = r.status, r.error
            return
        for a in r.assertions:
            sr.assertions.append({**a, "check": a["check"], "detail": a.get("detail", "")})
        sr.assertions.append({"check": f"condition met within {within:g}s", "ok": r.status == "passed",
                              "detail": f"after {tries} check(s)" if r.status == "passed" else f"not met after {tries} check(s)"})


def _ok(body: Any, path: str, expected: Any) -> bool:
    found, val = json_path(body, path) if isinstance(body, (dict, list)) else (False, None)
    return check(val, expected, found)[0]


def flow_files(root: Path, targets: list[str] | None = None) -> list[Path]:
    from k11_contract_qa.spec import inside

    if not targets:
        base = Path(root) / "tests" / "integration"
        return sorted(p for p in base.rglob("*.y*ml") if p.suffix in (".yaml", ".yml")) if base.is_dir() else []
    out = []
    for t in targets:
        p = inside(Path(root), t)
        out += sorted(x for x in p.rglob("*.y*ml") if x.suffix in (".yaml", ".yml")) if p.is_dir() else [p]
    return out


def run_files(api_cfg: ApiConfig, cfg: IntegrationConfig, files: list[Path], environment: str | None = None,
              tags: list[str] | None = None) -> tuple[list[FlowResult], str]:
    runner = IntegrationRunner(api_cfg, cfg, environment)
    results = []
    try:
        for f in files:
            rel = f.relative_to(api_cfg.root).as_posix() if f.is_relative_to(api_cfg.root) else f.name
            try:
                flow = load_flow(f)
            except FlowError as e:
                results.append(FlowResult(f.stem, rel, runner.env_name, "error", error=str(e)))
                continue
            if tags and not set(tags) & set(flow.get("tags") or []):
                continue
            results.append(runner.run_flow(flow, rel))
    finally:
        runner.close()
    return results, runner.env_name


def to_json(results: list[FlowResult]) -> list[dict]:
    from dataclasses import asdict

    return [json.loads(json.dumps(asdict(r), default=str)) for r in results]
