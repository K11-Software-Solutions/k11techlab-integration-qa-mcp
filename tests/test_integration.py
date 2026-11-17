"""K11 Shop end to end: every flow passes on the correct shop, and each deliberate bug is caught by the flow
written for it. Plus the safety rules, flow validation, the report, the CLI and the MCP tools."""

import json
import os

import anyio
import pytest

from k11_integration_qa import config
from k11_integration_qa.cli import main
from k11_integration_qa.flows import FlowError, validate
from k11_integration_qa.run import run

FLOWS = "tests/integration"


def by_name(out):
    return {r.file.rsplit("/", 1)[-1]: r for r in out["results"]}


def failing(r):
    return [s for s in r.steps if s.status != "passed"]


def test_the_correct_shop_passes(project, make_shop):
    make_shop()
    out = run(project)
    doc = out["report"]
    assert doc["status"] == "completed" and doc["reason"] == "all flows passed", [failing(r) for r in out["results"]]
    s = doc["summary"]
    assert s["flows"] == 4 and s["passed"] == 4
    assert s["messages_published"] == 2 and s["jobs_run"] == 2 and s["schema_violations"] == 0
    assert doc["tool"].startswith("k11-integration-qa-mcp ")
    # evidence: the matched message is in the report
    order = by_name(out)["order_reserves_stock.yaml"]
    reserved = next(x for x in order.steps if x.name.startswith("the inventory consumer"))
    value = reserved.response["message"]["value"]
    assert value["type"] == "StockReserved" and value["qty"] == 3
    assert (project / ".qa-mcp/integration-report/integration-report.json").is_file()
    assert (project / ".qa-mcp/integration-report/integration-junit.xml").is_file()


def test_consumer_bug_is_caught_with_the_closest_message(project, make_shop):
    make_shop("consumer")
    r = run(project, targets=[f"{FLOWS}/order_reserves_stock.yaml"])["results"][0]
    assert r.status == "failed"
    step = failing(r)[0]
    assert step.kind == "expect_message"
    detail = step.assertions[0]["detail"]
    assert "0 of 1 matching" in detail and "$.qty: expected 3, got 1" in detail, detail


def test_dead_letter_bug_is_caught(project, make_shop):
    make_shop("dlq")
    r = run(project, targets=[f"{FLOWS}/poison_message_dead_lettered.yaml"])["results"][0]
    assert r.status == "failed" and "0 message(s) on orders.dlq" in failing(r)[0].assertions[0]["detail"]


def test_non_idempotent_job_is_caught(project, make_shop):
    make_shop("job")
    r = run(project, targets=[f"{FLOWS}/sku_totals_job.yaml"])["results"][0]
    assert r.status == "failed"
    bad = failing(r)[0]
    assert bad.kind == "db" and bad.assertions[0]["detail"] == "expected 2, got 4"
    jobs = [s for s in r.steps if s.kind == "run_job"]
    assert len(jobs) == 2 and all(j.status == "passed" and j.response["exit_code"] == 0 for j in jobs)


def test_schema_violation_is_reported(project, make_shop):
    make_shop()
    api = project / "asyncapi.yaml"
    api.write_text(api.read_text("utf8").replace("required: [type, eventId, orderId, sku, qty, at]",
                                                 "required: [type, eventId, orderId, sku, qty, at, currency]"), "utf8")
    out = run(project, targets=[f"{FLOWS}/order_reserves_stock.yaml"])
    step = failing(out["results"][0])[0]
    bad = [a for a in step.assertions if not a["ok"]]
    assert bad[0]["check"] == "matches AsyncAPI message OrderCreated" and "currency" in bad[0]["detail"]
    assert out["report"]["summary"]["schema_violations"] == 1


def test_production_environment_refuses_publishing_and_jobs(project, make_shop):
    make_shop()
    out = run(project, "prod", targets=[f"{FLOWS}/poison_message_dead_lettered.yaml", f"{FLOWS}/sku_totals_job.yaml"])
    for r in out["results"]:
        assert r.status == "blocked", (r.name, failing(r))
    pub = next(s for s in out["results"][0].steps if s.kind == "publish")
    assert "refused on 'prod'" in pub.error
    # the fixture insert is refused first in the job flow, so no job ever runs on prod
    assert not any(s.kind == "run_job" and s.status == "passed" for s in out["results"][1].steps)


def test_protected_topics_are_never_published_to(project, make_shop):
    make_shop()
    flow = project / FLOWS / "to_prod.yaml"
    flow.write_text("name: oops\nsteps:\n  - publish: { broker: main, topic: prod.orders.commands, value: {a: 1} }\n", "utf8")
    r = run(project, targets=[f"{FLOWS}/to_prod.yaml"])["results"][0]
    assert r.status == "blocked" and "protected_topics" in r.steps[0].error


def test_no_flows_means_not_run(project, make_shop):
    make_shop()
    for f in (project / FLOWS).glob("*.yaml"):
        f.unlink()
    doc = run(project)["report"]
    assert doc["status"] == "not_run" and doc["summary"]["flows"] == 0


@pytest.mark.parametrize("flow, msg", [
    ({"name": "x", "steps": [{"publish": {"topic": "t"}, "call": "GET /"}]}, "exactly one of"),
    ({"name": "x", "steps": [{"listen": {"topik": "t"}}]}, "listen.topik"),
    ({"name": "x", "steps": [{"expect_message": {"match": {}}}]}, "needs 'from'"),
    ({"name": "x", "steps": [{"run_job": "nightly"}]}, "needs a mapping"),
    ({"name": "x", "stepz": []}, "needs 'name' and 'steps'"),
])
def test_flow_validation(flow, msg):
    with pytest.raises(FlowError, match=msg):
        validate(flow, "f.yaml")


def test_config_validation(tmp_path):
    (tmp_path / ".qa-mcp").mkdir()
    p = tmp_path / config.CONFIG
    p.write_text("jobs:\n  j: { command: 'rm -rf /' }\n", "utf8")
    with pytest.raises(ValueError, match="list of strings"):
        config.load(tmp_path)
    p.write_text("brokers:\n  b: { type: rabbit }\n", "utf8")
    with pytest.raises(ValueError, match="kafka or memory"):
        config.load(tmp_path)
    assert config.seconds("500ms") == 0.5 and config.seconds("2m") == 120


def test_cli_exit_codes(project, make_shop, capsys):
    make_shop("job")
    assert main(["run", f"{FLOWS}/sku_totals_job.yaml"]) == 1
    assert "the total is still 2" in capsys.readouterr().out
    assert main(["run", "--fail-on", "never", f"{FLOWS}/sku_totals_job.yaml"]) == 0
    md = (project / ".qa-mcp/integration-report/integration-report.md").read_text("utf8")
    assert "❌" in md and "expected 2, got 4" in md


def test_mcp_tools(project, make_shop):
    from mcp.shared.memory import create_connected_server_and_client_session

    from k11_integration_qa import __version__
    from k11_integration_qa.server import create_server

    make_shop()

    async def go():
        async with create_connected_server_and_client_session(create_server()._mcp_server) as c:
            info = await c.initialize()
            assert info.serverInfo.version == __version__
            names = {t.name for t in (await c.list_tools()).tools}
            assert names == {"integration_config", "run_integration_tests", "validate_flow", "watch_topic", "get_report",
                             "init_integration_config"}
            cfg = json.loads((await c.call_tool("integration_config", {})).content[0].text)
            assert cfg["brokers"] == {"main": {"type": "memory"}} and "sku_totals" in cfg["jobs"]
            assert cfg["environments"]["prod"]["publish_and_jobs"].startswith("refused")
            res = json.loads((await c.call_tool("run_integration_tests", {"tags": ["dlq"]})).content[0].text)
            assert res["summary"]["flows"] == 1 and res["summary"]["passed"] == 1
            rep = json.loads((await c.call_tool("get_report", {"flow": "An unreadable command goes to the dead-letter queue"})).content[0].text)
            assert rep["status"] == "passed"
            v = json.loads((await c.call_tool("validate_flow", {"path": f"{FLOWS}/cancel_releases_stock.yaml"})).content[0].text)
            assert v["valid"] and "expect_no_message" in v["kinds"]

    anyio.run(go)


@pytest.mark.skipif(not os.environ.get("KAFKA_BOOTSTRAP"), reason="set KAFKA_BOOTSTRAP to run against a real Kafka")
def test_real_kafka(project, make_shop):
    cfg = project / config.CONFIG
    cfg.write_text(cfg.read_text("utf8").replace("main: { type: memory, name: shop }",
                                                 'main: { type: kafka, bootstrap: "${ENV:KAFKA_BOOTSTRAP}" }'), "utf8")
    make_shop(kafka=True)
    out = run(project)
    assert out["report"]["reason"] == "all flows passed", [(r.name, failing(r)) for r in out["results"]]
    order = by_name(out)["order_reserves_stock.yaml"]
    msg = next(s for s in order.steps if s.kind == "expect_message").response["message"]
    assert msg["partition"] is not None and msg["offset"] is not None   # real Kafka metadata
