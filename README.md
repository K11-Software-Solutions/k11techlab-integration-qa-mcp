# k11 Integration QA: MCP server and CI tool

<!-- mcp-name: io.github.K11-Software-Solutions/integration-qa -->

[![PyPI](https://img.shields.io/pypi/v/k11-integration-qa-mcp?label=PyPI)](https://pypi.org/project/k11-integration-qa-mcp/)
[![CI](https://github.com/K11-Software-Solutions/k11techlab-integration-qa-mcp/actions/workflows/ci.yml/badge.svg)](https://github.com/K11-Software-Solutions/k11techlab-integration-qa-mcp/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue)](LICENSE)

**The API returned 201. Did the consumer actually reserve the stock?**

In event-driven and batch systems the bugs live *between* the parts: the event is published with the wrong quantity,
a consumer drops a message it can't read instead of dead-lettering it, a nightly job double-counts when it's retried.
Unit tests and API tests each pass. k11 Integration QA tests the whole chain in one flow: the API call, the Kafka
message, the consumer's effect in the database, the dead-letter queue and the batch job, with evidence for the release decision.

```yaml
name: An order reserves stock through the inventory consumer
fixtures: [sku]                                   # a fresh SKU in the test database, deleted afterwards
steps:
  - listen: { broker: main, topic: orders.events, as: events }    # subscribe BEFORE acting
  - call: POST /orders
    body: { sku: "${fixture.sku.sku}", qty: 3 }
    expect: { status: 201 }
    save: { orderId: "$.id" }
  - name: OrderCreated is published and matches the AsyncAPI contract
    expect_message:
      from: events
      match: { "$.type": OrderCreated, "$.orderId": "${orderId}" }
      schema: OrderCreated
      within: 10s
  - name: the inventory consumer reserves all 3
    expect_message: { from: events, match: { "$.type": StockReserved, "$.orderId": "${orderId}", "$.qty": 3 }, within: 10s }
  - name: the reservation is in the database
    wait_until:
      db: SELECT reserved FROM inventory WHERE sku = :sku
      params: { sku: "${fixture.sku.sku}" }
      expect: { row: { reserved: 3 } }
      within: 10s
```

## See it work

The included demo, K11 Shop, has an order API, two consumers, a dead-letter queue and a batch job, each with a bug you
can switch on. With all three bugs on:

```text
4 of 4 flow(s) did not pass: 0 passed, 4 failed (2 published, 6 message checks, 2 job runs)
  - An order reserves stock through the inventory consumer — the inventory consumer reserves all 3:
      0 of 1 matching message(s) within 10s (2 message(s) on orders.events); closest: $.qty: expected 3, got 1
  - Cancelling an order releases its stock — the stock is released: reserved 0: expected 0, got -1; not met after 41 check(s)
  - An unreadable command goes to the dead-letter queue — it is dead-lettered with the reason:
      0 of 1 matching message(s) within 10s (0 message(s) on orders.dlq)
  - The sku_totals batch job is safe to run twice — the total is still 2: qty 2: expected 2, got 4
```

When nothing matches, the report shows the **closest message** and how it differed, so "the event never came" and
"the event came with the wrong quantity" are different findings. With the bugs off, all four flows pass. CI runs the
same flows against a real Apache Kafka broker.

```bash
pip install "k11-integration-qa-mcp[kafka]"
python demo/shop/shop.py --bugs consumer          # in-process broker; add --kafka localhost:9092 for Kafka
```

## Step types

Everything from [k11 API Functional QA](https://github.com/K11-Software-Solutions/k11techlab-api-functional-qa-mcp)
(`call`, `db`, fixtures, cleanup, auth profiles, `${ENV:…}` secrets, matchers like `{ gt: 0 }`), plus:

| Step | What it does |
|---|---|
| `listen` | Subscribe to a topic from *now on*, before the action that should produce the event |
| `publish` | Send a message (JSON or text, key, headers): commands, or a poison message for the DLQ |
| `expect_message` | A matching message arrives within the time limit; match on JSON paths, `key`, `header:Name`, `value`; `count`, `save`, AsyncAPI `schema` |
| `expect_no_message` | Nothing matching arrives (no `OrderFailed`, no duplicate) for the given time |
| `run_job` | Run a batch job: a command (a list, no shell) or an HTTP trigger with polling (`until`, `fail_if`) |
| `wait_until` | Poll a `SELECT` or `GET` until the expectation holds: eventual consistency without sleeps |

## Configure

`.qa-mcp/integration.yaml` (environments, auth, databases and fixtures come from `.qa-mcp/api-test.yaml`, shared with API Functional QA):

```yaml
brokers:
  main: { type: kafka, bootstrap: "${ENV:KAFKA_BOOTSTRAP}" }     # extra keys go to kafka-python (security_protocol, sasl_*…)
jobs:
  sku_totals:
    command: [python, jobs/sku_totals.py]
    env: { SHOP_DB: shop.db }
    timeout: 60s
  reindex:
    http: { call: POST /admin/jobs/reindex, save: { jobId: "$.id" } }
    poll: { call: "GET /admin/jobs/${jobId}", until: { "$.status": done }, fail_if: { "$.status": failed } }
asyncapi: asyncapi.yaml
protected_topics: ["prod.*", "*.prod.*", "*production*"]
```

## Safety

- **Publishing and jobs change data**, so they follow API Functional QA's write rules: local hosts, test-named environments or `allow_writes: true`; production-like names are refused.
- **Protected topics** (production-named by default) are never published to, on any environment.
- **Jobs run without a shell** (the command is a list), inside the project, with a time limit.
- **Every wait has a time limit;** listeners are closed and cleanup always runs.
- **Read-only listening:** listeners use no consumer group, so they never take messages from your real consumers or move their offsets.
- Secrets come from `${ENV:NAME}` and are masked in reports.

## Tools (MCP)

| Tool | What it does |
|---|---|
| `integration_config` | Brokers, jobs, AsyncAPI contract, protected topics, environments (and whether publishing and jobs may run there), flows |
| `run_integration_tests` | Runs flows (all, by path or by tag); writes `.qa-mcp/integration-report/` (Markdown, JSON, JUnit) |
| `validate_flow` | Checks a flow file's structure without running it |
| `watch_topic` | Listens to a topic for a few seconds and shows the messages, to learn their shape |
| `get_report` | The last report, or one flow's steps with full evidence (messages, queries, job output) |
| `init_integration_config` | Writes a starter `.qa-mcp/integration.yaml` and an example flow |

Command: **`/integration-check`**.

## Install and CI

```bash
pip install "k11-integration-qa-mcp[kafka]"
k11-integration-qa-mcp init                   # .qa-mcp/integration.yaml + tests/integration/example.yaml
k11-integration-qa-mcp run --env ci           # exit 1 when a flow fails (--fail-on never to only report)
```

Claude Code:

```text
/plugin install integration-qa --marketplace K11-Software-Solutions/k11techlab-integration-qa-mcp
```

On Claude Code before 2.1.275: `/plugin marketplace add K11-Software-Solutions/k11techlab-integration-qa-mcp`, then `/plugin install integration-qa@k11-integration-qa`.

[`docs/integration-tests.yml`](docs/integration-tests.yml) starts Kafka and runs the flows on every pull request.
`integration-report.json` is read by the [k11 Release Gate](https://github.com/K11-Software-Solutions/k11techlab-release-gate-mcp) as integration evidence.

## Part of the K11 QA suite

[Playwright QA](https://github.com/K11-Software-Solutions/k11techlab-playwright-qa-mcp) · [Selenium QA](https://github.com/K11-Software-Solutions/k11techlab-selenium-qa-mcp) · [Appium QA](https://github.com/K11-Software-Solutions/k11techlab-appium-qa-mcp) · [API Contract QA](https://github.com/K11-Software-Solutions/k11techlab-api-contract-qa-mcp) · [API Functional QA](https://github.com/K11-Software-Solutions/k11techlab-api-functional-qa-mcp) · [API Security QA](https://github.com/K11-Software-Solutions/k11techlab-api-security-qa-mcp) · **Integration QA** · [Performance QA](https://github.com/K11-Software-Solutions/k11techlab-performance-qa-mcp) · [LLM Assure](https://github.com/K11-Software-Solutions/k11techlab-llm-assurance-toolkit) · [Release Gate](https://github.com/K11-Software-Solutions/k11techlab-release-gate-mcp) · [Release Readiness Agent](https://github.com/K11-Software-Solutions/k11techlab-release-readiness-agent)

## Roadmap

- RabbitMQ, Amazon SQS/SNS and Google Pub/Sub brokers
- Schema Registry (Avro, Protobuf) payloads
- Ordering and exactly-once checks (per-key order, duplicate detection)
- Generating flows from an AsyncAPI document

---

Built by [K11 Software Solutions](https://k11softwaresolutions.com). MIT licensed.
