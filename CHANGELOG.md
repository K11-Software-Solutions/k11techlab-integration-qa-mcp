# Changelog

## 0.1.0 (16 November 2026)

- First release: integration flows (`tests/integration/*.yaml`) that combine HTTP calls and database checks
  (shared with k11 API Functional QA: environments, auth, fixtures, cleanup) with message and job steps:
  `listen`, `publish`, `expect_message` / `expect_no_message` (JSON-path, key and header matching, time limits,
  the closest message when nothing matches), `run_job` (a command without a shell, or an HTTP job with polling)
  and `wait_until` (poll a SELECT or GET until it holds).
- Brokers: Apache Kafka (`[kafka]` extra) and an in-process broker for tests. AsyncAPI payload checks.
- Publishing and jobs only where writes are allowed; protected (production-named) topics are never published to.
- Demo K11 Shop with switchable bugs (consumer, dead-letter queue, non-idempotent job); CI runs it on a real Kafka.
- MCP tools, CLI for CI, `/integration-check` command; `integration-report.json` is read by k11 Release Gate.
