---
description: Run the integration flows (API, Kafka, batch jobs, database) and explain what broke
argument-hint: "[environment] [flow paths or tags]"
---

Test how this system's parts work together with the integration-qa MCP tools, and explain the result.

Input: $ARGUMENTS

1. Call `integration_config`. Use a test environment; publishing and jobs are refused where writes are not allowed and on protected topics. Never try to get around that, and never point a broker at production.
2. If there are no flows yet, read the code that produces and consumes messages (producers, consumers, listeners, job schedulers) and propose flows for the riskiest paths: an event is published with the right payload, each consumer does its work (check the database with `wait_until`), unreadable messages go to the dead-letter queue, a retried job does not double-count. Use `watch_topic` to learn message shapes. Ask before writing files.
3. Call `run_integration_tests` (with the environment, paths or tags given).
4. For each failing flow, say which part of the chain broke, in order: the API call, the event (missing, or the closest message and how it differed), the consumer's effect in the database, the dead-letter queue, the job. Quote the evidence: the matched or closest message, the query result, the job output.
5. Say what was not covered: topics with no flow, jobs never run, no AsyncAPI contract.

Never lengthen time limits, loosen a match or delete a step just to make a flow pass; a slow consumer is a finding.
