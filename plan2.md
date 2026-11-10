# Plan 2: Restate-native Scion Hub

## Objective

Build a Scion Hub whose application state and durable orchestration live entirely in Restate. No SQLite, PostgreSQL, AlloyDB, Ent ORM, relational store adapter, or hidden relational fallback in the resulting deployment.

Preserve the existing CLI and web-facing HTTP and streaming contracts. Use Restate's object state, durable execution, timers, and query facilities directly rather than recreating the SQL-backed Hub inside Restate.

This is a proposed implementation plan, not a claim that the migration or feasibility checks have been completed.

## Architectural decision

Implement a Restate-native Hub behind Scion's existing network boundary. Do not first introduce a universal in-process `Hub` or `Store` interface.

Supporting users who prefer the relational Hub is a legitimate product concern, not inherently speculative abstraction. The network contract is the interoperability boundary: a relational Hub and a Restate Hub can implement it independently without forcing either engine into the other's persistence or execution model. This plan delivers the Restate-only implementation; it does not commit to maintaining both implementations indefinitely.

During development, the existing Hub remains available as a behavioral reference. The Restate deployment must not call it or depend on its database. No dual writes or per-request fallback. Decide distribution and ownership of any retained relational implementation before final removal from the Restate build.

### Target responsibilities

- **API gateway:** existing authentication, authorization, validation, HTTP response semantics, and client stream protocols; translates requests to Restate operations. No durable state in process memory.
- **Restate services:** entity state, serialized mutations, lifecycle workflows, durable dispatch, timers, indexes, and application events.
- **Runtime Broker:** container and workspace execution, runtime observation, and idempotent lifecycle effects. Hub decisions remain outside the broker; harness-specific behavior stays in `pkg/harness`.
- **Query adapter:** authenticated, authorization-scoped listing and filtering over verified Restate query facilities. No public exposure of Restate administrative/query endpoints.
- **Stream adapter:** preserves current client protocols while implementing the selected broker-to-client transport. Durable lifecycle state and transient terminal bytes are distinct concerns.

## Corrections to earlier design assumptions

These are mandatory implementation constraints:

1. Restate's internal bidirectional service protocol is not, by itself, proof of an arbitrary client-facing PTY streaming API. Invocation attachment is not proof of incremental terminal output delivery.
2. Compensation must be explicitly implemented and tested. Do not use the previously illustrated `ctx.OnCompensation` API without verifying an actual SDK equivalent. Durable execution does not automatically reverse external effects.
3. Broker operations can execute an external effect before their result is durably recorded. Stable operation IDs, idempotency, and reconciliation are required; do not promise exactly-once container creation or zero leaks from journaling alone.
4. Per-key serialization does not supply cross-object transactions. Long-running exclusive handlers can block stop/cancel commands; workflow ownership and short object mutations must be designed accordingly.
5. The Cloud tunnel client proves a client exists, not that an equivalent tunnel server is available in the team's self-hosted deployment. Verify deployment support and broker targeting before removing Scion's transport.
6. The earlier DataFusion SQL was illustrative, not verified syntax. Verify schemas, functions, consistency, pagination, access controls, and query cost against the actual deployed version.
7. Timer benchmark results are useful evidence, not universal latency guarantees under the new workload.

## Phase 0 — Inventory and prove the replacement boundary

### Work

- Inventory public routes, request/response types, errors, status codes, pagination, authorization rules, SSE events, terminal protocols, and broker-facing contracts.
- Trace consumers in `pkg/hubclient`, `cmd`, the web client, and Runtime Broker. Include local Combo server startup and shutdown, not just hosted HTTP clients.
- Inventory every durable entity and background responsibility in `pkg/store`, `pkg/ent`, and `pkg/hub`. Assign each to a Restate owner or explicitly identify non-database storage such as workspace files and artifact blobs.
- Include identities, memberships, permissions, sessions/tokens, secrets and key handling, projects, agents, broker presence, placement, templates, harness configuration, skills, schedules, messages, notifications, quotas, and audit records. Confirm the complete list from source.
- Record exact Restate server and Go SDK versions from live sources. Review SDK APIs and deployment compatibility before pinning dependencies.
- Exercise small proofs against a disposable Restate environment: keyed state mutation/read, workflow retry/replay, delayed invocation, query access, broker routing, and streaming transport.
- Verify whether existing Restate production hosting meets all required features. Do not modify production or active project agents for these proofs.

### Acceptance

A contract/ownership matrix covers the full Hub surface. Each required platform capability has observed evidence or a concrete Restate-only implementation approach. No unresolved assumption about tunnels, queries, or terminal streaming is treated as implemented functionality.

## Phase 1 — Design state ownership and operation semantics

### Work

- Define versioned state schemas and stable IDs for projects, agents, broker registrations, identities, configuration, and the remaining inventoried entities.
- Define `AgentObject[agentID]` as the authority for agent lifecycle state, current run/generation, broker assignment, and operation ownership.
- Define `ProjectObject[projectID]` for project configuration. Design bounded or partitioned membership indexes rather than an unbounded agent array.
- Define quota ownership by scope with idempotent reservation tokens. Reserve/release must survive retries and partial workflow failure.
- Use uniquely keyed lifecycle workflows per operation/run; do not assume a completed workflow key is reusable for later launches.
- Specify create/start/stop/delete/resume concurrency, cancellation, stale broker reports, duplicate requests, timeout policy, and terminal failure states.
- Keep exclusive object mutations short. Do not hold an agent object lock while waiting for a workflow that must call the same object.
- Define explicit compensation order, retry behavior, operator intervention, and reconciliation for effects with uncertain outcomes.
- Define indexes and uniqueness reservations for names, identities, and other lookup keys. Document cross-object consistency and recovery rather than assuming transactions.
- Define encryption/key ownership and redact secrets from workflow inputs, journals, logs, query results, and client responses where possible. Restate state and journal retention must be included in the threat model.

### Acceptance

State-transition and failure tables cover every lifecycle operation. Each datum has one authoritative owner. Duplicate operations, cancellation, stale generations, and partial cross-object updates have defined outcomes.

## Phase 2 — Deliver one complete lifecycle slice

### Work

- Implement the initial Restate service package and a separately runnable gateway, following repository naming and packaging conventions established during Phase 0.
- Implement project registration, agent creation, quota reservation, broker selection, provisioning, launch, status read, stop, and delete.
- Preserve the corresponding existing `/api/v1` contracts from the start. Reuse boundary types and pure validation where appropriate; do not pull SQL-backed Hub initialization into the new process.
- Initially invoke the existing broker execution API if suitable. Add stable operation IDs and broker-side reconciliation where needed. Avoid changing the broker transport at the same time unless required for reachability.
- Implement explicit durable compensation for failed launch and teardown. Record uncertain outcomes until reconciled; do not release ownership prematurely and permit duplicate resources.
- Gate new user-facing experimental selection through Scion's experiment registry and enforce it server-side, following repository rules.

### Acceptance

Against a real disposable runtime and Restate instance, an unchanged CLI creates, lists, stops, and deletes an agent through the new gateway. Crash/restart between provision and result recording does not create a second container. Retried cleanup eventually releases the matching quota reservation. No relational database is configured, contacted, or required.

## Phase 3 — Complete state, querying, and background work

### Work

- Migrate every remaining responsibility from the Phase 0 matrix, preserving authorization semantics, not merely route names.
- Implement broker presence, placement, scheduled work, notifications, and expiry/reconciliation using durable timers and invocations. Validate stale timer handling with generation checks.
- Implement DataFusion-backed listing only after verifying actual schemas and supported SQL. Apply tenant/project authorization before results leave the gateway; use safe query construction.
- Measure freshness, sorting, pagination stability, and scan cost with representative data. Use Restate-maintained query indexes where the product requires stronger lookup semantics or bounded work; no relational read model fallback.
- Establish state schema evolution, workflow replay compatibility, retention/compaction, audit access, and operational inspection procedures.

### Acceptance

Every stateful Hub capability is implemented without a relational store. Existing API behavior checks pass against the Restate gateway. Representative query/load scenarios meet explicitly agreed correctness and performance criteria, including read-after-write expectations.

## Phase 4 — Broker connectivity and client streams

### Work

- Prove authenticated dispatch to a specific broker, reconnection, offline buffering policy, credential rotation, and broker replacement.
- If supported by the actual hosting environment, integrate the Restate tunnel facility. Otherwise implement a supported Restate-native dispatch/pull approach or retain a stateless transport adapter; no SQL queue or shadow Hub.
- Preserve current CLI/web terminal, log, and event protocols. Replacing WebSockets with POST/SSE at the client boundary would violate the zero-client-rewrite goal.
- Select durable event/pubsub implementation only after verifying SDK/language support. Define event IDs, retention, replay, slow-consumer limits, and gap handling.
- Exercise terminal resize, binary data, interactive input, disconnect/reconnect, exit, and authorization expiry. Do not journal every keystroke by default without latency, retention, and security evidence.
- Remove the custom broker transport only when replacement semantics are demonstrated. A stateless PTY proxy is compatible with Restate-only persistence and orchestration; it is not a second database.

### Acceptance

Existing `scion attach` and the actual web terminal work without client protocol changes. Logs/events reconnect with documented replay behavior. NAT-isolated brokers receive correctly targeted work through the selected supported transport. No application data store besides Restate is introduced.

## Phase 5 — Deployment, data migration, and cutover

### Work

- Implement explicit workstation and hosted startup/shutdown paths. A workstation Restate dependency must be intentionally provisioned and started; never start infrastructure implicitly from ordinary agent commands.
- Validate readiness, backup/restore, disaster recovery, credentials, network isolation, upgrades, and state/workflow compatibility.
- If existing data must be retained, implement a bounded export/import tool with stable ID preservation, encrypted-secret handling, integrity checks, and counts. Run it during a controlled write freeze, not through dual writes.
- Decide how to drain existing agents and in-flight operations. Do not run competing Hub authorities for the same agent.
- Define rollback before migration. Once Restate accepts new writes, switching back to the old database is not safe without an explicit reverse migration; snapshot rollback alone loses those writes.
- Run the complete CLI/web/broker acceptance matrix and failure scenarios in an isolated deployment.
- Resolve release packaging: the Restate distribution must exclude relational dependencies and initialization. Any separately retained relational implementation needs explicit ownership and independent compatibility testing.

### Acceptance

A fresh installation and, where required, a migrated installation run entirely on Restate. Restart and restore preserve authoritative state and resumable operations. Operators can diagnose failed work and recover it without database surgery.

## Phase 6 — Remove replaced implementation and ship

### Work

- Remove obsolete Hub coordination, leased claims, SQL dispatch queues, pollers, Ent/store adapters, migrations, and relational configuration from the Restate implementation after all consumers have moved.
- Do not blindly delete `pkg/hub`: relocate or retain necessary HTTP, authentication, authorization, validation, and streaming behavior first.
- Remove unused SQL dependencies from the module/build graph. Verify the shipped binary and deployment manifests do not require a relational service or embedded SQLite.
- Update architecture, installation, operations, configuration, and migration documentation plus release notes. Record intentional contract differences explicitly rather than silently narrowing support.
- Run focused tests and real-path smoke scenarios, then repository-required CI before committing. Follow local-main integration/rebase policy and re-test conflict resolutions.

### Acceptance

The release satisfies the full ownership/contract matrix, not only the agent-launch prototype. There is no relational runtime fallback, no stubbed endpoint, and no dependency on the old Hub. Unchanged clients pass the agreed compatibility suite against the Restate-only deployment.

## Required failure and compatibility scenarios

- Concurrent start/start, start/stop, stop/delete, and repeated delete.
- Broker timeout before execution and timeout after the effect succeeds.
- Gateway, service worker, broker, and Restate node failure during lifecycle transitions.
- Duplicate or late broker reports from an earlier agent generation.
- Quota exhaustion, duplicated reservation/release, and compensation interrupted by restart.
- Cancellation while provisioning and while awaiting broker availability.
- Unauthorized cross-project reads, query filtering, stream attachment, and mutation.
- Query visibility immediately after mutation and across pagination under concurrent changes.
- Timer recovery and stale timer suppression after restart or operation replacement.
- Terminal interaction and reconnect using the actual CLI and web surfaces.
- Backup/restore and upgrade with in-flight workflows and versioned object state.

## First implementation milestone

Complete Phases 0 and 1, then deliver the Phase 2 lifecycle slice in isolation. The first proof is an unchanged CLI driving a real agent through create → list → stop → delete, followed by fault-injected recovery and verified cleanup, with Restate as the only application persistence and orchestration system.

Do not delete the current implementation, modify active agents, or claim full replacement until the remaining phases satisfy their acceptance criteria.
