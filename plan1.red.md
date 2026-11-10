# Red-Team Comparison: `plan1.md` vs. `plan2.md`

This document brutally evaluates `plan1.md` and `plan2.md` against the actual Scion (`./`) and Restate (`../restate`) codebases, exposing where each plan is right, where each plan fails, where they overlap, and what must be fixed before writing code.

---

## 1. Executive Summary Matrix

| Dimension | `plan1.md` | `plan2.md` | Winner & Why |
| :--- | :--- | :--- | :--- |
| **Codebase Grounding** | Names exact files, symbols, interfaces, and LOC counts (`agent_create_tx.go`, `deletionEngine`, `BrokerDispatchStore`). | Zero Scion file names, zero symbol names, zero line counts. Pure high-level prose. | **`plan1.md`** — `plan2.md` punts reading the codebase to a bureaucratic "Phase 0". |
| **Concurrency & Lock Design** | **Fatal flaw**: Runs long `Create` sagas inside `AgentObject`'s exclusive handler, blocking `Stop`/`Delete` while awaiting broker image pulls. | **Airtight**: Separates short state-mutation locks on `AgentObject` from long-running `Workflow` executions so `Stop`/`Delete` never deadlock. | **`plan2.md`** — Prevents head-of-line blocking on Virtual Objects. |
| **Idempotency & Crash Safety** | Assumes `restate.Run` alone prevents duplicate containers. | Identifies the exact window where a broker spawns a container before `ProposeRunCompletionMessage` is journaled; mandates deterministic `run_id` reconciliation. | **`plan2.md`** — Understands the real wire protocol in `../restate`. |
| **State Granularity** | **Flawed**: Stores `[]Message` and `map[string]AgentSummary` as monolithic blobs in single state keys ($O(N^2)$ RocksDB write amplification). | Mandates bounded/partitioned state keys (`msg:<seq>`, `agent:<id>`) rather than unbounded arrays. | **`plan2.md`** — Respects Restate's `SetStateCommandMessage` mechanics. |
| **Query Architecture** | Uses Virtual Object index keys (`ProjectObject`) for fast $O(1)$ scoped lookups. | Tries to use Restate's internal DataFusion SQL engine (`storage-query-datafusion`) for user-facing list/filter APIs. | **`plan1.md`** — DataFusion in Restate is an admin/introspection scan engine, not a low-latency multi-tenant OLTP index. |
| **Secrets & Threat Model** | Ignores the fact that Restate journals all handler inputs/outputs in plaintext in RocksDB. | Explicitly requires envelope encryption (`enc:v2:`) and redaction so secrets never hit Restate journals or UI traces in plaintext. | **`plan2.md`** — Catches a major security regression. |
| **Cutover & Alpha Policy** | Clean cutover. Deletes 630K LOC (`pkg/ent`, `pkg/store`) per `AGENTS.md` ("Alpha: changing or removing behavior needs no deprecation path"). | **Bloated**: Proposes maintaining both Relational and Restate Hubs behind a network boundary, building data export/import tools, reverse migrations, and experiment flags. | **`plan1.md`** — `plan2.md` violates `AGENTS.md` and doubles maintenance burden instead of deleting complexity. |
| **Client/Streaming Boundary** | Suggests `@restatedev/pubsub` (a TypeScript-only package) and changing CLI/Web stream endpoints. | Preserves the exact `/api/v1` HTTP/SSE/WebSocket gateway contract so CLI and Web require zero rewrite. | **`plan2.md`** — Keeps blast radius strictly inside the Hub/Broker. |

---

## 2. Brutal Critique of `plan1.md` (Where Our Plan Fails)

### Failure 1: The Virtual Object Deadlock Trap (§4.1 `AgentObject.Create`)
`plan1.md` puts the entire provisioning saga inside an exclusive handler on `AgentObject[agent_id]`, including waiting on an Awakeable while the Runtime Broker pulls a container image and provisions the workspace.
- **Why this breaks in Restate**: When a Restate Virtual Object handler suspends (`SuspensionMessage` in `protocol.proto`), **it keeps the exclusive lock on `agent_id`** (unless handler-level concurrency is explicitly relaxed, which breaks single-writer safety).
- **The Consequence**: While an agent is stuck pulling a 5GB container image or waiting on an offline broker, a user running `scion stop <agent>` or `scion delete <agent>` will hang in Restate's virtual queue (`crates/vqueues`) waiting for `Create` to finish.
- **The Fix (from `plan2.md`)**: `AgentObject` handlers must be **short, non-blocking state transitions** (`BeginLaunch`, `CommitLaunch`, `MarkFailed`, `BeginDelete`). The multi-step orchestration wait and saga compensation belong in a per-run **`AgentLaunchWorkflow[run_id]`** (`restate.Workflow`). If `Delete` arrives on `AgentObject` while a launch workflow is in flight, `AgentObject` bumps its generation counter and cancels `AgentLaunchWorkflow[run_id]` immediately via `restate.CancelInvocation`.

### Failure 2: The `restate.Run` Container Leak Illusion (§2 & §4.1)
`plan1.md` claims that wrapping `BrokerProvisionAndStart` inside `restate.Run` guarantees crash safety without extra work.
- **Why this is wrong**: Look at `../restate/service-protocol/dev/restate/service/protocol.proto` (`ProposeRunCompletionMessage`). When the Go SDK executes `restate.Run`, it runs the side-effect closure *first*, and then proposes the result back to `restate-server` to be journaled.
- If the Broker creates the Docker container, and the Hub process crashes (or the TCP connection to the Broker drops) *1 millisecond before* `ProposeRunCompletionMessage` is committed to Restate’s Bifrost log, Restate will replay the invocation and **execute the `restate.Run` block a second time**.
- **The Fix (from `plan2.md`)**: Every broker command must carry a deterministic `run_id` / `op_id` generated via `restate.UUID(ctx)` (seeded by `StartMessage.random_seed` in `protocol.proto`). The Broker must make container creation idempotent on `run_id` (if container with label `scion.run_id=X` already exists, adopt it instead of creating a second container).

### Failure 3: $O(N^2)$ State Blob Write Amplification (§4.2 & §4.4)
`plan1.md` proposes storing `"messages": []Message` in `ConversationObject` and `"agents": map[string]AgentSummary` in `ProjectObject`.
- **Why this breaks at scale**: Restate’s `SetStateCommandMessage` writes a `bytes value` for a `bytes key`. It does not have a "append to JSON array" opcode. Reading a 5,000-message slice with `restate.Get`, appending 1 message, and calling `restate.Set` re-serializes and writes all 5,001 messages into the Raft/Bifrost log on every single turn.
- **The Fix**: Use granular per-item keys inside Virtual Objects!Restate supports prefix/key listing (`GetEagerStateKeysCommandMessage` / `restate.Keys(ctx)`). Store each message under key `"msg:" + fmt.Sprintf("%08d", seq)` and each agent summary in `ProjectObject` under `"agent:" + agentID`. Each mutation is then an $O(1)$ single-entry write.

### Failure 4: Plaintext Secrets in the Restate Journal (§4.2)
`plan1.md` stores and resolves secrets through `ProjectObject.ResolveEnvironment` without mentioning encryption.
- **Why this is dangerous**: Restate records every RPC payload and state value in RocksDB and exposes them to `restate invocations inspect` and the Restate UI. Passing plaintext secret values as Restate RPC return values writes plaintext secrets to the WAL.
- **The Fix (from `plan2.md`)**: Keep Scion’s `pkg/secret` envelope encryption (`enc:v2:` ciphertexts). Only pass ciphertext or secret references through Restate state and workflow journals; decrypt only inside the ephemeral dispatch step (`restate.Run`) or on the Broker.

### Failure 5: `@restatedev/pubsub` Doesn't Exist in Go (§3 & §5)
`plan1.md` references `@restatedev/pubsub`, which is a TypeScript helper library, while Scion’s Hub is written in Go. While the *pattern* (a Virtual Object holding subscribers as Awakeable IDs) is 30 lines of Go using `restate.dev/sdk-go`, `plan1.md` also unnecessarily proposed changing CLI/Web stream endpoints instead of keeping Scion's existing `/api/v1` SSE/WebSocket routes at the HTTP gateway layer.

---

## 3. Brutal Critique of `plan2.md` (Where Plan 2 Fails)

### Failure 1: Violating `AGENTS.md` with Dual-Hub Maintenance & Migration Bureaucracy
Scion’s `AGENTS.md` line 3 states:
> **`Alpha: changing or removing behavior needs no deprecation path.`**

Yet `plan2.md` invents an enterprise legacy-migration nightmare:
- **Dual-Implementation Support (§Architectural decision)**: *"Supporting users who prefer the relational Hub is a legitimate product concern... a relational Hub and a Restate Hub can implement it independently... Decide distribution and ownership of any retained relational implementation"*. Keeping a 630,000-line SQL/Ent Hub alive alongside a new Restate Hub defeats the entire purpose of moving to Restate!
- **Data Export/Import & Reverse Migration Tools (Phase 5)**: *"implement a bounded export/import tool... switching back to the old database is not safe without an explicit reverse migration"*. Building bidirectional database migration tools between Ent/PostgreSQL and Restate RocksDB for an alpha product is pure waste.
- **Experiment Flag Gating (Phase 2)**: You do not gate a complete persistence engine replacement behind a runtime feature flag in `pkg/experiments/registry.go`.

### Failure 2: Zero Codebase Research ("Phase 0" Procrastination)
`plan2.md` contains **zero** file paths, **zero** Go struct/interface names, and **zero** line counts from Scion:
- Instead of inspecting `pkg/store/store.go` (which takes 30 seconds and reveals the exact 31 sub-interfaces), `plan2.md` writes a 15-line "Phase 0" task list instructing someone in the future to *"Inventory every durable entity and background responsibility in `pkg/store`, `pkg/ent`, and `pkg/hub`"*.
- A plan that hasn't looked at the code it plans to replace is a wish list, not an engineering plan.

### Failure 3: Misusing Restate's DataFusion Engine as an OLTP Database (Phase 3)
`plan2.md` wants to use Restate’s DataFusion SQL interface (`../restate/crates/storage-query-datafusion`) to serve application list/filter endpoints (`scion list`, web dashboard tables).
- **Why the Restate source code proves this is wrong**: In `../restate/crates/storage-query-datafusion`, DataFusion is wired over the internal `state` and `sys_journal` tables for operational introspection (`restate sql` CLI and Admin UI). State values are stored as opaque `bytes` blobs per `(service, service_key, key)`.
- Querying JSON fields inside state blobs via DataFusion requires scanning and deserializing every Virtual Object's state in the partition on every HTTP `GET /api/v1/projects/{id}/agents` call, with no secondary B-tree indexes!
- **Why `plan1.md` is right here**: Scion’s queries are 99% scoped to a `project_id` (`ListAgentsByProject`, `ListTemplatesByProject`, `ListConversationsByProject`). Storing `agent:<id> -> AgentSummary` keys inside `ProjectObject[project_id]` gives $O(1)$ strongly consistent, single-partition reads via `restate.Get` without touching DataFusion SQL.

### Failure 4: Defensive Scar Tissue ("Corrections to earlier design assumptions")
Section 2 of `plan2.md` (*"Corrections to earlier design assumptions"*, items 1–7) reads like a transcript of someone losing an argument with an LLM that hallucinated `ctx.OnCompensation` and Cloud tunnels. Those points belong baked into the architecture design (as concrete Go SDK patterns), not listed as defensive disclaimers.

---

## 4. Where Both Plans Overlap (The Validated Core)

Despite their differences, both plans independently converge on the same fundamental truths:
1. **Total Elimination of Relational Persistence**: Neither SQLite, PostgreSQL, nor Ent ORM survives in the target architecture. No dual-writes, no hybrid SQL read-models.
2. **Entity-Keyed State (`AgentObject`, `ProjectObject`, `QuotaObject`)**: Both model Agents, Projects, and Quotas as keyed Restate Virtual Objects rather than normalized relational tables.
3. **Explicit Saga Compensations**: Both use explicit reverse compensation stacks (`defer` in Go) with `restate.Run` steps to replace `compensateAgentCreate` and `deletionEngine`.
4. **Separation of PTY Byte Streams from Durable Orchestration**: Both recognize that interactive `tmux` terminal attach (`scion attach`) is an ephemeral byte stream that bypasses Restate’s durable journal via a lightweight WebSocket PTY proxy.
5. **Preserving the `/api/v1` HTTP Surface**: Both keep the HTTP API boundary so `cmd/` (`scion` CLI) and `web/` (Lit frontend) do not require architectural rewrites.

---

## 5. The Unified "Best-of-Both" Blueprint (What We Should Actually Build)

By taking `plan1.md`'s concrete codebase mapping and ruthless alpha cutover, and patching its 5 flaws with `plan2.md`'s distributed-systems invariants, we get the definitive architecture:

1. **Keep `/api/v1` Gateway Intact (`pkg/hub/server.go` HTTP layer)**:
   - Keep the HTTP router, auth middleware, RBAC evaluators, and SSE/WebSocket framing in `pkg/hub`.
   - Rip out `s.store` (`pkg/store`, `pkg/ent`) and replace handler internals with Restate client calls (`pkg/restatehub`).
2. **Split `AgentObject` (Short Locks) from `AgentLifecycleWorkflow` (Long Sagas)**:
   - `AgentObject[agent_id]` holds authoritative state (`"info"`, `"generation"`, `"active_op"`) and executes only fast, non-blocking mutations (`ClaimOp`, `CompleteOp`, `FailOp`, `RequestCancel`).
   - `AgentLifecycleWorkflow[op_id]` runs the long-running `Create` / `Start` / `Delete` sagas (`ReserveQuota` &rarr; `MintCreds` &rarr; `BrokerDispatch` &rarr; `AwaitLaunch` / `Compensate`).
   - If `Delete` arrives while `Create` is mid-flight, `AgentObject` increments `"generation"`, cancels the in-flight `AgentLifecycleWorkflow[op_id]`, and triggers its compensation stack immediately—zero head-of-line blocking.
3. **Granular Keyed State (Zero Blob Array Rewrites)**:
   - Use per-entry keys (`"agent:<id>"` in `ProjectObject`, `"msg:<seq>"` in `ConversationObject`, `"res:<op_id>"` in `QuotaObject`) instead of monolithic JSON slices.
4. **End-to-End Idempotency & Secret Redaction**:
   - Pass deterministic `op_id` / `run_id` (`restate.UUID(ctx)`) to the Runtime Broker so retried `restate.Run` calls reconcile existing containers instead of leaking duplicates.
   - Keep `pkg/secret` `enc:v2:` envelope encryption on all secret payloads stored in Restate state or passed through workflow parameters.
5. **Ruthless Alpha Cutover (No Dual-Hub, No Data Migration Tooling)**:
   - Execute the vertical slice (`Create` &rarr; `List` &rarr; `Stop` &rarr; `Delete`), migrate the remaining `store.Store` sub-interfaces (`Project`, `Broker`, `Quota`, `Authz`, `Conversation`), and delete all **631,841 lines** of `pkg/ent`, `pkg/store`, `agent_create_tx.go`, `agent_delete_engine.go`, `start_claim.go`, and `controlchannel.go`.
