# Red-team review: plan1.md versus plan2.md

## Verdict

**Use Plan 2 as the migration framework, borrow selected concrete designs from Plan 1, and approve neither for broad implementation yet.**

Plan 1 is an aggressive implementation sketch with several correctness holes disguised as platform guarantees. Plan 2 is a substantially safer migration framework, but it repeatedly says “define,” “verify,” and “decide” where an implementation design needs an answer. Plan 1 risks building the wrong thing quickly. Plan 2 risks producing an inventory and an ever-expanding set of prerequisites instead of a working replacement.

These are not two fundamentally different solutions. Both propose Restate as Scion's authoritative state and execution engine. They differ in lifecycle ownership, client boundaries, query strategy, migration sequencing, and how honestly they account for the application logic that Restate cannot delete.

**The requirement remains non-negotiable:** the delivered Restate Scion has no SQLite, PostgreSQL, Ent, relational read model, or hidden dependency on the old Hub. That does not imply that every terminal byte, workspace file, artifact blob, or authentication decision belongs inside a durable workflow journal.

## Scope and evidence

This review compares the complete texts of `plan1.md` and `plan2.md`. References below use their current line numbers. It does not independently validate the plans' code-size counts, SDK signatures, endpoint paths, performance claims, or Restate deployment capabilities. Those are explicitly treated as unverified when material.

“Blocking” means the design must be resolved before implementing the affected boundary, not that every question must be answered before any prototype can run. The scenarios below are proposed acceptance checks, not tests executed during this document review.

## 1. What genuinely overlaps

| Area | Shared intent | Important qualification |
|---|---|---|
| Persistence | Replace SQL/Ent with Restate object state | Neither has completed the full entity-to-owner mapping |
| Agent lifecycle | Serialize agent mutations by agent ID | They disagree on whether slow work runs inside the exclusive object handler |
| Failure recovery | Replace manual saga machinery with journaled execution | Compensation and external-effect reconciliation still need application code |
| Quotas | Serialize capacity decisions by scope | Cross-scope reservation and transfer are not one object-local transaction |
| Scheduled work | Use durable timers and sends instead of SQL polling | Cron semantics, cancellation, stale timers, and reconciliation still need design |
| Broker control | Replace SQL dispatch outboxes and custom control RPC | Routing, authenticated delivery, and uncertain results remain unresolved |
| Events and messages | Use Restate-backed durable event/message state | Pubsub/SSE is a design to implement or integrate, not a blanket platform promise |
| Terminal traffic | Separate interactive traffic from durable control | Plan 1 explicitly retains raw PTY WebSockets; Plan 2 validates and preserves the client protocol |
| Deployment | Support workstation and hosted modes | Neither supplies a complete distribution, versioning, and upgrade design |
| End state | Remove obsolete relational infrastructure | Removal timing and retained relational distribution policy differ |

The overlap is large enough that maintaining two parallel design tracks would mostly duplicate work. The useful result is one corrected design, not a vote between two technology stacks.

## 2. Comparative strengths and weaknesses

| Dimension | Plan 1 | Plan 2 | Judgment |
|---|---|---|---|
| Actionability | Names objects, handlers, state keys, and files | Defines responsibilities and acceptance gates | Plan 1 is easier to start coding, but specificity is not evidence of correctness |
| Client boundary | Diagram exposes Restate ingress; later phases modify existing handlers and clients | Explicit gateway preserving existing contracts | Plan 2 is coherent; Plan 1 describes conflicting integration strategies |
| Lifecycle concurrency | Long exclusive object handlers | Short object mutations plus operation/run workflows | Plan 2 better accommodates stop/cancel, but must specify fencing |
| Side effects | Relies heavily on `Run`, `defer`, and serialization | Explicit idempotency, uncertain outcomes, and reconciliation | Plan 2 is materially safer |
| Querying | Project-local summary map | DataFusion plus bounded Restate indexes where needed | Plan 1 has a useful fast-path idea; neither completes query consistency design |
| Broker delivery | Concrete push/pull sketch | Multiple supported options deferred | Plan 1 exposes the work; Plan 2 avoids falsely claiming it is solved |
| Security | Has an authorization object, little boundary detail | Broad auth, secret, query, and stream constraints | Plan 2 is stronger, but still lacks an implementable trust model |
| Migration safety | Deletes engines and tests early | Isolated replacement, explicit cutover and rollback limits | Plan 2 wins on safety; costs more temporary duplication |
| Operational honesty | Predicts elimination of migrations and lease engines | Acknowledges schema/replay compatibility and recovery | Plan 1 overstates what disappears |
| Scope control | Narrow examples presented as full replacement | Full capability inventory required | Plan 2 avoids silent omissions but needs bounded milestones |
| Alternative implementations | Clean removal assumed | Wire-compatible relational alternative acknowledged | Plan 2 respects the product concern but leaves its release policy unresolved |

## 3. Blocking findings in Plan 1

### P1-01 — The public architecture contradicts its implementation sequence

**Evidence:** diagram lines 36–43; handler rewrite line 156; client stream changes line 183.

The diagram sends CLI/web traffic to Restate ingress and terminal traffic directly to brokers. The execution plan instead rewrites existing Hub handlers to call Restate, then changes clients to new stream endpoints. These are different products:

- A new public Restate API requires client changes and a replacement authentication/authorization boundary.
- Existing `/api/v1` handlers imply an API gateway or retained Hub front door.
- Direct broker PTY connections require broker reachability and a separate browser/CLI security story.

**Consequence:** implementation teams can all follow the plan and still build incompatible pieces. A private broker behind NAT makes the direct PTY arrow especially suspect.

**Required correction:** explicitly preserve the Scion gateway and existing client protocols. Put Restate ingress/admin behind trusted boundaries. Draw the real PTY route, including its NAT path and authentication.

### P1-02 — Long exclusive lifecycle handlers serialize away responsiveness

**Evidence:** `AgentObject.Create` lines 83–90, `Delete` lines 93–98.

The proposed `Create` handler waits for provisioning while holding exclusive ownership. A queued `Stop` or `Delete` cannot change that object's state while creation remains suspended. Saying “no interleaving” solves one class of races by potentially making cancellation and cleanup wait behind a stuck operation.

Shared `Get` does not solve this; it only makes reads possible. The plan sets `info` to running at the end without specifying which intermediate states a reader sees.

**Required correction:** use short exclusive transitions to claim a generation/operation, dispatch slow work outside the object lock, and accept completion only for the current operation. Define cancellation while the broker is offline. Never make a workflow wait for an object handler that is itself waiting for that workflow.

### P1-03 — `defer` is not a sufficient cancellation or compensation specification

**Evidence:** line 90 claims all compensations run on terminal failure or invocation cancellation and then clear state.

The plan does not distinguish a retryable failure, terminal business rejection, cancellation, forced termination, worker crash, or an error during compensation. It does not establish whether the relevant SDK permits compensation calls from the cancelled execution context or how a failed compensation is resumed.

A language-level cleanup construct is not proof of durable external cleanup. This is an unsupported guarantee, not a verified SDK limitation.

**Required correction:** define the supported cancellation semantics against the pinned SDK/server, persist operation outcomes and cleanup progress, and prove interruption during compensation. Preserve an inspectable cleanup-required state until effects are actually reconciled.

### P1-04 — Broker cleanup is registered too late for ambiguous provisioning

**Evidence:** provisioning followed by compensation registration at line 88; broker deletion inside `Run` at line 95.

Counterexample:

1. Broker creates the container.
2. Its response is lost.
3. The Hub has not recorded the returned resource identity or registered the next compensation.
4. A retry provisions another container, or cleanup cannot identify the first.

`Run` journaling alone does not close the gap between an external effect and recording its result.

**Required correction:** assign a durable operation ID before provisioning. Have the broker associate resources with that ID, return the same outcome on retries, and support lookup/cleanup by it. Fence obsolete operations so a late provision cannot resurrect a deleted agent.

### P1-05 — Awakeables do not constitute a broker delivery protocol

**Evidence:** `BrokerObject` lines 116–125 and broker integration lines 164–171.

A result rendezvous does not by itself provide command delivery, reconnect catch-up, acknowledgments, command deduplication, expired-result handling, or resource reconciliation. The proposed `pending_tasks` array means Plan 1 is still designing a queue—now in object state.

The named `Dispatch` is a shared handler, yet the prose requires durable pending-task publication. The plan does not identify the exclusive mutation path that owns that write. It also does not specify how a Restate service call targets the particular broker that owns the container rather than another service endpoint.

**Required correction:** define command enqueue, delivery claim, acknowledgment, result acceptance, reconnect, timeout, and removal transitions; specify who authenticates result resolution. Verify the exact SDK calls and URL path rather than treating the example as an API contract.

### P1-06 — Deleting all object state is not a safe lifecycle boundary by itself

**Evidence:** `ClearAll` at line 98.

Deleting current state does not explain what happens to delayed sends, old broker reports, retried client requests, pending cleanup, or reused IDs. The plan removes the evidence needed to reject stale activity without defining another owner for that evidence.

**Required correction:** retain a tombstone/generation or equivalent stale-operation fence for a defined lifetime. Coordinate deletion with quota, indexes, outstanding operations, and retention. Define whether IDs can ever be reused.

### P1-07 — Quota counters are not a reservation protocol

**Evidence:** `Reserve(1)` and `Release(1)` lines 86 and 96; transfers lines 66 and 155.

Counters lack reservation identity. The design cannot explain a repeated release, an uncertain reserve result, quota transfer between brokers, or cleanup after an interrupted transfer. Serializing each counter does not make a multi-object transfer atomic.

**Required correction:** keyed reservation tokens, explicit owner/run association, idempotent release, and a durable transfer protocol. Define whether transient over-reservation is acceptable and ensure transient under-accounting never permits excess allocation.

### P1-08 — Proposed object state contains several unbounded aggregates

**Evidence:** run history line 81, project summary map line 106, pending tasks line 120, messages and subscribers lines 130–131.

These collections have no retention, paging, partitioning, or write-amplification design. A project-wide summary map is a useful read optimization until every agent update contends on one key and rewrites a growing serialized value. A whole-conversation message array has the same problem.

**Required correction:** specify bounded state units, page/partition keys, retention, subscriber expiry, and index update frequency. Keep stable project metadata separate from high-frequency agent activity.

### P1-09 — Authorization revocation is only partially modeled

**Evidence:** lines 136–139 and 173–177.

An atomic policy mutation followed by asynchronous stop messages does not establish immediately enforced revocation. Existing sessions, streams, tokens, delegated credentials, and in-flight commands may remain authorized unless their enforcement points observe the new policy.

**Required correction:** distinguish “access denied from now on” from “runtime eventually stopped.” Define policy versions, cache invalidation, stream termination, and broker enforcement. Do not replace membership-loss processing until all its behavioral obligations are accounted for.

### P1-10 — Early deletion destroys the reference and may destroy safeguards

**Evidence:** engine and test deletion lines 157–162, before broker/auth/state migration is finished.

The plan tells implementers to delete named test files without distinguishing obsolete implementation tests from important behavioral regression tests. It rewrites the old Hub in place before establishing that the new process can run independently of SQL.

**Required correction:** port consumer-visible invariants first. Remove obsolete implementation tests only after their meaningful guarantees are covered. Delete engines when their actual callers have moved, not because the phase title says “replace.”

## 4. Major weaknesses in Plan 2

### P2-01 — It is a migration framework, not yet an executable design

**Evidence:** Phases 0–1, lines 39–72.

Plan 2 correctly identifies nearly every difficult issue and often stops there. It does not choose operation schemas, transition tables, broker command envelopes, query layouts, or cleanup state machines. The text is useful for preventing mistakes; it is not enough to implement consistent components independently.

**Required correction:** turn the first lifecycle slice into a concrete design: request idempotency key, operation/run ID, state owner, allowed transitions, broker result schema, compensation state, and finalization conditions. Those decisions should precede lifecycle code, not the whole platform.

### P2-02 — “Inventory everything first” can become a gate with no exit

**Evidence:** complete matrix required at line 53; first milestone requires Phases 0 and 1 at line 164.

The discovery scope includes virtually all Scion behavior. Requiring fully resolved schemas and semantics for every capability before the first working agent would postpone the most useful evidence: whether the replacement lifecycle actually works.

**Required correction:** require broad inventory for omission detection, but deep design only for the first slice and shared security/runtime prerequisites. Keep the full matrix as the release gate. Give every feasibility investigation a specific question, experiment, and resulting decision.

### P2-03 — Short handlers move concurrency complexity; they do not remove it

**Evidence:** lines 60–67.

Plan 2 fixes Plan 1's lock-duration problem, but once the workflow releases the object lock, stop/delete and launch completion can race again. The object can reject a stale completion while a broker still creates a stale physical container.

**Required correction:** propagate generation/operation identity across the object, workflow, broker command, and physical resource. Specify broker-side fencing and reconciliation, not just state-level rejection. This protocol replaces leased claims; it is not “zero concurrency code.”

### P2-04 — Gateway duplication is underpriced

**Evidence:** standalone gateway line 78; preservation/reuse line 80; no old Hub dependency line 17.

The existing front door may mix validation and authorization with store-backed services. “Reuse boundary types and pure validation” does not establish that auth, sessions, policy lookup, uploads, streaming, and errors can be extracted without broad dependency work.

**Required correction:** produce an import/dependency boundary for the first gateway slice. Prefer extracting reusable transport/policy code over copying handlers wholesale. Verify it starts and serves the real path with no SQL initialization. Account for temporary duplicate implementation maintenance.

### P2-05 — The query strategy remains an escape hatch

**Evidence:** lines 95–96.

“DataFusion where suitable, indexes where needed” is sensible but does not select either approach for any actual route. It leaves read-after-write, multi-key snapshot behavior, pagination, search, and authorization joins unresolved. A query engine over stored state is not automatically the application's required read model.

**Required correction:** assign each query family to keyed reads, project indexes, or DataFusion; specify freshness and pagination semantics. Do not route every `list` call through broad state scans by default. Do not build duplicate summaries without a repair protocol.

### P2-06 — Connectivity, authorization, and streams cannot all wait for late phases

**Evidence:** lifecycle slice lines 78–87; complete auth/state in Phase 3; connectivity/streams in Phase 4.

A local reachable broker is a reasonable first proof, but it can conceal the hardest hosted architecture problem. Likewise a lifecycle slice with bypassed authorization is not evidence of compatibility with normal clients.

**Required correction:** prove the hardest required NAT route and minimal real authentication/authorization early. Implement all lifecycle operations with real boundary checks from their first usable milestone. Leave bulk feature migration and full stream functionality for later, not the feasibility of their transport.

### P2-07 — The first acceptance gate omits half the product surface

**Evidence:** Phase 2 acceptance line 87 covers CLI create/list/stop/delete; web and terminal acceptance arrives at line 116.

The CLI slice is valuable, but it does not exercise browser sessions, CSRF/origin behavior, cookies, or stream attachment. These can invalidate the gateway topology even when command-line requests work.

**Required correction:** include an early browser login/list/status proof through the real gateway. Full terminal behavior can follow, but its route and trust model must already be demonstrated.

### P2-08 — Packaging promises are unresolved and potentially incompatible

**Evidence:** alternative implementation policy lines 15–17; release packaging line 128; module/build removal line 140.

Keeping the relational implementation in the same module while removing its dependencies from that module is not a settled plan. A runtime that never opens SQLite, a binary that contains no SQL implementation, and a source repository that contains no SQL dependencies are three different acceptance criteria.

**Required correction:** specify the shipping unit and ownership policy. Separate binary, module, branch, or repository are choices—not interchangeable descriptions. Do not silently convert a pure Restate deliverable into a dual-engine binary. Do not claim that endpoint compatibility alone promises ongoing support for another implementation.

### P2-09 — “No other data store” needs precise scope

**Evidence:** inventory allows artifact/workspace storage at line 45; acceptance says no application data store besides Restate at line 116.

This wording can be read as prohibiting filesystem/GCS artifact payloads that the inventory explicitly allows. Plan 1 is clearer that template blobs stay in existing storage (line 175).

**Required correction:** define Restate-only as the authoritative metadata, coordination, indexing, and durable application-event store, with explicit rules for bulk payloads and runtime files. If the intended requirement also puts bulk blobs in Restate, design and validate that separately rather than implying it is free.

### P2-10 — Compatibility cannot be both absolute and casually negotiable

**Evidence:** unchanged contracts lines 7 and 146, but “intentional contract differences” at line 141.

Documenting a break is not the same as satisfying a no-client-rewrite requirement. Conversely, preserving every incidental historical behavior can prevent useful architectural simplification.

**Required correction:** identify required compatibility behavior from consumers and tests. Changes to that boundary need explicit approval; documentation alone does not authorize them. Keep internal implementation behavior out of the compatibility contract.

## 5. Claims neither plan should get away with

### “Hundreds of thousands of lines deleted” is not the business case

Plan 1 claims approximately 630,000 lines of accidental complexity at line 13 and an 800,000-plus reduction in its final table. It attributes additional removal to tests, but supplies no reproducible measurement or net accounting for new code. These numbers are unverified here and are not directly comparable to authored orchestration complexity.

Generated ORM output is not equivalent to hand-maintained lifecycle logic. Deleting it may reduce build cost and repository volume, but does not prove equivalent reductions in maintenance risk. Replacing it with hand-written indexes, auth state, queue recovery, and migration logic could increase the amount of subtle code humans own.

Measure authored logic removed, replacement logic introduced, correctness obligations eliminated, build/runtime dependency reduction, and operational burden. Report generated code separately. Do not use deletion counts as an acceptance criterion.

### “No database migrations” is false as an application-design claim

Plan 1 predicts zero migrations/downgrade floors (lines 192 and 204). Removing SQL migrations does not remove state schema evolution, replay compatibility, index backfills, retention transformations, or safe downgrade requirements.

Plan 2 acknowledges these but does not yet implement a policy. Preserve or replace version-safety checks based on actual compatibility needs; do not delete them solely because their current implementation mentions databases.

### Restate replaces mechanisms, not all invariants

Leases, pollers, and outboxes can disappear or shrink. Resource ownership, stale-event rejection, authorization, idempotency, cleanup, and cross-entity consistency do not disappear. Both plans should judge each mechanism by its obligations before deleting it.

### Retrying is not a recovery policy

Neither plan concretely specifies retryable versus terminal broker errors, indefinite infrastructure outage behavior, cancellation deadlines, poison work, or operator repair permissions. Durable retries can preserve a mistake just as reliably as a valid operation.

### Shared Restate infrastructure expands the blast radius

The team's existing Restate operation experience is an advantage. It does not answer whether Scion shares failure domains and resource limits with other workloads. Query scans, event retention, broker reconnect storms, and hot quota/project keys can compete with lifecycle execution.

Both plans need resource limits, admission/backpressure policy, workload isolation, and observability for queued work, stuck operations, compensation backlog, index lag, and storage growth.

### Backups are not enough when the runtime lives elsewhere

Plan 2 explicitly requires backup/restore, which is good. Neither plan explains how restored control-plane state reconciles with containers and workspaces created after that backup. Restoring metadata can resurrect desired operations, forget live resources, or roll back deduplication records.

The recovery proof must include reconciliation with the live broker/runtime and rules for stale credentials and operation epochs, not only restoring Restate state successfully.

### “Pluggable” is a legitimate requirement—but needs a compatibility definition

Plan 2 correctly retracts the earlier dismissal of relational alternatives as inherently speculative. A whole-Hub wire boundary allows implementations to use different internals without reducing Restate to a CRUD adapter.

But `/api/v1` path compatibility alone is insufficient. Authentication, errors, streams, uploads, broker registration, local startup, and version negotiation are part of the product. A conformance suite and an explicit support policy make replacement meaningful. A generic Go interface neither guarantees that compatibility nor is necessary to achieve it.

## 6. What to keep, reject, and combine

### Keep from Plan 1

- Concrete entity and subsystem mapping as a starting inventory, not proof of completeness.
- Explicit broker identity and presence ownership.
- Project-local summaries as a candidate fast read path, redesigned into bounded indexes.
- Separation of raw interactive PTY transport from durable orchestration.
- Explicit treatment of artifact blobs outside the metadata store, subject to the intended storage scope.
- Workstation process supervision as a real delivery requirement, not an afterthought.

### Reject from Plan 1

- Direct public Restate ingress as an unexplained substitute for the existing API.
- Long exclusive handlers that wait indefinitely for runtime effects.
- Guaranteed compensation via `defer`, or guaranteed external exactly-once effects via journaling.
- Anonymous quota increments/decrements and unconditional state erasure on deletion.
- Unbounded arrays/maps as the default state layout.
- Automatic deletion of regression tests and version safety checks.
- Unverified code-size predictions as justification for the rewrite.

### Keep from Plan 2

- Strictly Restate-only deployed state/orchestration, without dual writes or fallback.
- Existing network boundary and actual client compatibility.
- Short mutations, per-operation workflows, idempotency, and uncertain-outcome handling.
- Failure-injection gates, explicit authority during cutover, and honest rollback limits.
- Security, retention, schema evolution, and complete feature coverage as release criteria.

### Strengthen in Plan 2

- Replace deferred decisions with executable protocols for the first slice.
- Separate broad release inventory from deep first-slice prerequisites.
- Prove the difficult network/auth topology early.
- Choose route-specific query strategies and index repair behavior.
- Define the shipping unit and what “Restate-only” excludes and permits.
- Add concrete performance/capacity criteria before load testing, rather than accepting whatever the prototype produces.

## 7. Recommended combined execution order

1. **Fix the boundary and shipping contract.** Preserve the Scion API; decide how the Restate-only artifact is built and what bulk storage it may use. Keep the relational alternative as an explicit distribution/ownership decision, not a shared persistence abstraction.
2. **Run narrow feasibility proofs in parallel with a broad capability inventory.** Verify real SDK APIs, broker targeting/NAT transport, query behavior, and browser authentication. Each proof must close a named decision.
3. **Write the lifecycle protocol.** Define object state, generation/operation IDs, broker idempotency/fencing, quota tokens, cancellation, tombstones, and cleanup transitions. This is the missing implementation design in both plans.
4. **Ship an isolated vertical slice to a disposable environment.** Real gateway auth, existing CLI create/list/stop/delete, browser status, real broker/runtime, Restate-only persistence, no old Hub initialization.
5. **Attack that slice.** Lose the provision response, cancel while the broker is offline, restart during compensation, replay stale results, and restore against surviving containers. Observe physical resources and quotas, not just HTTP success.
6. **Expand using the capability matrix.** Add remaining entities, bounded query indexes, schedules, messaging, and full stream behavior. Keep schema/replay compatibility and retention part of each feature.
7. **Prove deployment and cutover.** Workstation supervision, hosted isolation, backups, upgrades, live-runtime reconciliation, controlled migration if needed, and explicit old/new authority handoff.
8. **Delete only after replacement evidence.** Remove SQL dependencies, engines, obsolete tests, and startup paths from the shipping artifact. Preserve meaningful behavioral tests in implementation-independent form where possible.

## 8. Decisive acceptance scenarios

| Scenario | What it must demonstrate |
|---|---|
| Provision succeeds; response disappears | Retrying identifies the same resource; no duplicate container or quota reservation |
| Stop arrives while launch waits on an offline broker | Stop intent can be recorded promptly; a late launch cannot become an accepted current run |
| Delete completes; an old result arrives | No resurrection, quota corruption, or stale index insertion |
| Quota transfer is interrupted | Reservation ownership remains inspectable; no capacity is silently lost or double granted |
| Cleanup crashes after one external deletion | Recovery completes the remaining work without releasing another operation's reservation |
| Broker reconnects with old commands/results | Broker identity, operation generation, and deduplication are enforced end to end |
| Membership is revoked during an active stream | Access enforcement does not wait for eventual runtime shutdown |
| Agent changes while listings paginate | Results obey a documented consistency contract without cross-project leakage |
| A large project/conversation accumulates history | Bounded state layout and retention prevent unbounded hot-key/value growth |
| Restate is restored while newer containers survive | Reconciliation finds divergent runtime state and resolves it safely |
| Hosted broker is NAT-isolated | The actual CLI/web attach path and lifecycle dispatch both work, not merely an outbound HTTP connection |
| Restate-only artifact starts in a clean environment | No SQL config, driver initialization, migration, or old Hub dependency is required |

## Bottom line

**Plan 1 sells deletion before proving replacement. Plan 2 identifies the risks but postpones too many decisions.**

The right synthesis is Plan 2's migration discipline plus Plan 1's subsystem specificity, with neither plan's gaps waved away as “Restate handles it.” The payoff is real if Restate removes infrastructure mechanisms while Scion retains clearly specified domain invariants. The rewrite fails if those invariants are deleted along with the mechanisms—or rebuilt as an undocumented database/queue framework inside giant Virtual Objects.
