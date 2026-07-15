# Multi-User Agent Sharing — Plan & Discussion Notes
*Condensed from the July 2 Yadu/Alok talk. Voice recordings → structured plan,
grounded in the current state of the `azht-action-decorators` branch.*

---

## 1. The capability, in one paragraph

Academy agents are currently single-owner: the entity that launches an agent is
the only one that can invoke it. We are adding **multi-user agent sharing** —
one long-lived agent (e.g. a model-serving agent with an expensive model loaded
in memory) can be invoked by many users, gated by Globus Group membership and
per-action sharing lists. The motivation is avoiding **duplication**: today,
either every user spins up their own copy of the agent (expensive, slow, needs
the right hardware) or the agent is exposed with no access control at all
(unsafe). Sharing gives a third option: one shared, authorized agent.

## 2. What is already built on this branch

The core access-control machinery is implemented and under test (+1338 lines,
+573 in `runtime_test.py`, +281 in `backend_test.py`):

- **Sender side** — `@action(sharing=[group_uuids])` decorator
  (`academy/agent.py`); `_action_sharing` on the action protocol; sender group
  membership carried in `Message.header.groups`.
- **Exchange** — mailbox sharing primitives in both backends:
  `share_mailbox` / `get_mailbox_shares` / `remove_mailbox_shares`
  (`academy/exchange/cloud/backend.py`, `PythonBackend` + `RedisBackend`);
  permission checks via `_has_permissions` / `_has_mailbox_ownership` /
  `_has_shared_mailbox_access` / `_shared_groups`; `ClientInfo` carries
  `client_id` + `group_memberships`; HTTP routes for share/get/remove
  (`academy/exchange/cloud/app.py`); Globus auth populates `client_groups`
  header → `ClientInfo` via `authenticate` middleware.
- **Receiver side** — `Runtime._authorized_for_action` (owner always passes;
  explicit `sharing` list → group intersection; empty list → owner-only; no
  groups declared → allowed), `Runtime._authorized`, `Runtime._is_owner`;
  `ErrorCode.FORBIDDEN` responses on rejected requests; response-side
  authorization detachment fix (matched-request-header lookup in `put()`).
- **Globus** — `GlobusAgentRegistration.owner`, `AcademyGlobusClient`,
  permitted-groups registered at mailbox creation.

**Implication:** the "wholesale change across sender/exchange/receiver" Yadu
described is mostly *done*. The remaining work is verification, demonstration,
measurement, and write-up — not greenfield implementation.

## 3. Remaining work, chunked (review-first, one chunk at a time)

Per Yadu: get one working, reviewed piece into the codebase before adding
bells and whistles. Do not keep stacking.

### Chunk A — Land and harden the access-control core *(priority)*
The one chunk to get reviewed and merged. Everything else depends on it.
- Audit `_authorized_for_action` / `_authorized` edge cases: owner bypass,
  empty-sharing (owner-only), no-groups (open) semantics. Confirm tests cover
  all four quadrants.
- Verify the response-detachment fix holds under both `PythonBackend` and
  `RedisBackend` (matched-request-header path).
- Confirm `discover()` only returns mailboxes the caller has permission to see
  (both backends already filter — verify, don't assume).
- **Style note from Yadu:** keep docstrings terse. Current Claude-generated
  docstrings are too verbose — trim before review.

### Chunk B — End-to-end Globus sharing smoke test
- A runnable example (extend `examples/12-globus-exchange/`) that: registers an
  agent with `sharing=[group]`, shares its mailbox, and has a *second* user
  (different identity, member of the group) discover + invoke an action.
  Confirm a non-member gets `FORBIDDEN`.
- This is the integration evidence that the three sides agree.

### Chunk C — X-ray / SAM3 demonstration use case *(near-term, realistic)*
The "cover story" for the poster. Focus is the **sharing capability**, not the
imaging science.
- **Agent 1 — image source:** fetches/loads X-ray images (stand-in: load from
  an S3 bucket; real generation if we get accelerator access). Represents the
  facility instrument output (APS, SSRL, etc. — all produce X-ray images the
  local instrument computer can't process).
- **Agent 2 — SAM3 processing agent:** hosted on shared HPC (Aurora/Polaris if
  we get access; otherwise a stand-in compute node). Loads the SAM3 model once
  as a sidecar. Exposes multiple processing actions.
- **Two fake users**, both members of the sharing group, each driving a
  *different* pipeline through the same shared agent.
- The point: one SAM3 loaded, shared across users — vs. the duplication
  baseline where each user loads their own.

### Chunk D — Malicious-agent discovery use case *(forward-looking)*
- An agent that discovers all agents on the exchange and probes what it can
  invoke. Less realistic today (no such agent exists yet), but shows the
  future threat the access control addresses. Good to include alongside Chunk
  C as "here's what we see coming."
- Decision: include both in the write-up if format allows (poster = yes).

### Chunk E — Overhead measurement
- Measure action-invocation cost **before** (no ACLs) vs **after** (full
  group-based access control) on the current branch. Goal: show overhead is
  negligible.
- Measure **duplication vs sharing** resource cost: N users each loading SAM3
  vs. one shared SAM3 serving N users (model-load time, memory, node
  allocation). This is the headline cost argument.

## 4. Auth model (the question we will get immediately)

> "How does this work with accounts?"

- **Facility-built service:** "I am a service maintained by the facility. I
  accept connections from identities in a service-account list and link them to
  a service account." The builder carries part of the security responsibility.
- **Globus Groups** offloads group membership: "to authenticate you must use
  the facility's identity provider, and be in the right group." The facility
  IdP can enforce additional rules (e.g. recent MFA within 30 min).
- We do **not** need to justify why an unsecured MCP endpoint is bad — just
  show the duplication baseline and the unsecured-endpoint baseline, then show
  sharing. The comparison speaks for itself.
- Open thread: how far beyond "user ∈ group" do we go? E.g. "not only are you
  in the group, you authenticated with the right facility identity in the right
  way." Left as a design question, not a blocker for Chunk A.

## 5. Deliverable: poster (ideal outcome)

Narrative arc:
1. **Gap:** existing agentic frameworks don't consider multiple users — no
   notion of sharing an agent or gating actions by caller.
2. **Baselines people use today:** duplication (each user runs their own agent
   on hardware they may not have) or an unsecured MCP endpoint anyone can hit.
3. **Our design:** agent-as-shared-service, inspired by how other services do
   auth; per-action `sharing` lists + mailbox-level Globus Group sharing;
   owner bypass; exchange enforces at the mailbox, runtime enforces per action.
4. **Evidence:** overhead of access control is negligible (Chunk E before/after
   measurements); duplication cost is large (Chunk E sharing-vs-duplication).
5. **Two use cases:** X-ray/SAM3 sharing (realistic, near-term) + malicious
   discovery agent (forward-looking). Strengths and what's still left to desire
   for each.

## 6. Open questions

- Deliverable format confirmed as poster? (Recording was uncertain: "I don't
  exactly know what we're shooting for.")
- Do we get real Aurora/Polaris access for Chunk C, or stand-in on S3 + a
  generic compute node?
- How far to push the auth model beyond group membership (facility-IdP
  enforcement) before the poster?
- Should both use cases land in one write-up, or dedicate to one path first?
