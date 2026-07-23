# Access Control

Academy provides two mechanisms for sharing agents with other users of the
hosted exchange. Both are driven by **Globus group membership**, which is
verified by the exchange server—membership is never self-reported by the sender.

!!! important

    Access control applies only to the hosted exchange
    (`HttpExchangeFactory` / `GlobusExchangeFactory`). Local and hybrid
    exchanges are trusted environments where none of the mechanisms
    described here are enforced.

## Grant Mechanisms

### Agent-Wide Groups (RuntimeConfig)

[`RuntimeConfig(access_groups=...)`][academy.runtime.RuntimeConfig] grants
members of the listed groups access to **every action** on the agent. This is
the common case: you want a team to interact with the agent and you do not need
per-action isolation.

Supply the group UUIDs at launch time:

```python
config = RuntimeConfig(
    access_groups={"11111111-1111-1111-1111-111111111111"},
)
```

No code changes are required inside the agent—the groups are purely a
deployment concern. The same agent can be launched with different groups in
different environments.

### Per-Action Groups (`@action` Decorator)

[`@action(sharing=[...])`][academy.agent.action] grants members of the listed
groups access to **only that specific action**. Use this when different actions
should be visible to different audiences.

```python
class MyAgent(Agent):
    @action(sharing=["11111111-1111-1111-1111-111111111111"])
    async def read_data(self) -> str: ...

    @action(sharing=["22222222-2222-2222-2222-222222222222"])
    async def write_data(self, value: str) -> None: ...
```

- An explicitly empty list (`sharing=[]`) makes the action **owner-only**.
  This cannot be widened by `access_groups`—it is a hard exclusion.
- A sender in *any one* of the listed groups may invoke the action (union
  semantics).

### Combining Both Mechanisms

When both `access_groups` and decorator `sharing` are used on the same agent:

- **`access_groups` members** can invoke every action (decorated or not).
- **Decorator `sharing` members** can invoke only the actions that list their
  group.

## Closed-Default (Fine-Grained Mode)

The moment **any** action on an agent is decorated with `sharing`, the agent
enters fine-grained mode: undecorated actions become **owner-only**.

| Setup | Result |
|---|---|
| Nothing declared | Fully open (legacy dev mode) |
| `access_groups` only | Listed groups reach all actions |
| `sharing` decorators only | Decorated actions restricted to their groups; undecorated = owner-only |
| Both mechanisms | `access_groups` reach everything; decorator groups reach only their actions |

!!! warning "Breaking change in v0.6"

    Previously, undecorated actions on a decorated agent inherited the union
    of all decorator groups. Now they are owner-only. See the
    [Migration Guide](../migration.md#academy-v06) for details.

## Control Groups

[`RuntimeConfig(control_groups={...})`][academy.runtime.RuntimeConfig] grants
members the ability to send lifecycle requests (e.g., shutdown) to the agent.
Control access **does not** confer action access—a control-group member who
also needs to invoke actions must be added to `access_groups` or a decorator
`sharing` list as well.

```python
config = RuntimeConfig(
    access_groups={"11111111-1111-1111-1111-111111111111"},
    control_groups={"33333333-3333-3333-3333-333333333333"},
)
```

Control groups are a RuntimeConfig-only feature; there is no decorator form.

!!! note

    Control access is a deployment concern, not a code concern. The owner
    always retains full control and can shut down the agent regardless of
    group membership.

## Registration Behavior

At launch, the agent's mailbox is automatically shared with the union of:

- Every group listed in any `@action(sharing=...)` decorator on the agent
- Every group in `access_groups`
- Every group in `control_groups`

This ensures the exchange stamps incoming messages with the sender's group
memberships so the runtime can perform authorization checks.

## Trust Model

- **Group membership** is verified by the exchange server via Globus groups.
  The sender cannot self-report membership.
- **The owner** always has full access to every action and lifecycle
  operation, regardless of any group configuration.
- **Local and hybrid exchanges** are trusted environments—if you host the
  entire system, none of the access-control mechanisms apply, and behavior is
  unchanged (fully open by default).
