# 13: Access Control

Demonstrates Academy's two access-control mechanisms on the hosted exchange:

- **Agent-wide groups** via `RuntimeConfig(access_groups=...)` — grants members access to every action.
- **Per-action groups** via `@action(sharing=[...])` — restricts individual actions.

The example agent has two actions: `public_data` (agent-wide only) and
`restricted_compute` (accessible to a specific group). The agent also
configures `control_groups` for lifecycle management without action access.

**What each group can do:**

| Group | `public_data` | `restricted_compute` | Shutdown |
|---|---|---|---|
| `access_groups` member | :white_check_mark: | :white_check_mark: | :x: |
| `sharing` decorator member | :x: | :white_check_mark: | :x: |
| `control_groups` member | :x: | :x: | :white_check_mark: |
| Owner | :white_check_mark: | :white_check_mark: | :white_check_mark: |

!!! important
    Access control only applies to the hosted exchange
    ([`HttpExchangeFactory`][academy.exchange.cloud.client.HttpExchangeFactory] /
    [`GlobusExchangeFactory`][academy.exchange.cloud.globus.GlobusExchangeFactory]).
    Local and hybrid exchanges are trusted environments and ignore these settings.
