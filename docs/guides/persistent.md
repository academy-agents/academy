# Building Persistent Agents

Academy allows you to build agents as "micro-services" for scientific workflows --- the same agent can outlive a single application and can be reused in different contexts. This guide walks through common techniques that you can use to deploy persistent agents.

!!! note

    Agents can outlive agents/managers but are tied to the life of the exchange. Only agents using the same exchange can communicate. That means `LocalExchangeFactory` is not appropriate to create persistent agents because the exchange lives in process memory.

    For the hosted exchange at `https://exchange.academy-agents.org`, authentication tokens are required to communicate with the exchange. These tokens are minuted by the `HttpExchangeFactory` when a user starts an agent, but are only valid for 48 hours. To run agents using the hosted exchange for longer than 48 hours you must use the `GlobusExchangeFactory` which uses refresh tokens as part of the authentication process. See the section on [setting up the `GlobusExchangeFactory`](#setting-up-the-globusexchangefactory)

## Running an Agent Without a Manager
The most common (and recommended) way to run agentic applications with Academy is to use the [`Manager`][academy.manager.Manager] class. However, the manager class explicitly ties the lifetime of your agents to the lifetime of your manager.

### Using the `Runtime` Class
The [`Runtime`][academy.runtime.Runtime] class can be used to run agents from a python script. You first need to create a mailbox for the agent using `exchange_client.register_agent` functionality.

```
from academy.exchange import HttpExchangeFactory
from academy.runtime import Runtime
from academy.runtime import RuntimeConfig

async def main():
    factory = HttpExchangeFactory()
    async with await factory.create_user_client() as client:
        registration = client.register_agent(MyAgentClass)
```

With the registration, you can initialize the agent and runtime:

```
async def main():
    ...

    agent = MyAgentClass(...) # Initialize the agent
    runtime = Runtime(
        agent=agent,
        exchange_factory=factory,
        registration=registration,
        config=RuntimeConfig(
            terminate_on_error=False,
            terminate_on_success=False,
        )
    )
```
In the code, the [`RuntimeConfig`][academy.runtime.RuntimeConfig] is used to tell the agent not to delete the mailbox if it is manually shutdown, or if it catches an error. This allows the registration to be reused across agent restarts.

Once the `Runtime` is initialized we can start the agent.
```
async def main():
    ...

    await runtime.run_until_complete()
```


### Running an Agent from a `toml` file

Academy also provides a commandline tool to run an agent from a `toml` file.
The `toml` file contains two required fields: `exchange` and `agent`. The `exchange` field specifies the type of exchange and arguments needed to construct the exchange factory. These are equivalent to the arguments that the `ExchangeFactory` type accepts. For instance, for the [`RedisExchangeFactory`][academy.exchange.redis.RedisExchangeFactory]:
```
[exchange]
exchange_type = 'redis'
hostname = '127.0.0.1'
port = '6789'

```

The agent field can either specify the fully qualifed path to an `Agent` constructor and the arguments the constructor accepts:

```
[agent]
constructor = 'testing.agents.SleepAgent'
loop_sleep = 1
```

or it can point to a pickle file of an Agent
```
[agent]
pickle = '~/agents/my_agent.pkl'
```

Then you run the agent using `academy.run`
```
python -m academy.run --config ~/agents/my_agent_config.toml
```

The `toml` file can also specify any arguments to [`RuntimeConfig`][academy.runtime.RuntimeConfig]. By default, the `Runtime` will be configured to not terminate the mailbox on either error or success like above. Academy will create a new `AgentRegistration` and save it to the config file so multiple runs with the same configuration file will use the same mailbox. To create a fresh mailbox, you can either delete the `agent_registration` section of the `toml` file, or to create a fresh mailbox on every run, you can configure `terminat_on_error` and `terminate_on_success` to be `True`.

### Running an Agent with `systemd`

To ensure an agent runs persistently, you can configure `systemd` to relaunch the agent when the process dies. Note, these are general instructions for setting up an agent as a service, you may need to speak to your site administrator about system specific policies.

First, define a `toml` file to configure the agent and exchange as described above. Then we can define a unit file at `~/.config/systemd/user/`.

For instance:
```
[Unit]
Description=MyAcademyAgent
After=network.target
StartLimitIntervalSec=0

[Service]
Type=simple
Restart=always
RestartSec=1
User=centos
ExecStart=~/.venv/bin/python -m academy.run --config <config.toml>
```

Once you have created a unit file, you can start the service:
```
systemctl --user start <service_name>
```

## Finding and Communicating with Running Agents

To communicate with running agents, you can create a `Handle` directly from the `AgentId`. This should be done inside the context of a `ExchangeClient`, a `Manager` or another `Agent`, since the handle needs to find a mailbox to send and receive requests to the agent:

```
async with await exchange_factory.create_user_client() as client:
    hdl = Handle(<AgentId>)
```

You can either copy the `AgentId` manually, or you can [`discover`][academy.exchange.client.ExchangeClient.discover] the `AgentId` by the class of your agent:

```
ids = await client.discover(MyAgentClass)
hdl = Handle(ids[0])
```

Discovery support discovery by the fully qualified class name as a string as well, or discovery by super types.

## Setting up the GlobusExchangeFactory

The hosted exchange service (`https://exchange.academy-agents.org/v1`) requires for users and agents to authenticate with the exchange. When using the `HttpExchangeFactory`, a single access token is minted by the `UserExchangeClient` and this access token is passed to all `ExchangeFactory` copies that are passed to agents launched from that workflow. Those access tokens expire after 48 hours, after which agents will receive a `UnauthorizedError` from the exchange.

To get around the issue of access tokens expiring, you can set up agents to have refresh tokens --- tokens that can be used to acquire new access tokens. To do so, you need an Authentication Client for each agent. The `GlobusExchangeFactory`/`GlobusExchangeTransport` creates a new Auth. client and refresh token when creating a new mailbox. To setup the `GlobusExchangeFactory`, you first have to create a globus auth project id. Go to [https://app.globus.org/settings/developers](https://app.globus.org/settings/developers), and click "+ add project". This will be the project space for your Auth. clients. Then copy the newly created project id to initialize the `GlobusExchangeFactory`:
```
factory = GlobusExchangeFactory(<project_id>)
```

The factory can be used just like any other exchange factory. Of note, since you are creating long-lived refresh and delegated tokens, you will have to consent 3 times during the launch -- we are working to improve the experience to avoid this authentication. (See below for how to batch launches to reduce the number of consents.)

!!! note
    The `GlobusExchangeFactory` currently does not support minting child agents from Agents. We are actively working to remove this limitation.

## Launching Multiple Agents at Once

When launching multiple agents from a manager, Academy provides [`launch_batch()`][academy.manager.Manager.launch_batch]. This method works with any exchange, but on the [Globus exchange][academy.exchange.cloud.globus.GlobusExchangeFactory] all agents within the batch are registered under a single consent prompt instead of one per agent.

```
async with manager.launch_batch() as batch:
    greeter = await batch.queue(Greeter)
    shouter = await batch.queue(Shouter)
    coordinator = await batch.queue(
        Coordinator,
        args=(greeter, shouter),
    )
```

Handles returned by `batch.queue()` are unbound until the batch is submitted. They can be passed as arguments to other agents *within* the same batch (as the coordinator does above), but reading `handle.agent_id` or pickling the handle will raise a `RuntimeError` until submission.

To drive a batch outside of a context manager you call `submit()`:

```
batch = manager.launch_batch()
greeter = await batch.queue(Greeter)
shouter = await batch.queue(Shouter)
coordinator = await batch.queue(
    Coordinator,
    args=(greeter, shouter),
)
await batch.submit()
```

Multiple batches can be used during the lifecycle of a manager.

See `examples/12-globus-exchange/` for working examples of both patterns.
